import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote
from airflow.providers.postgres.hooks.postgres import PostgresHook
from requests.auth import HTTPDigestAuth

def member_content(**kwargs):
    AGGREGATE_URL = kwargs["AGGREGATE_URL"].rstrip("/")
    AGG_USERNAME = kwargs["AGG_USERNAME"]
    AGG_PASSWORD = kwargs["AGG_PASSWORD"]
    POSTGRES_CONN_ID = kwargs["POSTGRES_CONN_ID"]

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            # 1. Get IDs to process from the member tracking table
            cursor.execute("""
                SELECT id FROM memberids
                WHERE status IS NULL OR status = 'failed'
                ORDER BY id
            """)
            ids_to_process = [row[0] for row in cursor.fetchall()]
            
            if not ids_to_process:
                print("No pending member submissions.")
                return

            session = requests.Session()
            session.auth = HTTPDigestAuth(AGG_USERNAME, AGG_PASSWORD)

            namespaces = {
                'odk': 'http://opendatakit.org/submissions',
                'orx': 'http://openrosa.org/xforms',
                'default': 'http://opendatakit.org/submissions'
            }
            ET.register_namespace('', 'http://opendatakit.org/submissions')

            for submission_id in ids_to_process:
                try:
                    # 2. Download - Note: ODK formId is likely 'household_member' based on XML
                    form_path = f"household_member[@version=null and @uiVersion=null]/data[@key={submission_id}]"
                    url = f"{AGGREGATE_URL}/view/downloadSubmission?formId={quote(form_path, safe='')}"
                    
                    resp = session.get(url, timeout=90)
                    resp.raise_for_status()
                    root = ET.fromstring(resp.content)

                    # 3. Target the inner <data id="household_member">
                    data_el = root.find(".//default:data/default:data[@id='household_member']", namespaces)
                    if data_el is None:
                        raise ValueError(f"Could not find household_member data block for {submission_id}")

                    def get_txt(tag, parent=data_el):
                        el = parent.find(f"./default:{tag}", namespaces)
                        return el.text.strip() if el is not None and el.text else None

                    # 4. Extract Meta fields
                    meta_el = data_el.find("orx:meta", namespaces)
                    row_id = meta_el.find("orx:rowID", namespaces).text if meta_el is not None else None

                    # 5. Map XML to the 'member' table schema
                    record = {
                        "instanceid": data_el.get("instanceID"),
                        "rowid": row_id,
                        "household_id": get_txt("household_id"),
                        "age_in_years": int(get_txt("age_in_years") or 0) if get_txt("age_in_years") else None,
                        "age_in_months": int(get_txt("age_in_months") or 0) if get_txt("age_in_months") else None,
                        "age_in_days": int(get_txt("age_in_days") or 0) if get_txt("age_in_days") else None,
                        "gender": get_txt("gender"),
                        "sleep_under_net": get_txt("sleep_under_net"),
                        "which_net": get_txt("which_net"),
                        "is_consent_given": get_txt("is_consent_given"),
                        "is_present_4_test": get_txt("is_present_4_test"),
                        "is_haemo_measured": get_txt("is_haemo_measured"),
                        "rdt_result": get_txt("rdt_result"),
                        "blood_slide": get_txt("blood_slide"),
                        "dbs": get_txt("dbs"),
                        "is_woman_consent_given": get_txt("is_woman_consent_given"),
                        "is_pregnant_now": get_txt("is_pregnant_now"),
                        "woman_quest_start_time": get_txt("woman_quest_start_time"),
                        "woman_quest_end_time": get_txt("woman_quest_end_time")
                    }

                    # 6. Upsert into 'member' table
                    cols = [k for k, v in record.items() if v is not None]
                    vals = [record[k] for k in cols]
                    placeholders = ", ".join(["%s"] * len(vals))
                    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "instanceid"])

                    sql = f"""
                        INSERT INTO member ({', '.join(cols)}) 
                        VALUES ({placeholders}) 
                        ON CONFLICT (instanceid) DO UPDATE SET {updates}
                    """
                    cursor.execute(sql, vals)

                    # 7. Update tracking table (memberids) and commit
                    cursor.execute("UPDATE memberids SET status='success' WHERE id=%s", (submission_id,))
                    conn.commit()
                    print(f"Successfully processed member: {submission_id}")

                except Exception as e:
                    print(f"FAILED member {submission_id}: {str(e)}")
                    conn.rollback()
                    # Mark as failed so it can be retried
                    cursor.execute("UPDATE memberids SET status='failed' WHERE id=%s", (submission_id,))
                    conn.commit()

    print("Member processing task completed.")
