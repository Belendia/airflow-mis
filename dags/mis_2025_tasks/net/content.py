import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote
from airflow.providers.postgres.hooks.postgres import PostgresHook

def net_content(**kwargs):
    AGGREGATE_URL = kwargs["AGGREGATE_URL"].rstrip("/")
    AGG_USERNAME = kwargs["AGG_USERNAME"]
    AGG_PASSWORD = kwargs["AGG_PASSWORD"]
    POSTGRES_CONN_ID = kwargs["POSTGRES_CONN_ID"]

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            # 1. Get IDs to process from the net tracking table
            cursor.execute("""
                SELECT id FROM netids
                WHERE status IS NULL OR status = 'failed'
                ORDER BY id
            """)
            ids_to_process = [row[0] for row in cursor.fetchall()]
            
            if not ids_to_process:
                print("No pending net submissions.")
                return

            session = requests.Session()
            session.auth = (AGG_USERNAME, AGG_PASSWORD)

            namespaces = {
                'odk': 'http://opendatakit.org/submissions',
                'orx': 'http://openrosa.org/xforms',
                'default': 'http://opendatakit.org/submissions'
            }
            ET.register_namespace('', 'http://opendatakit.org/submissions')

            for submission_id in ids_to_process:
                try:
                    # 2. Download - Adjusting formId to 'net'
                    form_path = f"net[@version=null and @uiVersion=null]/data[@key={submission_id}]"
                    url = f"{AGGREGATE_URL}/view/downloadSubmission?formId={quote(form_path, safe='')}"
                    
                    resp = session.get(url, timeout=90)
                    resp.raise_for_status()
                    root = ET.fromstring(resp.content)

                    # 3. Target the inner data block
                    data_el = root.find(".//default:data/default:data[@id='net']", namespaces)
                    if data_el is None:
                        # Fallback search if the ID differs slightly
                        data_el = root.find(".//default:data/default:data", namespaces)
                    
                    if data_el is None:
                        raise ValueError(f"Could not find net data block for {submission_id}")

                    def get_txt(tag, parent=data_el):
                        el = parent.find(f"./default:{tag}", namespaces)
                        return el.text.strip() if el is not None and el.text else None

                    # 4. Extract Meta fields
                    meta_el = data_el.find("orx:meta", namespaces)
                    row_id = meta_el.find("orx:rowID", namespaces).text if meta_el is not None else None

                    # 5. Map XML to the 'net' table schema
                    record = {
                        "instanceid": data_el.get("instanceID"),
                        "rowid": row_id,
                        "household_id": get_txt("household_id"),
                        "any_one_sleep_under_this_net": get_txt("any_one_sleep_under_this_net")
                    }

                    # 6. Upsert into 'net' table
                    cols = [k for k, v in record.items() if v is not None]
                    vals = [record[k] for k in cols]
                    placeholders = ", ".join(["%s"] * len(vals))
                    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "instanceid"])

                    sql = f"""
                        INSERT INTO net ({', '.join(cols)}) 
                        VALUES ({placeholders}) 
                        ON CONFLICT (instanceid) DO UPDATE SET {updates}
                    """
                    cursor.execute(sql, vals)

                    # 7. Update tracking table (netids) and commit
                    cursor.execute("UPDATE netids SET status='success' WHERE id=%s", (submission_id,))
                    conn.commit()
                    print(f"Successfully processed net: {submission_id}")

                except Exception as e:
                    print(f"FAILED net {submission_id}: {str(e)}")
                    conn.rollback()
                    cursor.execute("UPDATE netids SET status='failed' WHERE id=%s", (submission_id,))
                    conn.commit()

    print("Net processing task completed.")