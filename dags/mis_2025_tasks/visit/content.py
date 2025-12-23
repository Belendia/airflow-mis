import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote
from airflow.providers.postgres.hooks.postgres import PostgresHook
from requests.auth import HTTPDigestAuth

def visit_content(**kwargs):
    AGGREGATE_URL = kwargs["AGGREGATE_URL"].rstrip("/")
    AGG_USERNAME = kwargs["AGG_USERNAME"]
    AGG_PASSWORD = kwargs["AGG_PASSWORD"]
    POSTGRES_CONN_ID = kwargs["POSTGRES_CONN_ID"]

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            # 1. Get IDs to process from the visit tracking table
            cursor.execute("""
                SELECT id FROM visitids
                WHERE status IS NULL OR status = 'failed'
                ORDER BY id
            """)
            ids_to_process = [row[0] for row in cursor.fetchall()]
            
            if not ids_to_process:
                print("No pending visit submissions.")
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
                    # 2. Download - formId targeting 'visit'
                    form_path = f"visit[@version=null and @uiVersion=null]/data[@key={submission_id}]"
                    url = f"{AGGREGATE_URL}/view/downloadSubmission?formId={quote(form_path, safe='')}"
                    
                    resp = session.get(url, timeout=90)
                    resp.raise_for_status()
                    root = ET.fromstring(resp.content)

                    # 3. Target the inner data block for 'visit'
                    data_el = root.find(".//default:data/default:data[@id='visit']", namespaces)
                    if data_el is None:
                        data_el = root.find(".//default:data/default:data", namespaces)
                    
                    if data_el is None:
                        raise ValueError(f"Could not find visit data block for {submission_id}")

                    def get_txt(tag, parent=data_el):
                        el = parent.find(f"./default:{tag}", namespaces)
                        return el.text.strip() if el is not None and el.text else None

                    # 4. Extract Meta fields
                    meta_el = data_el.find("orx:meta", namespaces)
                    row_id = meta_el.find("orx:rowID", namespaces).text if meta_el is not None else None

                    # 5. Map XML to the 'visit' table schema
                    record = {
                        "instanceid": data_el.get("instanceID"),
                        "rowid": row_id,
                        "household_id": get_txt("household_id"),
                        "visit_number": get_txt("visit_number"),
                        "visit_result": get_txt("visit_result")
                    }

                    # 6. Upsert into 'visit' table
                    cols = [k for k, v in record.items() if v is not None]
                    vals = [record[k] for k in cols]
                    placeholders = ", ".join(["%s"] * len(vals))
                    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "instanceid"])

                    sql = f"""
                        INSERT INTO visit ({', '.join(cols)}) 
                        VALUES ({placeholders}) 
                        ON CONFLICT (instanceid) DO UPDATE SET {updates}
                    """
                    cursor.execute(sql, vals)

                    # 7. Update tracking table (visitids) and commit
                    cursor.execute("UPDATE visitids SET status='success' WHERE id=%s", (submission_id,))
                    conn.commit()
                    print(f"Successfully processed visit record: {submission_id}")

                except Exception as e:
                    print(f"FAILED visit {submission_id}: {str(e)}")
                    conn.rollback()
                    cursor.execute("UPDATE visitids SET status='failed' WHERE id=%s", (submission_id,))
                    conn.commit()

    print("Visit processing task completed.")
