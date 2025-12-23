import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote
from airflow.providers.postgres.hooks.postgres import PostgresHook
from requests.auth import HTTPDigestAuth

def household_content(**kwargs):
    AGGREGATE_URL = kwargs["AGGREGATE_URL"].rstrip("/")
    AGG_USERNAME = kwargs["AGG_USERNAME"]
    AGG_PASSWORD = kwargs["AGG_PASSWORD"]
    POSTGRES_CONN_ID = kwargs["POSTGRES_CONN_ID"]

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            # 1. Get IDs to process
            cursor.execute("""
                SELECT id FROM householdids
                WHERE status IS NULL OR status = 'failed'
                ORDER BY id
            """)
            ids_to_process = [row[0] for row in cursor.fetchall()]
            
            if not ids_to_process:
                print("No pending submissions.")
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
                    # 2. Download
                    form_path = f"household[@version=null and @uiVersion=null]/data[@key={submission_id}]"
                    url = f"{AGGREGATE_URL}/view/downloadSubmission?formId={quote(form_path, safe='')}"
                    
                    resp = session.get(url, timeout=90)
                    resp.raise_for_status()
                    root = ET.fromstring(resp.content)

                    # 3. Target the inner <data id="household">
                    data_el = root.find(".//default:data/default:data[@id='household']", namespaces)
                    if data_el is None:
                        raise ValueError(f"Could not find household data block for {submission_id}")

                    def get_txt(tag, parent=data_el):
                        el = parent.find(f"./default:{tag}", namespaces)
                        return el.text.strip() if el is not None and el.text else None

                    # 4. Extract Meta fields (from orx:meta)
                    meta_el = data_el.find("orx:meta", namespaces)
                    row_id = meta_el.find("orx:rowID", namespaces).text if meta_el is not None else None
                    savepoint = meta_el.find("orx:savepointTimestamp", namespaces).text if meta_el is not None else None

                    # 5. Extract GPS (format: "lat lon alt acc")
                    gps_text = get_txt("gps_location")
                    lat = lon = alt = acc = None
                    if gps_text:
                        parts = gps_text.split()
                        try:
                            lat = float(parts[0]) if len(parts) > 0 else None
                            lon = float(parts[1]) if len(parts) > 1 else None
                            alt = float(parts[2]) if len(parts) > 2 else None
                            acc = float(parts[3]) if len(parts) > 3 else None
                        except (ValueError, IndexError):
                            pass

                    # 6. Map to your DB Schema
                    record = {
                        "instanceid": data_el.get("instanceID"),
                        "rowid": row_id,
                        "savepointtimestamp": savepoint,
                        "region": get_txt("region"),
                        "zone": get_txt("zone"),
                        "district": get_txt("district"),
                        "ea": get_txt("ea"),
                        "latitude": lat,
                        "longitude": lon,
                        "altitude": alt,
                        "accuracy": acc,
                        "data_collector": get_txt("data_collector"),
                        "data_collector_name": get_txt("data_collector_name"),
                        "have_nets": get_txt("have_nets"),
                        "how_many_nets": get_txt("how_many_nets"),
                        "is_consent_given": get_txt("is_consent_given"),
                        "hh_quest_start_time": get_txt("hh_quest_start_time"),
                        "hh_quest_end_time": get_txt("hh_quest_end_time")
                    }

                    # 7. Upsert into 'household' table
                    cols = [k for k, v in record.items() if v is not None]
                    vals = [record[k] for k in cols]
                    placeholders = ", ".join(["%s"] * len(vals))
                    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "instanceid"])

                    sql = f"""
                        INSERT INTO household ({', '.join(cols)}) 
                        VALUES ({placeholders}) 
                        ON CONFLICT (instanceid) DO UPDATE SET {updates}
                    """
                    cursor.execute(sql, vals)

                    # 8. Update tracking table and commit
                    cursor.execute("UPDATE householdids SET status='success' WHERE id=%s", (submission_id,))
                    conn.commit()
                    print(f"Successfully processed {submission_id}")

                except Exception as e:
                    print(f"FAILED {submission_id}: {str(e)}")
                    conn.rollback()
                    cursor.execute("UPDATE householdids SET status='failed' WHERE id=%s", (submission_id,))
                    conn.commit()
