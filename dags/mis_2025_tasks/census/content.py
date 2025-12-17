import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote
from airflow.providers.postgres.hooks.postgres import PostgresHook

def census_content(**kwargs):
    AGGREGATE_URL = kwargs["AGGREGATE_URL"].rstrip("/")
    AGG_USERNAME = kwargs["AGG_USERNAME"]
    AGG_PASSWORD = kwargs["AGG_PASSWORD"]
    POSTGRES_CONN_ID = kwargs["POSTGRES_CONN_ID"]

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Using 'with' ensures the connection is closed and transactions are handled
    with pg.get_conn() as conn:
        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT id FROM censusids
                WHERE status IS NULL OR status = 'failed'
                ORDER BY id
            """)
            ids_to_process = [row[0] for row in cursor.fetchall()]
            print(f"Found {len(ids_to_process)} submissions to process")

            if not ids_to_process:
                print("Nothing to do.")
                return

            session = requests.Session()
            session.auth = (AGG_USERNAME, AGG_PASSWORD)

            namespaces = {
                'odk': 'http://opendatakit.org/submissions',
                'orx': 'http://openrosa.org/xforms',
                'default': 'http://opendatakit.org/submissions'
            }
            ET.register_namespace('', 'http://opendatakit.org/submissions')

            total_success = 0
            total_failed = 0

            for submission_id in ids_to_process:
                try:
                    # 1. Download
                    form_path = f"census[@version=null and @uiVersion=null]/data[@key={submission_id}]"
                    url = f"{AGGREGATE_URL}/view/downloadSubmission?formId={quote(form_path, safe='')}"

                    resp = session.get(url, timeout=90)
                    resp.raise_for_status()
                    root = ET.fromstring(resp.content)

                    # 2. Parse XML
                    census_data_el = root.find(".//default:data/default:data[@id='census']", namespaces)
                    if census_data_el is None:
                        census_data_el = root.find(".//{http://opendatakit.org/submissions}data/{http://opendatakit.org/submissions}data[@id='census']")

                    if census_data_el is None:
                        raise ValueError(f"XML structure invalid for ID {submission_id}")

                    def get_text(tag):
                        el = census_data_el.find(f"./default:{tag}", namespaces)
                        return (el.text.strip() if el is not None and el.text else None)

                    meta_el = census_data_el.find(".//orx:meta", namespaces)
                    row_id = meta_el.find("orx:rowID", namespaces).text if meta_el else None

                    location_text = get_text("location")
                    lat = lon = alt = acc = None
                    if location_text:
                        parts = location_text.split()
                        try:
                            lat, lon = float(parts[0]), float(parts[1])
                            alt = float(parts[2]) if len(parts) > 2 else None
                            acc = float(parts[3]) if len(parts) > 3 else None
                        except (ValueError, IndexError):
                            pass

                    record = {
                        "instanceID": census_data_el.get("instanceID"),
                        "rowID": row_id,
                        "createdDate": get_text("createdDate"),
                        "dateLastSelected": get_text("dateLastSelected"),
                        "deviceId": get_text("deviceId"),
                        "excluded": get_text("excluded"),
                        "placeName": get_text("placeName"),
                        "headName": get_text("headName"),
                        "houseNumber": get_text("houseNumber"),
                        "latitude": lat,
                        "longitude": lon,
                        "altitude": alt,
                        "accuracy": acc,
                        "random": float(get_text("random") or 0),
                        "selected": int(get_text("selected") or 0),
                        "valid": int(get_text("valid") or 0),
                        "sampleFrame": float(get_text("sampleFrame") or 0),
                    }

                    # 3. Save Record
                    cols = [k for k, v in record.items() if v is not None]
                    vals = [record[k] for k in cols]
                    placeholders = ", ".join(["%s"] * len(vals))
                    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "instanceID"])

                    upsert_sql = f"INSERT INTO census ({', '.join(cols)}) VALUES ({placeholders}) ON CONFLICT (instanceID) DO UPDATE SET {updates}"
                    cursor.execute(upsert_sql, vals)

                    # 4. Update Status & Commit
                    cursor.execute("UPDATE censusids SET status='success' WHERE id=%s", (submission_id,))
                    conn.commit() # Commit each record so progress is saved if the task crashes
                    total_success += 1

                except Exception as e:
                    print(f"FAILED {submission_id}: {e}")
                    conn.rollback()
                    # Mark as failed in a new transaction
                    cursor.execute("UPDATE censusids SET status='failed' WHERE id=%s", (submission_id,))
                    conn.commit()
                    total_failed += 1

    print(f"DONE → Success: {total_success}, Failed: {total_failed}")