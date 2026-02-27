import requests
import logging
import time
import xml.etree.ElementTree as ET
from airflow.providers.postgres.hooks.postgres import PostgresHook
from requests.auth import HTTPDigestAuth

def fetch_odk_submission_list(**kwargs):
    """
    Refactored to prevent ODK Aggregate server crashes.
    """
    form_id = kwargs["form_id"]
    target_table = kwargs["target_table"]
    
    aggregate_url = kwargs["AGGREGATE_URL"].rstrip("/")
    username = kwargs["AGG_USERNAME"]
    password = kwargs["AGG_PASSWORD"]
    postgres_conn_id = kwargs["POSTGRES_CONN_ID"]
    
    # Increased default to 500 to reduce total number of requests
    num_entries = int(kwargs.get("NUM_ENTRIES", 500))

    logging.info(f"Starting sync for form: {form_id} into table: {target_table}")

    session = requests.Session()
    session.auth = HTTPDigestAuth(username, password)
    pg = PostgresHook(postgres_conn_id=postgres_conn_id)

    cursor_val = ""
    total_checked = 0
    page_count = 0

    with pg.get_conn() as conn:
        with conn.cursor() as cursor:
            while True:
                page_count += 1
                url = f"{aggregate_url}/view/submissionList"
                params = {"formId": form_id, "numEntries": num_entries}
                if cursor_val:
                    params["cursor"] = cursor_val
                
                logging.info(f"Fetching page {page_count} (Total IDs so far: {total_checked})")

                try:
                    # Increased timeout to 60s because large data queries take time
                    response = session.get(
                        url,
                        params=params,
                        headers={"Accept": "application/xml"},
                        timeout=60 
                    )
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    logging.error(f"Network error on page {page_count}: {e}")
                    raise

                root = ET.fromstring(response.text)
                ns = {"odk": "http://opendatakit.org/submissions"}
                ids = [el.text for el in root.findall(".//odk:idList/odk:id", ns)]

                if not ids:
                    logging.info("No more IDs found. Breaking loop.")
                    break

                logging.info(f"Found {len(ids)} IDs. Performing UPSERT.")

                upsert_sql = f"""
                    INSERT INTO {target_table} (id, status)
                    VALUES (%s, NULL)
                    ON CONFLICT (id) DO NOTHING;
                """
                
                for id_val in ids:
                    cursor.execute(upsert_sql, (id_val,))
                
                conn.commit()
                total_checked += len(ids)

                # --- THE "BREATHER" FOR TOMCAT ---
                # This prevents the CPU from locking up on the server
                time.sleep(1.5) 

                cursor_el = root.find(".//odk:resumptionCursor", ns)
                
                if cursor_el is None or not cursor_el.text:
                    logging.info("No resumption cursor found. Sync finished.")
                    break
                
                if cursor_el.text == cursor_val:
                    logging.warning("Server returned the SAME cursor. Breaking loop.")
                    break

                cursor_val = cursor_el.text

    logging.info(f"DONE! Sync complete for {form_id}. Total checked: {total_checked} IDs.")
