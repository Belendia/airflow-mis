# import requests
# import xml.etree.ElementTree as ET
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from requests.auth import HTTPDigestAuth

# def fetch_odk_submission_list(**kwargs):
#     """
#     Generic function to fetch submission IDs for any ODK form 
#     and UPSERT them into a specific tracking table.
#     """
#     # Parameters from op_kwargs
#     form_id = kwargs["form_id"]
#     target_table = kwargs["target_table"]
    
#     # Airflow Variables/Settings
#     aggregate_url = kwargs["AGGREGATE_URL"].rstrip("/")
#     username = kwargs["AGG_USERNAME"]
#     password = kwargs["AGG_PASSWORD"]
#     postgres_conn_id = kwargs["POSTGRES_CONN_ID"]
#     num_entries = int(kwargs.get("NUM_ENTRIES", 100))

#     session = requests.Session()
#     session.auth = HTTPDigestAuth(username, password)
#     pg = PostgresHook(postgres_conn_id=postgres_conn_id)

#     cursor_val = ""
#     total_checked = 0

#     with pg.get_conn() as conn:
#         with conn.cursor() as cursor:
#             while True:
#                 #url = f"{aggregate_url}/view/submissionList?formId={form_id}&numEntries={num_entries}&cursor={cursor_val}"
                
#                 #response = session.get(url, headers={"Accept": "application/xml"})
#                 url = f"{aggregate_url}/view/submissionList"

#                 params = {
#                     "formId": form_id,
#                     "numEntries": num_entries,
#                 }
                
#                 if cursor_val:
#                     params["cursor"] = cursor_val
                
#                 response = session.get(
#                     url,
#                     params=params,
#                     headers={"Accept": "application/xml"},
#                 )
#                 response.raise_for_status()
#                 root = ET.fromstring(response.text)

#                 ns = {"odk": "http://opendatakit.org/submissions"}
#                 ids = [el.text for el in root.findall(".//odk:idList/odk:id", ns)]

#                 if not ids:
#                     break

#                 # Dynamic table name and safe UPSERT logic
#                 upsert_sql = f"""
#                     INSERT INTO {target_table} (id, status)
#                     VALUES (%s, NULL)
#                     ON CONFLICT (id) DO NOTHING;
#                 """
                
#                 for id_val in ids:
#                     cursor.execute(upsert_sql, (id_val,))
                
#                 conn.commit()
#                 total_checked += len(ids)

#                 cursor_el = root.find(".//odk:resumptionCursor", ns)
#                 if cursor_el is None or cursor_el.text is None:
#                     break
#                 cursor_val = cursor_el.text

#     print(f"Sync complete for {form_id}. Checked {total_checked} IDs in {target_table}.")

import requests
import logging
import xml.etree.ElementTree as ET
from airflow.providers.postgres.hooks.postgres import PostgresHook
from requests.auth import HTTPDigestAuth

def fetch_odk_submission_list(**kwargs):
    """
    Generic function to fetch submission IDs for any ODK form 
    and UPSERT them into a specific tracking table.
    """
    form_id = kwargs["form_id"]
    target_table = kwargs["target_table"]
    
    aggregate_url = kwargs["AGGREGATE_URL"].rstrip("/")
    username = kwargs["AGG_USERNAME"]
    password = kwargs["AGG_PASSWORD"]
    postgres_conn_id = kwargs["POSTGRES_CONN_ID"]
    num_entries = int(kwargs.get("NUM_ENTRIES", 100))

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
                
                logging.info(f"Fetching page {page_count} (Cursor: {cursor_val[:20] if cursor_val else 'None'}...)")

                try:
                    # Added a 30-second timeout so the task doesn't hang if the server is slow
                    response = session.get(
                        url,
                        params=params,
                        headers={"Accept": "application/xml"},
                        timeout=30 
                    )
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    logging.error(f"Network error on page {page_count}: {e}")
                    raise

                root = ET.fromstring(response.text)
                ns = {"odk": "http://opendatakit.org/submissions"}
                ids = [el.text for el in root.findall(".//odk:idList/odk:id", ns)]

                if not ids:
                    logging.info("No more IDs found in this response. Breaking loop.")
                    break

                logging.info(f"Found {len(ids)} IDs on this page. Performing UPSERT.")

                upsert_sql = f"""
                    INSERT INTO {target_table} (id, status)
                    VALUES (%s, NULL)
                    ON CONFLICT (id) DO NOTHING;
                """
                
                for id_val in ids:
                    cursor.execute(upsert_sql, (id_val,))
                
                conn.commit()
                total_checked += len(ids)

                cursor_el = root.find(".//odk:resumptionCursor", ns)
                
                # SAFETY CHECK: If cursor is missing or the same as before, stop to avoid infinite loop
                if cursor_el is None or cursor_el.text is None:
                    logging.info("No resumption cursor found. Finished all pages.")
                    break
                
                if cursor_el.text == cursor_val:
                    logging.warning("Server returned the SAME cursor. Breaking to prevent infinite loop.")
                    break

                cursor_val = cursor_el.text

    logging.info(f"DONE! Sync complete for {form_id}. Total checked: {total_checked} IDs.")
