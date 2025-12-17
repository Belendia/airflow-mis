from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

from mis_2025_tasks.utils.fetch_odk_submission_list import fetch_odk_submission_list

from mis_2025_tasks.census.content import census_content
from mis_2025_tasks.census.remove_duplicate import remove_duplicate_census
from mis_2025_tasks.household.content import household_content
from mis_2025_tasks.member.content import member_content
from mis_2025_tasks.net.content import net_content
from mis_2025_tasks.child.content import child_content
from mis_2025_tasks.visit.content import visit_content

# Common configuration
COMMON_CONFIG = {
    "AGGREGATE_URL": Variable.get("AGGREGATE_URL"),
    "AGG_USERNAME": Variable.get("AGG_USERNAME"),
    "AGG_PASSWORD": Variable.get("AGG_PASSWORD"),
    "NUM_ENTRIES": Variable.get("NUM_ENTRIES", default_var=100),
    "POSTGRES_CONN_ID": "PG-MIS-2025",
}

with DAG(
    dag_id="MIS-2025",
    start_date=datetime(2025, 1, 1),
    schedule="0 */6 * * *", # Every 6 hours
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["odk", "aggregate"],
) as dag:

    # --- CENSUS ---
    census_list = PythonOperator(
        task_id="census_list",
        python_callable=fetch_odk_submission_list,
        op_kwargs={**COMMON_CONFIG, "form_id": "census", "target_table": "censusids"},
    )

    census_data = PythonOperator(
        task_id="census_content",
        python_callable=census_content,
        op_kwargs=COMMON_CONFIG,
    )

    remove_duplicate = PythonOperator(
        task_id="remove_duplicate_census",
        python_callable=remove_duplicate_census,
        op_kwargs=COMMON_CONFIG,
    )

    # --- HOUSEHOLD ---
    household_list = PythonOperator(
        task_id="household_list",
        python_callable=fetch_odk_submission_list,
        op_kwargs={**COMMON_CONFIG, "form_id": "household", "target_table": "householdids"},
    )

    household_data = PythonOperator(
        task_id="household_content",
        python_callable=household_content,
        op_kwargs=COMMON_CONFIG,
    )

    # --- MEMBER ---
    member_list = PythonOperator(
        task_id="member_list",
        python_callable=fetch_odk_submission_list,
        op_kwargs={**COMMON_CONFIG, "form_id": "household_member", "target_table": "memberids"},
    )

    member_data = PythonOperator(
        task_id="member_content",
        python_callable=member_content,
        op_kwargs=COMMON_CONFIG,
    )

    # --- NET ---
    net_list = PythonOperator(
        task_id="net_list",
        python_callable=fetch_odk_submission_list,
        op_kwargs={**COMMON_CONFIG, "form_id": "net", "target_table": "netids"},
    )

    net_data = PythonOperator(
        task_id="net_content",
        python_callable=net_content,
        op_kwargs=COMMON_CONFIG,
    )

    # --- CHILDREN ---
    child_list = PythonOperator(
        task_id="child_list",
        python_callable=fetch_odk_submission_list,
        op_kwargs={**COMMON_CONFIG, "form_id": "child", "target_table": "childids"},
    )

    child_data = PythonOperator(
        task_id="child_content",
        python_callable=child_content,
        op_kwargs=COMMON_CONFIG,
    )

    # --- VISIT ---
    visit_list = PythonOperator(
        task_id="visit_list",
        python_callable=fetch_odk_submission_list,
        op_kwargs={**COMMON_CONFIG, "form_id": "visit", "target_table": "visitids"},
    )

    visit_data = PythonOperator(
        task_id="visit_content",
        python_callable=visit_content,
        op_kwargs=COMMON_CONFIG,
    )

    # Dependency Chain
    census_list >> census_data >> remove_duplicate >> household_list >> household_data >> member_list >> member_data >> \
    net_list >> net_data >> child_list >> child_data >> visit_list >> visit_data

