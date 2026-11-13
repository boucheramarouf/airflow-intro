"""DAG user_processing : récupère un utilisateur fictif depuis une API et le stocke dans Postgres."""
import csv

import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue

API_URL = "https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json"
CSV_PATH = "/tmp/user_info.csv"


@dag
def user_processing():

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        response = requests.get(API_URL, timeout=10)
        if response.status_code == 200:
            return PokeReturnValue(is_done=True, xcom_value=response.json())
        return PokeReturnValue(is_done=False, xcom_value=None)

    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }

    @task
    def process_user(user_info):
        with open(CSV_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)

    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="COPY users (id, firstname, lastname, email) FROM STDIN WITH CSV HEADER",
            filename=CSV_PATH,
        )

    fake_user = is_api_available()
    create_table >> fake_user
    process_user(extract_user(fake_user)) >> store_user()


user_processing()
