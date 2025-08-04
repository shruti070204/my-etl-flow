import psycopg2
from prefect import flow, task
from prefect.blocks.system import Secret

@task
def connect_to_postgres():
    url = Secret.load("db-url").get()
    conn = psycopg2.connect(url)
    print("Connected to DB ✅")
    conn.close()

@flow
def my_flow():
    connect_to_postgres()

if __name__ == "__main__":
    my_flow()
