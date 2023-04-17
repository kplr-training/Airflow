from pendulum import datetime
from airflow import DAG, settings, Dataset
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_dag(dag_id, schedule, pg_conn_id, default_args):
    suffix = pg_conn_id[-2:]
    table_name = "table_stock_"+suffix

    pg_dataset=[Dataset(f"postgres://airflow:airflow@postgres:5432/mydatabase{suffix}?table={table_name}")]

    def write_to_postgres(*args):
        print("Hello Dynamic Postgre DAGS")
        print("This is DAG: {}".format(str(pg_conn_id)))

        # Create a PostgresHook
        hook = PostgresHook(postgres_conn_id=pg_conn_id)

        # Execute a query
        query = (f"""CREATE TABLE IF NOT EXISTS {table_name} (price TEXT,date TEXT)""")
        hook.run(query)

        query = (f"INSERT INTO {table_name} (col1, col2) VALUES ('price_stock_{suffix}', 'date_stock_{suffix}')")
        hook.run(query)

        query = (f"SELECT * FROM {table_name}")
        rows = hook.get_records(query)
        for row in rows:
                print(row)

    dag = DAG(
        dag_id,
        schedule=schedule,
        default_args=default_args)

    with dag:
        t1 = PythonOperator(
            task_id="write_to_postgres",
            outlets=pg_dataset,
            python_callable=write_to_postgres)

    return dag

session = settings.Session()
conns = (
    session.query(Connection.conn_id)
    .filter(Connection.conn_id.ilike("%MY_DATABASE_CONN%"))
    .all()
)

for conn in conns:

    # BEWARE : the returned connection ID format is messed UP 
    # and needs to be cleansed and sanitized first 
    # otherwise all the rest of the code will break.
    conn = str(conn).strip("(),'")

    dag_id = "pg_dynamic_{}".format(conn)

    default_args = {"owner": "airflow", "start_date": datetime(2023, 1, 1)}

    schedule = "@daily"
    pg_conn_id = conn

    globals()[dag_id] = create_dag(dag_id, schedule, pg_conn_id, default_args)


# Prepare pg_datasets
all_pg_datasets=[]

for conn in conns:
    conn = str(conn).strip("(),'")
    suffix = conn[-2:]
    table_name = "table_stock_"+suffix
    all_pg_datasets.append(Dataset(f"postgres://airflow:airflow@postgres:5432/mydatabase{suffix}?table={table_name}"))

with DAG(
    'read_from_postgres_hook_aware_MULTI',
    start_date=datetime(2023, 1, 1),
    schedule = all_pg_datasets
) as dag:
   
    def read_from_postgres():

        for conn in conns:
            conn = str(conn).strip("(),'")
            suffix = conn[-2:]
            table_name = "table_stock_"+suffix

            # Execute hook
            hook = PostgresHook(postgres_conn_id=conn)

            # Execute query
            query = (f"SELECT * FROM {table_name}")
            rows = hook.get_records(query)
            for row in rows:
                    print(row)

    read_task = PythonOperator(
        task_id='read_task',
        python_callable=read_from_postgres,
        dag=dag
    )
