from pendulum import datetime
from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_dag(dag_id, schedule, pg_conn_id, default_args):
    
    def write_to_postgres(*args):
        print("Hello Dynamic Postgre DAGS")
        print("This is DAG: {}".format(str(pg_conn_id)))

        # Create a PostgresHook
        hook = PostgresHook(postgres_conn_id=pg_conn_id)
        table_name = "table_worx"
        # Execute a query
        
        query = (f"""CREATE TABLE IF NOT EXISTS {table_name} (col1 TEXT,col2 TEXT)""")
        hook.run(query)

        query = (f"INSERT INTO {table_name} (col1, col2) VALUES ('value1', 'value2')")
        hook.run(query)

        query = (f"SELECT * FROM {table_name}")
        rows = hook.get_records(query)
        for row in rows:
                print(row)

    dag = DAG(dag_id, schedule=schedule, default_args=default_args)

    with dag:
        t1 = PythonOperator(task_id="write_to_postgres", python_callable=write_to_postgres)

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
