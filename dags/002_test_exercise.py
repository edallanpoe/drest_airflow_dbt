
from airflow.decorators import dag, task
from datetime import datetime, timedelta


@dag(
    start_date=datetime.today() - timedelta(days=15),
    schedule='@daily',
    tags=['test',],
    # When catchup is enabled Airflow will run all the times between
    # the start_date and current date according to the schedule, in
    # this case it'll run 14 times to catchup the DAG with the current
    # runs.
    # catchup=False, 
)
def dag002_test_exercise():
    
    @task(retries=3)
    def start():
        print("hello world! This is a drill.")
        # raise RuntimeError("Something very wrong happened.")

    start()


dag002_test_exercise()
