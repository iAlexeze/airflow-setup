from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime
import requests

API = "https://bored-api.appbrewery.com/random"

@dag(
    start_date=datetime(2024, 10, 15),
    schedule="* * * * *",
    tags=["asalex"],
    catchup=False,
)

def asalex():
    @task
    def get_asalex():
            r = requests.get(API, timeout=10)
            return r.json()
    @task
    def write_asalex_to_file(response):
           filepath = Variable.get("asalex_file")
           with open(filepath, "a") as f:
                 f.write(f"Today, you will: {response['activity']}\r\n")
           return filepath
    @task
    def read_asalex_to_file(filepath):
           with open(filepath, "r") as f:
            print(f.read())

    # get_asalex() >> write_asalex_to_file () >> read_asalex_to_file ()
    response = get_asalex()
    filepath = write_asalex_to_file(response)
    read_asalex_to_file(filepath)

asalex()

