import json
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.python import task
import boto3
from datetime import date

def dag_success_alert(context):
    
    print(f"DAG has succeeded, run_id: {context['run_id']}")

    json_details ={
        "name" : "sridhar"
    }
    client = boto3.client('events', region_name = "us-east-1")
    response = client.put_events(
            Entries = [
                {
                    'Source' : 'airflow',
                    'DetailType' : 'user-preferences',
                    'Detail' : json.dumps(json_details),
                    'EventBusName' : 'american_logistics_poc'
                }
            ]
           
    )

    print("This is event response = ", response ) 



@dag(
   
    dag_id = "Event_trigger",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2023, 2, 10),
    catchup=False,
    default_args={
        "retries": 2, 
    },
    tags=["migration"],
   
)  
def example_dag_basic():
    
    @task(on_success_callback=dag_success_alert)
    def extract():
       
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict
    
    
    extract()


example_dag_basic = example_dag_basic()
