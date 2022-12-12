from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

import requests
import json
import os

default_args = {
    "owner": "tsystems",
    "depends_on_past": False,
    "start_date": datetime(2020, 8, 12),
    "email": "micael@tsystems.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(days=1),
    "concurrency": 2,
}

#test

#TODO using OS python and .env
urlSignIn = "http://host.docker.internal:4040/users/signin"
urlUsers = "http://host.docker.internal:4040/users"
userName = 'admin'
passWord = 'admin'


def PostSignInBaseUrl():
    response = requests.post(urlSignIn, data={'username': userName, 'password': passWord})
    token = response.text
    return token

def GetUsersInBaseUrl():
   
    response = requests.get(
        url=f"{urlUsers}/{userName}",
        headers={"Authorization": f"Bearer {PostSignInBaseUrl()}"
        })
    
    print(response.status_code)
    
    #TODO issolate status code treatment in a separated function
"""     if (response.status_code == 200):
        return 'status_code_200'
    else:
        return 'status_code_fail' """
    
    
with DAG('rest_api_call_beta', start_date =
        datetime(2021,12,1),
        schedule_interval = '10 * * * * *',
        catchup = False) as dag:

       postSignInBaseUrl = PythonOperator(
            task_id = 'requestBaseUrlToken',
            python_callable = PostSignInBaseUrl
        )
       
       getUsersInBaseUrl = BranchPythonOperator(
           task_id = 'getUsersInBaseUrl',
           python_callable= GetUsersInBaseUrl
       )

postSignInBaseUrl  >> getUsersInBaseUrl
