import sys, os, subprocess, logging, shutil, requests, subprocess
from datetime import datetime, timedelta
import argparse
import csv

from airflow import DAG
from airflow import settings
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago
import docker
from docker.types import Mount 

cmd = 'airflow users list'
result = subprocess.run(cmd.split(' '), stdout=subprocess.PIPE)
owner = result.stdout.decode('utf-8').split('\n')[2].split('|')[1].strip()

default_args = {
            'owner' : owner,
            'depends_on_past': False,
            'retries' : 0,
        }

with DAG('cassandra_rapids_dag', catchup=False, default_args=default_args, schedule_interval=None, start_date=days_ago(1),) as dag:
    #from utils import *

    docker_command = """
        /bin/bash -c '
        git clone https://gitlab+deploy-token-1950569:125QUNzezM6ddxcjYiE2@gitlab.com/telconetcv/cassandra_rapids_dags.git &&
        python /workspace/cassandra_rapids_dags/airflow/utils/utils.py
        '
    """

    start_dag = DummyOperator(
        task_id='start_dag'
    )   

    test_cupy_node = DockerOperator(
        task_id="test_cupy_task",
        image="rapidsai/rapidsai-core:23.02-cuda11.5-base-ubuntu20.04-py3.10",
        device_requests=[docker.types.DeviceRequest(device_ids=["0"], capabilities=[['gpu']])],
        working_dir="/workspace",
        command=docker_command
    )
 
    end_dag = DummyOperator(
        task_id='end_dag' 
    )

    start_dag >> test_cupy_node >> end_dag
