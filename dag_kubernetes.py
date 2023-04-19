import sys, os, subprocess, logging, shutil, requests, subprocess
from datetime import datetime, timedelta
import argparse
import csv

from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow import settings

cmd = 'airflow users list'
result = subprocess.run(cmd.split(' '), stdout=subprocess.PIPE)
owner = result.stdout.decode('utf-8').split('\n')[2].split('|')[1].strip()


default_args = {
            'owner' : owner,
            'depends_on_past': False,
            'retries' : 0,
        }

dag = DAG(
    'kubernetes_sample', catchup=False, default_args=default_args, schedule_interval=None, start_date=days_ago(1),)

bash_command = """cd / && mkdir workspace && cd workpace && \\
                git clone https://gitlab+deploy-token-1950569:125QUNzezM6ddxcjYiE2@gitlab.com/telconetcv/cassandra_rapids_dags.git && \\
                python /workspace/cassandra_rapids_dags/airflow/utils/utils.py"""

start = DummyOperator(task_id='run_this_first', dag=dag)

passing = KubernetesPodOperator(namespace='default',
                          startup_timeout_seconds=900,
                          image="rapidsai/rapidsai-core:23.02-cuda11.5-base-ubuntu20.04-py3.10",
                          cmds=["/bin/bash", "-c"],
                          arguments=[bash_command],
                          labels={"foo": "bar"},
                          name="passing-test",
                          task_id="passing-task",
                          get_logs=True,
                          dag=dag
                          )


passing.set_upstream(start)
