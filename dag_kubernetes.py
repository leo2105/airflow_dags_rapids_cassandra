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


start = DummyOperator(task_id='run_this_first', dag=dag)

passing = KubernetesPodOperator(namespace='default',
                          image="python:3.6.15",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="passing-test",
                          task_id="passing-task",
                          get_logs=True,
                          dag=dag
                          )

failing = KubernetesPodOperator(namespace='default',
                          image="ubuntu:16.04",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="fail",
                          task_id="failing-task",
                          get_logs=True,
                          dag=dag
                          )

passing.set_upstream(start)
failing.set_upstream(start)
