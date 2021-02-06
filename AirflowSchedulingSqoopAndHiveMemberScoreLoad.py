# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 17:56:59 2021

@author: suresh
"""

######### importing dependencies ###############

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
#from airflow.sensors.http_sensor import HttpSensor
from datetime import datetime, timedelta
#from airflow.utils.dates import days_ago

###############  Default parameters to pass in DAG class ###################

default_args = {
    'owner': 'BigDataProject',
    'depends_on_past': False,
    'start_date': datetime(2021, 2, 6),
    'schedule_interval': '@weekly',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)    
}

######################## Dag Class ###############################
dag = DAG(
dag_id = 'shellScriptTesting', catchup=False, default_args=default_args)

############  function for sqoop import commands ################# 

def fetch_member_score():
    c1 = "hadoop fs -rm -R -f member_score"
    c2 = "sqoop import --connect jdbc:mysql://database-2.cl4c0rtglkdz.ap-south-1.rds.amazonaws.com/BankingPrj --username admin --password Bankingprj1 --table member_score --warehouse-dir /user/suri4433/"
    
    return f'{c1}&&{c2}'

############## function for hive commands #########################

def load_member_score_into_hive():
    managed_table = 'hive -e "create table if not exists suri44.member_score_dummy(member_id bigint, score double) row format delimited fields terminated by \',\'"'
    load_data_managed = 'hive -e "load data inpath \'member_score\' overwrite into table suri44.member_score_dummy"'    
    bucketing_property  = 'hive -e "set hive.enforce.bucketing = true"'
    external_bucketed_table = 'hive -e "create external table if not exists suri44.memberScore(member_id bigint, score double) clustered by (member_id) sorted by (member_id) into 4 buckets"'
    table_to_table_loading = 'hive -e "insert overwrite table suri44.memberScore select * from suri44.member_score_dummy"'

    return f'{managed_table}&&{load_data_managed}&&{bucketing_property}&&{external_bucketed_table}&&{table_to_table_loading}'

############## scheduling the sqoop task #########################

task1 = SSHOperator(
        task_id = 'sqoopload',
        ssh_conn_id = 'itversity',
        command = fetch_member_score(),
        dag = dag)

################# scheduling hive task####################

task2 = SSHOperator(
        task_id = 'hiveload',
        ssh_conn_id = 'itversity',
        command = load_member_score_into_hive(),
        dag = dag)


task1 >> task2



