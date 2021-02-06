# -*- coding: utf-8 -*-
"""
Created on Fri Feb  5 19:44:11 2021

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
    'schedule_interval': '0 */8 * * *',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)    
    }

######################## Dag Class ###############################

dag = DAG(
dag_id = 'MDetailsTesting', catchup=False, default_args=default_args)


############  function for sqoop import commands ################# 

def fetch_member_details():
    c1 = "sqoop job --exec memberdetails"
    return f'{c1}'


############## function for hive commands #########################

def load_member_details_into_hive():
    managed_table = 'hive -e "create table if not exists suri44.member_details_dummy(card_id bigint,member_id bigint, member_joining_dt timestamp,card_purchase_dt string,country string,city string) row format delimited fields terminated by \',\'"'
    load_data_managed = 'hive -e "load data inpath \'member_details\' overwrite into table suri44.member_details_dummy"'    
    bucketing_property  = 'hive -e "set hive.enforce.bucketing = true"'
    external_bucketed_table = 'hive -e "create external table if not exists suri44.memberdetails(card_id bigint,member_id bigint, member_joining_dt timestamp,card_purchase_dt string,country string,city string) clustered by (card_id) sorted by (card_id) into 4 buckets"'
    table_to_table_loading = 'hive -e "insert overwrite table suri44.memberdetails select * from suri44.member_details_dummy"'

    return f'{managed_table}&&{load_data_managed}&&{bucketing_property}&&{external_bucketed_table}&&{table_to_table_loading}'

############## scheduling the sqoop task #########################    

task1 = SSHOperator(
        task_id = 'sqoopload',
        ssh_conn_id = 'itversity',
        command = fetch_member_details(),
        dag = dag)

################# scheduling hive task####################

task2 = SSHOperator(
        task_id = 'hiveload',
        ssh_conn_id = 'itversity',
        command = load_member_details_into_hive(),
        dag = dag)


task1 >> task2

