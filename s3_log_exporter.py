'''
Log Report Exporter for Cloudwatch log group to S3
Report's are saved to bucket
'''

import boto3
import json
import logging
import os
import time
import csv

from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# The Slack channel to send a message to stored in the slackChannel environment variable
SLACK_CHANNEL = os.environ['slackChannel']
HOOK_URL = os.environ['HOOK_URL']

# cloudwatch log insights
client = boto3.client('logs')
log_group = ''

# S3 bucket for report
s3 = boto3.client('s3')
bucket_name = os.environ['bucket_name']
bucket_path = os.environ['bucket_path'] if os.environ['bucket_path'] else ''

# time
now = datetime.now()
year = now.year
month = now.month
day = now.day
hour = now.hour
minute = now.minute


# create task to export cloudwatch log group    
def create_task():
    endTime = datetime.now()
    startTime = endTime - timedelta(minutes=10)
    task_id = client.create_export_task(
        logGroupName=log_group,
        destination=f"{bucket_name}",
        fromTime=int(startTime.timestamp())*1000,
        to=int(endTime.timestamp())*1000,
        destinationPrefix=f"{bucket_path}/{year}/{month}/{day}"
        )
    return task_id['taskId']
    
# get task result with task id
def get_task_result(task_id):
    retry = 0
    sleep_time, max_retry=2, 5
    status = 'RUNNING'
    while status == 'RUNNING' and retry < max_retry:
        time.sleep(sleep_time)
        response = client.describe_export_tasks(taskId=task_id)
        status = response['exportTasks'][0]['status']['code']
        logger.info(f"TASK STATUS : {status}")
        retry += 1
    if status == 'COMPLETED':
        logger.info(f"Task {task_id} is completed")
        return status


def lambda_handler(event, context):
    task_id = create_task()
    status = get_task_result(task_id)

    if status == 'COMPLETED':
        if hour == 0 and minute == 0:
            y = now - timedelta(days=1)
            obj_list = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=f"{bucket_path}/{y.year}/{y.month}/{y.day}"
            )
        
            logger.info(f"{y.year}/{y.month}/{y.day} Summary")
            logger.info(f"{len(obj_list['Contents'])} reports saved")
            slack_message = {
                'channel': SLACK_CHANNEL,
                'text': f"*[WAF-log-exporter]*\t{len(obj_list['Contents'])} task has completed"
            } 
            req = Request(HOOK_URL, json.dumps(slack_message).encode('utf-8'))
            try:
                response = urlopen(req)
                response.read()
                logger.info("Message posted to %s", slack_message['channel'])
            except HTTPError as e:
                logger.error("Request failed: %d %s", e.code, e.reason)
            except URLError as e:
                logger.error("Server connection failed: %s", e.reason)
        else:
            logger.info(f"Task {task_id} is completed")
    else:
        logger.error(f"Task {task_id} is failed")