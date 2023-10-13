'''
CSV format Log Report Generator for Cloudwatch log group using log insights 
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
bucket_name = os.environ['bucket_name']
bucket_path = os.environ['bucket_path'] if os.environ['bucket_path'] else ''

def start_query():
    # cloudwatch log insights query
    query = 'stats count(*)'
    endTime = datetime.now()
    startTime = endTime - timedelta(days=1)
    
    try:
        response = client.start_query(
            logGroupName=log_group,
            startTime=int(startTime.timestamp()),
            endTime=int(endTime.timestamp()),
            queryString=query,
            limit=20
        )
        logger.info(f'StartQuery : {startTime} ~ {endTime}')
        return response
    except Exception as e:
        logger.error(f'StartQuery Exception : {e}')
        return None

def get_insights_query(queryId):
    logger.info(f"QueryId : {queryId}")
    try:
        response = client.get_query_results(queryId=queryId)
        return response
    except Exception as e:
        logger.error(f"GetQueryResults Exception : {e}")
        return None
        
def save_report(results):
    # filename format : [lambda name]-[yyyy]-[mm]-[dd]-[HH]-[mm]-[ss].csv
    n = datetime.now()
    filename = f"{os.environ['AWS_LAMBDA_FUNCTION_NAME']}-{n.year}-{n.month}-{n.day}-{n.hour}-{n.minute}-{n.second}.csv"
    logger.info(f'filename : {filename}')

    # preprocessing
    res_list = []
    for record in results['results']:
        r = {}
        for data in record:
            key=data['field']
            val=data['value']
            r[key]=val
        res_list.append(r)
    logger.info(f"Preprocessing : {res_list}")

    # formatting to csv
    with open(f"/tmp/{filename}", "w") as f:
        field_name = list(res_list[0].keys())
        writer = csv.DictWriter(f, fieldnames=field_name)
        writer.writeheader()
        for row in res_list:
            writer.writerow(row)

    # save to bucket
    s3 = boto3.client('s3')
    s3.upload_file(f"/tmp/{filename}", bucket_name, bucket_path+'/'+filename)
    return filename

def lambda_handler(event, context):
    # start query
    start_query_res = start_query()
    if start_query_res:
        queryId = start_query_res['queryId']
        
    # get query
    insights_response = None
    while insights_response == None or insights_response['status'] == 'Running':
        time.sleep(2)
        insights_response = get_insights_query(queryId)
    
    if insights_response.get('status') == "Complete":
        scanned = insights_response['statistics']['recordsScanned']
        matched = insights_response['statistics']['recordsMatched']
        logger.info(f"Querying Done, {scanned} Scanned / {matched} Matched")
        logger.info(insights_response)
        
        # S3 saving
        filename = save_report(insights_response)
    else:
        scanned, matched = None, None
        logger.error("Querying Failed")
        return None
    slack_message = {
        'channel': SLACK_CHANNEL,
        'text': f"*[log-insight-exporter]*\t{matched} matched, saved as {filename}"
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
