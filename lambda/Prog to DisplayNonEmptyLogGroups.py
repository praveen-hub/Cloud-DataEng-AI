import json
import boto3
import datetime
import os
import logging
from botocore.client import ClientError

#Initiation of Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

cwlogs=boto3.client('logs')


def logGroupFns():
    
    error = {
        'error':''
    },
    
    results = []                 

    log_groups = cwlogs.describe_log_groups(logGroupNamePrefix=('/aws'))
    for log_group in log_groups['logGroups']:
        log_group_name = log_group['logGroupName']
        results.append(log_group_name)
    return results


def get_log_streams():
    """Return a list of log stream names in group `group_name`."""
    client = boto3.client('logs')
    Groups = []
    found_all = False
    next_token = None
    stream_names = []
    
    for group_name in logGroupFns():
        response = client.describe_log_streams(logGroupName=group_name,logStreamNamePrefix='2020')
        if 'logStreams' in response and len(response['logStreams']) > 0:
            Groups.append(group_name)
            print(group_name)
    return Groups

def lambda_handler(event, context):
    
    logger.info(event)
    response = logGroupFns()
    resul= get_log_streams()
    resp = json.dumps(resul)
    res = json.loads(resp)

    return res
    


    
        
            

    
    
    
    
    
    





    

    

   
