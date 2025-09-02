import json
import boto3
import math
from botocore.config import Config
from datetime import datetime,timedelta

config=Config(
    retries={
        'max_attempts' :10,
        'mode' : 'standard'
    })

sns=boto3.client('sns')
cwlogs=boto3.client('logs', config=config)
lamb = boto3.client('lambda')
paginator=cwlogs.get_paginator('describe_log_groups')
iterator=paginator.paginate()

def logGroupFn():
    
    Days=1
    startDate= datetime.now()-timedelta(days=Days)
    startofDate = startDate.replace(hour=0,minute=0,second=0,microsecond=0)
    endofDate = startDate.replace(hour=23,minute=59,second=59,microsecond=999999)
    edate = math.floor(endofDate.timestamp()*1000)
    sdate = math.floor(startofDate.timestamp()*1000)
    
    results = {
        'LogGroupName':[],
        'LastEventTime':[],
        'StreamName' : []
    }
    
    try:
        iterator = paginator.paginate(PaginationConfig = {'MaxItems':500, 'PageSize':50, 'StatingToken': None})
        for log_group in iterator:
            for i in log_group['logGroups']:
                log_group_name = i['logGroupName']
                logStreams = cwlogs.describe_log_streams(logGroupName=log_group_name,orderBy='LastEventTime',descending=True)
                if len(logStreams['logStreams']) > 0:
                    if 'lastEventTimeStamp' in logStreams['logStreams'][0].keys():
                        if logStreams['logStreams'][0]['lastEventTimeStamp'] > sdate and logStreams['logStreams'][0]['lastEventTimeStamp'] > edate:
                            results['LogGroupName'] += [{'LogGroupName' : log_group_name}]
                            results['LastEventTime'] += [{'LastEventTime' : logStreams['logStreams'][0]['lastEventTimeStamp']}]
                            results['StreamName'] += [{'StreamName' : logStreams['logStreams'][0]['logStreamName'] }]
                            
                    else:
                        print("stream '{}' in group '{}' does not have 'lasteventtimestamp' ". format(logStreams['logStreams'][0]['logStreamName'], log_group_name))
    except Exception as e:
        print(e)
        
        resp = sns.publish(
            TopicArn= 'arn:aws:sns:ap-south-eat:652299490072',
            Message= e,
            MessageStructure= 'string',
            Subject= "retrirving log groups")
            
        return results

def lambda_handler(event, context):
    
    response = logGroupFn()
    resp = json.dumps(response)
    res = json.loads(resp)
    
    respie = lamb.invoke(
        FunctionName= 'arn:aws:ap-south-east:LogGroupNameFn',
        InvocationType= 'Event',
        Payload= json.dumps(response))
        
    return res
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        

    