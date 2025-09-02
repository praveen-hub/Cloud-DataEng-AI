import json
import boto3
import uuid
from datetime import datetime, timedelta
logs = boto3.client('logs')

step=boto3.client('stepfunctions')

def lambda_handler(event, context):
    # TODO implement
    Id= str(uuid.uuid1())
    log_groups = logs.describe_log_groups(logGroupNamePrefix='log')
    for log_group in log_groups['logGroups']:
        log_group_name = log_group['logGroupName']
        input= {     
        "log groups are:", log_group_name
        }
        
        print(input)
    ''' response = step.start_execution(
        stateMachineArn='arn:aws:states:ap-south-1:535464033227:stateMachine:MyStateMachine',
        name = Id,
        input=json.dumps(input)
        )'''
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

 