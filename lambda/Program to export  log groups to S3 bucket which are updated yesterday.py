import json
import boto3
import time as t
from datetime import datetime, timedelta
import math
logs=boto3.client('logs')
sns = boto3.client('sns')
def lambda_handler(event,context):
    result= main(event)
    resul = status(result)
    return result
def ymd(t,d) -> str:
    ymd=(str(t.year)+d+str(t.month)+d+str(t.day))
    return ymd
def status(result):
    print(result['taskId'])
    resp = logs.describe_export_tasks(taskId=result['taskId'])
    if resp['exportTasks'][0]['status']['code'] in 'COMPLETED':
        resp_end = sns.publish(
        TopicArn='arn:aws:sns:ap-southeast-2:652299490072:CW_Archival',
        Message='Exporting Log Groups to S3 Completed',
        MessageStructure='string',
        Subject='Exporting Log Groups to S3 Completed')
def main(event):
    Days = 1
    startDate = datetime.now()-timedelta(days=Days)
    #endDate = datetime.now()
    startOfDay = startDate.replace(hour=0, minute=0, second=0, microsecond=0)
    endOfDay = startDate.replace(hour=23, minute=59, second=59, microsecond=999999)
    sDate=math.floor(startOfDay.timestamp()*1000)
    eDate=math.floor(endOfDay.timestamp()*1000)
    DateNow=print("Job ran for date "+(startDate).strftime('%Y-%m-%d'))
    for grp in event['LogGroupName']:
        group=grp['LogGroupName']
        #print(group)
        #time=datetime.now()
        #dtime=time-timedelta(days=1)
        d_prefix= str("CWLogs/652299490072/")+str(ymd(startDate,'-'))+"/"+str(group.replace("/","-"))
        #print(d_prefix)
        response = logs.create_export_task(
        taskName='My_task',
        logGroupName= group,
        fromTime= sDate,
        to= eDate,
        destination='test1tw03',
        destinationPrefix=d_prefix)
        #t.sleep(10)
        if export_completed_successfully(response['taskId']):
            #print("Status for the log group "+" "+group+" "+"is"+" "+resp['exportTasks'][0]['status']['code'])
            print("Log Group is successfully exported " +" "+ group +" "+"with task id"+" "+ response['taskId'])
        else:
            resp = logs.describe_export_tasks(taskId=response['taskId'])
            print("log group exporting failed"+" "+ group)
            print("Status for the log group "+" "+group+" "+"is"+" "+resp['exportTasks'][0]['status']['code'])
            respon = sns.publish(
            TopicArn='arn:aws:sns:ap-southeast-2:652299490072:CW_Archival',
            Message="Exporting Failed for log group"+ " "+group +" "+"with task id"+" "+ response['taskId'],
            MessageStructure='string',
            Subject='Exporting of Cloud watch Logs Failed')
            break
    return response
def export_completed_successfully(task_id):
    failure_states = ['FAILED', 'CANCELLED', 'PENDING_CANCEL']
    status = ''
    while status != 'COMPLETED':
        response = logs.describe_export_tasks(taskId=task_id)
        if len(response['exportTasks']) > 0:
            status = response['exportTasks'][0]['status']['code']
            t.sleep(1)
        else:
            print("Error: export could not be found with task id '{}'".format(task_id))
            return False
        if status in failure_states:
            print("Error: export did not complete successfully. Status for task '{}' is {}".format(task_id, status))
            return False
    return True