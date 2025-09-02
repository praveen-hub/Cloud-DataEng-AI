import boto3
import datetime
import json
from botocore.errorfactory import ClientError
from datetime import datetime, timedelta
client=boto3.client('s3')
#group='CWLogs/652299490072/2020-9-13/-aws-lambda-AemSnapshotPurgev2/'
def lambda_handler(event, context):
    mains=main()
    mainz=handler(mains)
    Days = 1
    startDate = datetime.now()-timedelta(days=Days)
    time=((startDate).strftime('%Y-%m-%d'))
    for grp in event['LogGroupName']:
        group=grp['LogGroupName']
        group=str("CWLogs/652299490072/")+str(ymd(startDate,'-'))+"/"+str(group.replace("/","-")+"/")
        if group in mainz:
            print("Log Group is copied on today's run"+" "+group)
        else:
            print("Log group is not copied on today's run"+" "+group)
    return mainz
def handler(mains):
    result =[]
    for i in mains:
        #print(i['prefixs']['Prefix'])
        result.append(i['prefixs']['Prefix'])
        #print(result)
    return result
def main():
    results = {
    'prefixs':[]
    }
    try:
        response = client.list_objects_v2(
        Bucket='test1tw03',
        Delimiter='/',
        EncodingType='url',
        MaxKeys=123,
        Prefix='CWLogs/652299490072/2020-9-13/')
        for prefix in response['CommonPrefixes']:
            results['prefixs'] += [{ 'prefixs' : prefix}]
        return results['prefixs']
    except ClientError:
        # Not found
        print("File Not found")
        pass
def ymd(t,d) -> str:
    ymd=(str(t.year)+d+str(t.month)+d+str(t.day))
    return ymd