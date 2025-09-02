#to read one file in a bucket

import json
import boto3
import os

def lambda_handler(event, context):
    # TODO implement
    
    s3 = boto3.client('s3')
    bucket='bucket-name'
    result = s3.list_objects(Bucket = bucket)
    #print(result.get('Contents'))
    
    data = s3.get_object(Bucket=bucket, Key= 'file-name.csv')
    contents = data['Body'].read().decode('utf-8') 
    print(contents)
    
    return "hello"

#to read all files in a bucket

import json
import boto3
import os

def lambda_handler(event, context):
    # TODO implement
    
    s3 = boto3.client('s3')
    bucket ='bucket-name'
    result = s3.list_objects(Bucket = bucket)
    #print(result.get('Contents'))
    for i in result.get('Contents'):
       data = s3.get_object(Bucket=bucket, Key=i.get('Key'))
       contents = data['Body'].read().decode('utf-8') 
       print(contents)
       #print(os.getcwd())
    
    return "hello"

