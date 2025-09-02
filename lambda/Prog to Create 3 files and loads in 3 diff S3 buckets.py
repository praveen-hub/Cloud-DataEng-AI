from __future__ import print_function
import boto3
import time, urllib
import json
import numpy as np
import pandas as pd
import random
import string
import io
import time as t
from datetime import datetime, timedelta
from dateutil import tz
import math

#below client variable helps to connect different objects in s3
s3_client=boto3.client('s3')

#Getting the current time stamp(ofcourse it is in UTC format)
startDate = datetime.now()
print(type(startDate))

#startofDate = startDate.replace(hour=0,minute=0,second=0,microsecond=0)
epochtime = math.floor(startDate.timestamp()*1000)
#print(epochtime)


#Function to concatenate the timestamp into string
def ymd(t,d) -> str:
    ymd=(str(t.year)+d+str(t.month)+d+str(t.day)+d+str(t.hour)+d+str(t.minute)+d+str(t.second))
    return ymd

#This is the main program of our lambda function
def lambda_handler(event, context):
    
    destination_bucket_name='aws-logs-197183385700-us-west-2'
    
    
    countries = ['United States', 'India', 'Mexico', 'Canada', 'Ireland', 'United Kingdom', 'Germany', 'Italy', 'Thailand', 'Russia', 'Japan']
    
    #folder = ''.join(random.choice(string.ascii_uppercase +string.ascii_lowercase + string.digits) for _ in range(4))
    
    print("start df1...")
   
    randomlist1 = []
    
    for i in range(0,100):
        n = np.random.randint(1,40)
        if n not in randomlist1:
             randomlist1.append(n)
    
    #print(randomlist1)

    df1 = pd.DataFrame(randomlist1,columns=['Age1'])
    #df1=df1.to_string(index = False)
    #print(df1)


    #df1 = pd.DataFrame(np.random.randint(1, 5, size=(20, 1)), columns=['Age1'])
    
    df1['Gender1'] = df1['Age1'].apply(lambda x: random.choice(["Male", "Female", "NA"]))
    
    df1['Country1'] = df1['Age1'].apply(lambda x: random.choice(countries))
    
    #df1['epoch'] = epochtime
    
    dflen = len(df1)
    
    df1['epochtimeSource'] = np.random.randint(1451480468252, 1651480474326, size=(dflen,1))
    
    print(df1)
    
    print("end df1...")
    
    print("start df2...")
    
    #df2 = pd.DataFrame(np.random.randint(1, 5, size=(20, 1)), columns=['Age2'])
    
    randomlist2 = []
    
    for i in range(0,100):
        n = np.random.randint(1,40)
        if n not in randomlist2:
            randomlist2.append(n)
    
    #print(randomlist2)

    df2 = pd.DataFrame(randomlist2,columns=['Age2'])
    
    #df2 = df2.to_string(index = False)
    
    df2['Gender2'] = df2['Age2'].apply(lambda x: random.choice(["Male", "Female", "NA"]))
    
    df2['Country2'] = df2['Age2'].apply(lambda x: random.choice(countries))
    
    print(df2)
    
    #df2['epoch'] = epochtime
    
    print("end df2...")
    
    print("start df3...")

    #df3 = pd.DataFrame(np.random.randint(1, 5, size=(20, 1)), columns=['Age3'])
    
    randomlist3 = []
    
    for i in range(0,100):
        n = np.random.randint(1,40)
        if n not in randomlist3:
            randomlist3.append(n)
    
    #print(randomlist3)

    df3 = pd.DataFrame(randomlist3,columns=['Age3'])
    
    #df3 = df3.to_string(index = False)
    
    df3['Gender3'] = df3['Age3'].apply(lambda x: random.choice(["Male", "Female", "NA"]))
    
    df3['Country3'] = df3['Age3'].apply(lambda x: random.choice(countries))
    
    print(df3)
    
    #df3['epoch'] = epochtime

    print("End df3...")
    
    filename = ymd(startDate,'-')
    
    #setting a filename for file in source1 folder in s3
    newfilename1 = "Merge"+"/"+"source1"+"/"+filename+'.csv'
    print("Generated filename for table1 " + newfilename1)
    
    #setting a filename for file in source2 folder in s3
    newfilename2 = "Merge"+"/"+"source2"+"/"+filename+'.csv'
    print("Generated filename for table2 " + newfilename2)
    
    #setting a filename for file in source3 folder in s3
    newfilename3 = "Merge"+"/"+"source3"+"/"+filename+'.csv'
    print("Generated filename for table3 " + newfilename3)
    
    #converting df1 object to csv 
    with io.StringIO() as csv_buffer1:
        df1.to_csv(csv_buffer1, index=False)
        

        response = s3_client.put_object(
            Bucket=destination_bucket_name, Key=newfilename1, Body=csv_buffer1.getvalue()
        )
        
    
    #converting df2 object to csv 
    with io.StringIO() as csv_buffer2:
        df2.to_csv(csv_buffer2, index=False)
        
        
        response1 = s3_client.put_object(
            Bucket=destination_bucket_name, Key=newfilename2, Body=csv_buffer2.getvalue()
        )
    
    
    #converting df3 object to csv 
    with io.StringIO() as csv_buffer3:
        df3.to_csv(csv_buffer3, index=False)
        
        
        response2 = s3_client.put_object(
            Bucket=destination_bucket_name, Key=newfilename3, Body=csv_buffer3.getvalue()
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
        
