##########################################################################

## Lambda job for starting and stopping EC2 instances, RDS CLusters and
## RDS Instances based on the Availability tag on the resources
## Supported tags for EC2 instances:
#### Availability: {"availability": "8x0"}
#### Availability: {"availability": "8x5"}
## Supported tags for EC2 clusters and instances:
#### Availability: "8x0"
#### Availability: "8x5"

##########################################################################

import sys
import json
import boto3
import datetime
from dateutil.tz import tzlocal,tz
from datetime import tzinfo
import time

##########################################################################

# Uncomment the following two lines to enable debug logging
#import logging
#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

##########################################################################

def get_time(zone_A,zone_B):
    # Get date/time and convert to timezone B.
    from_zone = tz.gettz(zone_A)
    to_zone = tz.gettz(zone_B)
    nowMax = datetime.datetime.now()
    nowMax = nowMax.replace(tzinfo=from_zone)
    nowMax = nowMax.astimezone(to_zone)
    now = nowMax.strftime("%H%M")

    nowDay = datetime.datetime.today()
    nowDay = nowDay.replace(tzinfo=from_zone)
    nowDay = nowDay.astimezone(to_zone)
    nowDay = nowDay.strftime("%a").lower()

    return (now,nowDay)

##########################################################################

def lambda_handler(event, context):

    print ("Running EC2/RDS Start/Stop Scheduler")

    ec2_client = boto3.client('ec2')
    asg = boto3.client('autoscaling',region_name='ap-southeast-2')

    rds_client = boto3.client('rds')

    ## Setting default values
    defaultStartTime = '0740'
    defaultStopTime = '1900'
    defaultDaysActive = "weekdays"
    defaultTimeZone = 'UTC'
    defaultZone = 'Australia/Sydney'

    try:
        # Declare Lists
        startList = []
        stopList = []
        # Create connection to the EC2 using Boto3 resources interface
        ec2 = boto3.resource('ec2')
        #asg = boto3.resource('asg',region_name='us-east-2')
        # List all instances
        instances = ec2.instances.all()

        # Declare Lists
        startRDSInstancesList = []
        stopRDSInstancesList = []
        startRDSClustersList = []
        stopRDSClustersList = []
        # Create connection to the RDS using Boto3 resources interface
        rdsClusters = rds_client.describe_db_clusters()
        rdsInstances = rds_client.describe_db_instances()

        print ("Creating","instance lists...")

        now,nowDay = get_time(defaultTimeZone,defaultZone)

##########################################################################
## EC2 instances start / stop
##########################################################################
        logs = ""
        for i in instances:
            logs = logs + i.id
            logs = logs + "\n"
            if i.tags != None:
                for t in i.tags:
                    if ((t['Value'] == "{\"availability\": \"8x5\"}") or (t['Value'] == "{\"availability\": \"8x0\"}")):
                        default1 = 'default'
                        default2 = 'true'
                        if (t['Value'] == "{\"availability\": \"8x5\"}"):
                            startTime = defaultStartTime
                            stopTime = defaultStopTime
                        elif (t['Value'] == "{\"availability\": \"8x0\"}"):
                            startTime = defaultStartTime
                            stopTime = '2350'
                        daysActive = defaultDaysActive
                        state = i.state['Name']
                        itype = i.instance_type

                        isActiveDay = False
                        daysActive = defaultDaysActive
                        # Days Interpreter
                        if daysActive == "all":
                            isActiveDay = True
                        elif daysActive == "weekdays":
                            weekdays = ['mon', 'tue', 'wed', 'thu', 'fri']
                            if (nowDay in weekdays):
                                isActiveDay = True
                            else:
                                daysActive = daysActive.split(",")
                            for d in daysActive:
                                if d.lower() == nowDay:
                                    isActiveDay = True
                        # Append to start list
                        if ((startTime <= str(now)) and (str(now) < stopTime) and (isActiveDay == True) and (state == "stopped")):
                            startList.append(i.instance_id)
                            print (i.instance_id, " added to START list")

                            # Append to stop list
                        if ((stopTime <= str(now)) and (str(now) > startTime ) and (isActiveDay == True) and (state == "running")):
                            stopList.append(i.instance_id)
                            print (i.instance_id, " added to STOP list")

        # Execute Start and Stop Commands
        if startList:
            print ("Starting", len(startList), "instances", startList)
            ec2.instances.filter(InstanceIds=startList).start()
            waiter = ec2_client.get_waiter('instance_status_ok')
            waiter.wait(InstanceIds=startList)
            waiter = ec2_client.get_waiter('system_status_ok')
            waiter.wait(InstanceIds=startList)
            for startInsts in startList:
                rm_instances = asg.describe_auto_scaling_instances(InstanceIds=startInsts.split())
                for inst in rm_instances['AutoScalingInstances']:
                    asg.resume_processes(AutoScalingGroupName=inst['AutoScalingGroupName'],ScalingProcesses=['Launch'])
                    asg.resume_processes(AutoScalingGroupName=inst['AutoScalingGroupName'],ScalingProcesses=['Terminate'])
                    asg.resume_processes(AutoScalingGroupName=inst['AutoScalingGroupName'],ScalingProcesses=['HealthCheck'])
        else:
            print ("No Instances to Start")

        if stopList:
            for stopInsts in stopList:
                rm_instances = asg.describe_auto_scaling_instances(InstanceIds=stopInsts.split())
                for inst in rm_instances['AutoScalingInstances']:
                    asg.suspend_processes(AutoScalingGroupName=inst['AutoScalingGroupName'],ScalingProcesses=['Launch'])
                    asg.suspend_processes(AutoScalingGroupName=inst['AutoScalingGroupName'],ScalingProcesses=['Terminate'])
                    asg.suspend_processes(AutoScalingGroupName=inst['AutoScalingGroupName'],ScalingProcesses=['HealthCheck'])
            print ("Stopping", len(stopList) ,"instances", stopList)
            ec2.instances.filter(InstanceIds=stopList).stop()
        else:
            print ("No Instances to Stop")

##########################################################################
## RDS Clusters Start / Stop
##########################################################################
        for i in rdsClusters['DBClusters']:
            logs = logs + i['DBClusterIdentifier']
            logs = logs + "\n"
            tags = rds_client.list_tags_for_resource(ResourceName=i['DBClusterArn'])
            if tags != None:
                for t in tags['TagList']:
                    if ((t['Key'] == "Availability") and ((t['Value'] == "8x5") or (t['Value'] == "8x0"))):
                        default1 = 'default'
                        default2 = 'true'
                        if (t['Value'] == "8x5"):
                            startTime = defaultStartTime
                            stopTime = defaultStopTime
                        elif (t['Value'] == "8x0"):
                            startTime = defaultStartTime
                            stopTime = '2350'
                        daysActive = defaultDaysActive
                        state = i['Status']

                        isActiveDay = False
                        daysActive = defaultDaysActive
                        # Days Interpreter
                        if daysActive == "all":
                            isActiveDay = True
                        elif daysActive == "weekdays":
                            weekdays = ['mon', 'tue', 'wed', 'thu', 'fri']
                            if (nowDay in weekdays):
                                isActiveDay = True
                            else:
                                daysActive = daysActive.split(",")
                            for d in daysActive:
                                if d.lower() == nowDay:
                                    isActiveDay = True
                        # Append to start list
                        if ((startTime <= str(now)) and (str(now) < stopTime) and (isActiveDay == True) and (state == "stopped") ):
                            startRDSClustersList.append(i['DBClusterIdentifier'])
                            print (i['DBClusterIdentifier'], " added to START list")

                        # Append to stop list
                        if ((stopTime <= str(now)) and (str(now) > startTime ) and (isActiveDay == True) and (state == "available")):
                            stopRDSClustersList.append(i['DBClusterIdentifier'])
                            print (i['DBClusterIdentifier'], " added to STOP list")

        # Execute Start and Stop Commands
        if startRDSClustersList:
            print ("Starting", len(startRDSClustersList), "Clusters", startRDSClustersList)
            for rdsCluster in startRDSClustersList:
                print ("Starting RDS Cluster", rdsCluster)
                start_db_cluster(DBClusterIdentifier=rdsCluster)
        else:
            print ("No Clusters to Start")

        if stopRDSClustersList:
            print ("Stopping", len(stopRDSClustersList) ,"Clusters", stopRDSClustersList)
            for rdsCluster in stopRDSClustersList:
                print ("Stopping RDS Cluster", rdsCluster)
                stop_db_cluster(DBClusterIdentifier=rdsCluster)
        else:
            print ("No Clusters to Stop")

##########################################################################
## RDS Instances Start / Stop
##########################################################################
        for i in rdsInstances['DBInstances']:
            logs = logs + i['DBInstanceIdentifier']
            logs = logs + "\n"
            tags = rds_client.list_tags_for_resource(ResourceName=i['DBInstanceArn'])
            if tags != None:
                for t in tags['TagList']:
                    if ((t['Key'] == "Availability") and ((t['Value'] == "8x5") or (t['Value'] == "8x0"))):
                        default1 = 'default'
                        default2 = 'true'
                        if (t['Value'] == "8x5"):
                            startTime = '0700'
                            stopTime = defaultStopTime
                        elif (t['Value'] == "8x0"):
                            startTime = '0700'
                            stopTime = '2350'
                        daysActive = defaultDaysActive
                        state = i['DBInstanceStatus']

                        isActiveDay = False
                        daysActive = defaultDaysActive
                        # Days Interpreter
                        if daysActive == "all":
                            isActiveDay = True
                        elif daysActive == "weekdays":
                            weekdays = ['mon', 'tue', 'wed', 'thu', 'fri']
                            if (nowDay in weekdays):
                                isActiveDay = True
                            else:
                                daysActive = daysActive.split(",")
                            for d in daysActive:
                                if d.lower() == nowDay:
                                    isActiveDay = True
                        # Append to start list
                        if ((startTime <= str(now)) and (str(now) < stopTime) and (isActiveDay == True) and (state == "stopped")):
                            startRDSInstancesList.append(i['DBInstanceIdentifier'])
                            print (i['DBInstanceIdentifier'], " added to START list")

                        # Append to stop list
                        if ((stopTime <= str(now)) and (str(now) > startTime ) and (isActiveDay == True) and (state == "available")):
                            stopRDSInstancesList.append(i['DBInstanceIdentifier'])
                            print (i['DBInstanceIdentifier'], " added to STOP list")

        # Execute Start and Stop Commands
        if startRDSInstancesList:
            print ("Starting", len(startRDSInstancesList), "instances", startRDSInstancesList)
            for rdsInstance in startRDSInstancesList:
                print ("Starting RDS Instance", rdsInstance)
                start_db_instance(DBInstanceIdentifier=rdsInstance)
                waiter = rds_client.get_waiter('db_instance_available')
                waiter.wait(DBInstanceIdentifier=rdsInstance)
        else:
            print ("No Instances to Start")

        if stopRDSInstancesList:
            print ("Stopping", len(stopRDSInstancesList) ,"instances", stopRDSInstancesList)
            for rdsInstance in stopRDSInstancesList:
                print ("Stopping RDS Instance", rdsInstance)
                stop_db_instance(DBInstanceIdentifier=rdsInstance)
        else:
            print ("No Instances to Stop")
##########################################################################
    except Exception as e:
        client = boto3.client('sns')
        message= ""
        message= message+ "Dear Cloudservice Team,"+ "\n\n"
        message= message+ "The lambda function which is used to start and stop EC2 instances is not executed." + "\n\n    "
        message= message+ "\n\n"
        message= message+ "Thanks and Regards,"
        message= message+ "\n"
        message= message+ "Cloud Services"
        #response = client.publish(
        #TargetArn="arn:aws:sns:us-east-1:809809395675:test-sg",
        #Subject='EC2 start/stop',
        #Message=message)
        #print (response)
        print(e)
##########################################################################