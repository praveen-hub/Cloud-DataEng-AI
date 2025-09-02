import csv
import json
import boto3
import sys
import os
profile = sys.argv[1]
env_name = sys.argv[2]
table_name = 'ahm_comms_' + env_name + "_int_comm_ref_data"
session = boto3.session.Session(profile_name=profile)
client = session.resource('dynamodb')

table = client.Table(table_name)
print("Table Name: ", table_name, " .Table Status: ", table.table_status)

script_dir = os.path.dirname(__file__)
rel_path = "../csv/INT_COMM_REF_DATA.csv"
csv_file_path = os.path.join(script_dir, rel_path)
sm_file = open(csv_file_path)
csv_reader = csv.reader(sm_file, delimiter=',')

# skip header
next(csv_reader)
# delete existing entries from table
print("Deleting existing entries from DynamoDB table: ", table_name)
scan = table.scan()
with table.batch_writer() as batch:
    for each in scan['Items']:
        batch.delete_item(
            Key={
                'seq_no': each['seq_no']
            }
        )
print("Adding Comm Ref Data entries to DynamoDB table: ", table_name)
i = 0
for row in csv_reader:
    if not row:
        continue
    i += 1
    entry = {
        'seq_no': i,
        'bus_rule_id': int(row[0]),
        'ref_data_id': int(row[1])
    }
    print(json.dumps(entry, indent=4))
    table.put_item(Item=entry)