import json
from dateutil import parser

response = ec2client.descibe_images(Owners=['564843554684'], Filters=filters)

filters= [{
    'Name' : 'GoldenAMIName',
    'Values' : [sys.argv[2]]
}]

def new_image(list_of_images):
    latest=None
    for image in list_of_images:
        if not latest:
            latest=image
            continue,
        if parser.parse(image['CreationDate']) > parser.parse(latest['CreationDate']):
            latest = image
    
    return latest

def lambda_handler(event, context):
    
    source_image = new_image(response['Images'])
    print(source_image['ImageId'])