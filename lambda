import json
import csv
import boto3
import os
import re
from botocore.exceptions import ClientError

# Environment Variables

# SNS Topic ARN
TOPIC_CORE_TEAM_EXERCITIU = os.environ['TOPIC_CORE_TEAM_EXERCITIU']
TOPIC_CORE_TEAM_REAL = os.environ['TOPIC_CORE_TEAM_REAL']
TOPIC_EXTENDED_TEAM_REAL = os.environ['TOPIC_EXTENDED_TEAM_REAL']
TOPIC_EXTENDED_TEAM_EXERCITIU = os.environ['TOPIC_EXTENDED_TEAM_EXERCITIU']
TOPIC_HEADOFFICE_EXERCITIU_BIROU = os.environ['TOPIC_HEADOFFICE_EXERCITIU_BIROU']
TOPIC_HEADOFFICE_EXERCITIU_WFH = os.environ['TOPIC_HEADOFFICE_EXERCITIU_WFH']
TOPIC_HEADOFFICE_REAL_BIROU	= os.environ['TOPIC_HEADOFFICE_REAL_BIROU']
TOPIC_HEADOFFICE_REAL_WFH = os.environ['TOPIC_HEADOFFICE_REAL_WFH']

# MESSAGE TO SEND TO SUBSCRIBERS
MESSAGE_CORE_TEAM_EXERCITIU = os.environ['MESSAGE_CORE_TEAM_EXERCITIU']
MESSAGE_CORE_TEAM_REAL = os.environ['MESSAGE_CORE_TEAM_REAL']
MESSAGE_EXTENDED_TEAM_REAL = os.environ['MESSAGE_EXTENDED_TEAM_REAL']
MESSAGE_EXTENDED_TEAM_EXERCITIU = os.environ['MESSAGE_EXTENDED_TEAM_EXERCITIU']
MESSAGE_HEADOFFICE_EXERCITIU_BIROU = os.environ['MESSAGE_HEADOFFICE_EXERCITIU_BIROU']
MESSAGE_HEADOFFICE_EXERCITIU_WFH = os.environ['MESSAGE_HEADOFFICE_EXERCITIU_WFH']
MESSAGE_HEADOFFICE_REAL_BIROU = os.environ['MESSAGE_HEADOFFICE_REAL_BIROU']
MESSAGE_HEADOFFICE_REAL_WFH = os.environ['MESSAGE_HEADOFFICE_REAL_WFH']

# Function to validate if a phone number is valid. 
def is_valid_romanian_phone_number(number: str) -> bool:
    pattern = r"^\+40[0-9]{9}$"
    return bool(re.match(pattern, number))

def lambda_handler(event, context):
    
    # AWS Boto3 Clients
    sns = boto3.client('sns')
    s3 = boto3.client('s3')
    
    # Get the S3 Bucket and key of the uploaded CSV File
    bucket = event['Records'][0]['s3']['bucket']['name']
    print(f"S3 Bucket is {bucket}")
    
    key = event['Records'][0]['s3']['object']['key']
    print(f"Uploaded Object is {key}")
    
    if 'core-team-real' in key:
        SNS_TOPIC=TOPIC_CORE_TEAM_REAL
        MESSAGE=MESSAGE_CORE_TEAM_REAL
    elif 'core-team-exercitiu' in key:
        SNS_TOPIC=TOPIC_CORE_TEAM_EXERCITIU
        MESSAGE=MESSAGE_CORE_TEAM_EXERCITIU
    elif 'extended-team-real' in key:
        SNS_TOPIC=TOPIC_EXTENDED_TEAM_REAL
        MESSAGE=MESSAGE_EXTENDED_TEAM_REAL
    elif 'extended-team-exercitiu' in key:
        SNS_TOPIC=TOPIC_EXTENDED_TEAM_EXERCITIU
        MESSAGE=MESSAGE_EXTENDED_TEAM_EXERCITIU
    elif 'headoffice-real-wfh' in key:
        SNS_TOPIC=TOPIC_HEADOFFICE_REAL_WFH
        MESSAGE=MESSAGE_HEADOFFICE_REAL_WFH
    elif 'headoffice-exercitiu-wfh' in key:
        SNS_TOPIC=TOPIC_HEADOFFICE_EXERCITIU_WFH
        MESSAGE=MESSAGE_HEADOFFICE_EXERCITIU_WFH
    elif 'headoffice-real-birou' in key:
        SNS_TOPIC=TOPIC_HEADOFFICE_REAL_BIROU
        MESSAGE=MESSAGE_HEADOFFICE_REAL_BIROU
    elif 'headoffice-exercitiu-birou' in key:
        SNS_TOPIC=TOPIC_HEADOFFICE_EXERCITIU_BIROU
        MESSAGE=MESSAGE_HEADOFFICE_EXERCITIU_BIROU
    else:
        print('SNS Topic and Message assignment failed')
        
        
    # Open the CSV File using the S3 boto3 client. 
    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_file = obj['Body'].read().decode('utf-8-sig').splitlines()
    reader = csv.reader(csv_file)
    
    # Store headers for CSV File
    headers = next(reader)
    
    # Iterate through each row of the CSV.
    for row in reader:
        print(f"{row[0]} cu numarul {row[1]} va fi contactat.")
        
        # Remove accidental whitespace.
        phone_number=row[1].replace(" ","")
        
        # First check if the phone number is valid.
        if is_valid_romanian_phone_number(phone_number):
            print("Numar de telefon valid.")
            try:
                # Try and subscribe the phone number to the SNS Topic. 
                response_sub = sns.subscribe(TopicArn=SNS_TOPIC,Protocol='sms',Endpoint=phone_number)
            except:
                print("Eroare la SNS Subscription")
        else:
            print("Numarul nu e valid")
         
    # Try and publish a message to the topic.    
    try:
        # Try and publish a notification
        response_push = sns.publish(TopicArn=SNS_TOPIC,Message=MESSAGE)
        print("Utilizatorii cu subscriptie au fost contactati.")
    except ClientError as e:
        print("Eroare la SNS Publish")
        print(e)
        
    
    
    
    
