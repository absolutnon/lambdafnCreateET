from datetime import datetime
import json
import urllib.parse
import os
import boto3
from time import gmtime, strftime
import botocore

# Initialise values
PIPELINE_ID = os.environ['PIPELINE_ID']
region_name = 'eu-west-1'
transcoder = boto3.client('elastictranscoder', region_name=region_name)
s3 = boto3.resource('s3')

# Time slot array to use for the file name
time = ['0630','0656','0730','0756','0830','0856','0930','0956','1030','1230','1330','1430', '1530', '1556', '1630', '1656', '1730', '1756', '1830', '1856']

# Get today's date
today_date = strftime('%d%m')
today_folder = strftime('%d%m%y')

def lambda_handler(event, context):
    print(event)
    # Initialize the S3 and Elastic Transcoder clients
    s3 = boto3.client('s3')
    transcoder = boto3.client('elastictranscoder', region_name=region_name)

    # Get the input file information from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    input_key = event['Records'][0]['s3']['object']['key']
    
    # Recall rename_file method - to rename the uploaded S3 file to the right naming convention
    output_name = rename_file(input_key)
    
    # Check the output file name
    print(output_name)
    
    # Check what is the input file name
    # print(input_key)
    
    # Define the output file prefix (output will be saved in the same bucket)
    output_key_prefix = 'to-mha/'

    # Define the output file name and key
    #output_key = output_key_prefix + input_key.rsplit('.', 1)[0] + '.mxf'
    output_key = output_key_prefix + output_name + '.mxf'
    
    # Check what is the output file name and prefix folder
    print(output_key)

    # Define the Elastic Transcoder pipeline ID
    pipeline_id = '1710868867557-60hfjd'

    # Define the preset ID for 1080i50 XDCAM422
    preset_id = '1351620000001-100230'

    # Create a job in Elastic Transcoder
    #try:
    print("try")
    transcoder.create_job(
        PipelineId=pipeline_id,
        Input={
            'Key': input_key
        },
        Outputs=[{
            'Key': output_key,
            'PresetId': preset_id
        }]
    )
    

    return {
        'statusCode': 200,
        'body': 'Transcoding job initiated successfully.'
    }
    
def rename_file(file_input_name):
    # Define bucket name and tmp path
    bucket_name = 'gbn-gtn'
    folder_path = 'tmp/' + today_folder
    
    # Remove file extension
    file_name_no_extension = file_input_name.rsplit('.', 1)[0]
    # Get the date and time part from the file name
    extracted_date = file_name_no_extension.rsplit('-', 1)[1]
    # Extract year, month, day, hour, minute from the received file name
    year = extracted_date[:4]
    month = extracted_date[4:6]
    day = extracted_date[6:8]
    hour = extracted_date[8:10]
    min = extracted_date[10:12]
    
    file_date = day + month
    global output_name
    global file_run
    
    # Recall check_object() method to count how many files recieved today
    # name_index used for  time array which gives the time slot for naming converntion
    name_index = check_object()
    
    if today_date == file_date and file_run == 0:
        output_name = 'INRIX_TRAFFIC_' + time[name_index - 1] + '_' + day + month + year
        name_index = name_index + 1
    else:
        output_name = 'INRIX_TRAFFIC_' + time[name_index - 1] + '_' + day + month + year
        name_index = name_index + 1
    
    return output_name
    
def check_object():
    # connect to s3 - assuming your creds are all set up and you have boto3 installed
    s3 = boto3.resource('s3')
    
    # identify the bucket - you can use prefix if you know what your bucket name starts with
    for bucket in s3.buckets.all():
        print(bucket.name)
    
    # get the bucket
    bucket = s3.Bucket('gbn-gtn')
    
    # use loop and count increment
    count_obj = 0
    for i in bucket.objects.all():
        count_obj = count_obj + 1
    print(count_obj)
    
    return count_obj
