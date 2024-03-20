from datetime import datetime
import json
import urllib.parse
import os
import boto3

PIPELINE_ID = os.environ['PIPELINE_ID']
region_name = '[region of your Elastic Transcoder PIPELINE]'
transcoder = boto3.client('elastictranscoder', region_name=region_name)
s3 = boto3.resource('s3')

def lambda_handler(event, context):
    print(event)
    # Initialize the S3 and Elastic Transcoder clients
    s3 = boto3.client('s3')
    transcoder = boto3.client('elastictranscoder', region_name=region_name)

    # Get the input file information from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    input_key = event['Records'][0]['s3']['object']['key']
    
    print(input_key)
    
    # Define the output file prefix (output will be saved in the same bucket)
    output_key_prefix = 'output/'

    # Define the output file name and key, with folder in the bucket as prefix
    output_key = output_key_prefix + input_key.rsplit('.', 1)[0] + '.mxf'
    
    print(output_key)

    # Define the Elastic Transcoder pipeline ID
    pipeline_id = '[PIPELINE_ID]'

    # Define the preset ID for 1080i50 XDCAM422
    preset_id = '[PRESET_ID]'

    # Define the output settings
    output_settings = [
        {
        'key': output_key,
        'preset_id': preset_id
        }
        ]

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
