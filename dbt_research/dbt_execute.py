import subprocess
import os
import boto3
import json
from datetime import datetime

run_type = 'run'

subprocess.run(["dbt", "--log-format=json", run_type])

if True:
    run_results = open('./target/run_results.json', "r")
    
    run_results_data = json.loads(run_results.read())
    run_results.close()
    email_message = 'DBT ' + run_type + '\n'

    if 'results' in run_results_data:
        for result in run_results_data['results']:
            #email_message = email_message + json.dumps(result['status'])
            email_message = email_message + ' - ' + str(result['unique_id']) + ' => ' + str(result['status']) + '\n'

    message = {"default": "default"}
    client = boto3.client(
            'sns',
            region_name=os.environ.get('AWS_REGION'))
    arn = os.environ.get('AWS_SNS_TOPIC_ARN')
    response = client.publish(
        TargetArn=arn,
        Message=json.dumps({'default': json.dumps(message),
                            'email': email_message
                            }),
        Subject='DBT Udemy run on '+ datetime.today().isoformat(),
        MessageStructure='json'
    )