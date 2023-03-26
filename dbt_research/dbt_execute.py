import subprocess
import os
import boto3
import json
from datetime import datetime

subprocess.call("dbt --log-format=json test")  

if True:
    #run_results = open('target/run_results.json', "r")
    run_results = open('/usr/app/dbt/dbt_ud_research/dbt_research/target/run_results.json', "r")
    
    run_results_data = json.loads(run_results.read())
    run_results.close()

    message = {"default": "default"}
    client = boto3.client('sns')
    arn = os.environ.get('AWS_SNS_TOPIC_ARN')
    response = client.publish(
        TargetArn=arn,
        Message=json.dumps({'default': json.dumps(message),
                            'email': json.dumps(run_results_data, indent=2)
                            }),
        Subject='DUR run on '+ datetime.today().isoformat(),
        MessageStructure='json'
    )