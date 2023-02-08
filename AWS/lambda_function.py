import time

import boto3
import json

from stacks_processor import get_stacks_from_text

client = boto3.client(" t", region_name="ap-southeast-1")


def get_job_id(bucket, object_key):
    response = client.start_document_text_detection(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": object_key}},
    )
    job_id = response["JobId"]
    return job_id


def is_job_complete(jobId):
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print(status)
    while status == "IN_PROGRESS":
        time.sleep(10)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print(status)
    return status


def getJobResults(jobId):
    pages = []
    nextToken = None
    while True:
        if nextToken:
            response = client.get_document_text_detection(
                JobId=jobId, NextToken=nextToken
            )
        else:
            response = client.get_document_text_detection(JobId=jobId)
        pages.append(response)
        nextToken = response.get("NextToken")
        if nextToken is None:
            break
    return pages


def lambda_handler(event, context):
    print(event)
    file_obj = event["Records"][0]
    filename = str(file_obj["s3"]["object"]["key"])

    s3BucketName = file_obj["s3"]["bucket"]["name"]
    text = []
    jobId = get_job_id(s3BucketName, filename)
    if is_job_complete(jobId):
        response = getJobResults(jobId)
        for resultPage in response:
            for item in resultPage["Blocks"]:
                if item["BlockType"] == "LINE":
                    data = item["Text"]
                    text.append(data)

    get_stacks_from_text(full_text=text, id=filename)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


if __name__ == '__main__':
    data = {"Records": [{"s3": {"object": {"key": "mamun.pdf"}}}]}
    lambda_handler(data, {})