import functools
import re
from datetime import datetime

import boto3

dynamodb = boto3.resource('dynamodb')

from tag import tags

comprehend_client = boto3.client("comprehend")
SOURCEADDRESS = "muazzem.mamun@gmail.com"
SITE_NAME = "Fractalslab"


def get_stack_info_of_cv(full_text):
    return get_stacks_from_text(full_text)


def _split_by_size(full_text):
    entities = []
    chunks = functools.reduce(
        lambda a, b, size=4000: a[-1].append(b) or a
        if a and sum(len(x) for x in a[-1]) + len(b) <= size
        else a.append([b]) or a,
        full_text,
        [],
    )
    if len(chunks) > 5:
        raise ValueError("Too big test size.")
    for chunk in chunks:
        response = comprehend_client.detect_entities(
            Text=" ".join(chunk), LanguageCode="en"
        )
        entities.extend(response["Entities"])
    return entities


def send_email(to_address, stacksList):
    client = boto3.client('ses')
    source = "{} <{}>".format(SITE_NAME, SOURCEADDRESS)
    subject = "Your best stacks"

    text_body = f"""Skills {stacksList}  """
    destination = {
        "ToAddresses": [to_address],
        "CcAddresses": [],
        "BccAddresses": [],
    }
    message = {
        "Subject": {"Data": subject},
        "Body": {
            "Text": {"Data": text_body, "Charset": "UTF-8"},
            # "Html": {"Data": html_body, "Charset": "UTF-8"},
        },
    }
    reply_addresses = ['muazzem2468@gmail.com']

    response = client.send_email(
        Source=source,
        Destination=destination,
        Message=message,
        ReplyToAddresses=reply_addresses,
    )

    return response.get("MessageId")


def saveStacks(id, stacksList):
    table = dynamodb.Table('user_stacks')

    response = table.put_item(
        Item={
            'id': id,

            'stack': stacksList
        }
    )

    status_code = response['ResponseMetadata']['HTTPStatusCode']
    print(status_code)


def get_stacks_from_text(full_text, id):
    entities = _split_by_size(full_text)
    valid_text = [entity["Text"].lower() for entity in entities]
    stacks = {text for text in valid_text if text in tags}
    stacks = list(stacks)
    stacks.sort()
    print(stacks)
    saveStacks(id=id.split("/")[-1].rsplit(".", 1, )[0], stacksList=stacks)
    send_email("rafiqul15-2546@diu.edu.bd", stacks)

    return stacks



