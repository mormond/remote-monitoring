import logging
import datetime
import json
import base64
import os
from io import BytesIO

import azure.functions as func
import azure.storage.blob as blob


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        now = datetime.datetime.now()
        desired_hour = req.params.get('hour')
        if desired_hour:
            desired_hour = int(desired_hour)
        else:
            desired_hour = now.hour

        blob_prefix = f'{os.environ["EVENT_HUB_NAME"]}/02/{now.year:02d}/{now.month:02d}/{now.day:02d}/{desired_hour:02d}'

        service = blob.BlobServiceClient(account_url=os.environ["STORAGE_ACCOUNT_URL"])
        container_client = service.get_container_client('readings')
        blobs_iterator = container_client.list_blobs(name_starts_with = blob_prefix)

        retreived_blobs = {}

        for i in blobs_iterator:
            index = int(i.name[-7:-5:])
            retreived_blobs[index] = i

        indexes = sorted(retreived_blobs, reverse=True)

        blob_client = container_client.get_blob_client(retreived_blobs[indexes[0]])
        stream_downloader = blob_client.download_blob()
        blob_contents = stream_downloader.content_as_text()
        blob_lines = blob_contents.split('\n')

        readings = []

        for i in blob_lines:
            reading = json.loads(i)
            body = base64.b64decode(reading['Body'])
            jbody = json.loads(body)
            if ('temperature' in jbody or 'humidity' in jbody):
                jbody['EnqueuedTimeUtc'] = reading['EnqueuedTimeUtc']
                readings.append(jbody)

        return func.HttpResponse(json.dumps(readings))

    except Exception as ex:
        return func.HttpResponse("Something went wrong.", status_code=500)
