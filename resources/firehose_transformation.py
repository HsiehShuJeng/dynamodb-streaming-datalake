import json
import base64
from datetime import datetime

def transform_records (record_item):
    event = record_item['eventName']
    if str(event) in ['INSERT', 'MODIFY']:
        item_json = record_item['dynamodb']['NewImage']
    elif str(event) in ['REMOVE']:
        item_json = record_item['dynamodb']['OldImage']

    for (column, value) in item_json.items():
        for (typ, val) in value.items():
            item_json[column] = str(val)

    #Adding Ingestion Timestamp and the event to do dedupe in the Lake,

    item_json['Event'] = str(event)
    item_json['ingestion_timestamp'] = str(datetime.now())
    item = item_json
    return item

def lambda_handler(event, context):
    print (event)
    output = []
    for record in event['records']:
        compressed_payload = base64.b64decode(record['data']).decode('utf-8')

        payload = compressed_payload
        print (payload)
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode((json.dumps(transform_records(json.loads(payload))) + "\n").encode('utf-8')).decode('utf-8')
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))
    return {'records': output}