import boto3
import datapackage
import json
import os
import time

s3 = boto3.resource('s3')
bucket = s3.Bucket('datastore.openspending.org')

for obj in bucket.objects.all():
    if (obj.key.endswith('datapackage.json')):
        updates = False
        dp_in = obj.get()['Body'].read().decode('utf8')
        dp_dict = json.loads(dp_in)

        # determine if any updates necessary, make them
        if 'mapping' in dp_dict:
            updates = True
            dp_dict['model'] = dp_dict['mapping']
            dp_dict.pop('mapping')

        # if updates necessary, load data package, validate, and re-upload
        if updates:
            dp = datapackage.DataPackage(dp_dict, schema='fiscal')
            try:
                dp.validate()
            except datapackage.exceptions.ValidationError as e:
                print(e)
            else:
                dp_out = json.dumps(dp_dict).encode('utf8')
                bucket.put_object(Key=obj.key, Body=dp_out)
                print("Updated: {}".format(obj.key))
        else:
            print("No updates necessary: {}".format(obj.key))

