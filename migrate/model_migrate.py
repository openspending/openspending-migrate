import os
import json
import datapackage
import io
import csv
from normality import slugify
from jsontableschema import infer

DIR = 'test_exports'

def slug(name):
    return slugify(name, sep='_')

def private_or_public(ds):
    if ds['private']:
        return 'private'
    else:
        return 'public'

def create_datapackage(ds):
    # Create datapackage based on dataset.json
    dp = datapackage.DataPackage()
    basepath = '{0}/{1}/{2}'.format(DIR,private_or_public(ds),ds['name'])
    dp.metadata['name'] = ds['name']
    dp.metadata['title'] = ds['label']
    dp.metadata['description'] = ds['description']
    if ds['territories']:
        dp.metadata['countryCode'] = ds['territories']
    dp.metadata['profiles'] = {'fiscal': '*','tabular': '*'}
    dp.metadata['resources'] = [{}]
    resource = dp.resources[0]
    resource.metadata['name'] = 'dataset'
    resource.metadata['path'] = 'dataset.csv'
    
    # Infer schema of dataset.csv file
    with io.open(basepath + '/dataset.csv') as stream:
        headers = stream.readline().rstrip('\n').split(',')
        values = csv.reader(stream)
        schema = infer(headers, values, row_limit=1000)
        resource.metadata['schema'] = schema

    # Translate mapping
    dp.metadata['mapping'] = transform_dataset(ds)
    return dp
    
def list_datasets():
    for root, dirs, files in os.walk(DIR):
        for f in files:
            if f == 'dataset.json':
                with open(os.path.join(root, f), 'r') as fh:
                    meta = json.load(fh)
                    print(root)
                    yield root, meta


def transform_dataset(source):
    mapping = source['data']['mapping']
    model = {'measures': {}, 'dimensions': {}}
    types = set()
    for name, src in mapping.items():
        norm_name = slug(name)
        if src.get('type') == 'measure':
            model['measures'][norm_name] = {
                'source': norm_name,
                'currency': source.get('currency')
            }
            continue

        dim = {
            'attributes': {}
        }
        if src.get('type') == 'date':
            dim['attributes'] = {
                'label': {
                    'source': norm_name + '_name'
                },
                'year': {
                    'source': norm_name + '_year'
                },
                'month': {
                    'source': norm_name + '_month'
                },
                'day': {
                    'source': norm_name + '_day'
                },
                'yearmonth': {
                    'source': norm_name + '_yearmonth'
                } 
            }
            dim['primaryKey'] = ['year','month','day']
            dim['dimensionType'] = 'datetime'
        if src.get('type') == 'attribute':
            dim['attributes'] = {
                'label': {
                    'source': norm_name
                }
            }
            dim['primaryKey'] = 'label'
        if src.get('type') == 'compound':
            for name, spec in src['attributes'].items():
                attr = slug(name)
                dim['attributes'][attr] = {
                    'source': norm_name + '_' + attr
                }
                if attr == 'label':
                    dim['attributes'][attr]['labelfor'] = 'name'
            if 'name' in dim['attributes']:
                dim['primaryKey'] = 'name'
        # content
        if norm_name == 'from' or norm_name == 'to':
            dim['dimensionType'] = 'entity'
        model['dimensions'][norm_name] = dim
    return model


if __name__ == '__main__':
    for dir, ds in list_datasets():
        if os.path.exists((os.path.join(dir, 'dataset.csv'))):
            dp = create_datapackage(ds)
            # Write datapackage.json
            with open(os.path.join(dir, 'datapackage.json'), 'w') as fh:
                fh.write(json.dumps(dp.metadata, indent=2, sort_keys=True))
        else:
            print(ds.get('name'))
                        
