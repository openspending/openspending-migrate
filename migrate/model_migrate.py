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

def create_datapackage(ds):
    # Create datapackage based on dataset.json
    dp = datapackage.DataPackage()
    basepath = 'test_exports/{}'.format(ds['name'])
    dp.metadata['name'] = ds['name']
    dp.metadata['title'] = ds['label']
    dp.metadata['description'] = ds['description']
    if ds['territories']:
        dp.metadata['countryCode'] = ds['territories']
    dp.metadata['license'] =  "ODbL-1.0"
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
    for name in os.listdir(DIR):
        ds_dir = os.path.join(DIR, name)
        if os.path.isdir(ds_dir):
            with open(os.path.join(ds_dir, 'dataset.json'), 'r') as fh:
                meta = json.load(fh)
                yield ds_dir, meta


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
            dim['primaryKey'] = norm_name  + '_name'
        if src.get('type') == 'attribute':
            dim['attributes'] = {
                'label': {
                    'source': norm_name
                }
            }
            dim['primaryKey'] = norm_name
        if src.get('type') == 'compound':
            for name, spec in src['attributes'].items():
                attr = slug(name)
                dim['attributes'][attr] = {
                    'source': norm_name + '_' + attr
                }
                if attr == 'label':
                    dim['attributes'][attr]['labelfor'] = norm_name + '_name'
            if 'name' in dim['attributes']:
                dim['primaryKey'] = 'name'
        model['dimensions'][norm_name] = dim
    return model


if __name__ == '__main__':
    for dir, ds in list_datasets():
        data = transform_dataset(ds)
        dp = create_datapackage(ds)
        # Write datapackage.json
        with open(os.path.join(dir, 'datapackage.json'), 'w') as fh:
            fh.write(dp.to_json())

