import dataset
from pprint import pprint
import json
import yaml
from collections import Counter
from datetime import datetime
from normality import slugify

DB_URI = os.environ.get('DB_URI')
DATASETS_NOT_TO_EXPORT = []

engine = dataset.connect(DB_URI,reflect_metadata=False)


def private_or_public(ds):
    if ds['private']:
        return 'private'
    else:
        return 'public'


def namespace(ds):
    if len(ds.get(['team'])):
        return ds.get(['team'])
    else:
        return 'core'


def json_default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()


def get_mappings():
    for ds in list(engine['dataset']):
        if ds['private']:
            continue

        if ds['name'] in DATASETS_NOT_TO_EXPORT:
            continue

        dp = {
            'name': '__os_imported__'+ds['name'],
            'originalName': ds['name'],
            'title': ds['label'],
            'description': ds['description'],
            'currencyCode': ds['currency'],
        }

        sources = []
        for src in engine['source'].find(dataset_id=ds['id']):
            sources.append({
                'name': str(src['id']),
                'web': src['url']
            })
        if len(sources) > 0:
            dp['sources'] = sources

        ds['territories'] = []
        for terr in engine['dataset_territory'].find(dataset_id=ds['id']):
            dp['countryCode'] = terr['code']
            break

        # Add team members for the dataset
        query_stmt = ('SELECT account.name as username FROM account '
                      'INNER JOIN account_dataset '
                      'ON account.id = account_dataset.account_id '
                      'WHERE account_dataset.dataset_id = {dataset_id}')
        query = engine.query(query_stmt.format(dataset_id = ds['id']))
        contributors = [member['username'] for member in query]
        if len(contributors) > 0:
            author = contributors.pop(0)
            dp['author'] = author
            if len(contributors) > 0:
                dp['contributors'] = contributors

        mapping = json.loads(ds['data']).get('mapping')
        if mapping is None or not len(mapping):
            continue

        resource = {
            'name': dp['name'],
            'schema': {
                'fields': []
            }
        }

        yield dp, resource, mapping


def get_queries():
    for dp, resource, mapping in get_mappings():

        type_id = 1
        schema_fields = resource['schema']['fields']
        os_types = {}
        options = {}

        # try:
        #     for key, value in mapping.items():
        #         if value['type'] == 'measure':
        #             # fields.append({
        #             #     'name': value['column'],
        #             #     'title': value['label'],
        #             # })
        #             # os_types[fields[-1]['name']] = 'value'
        #             # options[fields[-1]['name']] = {'currency': ds['currency']}
        #         elif value['type'] == 'date':
        #             # fields.append({
        #             #     'name': value['column'],
        #             #     'title': value['label'],
        #             # })
        #             # os_types[fields[-1]['name']] = 'date:generic'
        #             # options[fields[-1]['name']] = {'format': '%Y-%m-%d'}
        #         elif value['type'] == 'attribute':
        #             # fields.append({
        #             #     'name': value['column'],
        #             #     'title': value['label'],
        #             # })
        #             # os_types[fields[-1]['name']] = 'unknown:string'
        #         elif value['type'] == 'compound':
        #             name_column = value['attributes']['name']['column']
        #             if name_column is None:
        #                 continue
        #             fields.append({
        #                 'name': name_column,
        #                 'title': value['label'],
        #             })
        #             os_types[fields[-1]['name']] = 'unknown:string-{}:code'.format(type_id)
        #             label_column = value['attributes']['label']['column']
        #             if label_column is not None and label_column != name_column:
        #                 fields[-1]['title'] += ' (id)'
        #                 fields.append({
        #                     'name': label_column,
        #                     'title': value['label'],
        #                 })
        #                 os_types[fields[-1]['name']] = 'unknown:string-{}:label'.format(type_id)
        #                 assert label_column is not None
        #             for attr, attr_desc in value['attributes'].items():
        #                 if attr in ['name', 'label']: continue
        #                 if attr_desc['column'] is None: continue
        #                 fields.append({
        #                     'name': attr_desc['column'],
        #                 })
        #                 os_types[fields[-1]['name']] = 'unknown:string'
        #             type_id += 1
        #         else:
        #             assert False
        #     assert type_id <= 40
        # except:
        #     pprint(mapping)
        #     raise

        ds_name = dp['originalName']
        table_pattern = ds_name + '__'
        entry_table = '"' + table_pattern + 'entry"'
        fields = []#(entry_table + '.id', '_openspending_id')]
        joins = []
        for dim, desc in mapping.items():
            dim_type = desc['type']
            if dim_type == 'compound':
                dim_table = '"%s%s"' % (table_pattern, dim)
                joins.append((dim_table, dim))
                # aliases = set()
                for attr, attr_desc in desc['attributes'].items():
                    # alias = attr_desc['column']
                    alias = '%s_%s' % (dim, attr)
                    # if alias in aliases:
                    #     continue
                    # aliases.add(alias)
                    fields.append(('%s."%s"' % (dim_table, attr), alias))
                    schema_fields.append({ 'name': alias })
                    os_types[alias] = {
                        'name': 'unknown:string-{}:code'.format(type_id),
                        'label': 'unknown:string-{}:label'.format(type_id)
                    }.get(attr, 'unknown:string')

                type_id += 1
            elif dim_type == 'date':
                dim_table = '"%s%s"' % (table_pattern, dim)
                joins.append((dim_table, dim))
                for attr in ['name', 'year', 'month', 'day', 'week',
                             'yearmonth', 'quarter']:
                    # alias = '%s_%s' % (desc['column'], attr)
                    alias = '%s_%s' % (dim, attr)
                    fields.append(('%s."%s"' % (dim_table, attr), alias))
                    schema_fields.append({ 'name': alias })
                    os_types[alias] = {
                        'name': 'date:generic',
                    }.get(attr, 'unknown:string-{}:code'.format(type_id))
                    if attr == 'name':
                        options[alias] = {'format': '%Y-%m-%d'}
                    else:
                        type_id += 1

                # fields.append(('%s.name' % dim_table, desc['column']))
                fields.append(('%s.name' % dim_table, dim))
                schema_fields.append({ 'name': dim })
                os_types[dim] = 'date:generic'
                options[dim] = {'format': '%Y-%m-%d'}
            else:
                # fields.append(('%s."%s"' % (entry_table, dim), desc['column']))
                fields.append(('%s."%s"' % (entry_table, dim), dim))
                if dim_type == 'measure':
                    schema_fields.append({ 'name': dim })
                    os_types[dim] = 'value'
                    options[dim] = {'currency': dp['currencyCode']}
                elif dim_type == 'attribute':
                    schema_fields.append({ 'name': dim })
                    os_types[dim] = 'unknown:string-{}:code'.format(type_id)
                    type_id += 1

        select_clause = []
        for src, alias in fields:
            select_clause.append('%s AS "%s"' % (src, slugify(alias, sep='_')))
        select_clause = ', '.join(select_clause)

        join_clause = []
        for table, dim in joins:
            qb = 'LEFT JOIN %s ON %s."%s_id" = %s.id' % (table, entry_table, dim, table)
            join_clause.append(qb)
        join_clause = ' '.join(join_clause)

        query = 'SELECT %s FROM %s %s' % (select_clause, entry_table,
                                          join_clause)

        # pprint(mapping)
        yield dp, resource, os_types, options, query


def freeze_all():
    count = 0
    pipeline = {}
    for dp, resource, os_types, options, query in get_queries():

        resource.update({
            'url': DB_URI,
            'query': query,

        })
        pipeline_steps = [
            {
                'run': 'add_metadata',
                'parameters': dp
            },
            {
                'run': 'add_resource',
                'parameters': resource,
            },
            {
                'run': 'query_from_dataset'
            },
            {
                'run': 'fiscal.model',
                'parameters': {
                    'options': options,
                    'os-types': os_types
                }
            },
            {
                'run': 'set_types'
            },
            {
                'run': 'dump.to_zip',
                'parameters': {
                    'out-file': 'dp.zip'
                }
            },
            {
                'run': 'fiscal.upload',
                'parameters': {
                    'in-file': 'dp.zip'
                }
            }
        ]
        pipeline[dp['originalName']] = {
            'pipeline': pipeline_steps
        }

        count += 1
        if count % 10 == 0: print(count)

        # out_base = os.path.join('exports5', private_or_public(ds))
        # try:
        #     ds['export_query'] = query
        #     path = os.path.join(out_base, dp['name'])
        #     if not os.path.isdir(path):
        #         os.makedirs(path)
        #
        #     pprint(dp)
        #     pprint(query)
        #     pprint(dims)
        #     ds_path = os.path.join(path, 'dataset.json')
        #     with open(ds_path, 'w') as fh:
        #         json.dump(ds, fh, default=json_default, indent=2)
        #     res = engine.query(query)
        #     dataset.freeze(res, filename='dataset.csv', prefix=path,
        #                    format='csv')
        # except Exception as e:
        #     print(e)
        # break
        # if count == 1: break

    yaml.dump(pipeline, open('pipeline-spec.yaml', 'w'))

freeze_all()

