import dataset
import io
import json
import os
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

        ds['data'] = json.loads(ds['data'])
        ds['languages'] = []
        for lang in engine['dataset_language'].find(dataset_id=ds['id']):
            ds['languages'].append(lang['code'])

        ds['territories'] = []
        for terr in engine['dataset_territory'].find(dataset_id=ds['id']):
            ds['territories'].append(terr['code'])

        ds['sources'] = []
        for src in engine['source'].find(dataset_id=ds['id']):
            ds['sources'].append(src)

        # Add team members for the dataset
        query_stmt = ('SELECT account.name as username FROM account '
                      'INNER JOIN account_dataset '
                      'ON account.id = account_dataset.account_id '
                      'WHERE account_dataset.dataset_id = {dataset_id}')
        query = engine.query(query_stmt.format(dataset_id = ds['id']))
        ds['team'] = [member['username'] for member in query]

        mapping = ds['data'].get('mapping')
        if mapping is None or not len(mapping):
            continue

        yield ds, mapping
    
def get_queries():
    for ds, mapping in get_mappings():
        ds_name = ds['name']
        table_pattern = ds_name + '__'
        entry_table = '"' + table_pattern + 'entry"'
        fields = [(entry_table + '.id', '_openspending_id')]
        joins = []
        for dim, desc in mapping.items():
            if desc.get('type') == 'compound':
                dim_table = '"%s%s"' % (table_pattern, dim)
                joins.append((dim_table, dim))
                for attr, attr_desc in desc.get('attributes').items():
                    alias = '%s_%s' % (dim, attr)
                    fields.append(('%s."%s"' % (dim_table, attr), alias))
            elif desc.get('type') == 'date':
                dim_table = '"%s%s"' % (table_pattern, dim)
                joins.append((dim_table, dim))
                for attr in ['name', 'year', 'month', 'day', 'week',
                             'yearmonth', 'quarter']:
                    alias = '%s_%s' % (dim, attr)
                    fields.append(('%s."%s"' % (dim_table, attr), alias))
                fields.append(('%s.name' % dim_table, dim))
            else:
                fields.append(('%s."%s"' % (entry_table, dim), dim))

        select_clause = []
        for src, alias in fields:
            select_clause.append('%s AS "%s"' % (src, slugify(alias, sep='_')))
        select_clause = ', '.join(select_clause)

        join_clause = []
        for table, dim in joins:
            qb = 'LEFT JOIN %s ON %s."%s_id" = %s.id'
            qb = qb % (table, entry_table, dim, table)
            join_clause.append(qb)
        join_clause = ' '.join(join_clause)

        yield ds, 'SELECT %s FROM %s %s' % (select_clause, entry_table,
                                            join_clause)

def freeze_all():
    for ds, query in get_queries():
        out_base = os.path.join('exports5', private_or_public(ds))
        try:
            ds['export_query'] = query
            path = os.path.join(out_base, ds[u'name'])
            if not os.path.isdir(path):
                os.makedirs(path)

            ds_path = os.path.join(path, 'dataset.json')
            with open(ds_path, 'w') as fh:
                json.dump(ds, fh, default=json_default, indent=2)
            res = engine.query(query)
            dataset.freeze(res, filename='dataset.csv', prefix=path,
                           format='csv')
        except Exception as e:
            print(e)

freeze_all()
