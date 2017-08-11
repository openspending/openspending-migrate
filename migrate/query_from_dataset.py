from datapackage_pipelines.wrapper import ingest, spew
import dataset
import logging

params, dp, res_iter = ingest()

resource = dp['resources'][0]
query = resource.pop('query')
engine = resource.pop('url')
resource['path'] = 'data/'+dp['name']
resource['mediatype'] = 'text/csv'


def query_from_dataset(engine, query):
    engine = dataset.connect(engine, reflect_metadata=False)
    res = engine.query(query)
    yield from res

spew(dp, [query_from_dataset(engine, query)])