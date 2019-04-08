import CPCons
#import requests
from elasticsearch import Elasticsearch
es = Elasticsearch(['http://proton:Proton2018.@172.22.2.124:9200/'])

def es_index(x, printout=True):
    r = es.index(index='cpcons', doc_type='codigo', body = x['json'])
    if printout: print r['_id'], r['result']
    return r

cpcons = CPCons.load_cpcons('../cp_cons')


df = cpcons['codigo']
df['json'] = df.apply(lambda x: x.to_json(), axis=1)
es.indices.delete(index='cpcons', ignore=[400, 404])
df['es_result'] = df.apply(lambda x: es_index(x),axis=1)


