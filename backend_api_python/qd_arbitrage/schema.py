"""OpenAPI schemas for the independent versioned JSON contract."""
from copy import deepcopy
from .contracts import defaults, ENGINE, TEMPLATE, PAIRS

DECIMAL = {'type':'string','pattern':r'^(?:0|[0-9]+)(?:\.[0-9]+)?$','maxLength':48}
INTEGER = {'type':'integer','minimum':0}
TIME = {'type':'string','format':'date-time','description':'Timezone required. Normalized to UTC; interval is [start,end).'}


def object_schema(value):
    properties = {}
    for key,item in value.items():
        if isinstance(item,dict):
            properties[key]=object_schema(item)
        elif type(item) is int:
            properties[key]={**INTEGER,'default':item}
        else:
            properties[key]={**DECIMAL,'default':item}
    return {'type':'object','additionalProperties':False,'properties':properties}


def run_schema(mode):
    schema=object_schema(defaults())
    p=schema['properties']
    p.update(schemaVersion={'type':'integer','enum':[1]},engineVersion={'type':'string','enum':[ENGINE]},
             template={'type':'string','enum':[TEMPLATE]},templateVersion={'type':'integer','enum':[1]},
             mode={'type':'string','enum':[mode]},pair={'type':'string','enum':list(PAIRS)})
    schema['required']=['mode'] if mode == 'paper' else []
    p['seed']['maximum']=2147483647
    for group in [p['strategy'],p['risk'],*p['venues']['properties'].values()]:
        for name,item in group['properties'].items():
            if item['type']=='integer':
                item.update(minimum=0 if name=='latencyMs' else 1,
                            maximum=1000000 if name=='maxCycles' else 60000)
    p['venues']['required']=['BINANCE','OKX']
    if mode=='backtest':
        p.update(start=deepcopy(TIME),end=deepcopy(TIME),datasetId={'type':'string','pattern':'^[0-9a-f]{64}$'})
        schema['required']+=['start','end','datasetId']
    return schema


IMPORT_SCHEMA={'type':'object','additionalProperties':False,'required':['pair','start','end'],
               'properties':{'pair':{'type':'string','enum':list(PAIRS)},'start':TIME,'end':TIME}}


def request_body(schema):
    return {'required':True,'content':{'application/json':{'schema':schema}}}


SUBMIT_DOC = {
    'parameters': [{'in':'header','name':'Idempotency-Key','required':False,
                    'schema':{'type':'string','maxLength':120},
                    'description':'Scoped by authenticated user and job kind; reuse with another config returns 409.'}],
    'responses': {202: {'description':'Durably queued', 'content':{'application/json':{'schema':{
        'type':'object','properties':{'code':{'type':'integer','enum':[1]},'msg':{'type':'string'},
        'data':{'type':'object','properties':{'jobId':{'type':'string'},'runId':{'type':'string','nullable':True},
                                           'status':{'type':'string'},'duplicate':{'type':'boolean'}}}}}}}}}
}

PAGE_DOC = {'parameters': [
    {'in':'query','name':'cursor','schema':{'type':'integer','minimum':0,'default':0}},
    {'in':'query','name':'page_size','schema':{'type':'integer','minimum':1,'maximum':500,'default':100}}
]}
