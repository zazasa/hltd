import os
import sys
from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.client import IndexAlreadyExistsError
from pyelasticsearch.client import ElasticHttpError
import json
import csv
import math

import logging

es_server_url = 'http://localhost:9200'

class elasticBand():

    def __init__(self,es_server_url,runstring):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.es = ElasticSearch(es_server_url)
        self.settings = {
            "analysis":{
                "analyzer": {
                    "prefix-test-analyzer": {
                        "type": "custom",
                        "tokenizer": "prefix-test-tokenizer"
                        }
                    },
                "tokenizer": {
                    "prefix-test-tokenizer": {
                        "type": "path_hierarchy",
                        "delimiter": "_"
                        }
                    }
             },
            "index":{
                'number_of_shards' : 16,
                'number_of_replicas' : 1
                }
            }

        self.run_mapping = {
            'prc-i-state' : {
                'properties' : {
                    'macro' : {'type' : 'integer'},
                    'mini'  : {'type' : 'integer'},
                    'micro' : {'type' : 'integer'},
                    'tp'    : {'type' : 'double' },
                    'lead'  : {'type' : 'double' },
                    'nfiles': {'type' : 'integer'}
                    },
                '_timestamp' : { 
                    'enabled' : True,
                    'store'   : "yes"
                    },
                '_ttl'       : { 'enabled' : True,                             
                                 'default' :  '5m'} 
                },
            'prc-s-state' : {
                'properties' : {
                    'macro'  : {'type' : 'integer'},
                    'mini'   : {'type' : 'integer'},
                    'micro'  : {'type' : 'integer'},
                    'tp'     : {'type' : 'double'},
                    'lead'   : {'type' : 'double'},
                    'nfiles' : {'type' : 'integer'},            
                    'ls'     : {'type' : 'integer'},
                    'process': {'type' : 'string'}
                    },
                },
            'fu-s-state' : {
                'properties' : {
                    'macro'  : {'type' : 'integer'},
                    'mini'   : {'type' : 'integer'},
                    'micro'  : {'type' : 'integer'},
                    'tp'     : {'type' : 'double'},
                    'lead'   : {'type' : 'double'},
                    'nfiles' : {'type' : 'integer'},            
                    'ls'     : {'type' : 'integer'},
                    'machine': {'type' : 'string'}
                    }
                },
            'prc-out': {
                '_routing' :{
                    'required' : True,
                    'path'     : 'source'
                    },
                'properties' : {
                    'definition': {'type': 'string'},
                    'data' : { 'properties' : {
                            'in' : { 'type' : 'integer'},
                            'out': { 'type' : 'integer'},
                            'file': { 'type' : 'string'}
                            }           
                               },
                    'ls' : { 
                        'type' : 'integer',
                        'store': "yes"
                        },
                    'stream' : {'type' : 'string'},
                    'source' : {
                        'type' : 'string',
                        'index_analyzer': 'prefix-test-analyzer',
                        'search_analyzer': "keyword",
                        'store' : "yes",
                        'index' : "analyzed"
                        }
                    },
                '_timestamp' : { 
                    'enabled' : True,
                    'store'   : "yes"
                     }
                },
            'prc-in': {
                '_routing' :{
                    'required' : True,
                    'path'     : 'dest'
                    },
                'properties' : {
                    'definition': {'type': 'string'},
                    'data' : { 'properties' : {
                            'out'    : { 'type' : 'integer'}
                            }
                               },
                    'ls'     : { 
                        'type' : 'integer',
                        'store': "yes"
                        },
                    'index'  : { 'type' : 'integer' },
                    'source' : { 'type' : 'string'  },
                    'dest' : {
                        'type' : 'string',
                        'index_analyzer': 'prefix-test-analyzer',
                        'search_analyzer': "keyword",
                        'store' : "yes",
                        'index' : "analyzed",
                        },
                    'process' : { 'type' : 'integer' }
                    },
                '_timestamp' : { 
                    'enabled' : True,
                    'store'   : "yes"
                    }
                },
            'fu-out': {
                '_routing' :{
                    'required' : True,
                    'path'     : 'source'
                    },
                'properties' : {
                    'definition': {'type': 'string'},
                    'data' : { 'properties' : {
                            'in' : { 'type' : 'integer'},
                            'out': { 'type' : 'integer'},
                            'files': {
                                'properties' : {
                                    'name' : { 'type' : 'string'}
                                    }
                                }
                            }
                               },
                    'ls' : { 'type' : 'integer' },
                    'stream' : {'type' : 'string'},
                    'source' : {
                        'type' : 'string',
                        'index_analyzer': 'prefix-test-analyzer',
                        'search_analyzer': "keyword"
                        }
                    },
                '_timestamp' : { 
                    'enabled' : True,
                    'store'   : "yes"
                    }
                },
            'bu-out': {
                'properties' : {
                    'definition': {'type': 'string'},
                    'out': { 'type' : 'integer'},
                    'ls' : { 'type' : 'integer' },
                    'source' : {'type' : 'string'}
                    }
                }
            }
        self.run = runstring
        try:
            self.es.create_index(runstring, settings={ 'settings': self.settings, 'mappings': self.run_mapping })
        except ElasticHttpError as ex:
#            print "Index already existing - records will be overridden"
            #this is normally fine as the index gets created somewhere across the cluster
            pass

    def imbue_jsn(self,path,file):
        with open(os.path.join(path,file),'r') as fp:
            document = json.load(fp)
            return document

    def imbue_csv(self,path,file):
        with open(os.path.join(path,file),'r') as fp:
            fp.readline()
            row = fp.readline().split(',')
            return row
    
    def elasticize_prc_istate(self,path,file):
        stub = self.imbue_csv(path,file)
        document = {}
        document['macro'] = int(stub[0])
        document['mini']  = int(stub[1])
        document['micro'] = int(stub[2])
        document['tp']    = float(stub[4])
        document['lead']  = float(stub[5])
        document['nfiles']= int(stub[6])
        self.es.index(self.run,'prc-i-state',document)

    def elasticize_prc_sstate(self,path,file):
        document = self.imbue_jsn(path,file)
        tokens=file.split('.')[0].split('_')
        datadict = {}
        datadict['ls'] = int(tokens[1][2:])
        datadict['process'] = tokens[2]
        if not document['data'][0] == "N/A":
          datadict['macro']   = [int(f) for f in document['data'][0].strip('[]').split(',')]
        if not document['data'][1] ==  "N/A":
          datadict['mini']    = [int(f) for f in document['data'][1].strip('[]').split(',')]
        if not document['data'][2] == "N/A":
          datadict['micro']   = [int(f) for f in document['data'][2].strip('[]').split(',')]
        datadict['tp']      = float(document['data'][4]) if not math.isnan(float(document['data'][4])) and not  math.isinf(float(document['data'][4])) else 0.
        datadict['lead']    = float(document['data'][5]) if not math.isnan(float(document['data'][5])) and not  math.isinf(float(document['data'][5])) else 0.
        datadict['nfiles']  = int(document['data'][6])
        self.es.index(self.run,'prc-s-state',datadict)
        os.remove(path+'/'+file)

    def elasticize_prc_out(self,path,file):
        document = self.imbue_jsn(path,file)
        tokens=file.split('.')[0].split('_')
        run=tokens[0]
        ls=tokens[1]
        stream=tokens[2]
        document['data'] = [int(f) if f.isdigit() else str(f) for f in document['data']]
        
        values = document["data"]
        keys = ["in","out","errorEvents","ReturnCodeMask","Filelist","InputFiles"]
        datadict = dict(zip(keys, values))

        document['data']=datadict
        document['ls']=int(ls[2:])
        document['stream']=stream
        self.es.index(self.run,'prc-out',document)
        return int(ls[2:])

    def elasticize_fu_out(self,path,file):
        self.logger.info("%r , %r" %(path,file))
        document = self.imbue_jsn(path,file)
        tokens=file.split('.')[0].split('_')
        run=tokens[0]
        ls=tokens[1]
        stream=tokens[2]
        document['data'] = [int(f) if f.isdigit() else str(f) for f in document['data']]

        values = document["data"]
        keys = ["in","out","errorEvents","ReturnCodeMask","Filelist","InputFiles"]
        datadict = dict(zip(keys, values))
        
        document['data']=datadict
        document['ls']=int(ls[2:])
        document['stream']=stream
        self.es.index(self.run,'fu-out',document)
        return int(ls[2:])

    def elasticize_prc_in(self,path,file):
        document = self.imbue_jsn(path,file)
        tokens=file.split('.')[0].split('_')
        ls=tokens[1]
        index=tokens[2]
        prc=tokens[3]
        document['data'] = [int(f) if f.isdigit() else str(f) for f in document['data']]
        datadict = {'out':document['data'][0]}
        document['data']=datadict
        document['ls']=int(ls[2:])
        document['index']=int(index[5:])
        document['dest']=os.uname()[1]
        document['process']=int(prc[3:])
        self.es.index(self.run,'prc-in',document)
#        os.remove(path+'/'+file)
        return int(ls[2:])

