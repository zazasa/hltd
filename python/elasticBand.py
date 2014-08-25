import os,socket,time
import sys
from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.client import IndexAlreadyExistsError
from pyelasticsearch.client import ElasticHttpError
import json
import csv
import math
import logging

from aUtils import *

#MONBUFFERSIZE = 50
es_server_url = 'http://localhost:9200'

class elasticBand():


    def __init__(self,es_server_url,runstring,indexSuffix,monBufferSize,fastUpdateModulo):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.istateBuffer = []  
        self.prcinBuffer = {}   # {"lsX": doclist}
        self.prcoutBuffer = {}
        self.fuoutBuffer = {}
        self.es = ElasticSearch(es_server_url) 
        self.hostname = os.uname()[1]
        self.hostip = socket.gethostbyname_ex(self.hostname)[2][0]
        self.number_of_data_nodes = self.es.health()['number_of_data_nodes']
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
                'number_of_shards' : 2,
                'number_of_replicas' : 0,
                "routing" : {
                    "allocation" : {
                        "require" : {
                            "_ip" : self.hostip
                            }
                        }
                    }
                }
            }
        
        
        

        self.run_mapping = {
            'prc-i-state' : {
                'properties' : {
                    'macro'     : {'type' : 'integer'},
                    'mini'      : {'type' : 'integer'},
                    'micro'     : {'type' : 'integer'},
                    'tp'        : {'type' : 'double' },
                    'lead'      : {'type' : 'double' },
                    'nfiles'    : {'type' : 'integer'},
                    'fm_date'   : {'type' : 'date'   }
                },
                '_timestamp' : { 
                    'enabled'   : True,
                    'store'     : "yes",
                    "path"      : "fm_date"
                },
                '_ttl'       : { 'enabled' : True,                             
                                 'default' :  '5m'
                } 
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
                    #'definition': {'type': 'string'},
                    'data' : { 'properties' : {
                            'in' : { 'type' : 'integer'},
                            'out': { 'type' : 'integer'},
                            'file': { 'type' : 'string','index' : 'not_analyzed'}
                            }           
                    },
                    'ls' : { 
                        'type' : 'integer',
                        'store': "yes"
                    },
                    'stream' : {'type' : 'string','index' : 'not_analyzed'},
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
                    #'definition': {'type': 'string',"index" : "not_analyzed"},
                    'data' : { 'properties' : {
                            'out'    : { 'type' : 'integer'}
                            }
                    },
                    'ls'     : { 
                        'type' : 'integer',
                        'store': 'yes'
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
                    #'definition': {'type': 'string',"index" : "not_analyzed"},
                    'data' : { 'properties' : {
                            'in' : { 'type' : 'integer'},
                            'out': { 'type' : 'integer'},
                            'errorEvents' : {'type' : 'integer'},
                            'returnCodeMask': {'type':'string',"index" : "not_analyzed"},
                            'fileSize' : {'type':'long'},
                            'files': {
                                'properties' : {
                                    'name' : { 'type' : 'string',"index" : "not_analyzed"}
                                    }
                                }
                             }
                    },
                    'ls' : { 'type' : 'integer' },
                    'stream' : {'type' : 'string','index' : 'not_analyzed'},
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
            'fu-complete' : {
                'properties' : {
                    'host'     : {'type' : 'string'},
                    'fm_date'   : {'type' : 'date' }
                },
                '_timestamp' : { 
                    'enabled'   : True,
                    'store'     : "yes",
                    "path"      : "fm_date"
                },
            },
            'bu-out': {
                'properties' : {
                    #'definition': {'type': 'string',"index" : "not_analyzed"},
                    'out': { 'type' : 'integer'},
                    'ls' : { 'type' : 'integer' },
                    'source' : {'type' : 'string'}#,"index" : "not_analyzed"}
                }
            },
            'cmsswlog' : {
                '_timestamp' : { 
                    'enabled'   : True,
                    'store'     : "yes"
                },
                '_ttl'       : { 'enabled' : True,
                                 'default' :  '30d'
                },
                '_routing' :{
                    'required' : True,
                    'path'     : 'host'
                },
                'properties' : {
                    'host'      : {'type' : 'string'},
                    'pid'       : {'type' : 'integer'},
                    'type'      : {'type' : 'string',"index" : "not_analyzed"},
                    'severity'  : {'type' : 'string',"index" : "not_analyzed"},
                    'severityVal'  : {'type' : 'integer'},
                    'category'  : {'type' : 'string'},

                    'fwkState'     : {'type' : 'string',"index" : "not_analyzed"},
                    'module'     : {'type' : 'string',"index" : "not_analyzed"},
                    'moduleInstance'     : {'type' : 'string',"index" : "not_analyzed"},
                    'moduleCall'     : {'type' : 'string',"index" : "not_analyzed"},
                    'lumi'     : {'type' : 'integer'},
                    'eventInPrc'     : {'type' : 'long'},

                    'message'   : {'type' : 'string'},#,"index" : "not_analyzed"},
                    'lexicalId' : {'type' : 'string',"index" : "not_analyzed"},
                    'msgtime' : {'type' : 'date','format':'dd-MMM-YYYY HH:mm:ss'},
                    'msgtimezone' : {'type' : 'string'}
                    #'context'   : {'type' : 'string'}
                 }
            }
        }
        self.run = runstring
        self.monBufferSize = monBufferSize
        self.fastUpdateModulo = fastUpdateModulo
        aliasName = runstring + "_" + indexSuffix
        self.indexName = aliasName + "_" + self.hostname 
        alias_command ={'actions': [{"add":
                                        {"index":self.indexName,
                                         "alias":aliasName
                                         }
                                    }]
                       }
        try:
            self.es.create_index(self.indexName, settings={ 'settings': self.settings, 'mappings': self.run_mapping })
            self.es.update_aliases(alias_command)
        except ElasticHttpError as ex:
#            print "Index already existing - records will be overridden"
            #this is normally fine as the index gets created somewhere across the cluster
            pass

    def imbue_jsn(self,infile):
        with open(infile.filepath,'r') as fp:
            try:
                document = json.load(fp)
            except json.scanner.JSONDecodeError,ex:
                logger.exception(ex)
                return None,-1
            return document,0

    def imbue_csv(self,infile):
        with open(infile.filepath,'r') as fp:
            fp.readline()
            row = fp.readline().split(',')
            return row
    
    def elasticize_prc_istate(self,infile):
        filepath = infile.filepath
        self.logger.debug("%r going into buffer" %filepath)
        #mtime = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(os.path.getmtime(filepath)))
        mtime = infile.mtime
        stub = self.imbue_csv(infile)
        document = {}
        if len(stub) == 0 or stub[0]=='\n':
          return;
        try:
            document['macro'] = int(stub[0])
            document['mini']  = int(stub[1])
            document['micro'] = int(stub[2])
            document['tp']    = float(stub[4])
            document['lead']  = float(stub[5])
            document['nfiles']= int(stub[6])
            document['fm_date'] = str(mtime)
            self.istateBuffer.append(document)
        except Exception:
            pass
        #if len(self.istateBuffer) == MONBUFFERSIZE:
        if len(self.istateBuffer) == self.monBufferSize and (len(self.istateBuffer)%self.fastUpdateModulo)==0:
            self.flushMonBuffer()

    def elasticize_prc_sstate(self,infile):
        document,ret = self.imbue_jsn(infile)
        if ret<0:return
        datadict = {}
        datadict['ls'] = int(infile.ls[2:])
        datadict['process'] = infile.pid
        if document['data'][0] != "N/A":
          datadict['macro']   = [int(f) for f in document['data'][0].strip('[]').split(',')]
        else:
          datadict['macro'] = 0
        if document['data'][1] != "N/A":
          datadict['mini']    = [int(f) for f in document['data'][1].strip('[]').split(',')]
        else:
          datadict['mini'] = 0
        if document['data'][2] != "N/A":
          datadict['micro']   = [int(f) for f in document['data'][2].strip('[]').split(',')]
        else:
          datadict['micro'] = 0
        datadict['tp']      = float(document['data'][4]) if not math.isnan(float(document['data'][4])) and not  math.isinf(float(document['data'][4])) else 0.
        datadict['lead']    = float(document['data'][5]) if not math.isnan(float(document['data'][5])) and not  math.isinf(float(document['data'][5])) else 0.
        datadict['nfiles']  = int(document['data'][6])
        self.es.index(self.indexName,'prc-s-state',datadict)

    def elasticize_prc_out(self,infile):
        document,ret = self.imbue_jsn(infile)
        if ret<0:return
        run=infile.run
        ls=infile.ls
        stream=infile.stream
        #removing 'stream' prefix
        if stream.startswith("stream"): stream = stream[6:]

        values = [int(f) if f.isdigit() else str(f) for f in document['data']]
        keys = ["in","out","errorEvents","returnCodeMask","Filelist","fileSize","InputFiles","test"]
        datadict = dict(zip(keys, values))

        document['data']=datadict
        document['ls']=int(ls[2:])
        document['stream']=stream
        self.prcoutBuffer.setdefault(ls,[]).append(document)
        #self.es.index(self.indexName,'prc-out',document)
        #return int(ls[2:])

    def elasticize_fu_out(self,infile):
        
        document,ret = self.imbue_jsn(infile)
        if ret<0:return
        run=infile.run
        ls=infile.ls
        stream=infile.stream
        #removing 'stream' prefix
        if stream.startswith("stream"): stream = stream[6:]

        values= [int(f) if f.isdigit() else str(f) for f in document['data']]
        keys = ["in","out","errorEvents","returnCodeMask","Filelist","fileSize","InputFiles","test"]
        datadict = dict(zip(keys, values))

        
        document['data']=datadict
        document['ls']=int(ls[2:])
        document['stream']=stream
        self.fuoutBuffer.setdefault(ls,[]).append(document)
        #self.es.index(self.indexName,'fu-out',document)
        #return int(ls[2:])

    def elasticize_prc_in(self,infile):
        document,ret = self.imbue_jsn(infile)
        if ret<0:return
        ls=infile.ls
        index=infile.index
        prc=infile.pid

        document['data'] = [int(f) if f.isdigit() else str(f) for f in document['data']]
        datadict = {'out':document['data'][0]}
        document['data']=datadict
        document['ls']=int(ls[2:])
        document['index']=int(index[5:])
        document['dest']=os.uname()[1]
        document['process']=int(prc[3:])
        self.prcinBuffer.setdefault(ls,[]).append(document)
        #self.es.index(self.indexName,'prc-in',document)
        #os.remove(path+'/'+file)
        #return int(ls[2:])

    def elasticize_fu_complete(self,timestamp):
        document = {}
        document['host']=os.uname()[1]
        document['fm_date']=timestamp
        self.es.index(self.indexName,'fu-complete',document)

    def flushMonBuffer(self):
        if self.istateBuffer:
            self.logger.info("flushing fast monitor buffer (len: %r) " %len(self.istateBuffer))
            self.es.bulk_index(self.indexName,'prc-i-state',self.istateBuffer)
            self.istateBuffer = []

    def flushLS(self,ls):
        self.logger.info("flushing %r" %ls)
        prcinDocs = self.prcinBuffer.pop(ls) if ls in self.prcinBuffer else None
        prcoutDocs = self.prcoutBuffer.pop(ls) if ls in self.prcoutBuffer else None
        fuoutDocs = self.fuoutBuffer.pop(ls) if ls in self.fuoutBuffer else None
        if prcinDocs: self.es.bulk_index(self.indexName,'prc-in',prcinDocs)        
        if prcoutDocs: self.es.bulk_index(self.indexName,'prc-out',prcoutDocs)
        if fuoutDocs: self.es.bulk_index(self.indexName,'fu-out',fuoutDocs)

    def flushAll(self):
        self.flushMonBuffer()
        lslist = list(  set(self.prcinBuffer.keys()) | 
                        set(self.prcoutBuffer.keys()) |
                        set(self.fuoutBuffer.keys()) )
        for ls in lslist:
            self.flushLS(ls)

        

