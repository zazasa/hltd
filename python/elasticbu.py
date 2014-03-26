#!/bin/env python

import sys,traceback
import os

import logging
import _inotify as inotify
import threading
import Queue

import hltdconf

from anelastic import *
from aUtils import *

from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.client import IndexAlreadyExistsError
from pyelasticsearch.client import ElasticHttpError
import csv

class elasticBandBU:

    def __init__(self,es_server_url,runnumber):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.es = ElasticSearch(es_server_url)
        self.runnumber = str(runnumber)
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
                'number_of_shards' : 1,
                'number_of_replicas' : 1
                }
            }

        self.run_mapping = {
            'run"' : {
                '_routing' :{
                    'required' : True,
                    'path'     : 'runnumber'
                },
                #"_id" : {
                #              "path" : "runnumber"
                #},
                'properties' : {
                       'runnumber':{'type':'integer'}
                },
                '_timestamp' : { 
                    'enabled' : True,
                    'store'   : "yes"
                    }
            },

            'microstatelegend' : {

                '_routing' :{
                    'required' : True,
                    'path'     : 'runnumber'
                },
                #'_parent':{'type':'run'},
                'properties' : {
                       'runnumber':{'type':'integer'},
                       'names':{'type':'string'}
                 }
            },

            'microstatelegend' : {

                '_routing' :{
                    'required' : True,
                    'path'     : 'runnumber'
                },
                #'_parent':{'type':'run'},
                'properties' : {
                       'runnumber':{'type':'integer'},
                       'names':{'type':'string'}
                }
            }
        }

        try:
            self.es.create_index('runindex_testing', settings={ 'settings': self.settings, 'mappings': self.run_mapping })
        except ElasticHttpError as ex:
            logger.info(ex)
#            print "Index already existing - records will be overridden"
            #this is normally fine as the index gets created somewhere across the cluster
            pass
        #write run number document
        document = {}
        document['runnumber'] = self.runnumber
        try:
            self.es.index('runindex_testing','run',document)
        except ElasticHttpError as ex:
            logger.info(ex)
            pass

    def read_line(self,fullpath):
        with open(fullpath,'r') as fp:
            return fp.readline()
    
    def elasticize_modulelegend(self,fullpath):

        self.logger.debug(os.path.basename(fullpath)+" going into buffer")
        stub = self.read_line(fullpath)
        document = {}
        #document['_parent']= self.runnumber
        document['runnumber']= self.runnumber
        document['names']= self.read_csv(fullpath)
        self.es.index('runindex_testing','microstatelegend',document)


    def elasticize_pathlegend(self,fullpath):

        self.logger.debug(os.path.basename(fullpath)+" going into buffer")
        stub = self.read_line(fullpath)
        document = {}
        #document['_parent']= self.runnumber
        document['runnumber']= self.runnumber
        document['names']= self.read_csv(fullpath)
        self.es.index('runindex_testing','pathlegend',document)

class elasticCollectorBU():
    stoprequest = threading.Event()
    emptyQueue = threading.Event()
    source = False
    infile = False
    
    def __init__(self, inMonDir):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.inputMonDir = inMonDir
        self.insertedModuleLegend = False
        self.insertedPathLegend = False

    def start(self):
        self.run()

    def stop(self):
        self.stoprequest.set()

    def run(self):
        self.logger.info("Start main loop") 
        while not (self.stoprequest.isSet() and self.emptyQueue.isSet()) :
            if self.source:
                try:
                    event = self.source.get(True,1.0) #blocking with timeout
                    self.eventtype = event.mask
                    self.infile = fileHandler(event.fullpath)
                    self.emptyQueue.clear()
                    self.process() 
                except (KeyboardInterrupt,Queue.Empty) as e:
                    self.emptyQueue.set() 
            else:
                time.sleep(1.0)

        es.flushBuffer()
        self.logger.info("Stop main loop")


    def setSource(self,source):
        self.source = source

    def process(self):
        self.logger.debug("RECEIVED FILE: %s " %(self.infile.basename))
        filepath = self.infile.filepath
        filetype = self.infile.filetype
        eventtype = self.eventtype
        if es and eventtype & (inotify.IN_CLOSE_WRITE | inotify.IN_MOVED_TO):
            if filetype in [MODULELEGEND]:
                es.elasticize_modulelegend(filepath)
            elif filetype in [PATHLEGEND]:
                es.elasticize_pathlegend(filepath)

if __name__ == "__main__":
    logging.basicConfig(filename="/tmp/elastic-test.log",
                    level=logging.INFO,
                    format='%(levelname)s:%(asctime)s-%(name)s.%(funcName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))

    #STDOUT AND ERR REDIRECTIONS
    sys.stderr = stdErrorLog()
    sys.stdout = stdOutLog()


    eventQueue = Queue.Queue()

    conf=hltdconf.hltdConf('/etc/hltd.conf')
    es_server = sys.argv[0]
    dirname = sys.argv[1]
    runnumber = sys.argv[2]
    monDir = os.path.join(dirname,"mon")

    monMask = inotify.IN_CLOSE_WRITE |  inotify.IN_MOVED_TO

    logger.info("starting elastic for "+monDir)

    try:
        logger.info("watch dir" + monDir)
        os.makedirs(monDir)
    except OSError,ex:
        logger.info(ex)
        pass

    mr = None
    try:
        #starting inotify thread
        mr = MonitorRanger()
        mr.setEventQueue(eventQueue)
        #mr.register_inotify_path(watchDir,mask)
        mr.register_inotify_path(monDir,monMask)
        mr.start_inotify()

        #TODO:conf
        #es = elasticBandBU(conf.elastic_runindex_url,dirname)
        es = elasticBandBU('http://localhost:9200',runnumber)

        #starting elasticCollector thread
        ec = elasticCollectorBU(runnumber)
        ec.setSource(eventQueue)
        ec.start()

    except Exception as e:
        logger.exception(e)
        print traceback.format_exc()
        logger.error("when processing files from directory "+monDir)

    logging.info("Closing notifier")
    if mr is not None:
      mr.stop_inotify()

    logging.info("Quit")
    sys.exit(0)

