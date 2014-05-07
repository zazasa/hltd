#!/bin/env python

import sys,traceback
import os
import datetime

import logging
import _inotify as inotify
import threading
import Queue

from hltdconf import *
from aUtils import *

from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.client import IndexAlreadyExistsError
from pyelasticsearch.client import ElasticHttpError
import csv

import requests
import simplejson as json

index_name = "runindex"

class elasticBandBU:

    def __init__(self,es_server_url,runnumber,startTime,runMode=True):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.es = ElasticSearch(es_server_url)
        self.runnumber = str(runnumber)
        self.startTime = startTime
        self.host = os.uname()[1]
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
                        "delimiter": " "
                    }
                }
             },
            "index":{
                'number_of_shards' : 1,
                'number_of_replicas' : 1
            },
        }

        self.run_mapping = {
            'run' : {
#                '_routing' :{
#                    'required' : True,
#                    'path'     : 'runNumber'
#                },
                '_id' : {
                    'path' : 'runNumber'
                },
                'properties' : {
                    'runNumber':{
                        'type':'integer'
                        },
                    'startTime':{
                        'type':'string'
                            },
                    'endTime':{
                        'type':'string'
                            }
                },
                '_timestamp' : {
                    'enabled' : True,
                    'store'   : 'yes'
                    }
            },
            'microstatelegend' : {

                '_id' : {
                    'path' : 'id'
                },
                '_parent':{'type':'run'},
                'properties' : {
                    'names':{
                        'type':'string'
                        },
                    'id':{
                        'type':'string'
                        }
                    }
            },
            'pathlegend' : {

                '_id' : {
                    'path' : 'id'
                },
                '_parent':{'type':'run'},
                'properties' : {
                    'names':{
                        'type':'string'
                        },
                    'id':{
                        'type':'string'
                        }

                    }
                },
            'boxinfo' : {
                '_id'        :{'path':'id'},
                #'_parent'    :{'type':'run'},
                'properties' : {
                    'fm_date'       :{'type':'date'},
                    'id'            :{'type':'string'},
                    'broken'        :{'type':'integer'},
                    'used'          :{'type':'integer'},
                    'idles'         :{'type':'integer'},
                    'quarantined'   :{'type':'integer'},
                    'usedDataDir'   :{'type':'integer'},
                    'totalDataDir'  :{'type':'integer'},
                    'usedRamdisk'   :{'type':'integer'},
                    'totalRamdisk'  :{'type':'integer'},
                    'usedOutput'    :{'type':'integer'},
                    'totalOutput'   :{'type':'integer'}
                    },
                '_timestamp' : { 
                    'enabled'   : True,
                    'store'     : "yes",
                    "path"      : "fm_date"
                    },
                },
            'eols' : {
                '_id'        :{'path':'id'},
                '_parent'    :{'type':'run'},
                'properties' : {
                    'fm_date'       :{'type':'date'},
                    'id'            :{'type':'string'},
                    'ls'            :{'type':'integer'},
                    'NEvents'       :{'type':'integer'},
                    'NFiles'        :{'type':'integer'},
                    'TotalEvents'   :{'type':'integer'}
                    },
                '_timestamp' : { 
                    'enabled'   : True,
                    'store'     : "yes",
                    "path"      : "fm_date"
                    },
                }
            }




        try:
            self.logger.info('writing to elastic index '+index_name)
            self.es.create_index(index_name, settings={ 'settings': self.settings, 'mappings': self.run_mapping })
        except ElasticHttpError as ex:
            self.logger.info(ex)
#            print "Index already existing - records will be overridden"
            #this is normally fine as the index gets created somewhere across the cluster
            pass
        #write run number document
        if runMode == True:
            document = {}
            document['runNumber'] = self.runnumber
            document['startTime'] = startTime
            try:
                self.es.index(index_name,'run',document)
            except ElasticHttpError as ex:
                self.logger.info(ex)
                pass

    def read_line(self,fullpath):
        with open(fullpath,'r') as fp:
            return fp.readline()
    
    def elasticize_modulelegend(self,fullpath):

        self.logger.info(os.path.basename(fullpath))
        stub = self.read_line(fullpath)
        document = {}
        document['_parent']= self.runnumber
        document['id']= "microstatelegend_"+self.runnumber
        document['names']= self.read_line(fullpath)
        documents = [document]
        self.es.bulk_index(index_name,'microstatelegend',documents)


    def elasticize_pathlegend(self,fullpath):

        self.logger.info(os.path.basename(fullpath))
        stub = self.read_line(fullpath)
        document = {}
        document['_parent']= self.runnumber
        document['id']= "pathlegend_"+self.runnumber
        document['names']= self.read_line(fullpath)
        documents = [document]
        self.es.bulk_index(index_name,'pathlegend',documents)

    def elasticize_runend_time(self,endtime):

        self.logger.info(str(endtime)+" going into buffer")
        document = {}
        document['runNumber'] = self.runnumber
        document['startTime'] = self.startTime
        document['endTime'] = endtime
        self.es.index(index_name,'run',document)


    def elasticize_box(self,infile):

        basename = infile.basename
        self.logger.debug(basename)
        document = infile.data
        #document['_parent']= self.runnumber
        document['id']= basename + '_' + document['fm_date'].split('.')[0] #strip seconds
        documents = [document]
        self.es.bulk_index(index_name,'boxinfo',documents)

    def elasticize_eols(self,infile):
        basename = infile.basename
        self.logger.info(basename)
        data = infile.data['data']
        data.append(infile.mtime)
        data.append(infile.ls[2:])
        

        values = [int(f) if f.isdigit() else str(f) for f in data]
        keys = ["NEvents","NFiles","TotalEvents","fm_date","ls"]
        document = dict(zip(keys, values))

        document['id'] = infile.name+"_"+os.uname()[1]
        document['_parent']= self.runnumber
        documents = [document]
        self.es.bulk_index(index_name,'eols',documents)

class elasticCollectorBU():

    
    def __init__(self, inMonDir, inRunDir, watchdir, rn):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.inputMonDir = inMonDir
        

        self.runCreated = False
        self.insertedModuleLegend = False
        self.insertedPathLegend = False
        self.eorCheckPath = inRunDir + '/run' +  str(rn) + '_ls0000_EoR.jsn'
        self.endingFilePath = watchdir+'/ending'+ str(rn)
        
        self.stoprequest = threading.Event()
        self.emptyQueue = threading.Event()
        self.source = False
        self.infile = False

    def start(self):
        self.run()

    def stop(self):
        self.stoprequest.set()

    def run(self):
        self.logger.info("Start main loop")
        count = 0
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
            #check for EoR file every 5 intervals
            count+=1
            if (count%5) == 0:
                if os.path.exists(self.eorCheckPath):
                    if es:
                        dt=os.path.getctime(self.eorCheckPath)
                        endtime = datetime.datetime.utcfromtimestamp(dt).isoformat()
                        es.elasticize_runend_time(endtime)
                        self.runCreated = True
                    #create endingXXXXXX file to signal main process to look for completition in appliance ES cluster
                    endingFile = open(self.endingFilePath, 'w+')
                    close(endingFile)
                    break
        self.logger.info("Stop main loop")


    def setSource(self,source):
        self.source = source


    def process(self):
        self.logger.debug("RECEIVED FILE: %s " %(self.infile.basename))
        filepath = self.infile.filepath
        filetype = self.infile.filetype
        eventtype = self.eventtype
        if es and eventtype & (inotify.IN_CLOSE_WRITE | inotify.IN_MOVED_TO):
            if filetype in [MODULELEGEND] and self.insertedModuleLegend == False:
                es.elasticize_modulelegend(filepath)
                self.insertedModuleLegend = True
            elif filetype in [PATHLEGEND] and self.insertedPathLegend == False:
                es.elasticize_pathlegend(filepath)
                self.insertedPathLegend = True          
            elif filetype == EOLS:
                self.logger.info(self.infile.basename)
                es.elasticize_eols(self.infile)



class elasticBoxCollectorBU():

    def __init__(self,esbox):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.stoprequest = threading.Event()
        self.emptyQueue = threading.Event()
        self.source = False
        self.infile = False
        self.es = esbox

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
        self.logger.info("Stop main loop")

    def setSource(self,source):
        self.source = source

    def process(self):
        self.logger.debug("RECEIVED FILE: %s " %(self.infile.basename))
        filepath = self.infile.filepath
        filetype = self.infile.filetype
        eventtype = self.eventtype
        if filetype == BOX:
            #self.logger.info(self.infile.basename)
            try:
                self.es.elasticize_box(self.infile)
            except Exception,ex:
                self.logger.info("Unable to send box info update to elastic server: "+ str(ex))


class BoxInfoUpdater(threading.Thread):

    def __init__(self,ramdisk):
        self.logger = logging.getLogger(self.__class__.__name__)

        try:
            threading.Thread.__init__(self)

            boxesDir =  os.path.join(ramdisk,'appliance/boxes')
            boxesMask = inotify.IN_CLOSE_WRITE 
            self.logger.info("starting elastic for "+boxesDir)
    
            self.eventQueue = Queue.Queue()
            self.mr = MonitorRanger()
            self.mr.setEventQueue(self.eventQueue)
            self.mr.register_inotify_path(boxesDir,boxesMask)

        except Exception,ex:
            self.logger.error(str(ex))

    def run(self):
        try:
            try:
                if conf.elastic_bu_test is not None:
                    self.es = elasticBandBU('http://localhost:9200',0,'',False)
                else:
                    self.es = elasticBandBU(conf.elastic_runindex_url,0,'',False)
            except:
                self.es = elasticBandBU(conf.elastic_runindex_url,0,'',False)

            self.ec = elasticBoxCollectorBU(self.es)
            self.ec.setSource(self.eventQueue)

            self.mr.start_inotify()
            self.ec.start()
        except Exception,ex:
            self.logger.error(str(ex))

    def stop(self):
        try:
            self.logger.debug("request to stop")
            if self.mr is not None:
                self.mr.stop_inotify()
            self.ec.stop()
            self.join()
        except Exception,ex:
            self.logger.error(str(ex))

class RunCompletedChecker(threading.Thread):

    def __init__(self,nr,nresources):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.nr = nr
        self.nresources = nresources
        self.url = 'http://localhost:9200/run'+str(nr).zfill(conf.run_number_padding)+'*/fu-complete/_count'
        self.urlclose = 'http://localhost:9200/run'+str(nr).zfill(conf.run_number_padding)+'*/_close'
        self.logurlclose = 'http://localhost:9200/log_run'+str(nr).zfill(conf.run_number_padding)+'*/_close'
        self.stop = False
        try:
            threading.Thread.__init__(self)

        except Exception,ex:
            self.logger.error(str(ex))

    def run(self):
        try:
            while self.stop == False:
                resp = requests.post(url, '')
                data = json.load(resp.content)
                if int(data['count']) == self.nresources:
                    #all hosts are finished, close the index
                    resp = requests.post(urlclose)
                    self.logger.info('closed appliance ES index for run '+str(self.nr))
                    #wait a bit for log index to be filled up
                    time.sleep(5)
                    resp = requests.post(logurlclose)
                    self.logger.info('closed appliance ES log index for run '+str(self.nr))
                    break
                #TODO:write completition time to global ES index
        except Exception,ex:
            self.logger.error('Error in run completition check:i ' +str(ex))

    def stop(self):
        self.stop = True
    

if __name__ == "__main__":
    logging.basicConfig(filename=os.path.join(conf.log_dir,"elasticbu.log"),
                    level=logging.INFO,
                    format='%(levelname)s:%(asctime)s - %(funcName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))

    #STDOUT AND ERR REDIRECTIONS
    sys.stderr = stdErrorLog()
    sys.stdout = stdOutLog()

    eventQueue = Queue.Queue()

    es_server = sys.argv[0]
    dirname = sys.argv[1]
    watchdir = sys.argv[2]
    runnumber = sys.argv[3]
    dt=os.path.getctime(dirname)
    startTime = datetime.datetime.utcfromtimestamp(dt).isoformat()
    index_name = conf.elastic_runindex_name
    
    #EoR file path to watch for

    mainDir = dirname
    mainMask = inotify.IN_CLOSE_WRITE |  inotify.IN_MOVED_TO
    monDir = os.path.join(dirname,"mon")
    monMask = inotify.IN_CLOSE_WRITE |  inotify.IN_MOVED_TO

    logger.info("starting elastic for "+mainDir)
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
        mr.register_inotify_path(monDir,monMask)
        mr.register_inotify_path(mainDir,mainMask)

        mr.start_inotify()

        try:
            if conf.elastic_bu_test is not None:
                es = elasticBandBU('http://localhost:9200',runnumber,startTime)
            else:
                es = elasticBandBU(conf.elastic_runindex_url,runnumber,startTime)
        except:
            es = elasticBandBU(conf.elastic_runindex_url,runnumber,startTime)

        #starting elasticCollector thread
        ec = elasticCollectorBU(monDir,dirname, watchdir, runnumber.zfill(conf.run_number_padding))
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

