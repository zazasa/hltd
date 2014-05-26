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
from pyelasticsearch.client import ConnectionError
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
                    'startTimeRC':{
                        'type':'date'
                            },
                    'stopTimeRC':{
                        'type':'date'
                            },
                    'startTime':{
                        'type':'date'
                            },
                    'endTime':{
                        'type':'date'
                            },
                    'completedTime' : {
                        'type':'date'
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
                },
            'minimerge' : {
                '_id'        :{'path':'id'},
                '_parent'    :{'type':'run'},
                'properties' : {
                    'fm_date'       :{'type':'date'},
                    'id'            :{'type':'string'}, #run+appliance+stream+ls
                    'appliance'     :{'type':'string'},
                    'stream'        :{'type':'string'},
                    'ls'            :{'type':'integer'},
                    'processed'     :{'type':'integer'},
                    'accepted'      :{'type':'integer'},
                    'errorEvents'   :{'type':'integer'},
                    'size'          :{'type':'integer'},
                    }
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

    def elasticize_minimerge(self,infile):
        basename = infile.basename
        self.logger.info(basename)
        data = infile.data['data']
        data.append(infile.mtime)
        data.append(infile.ls[2:])
        data.append(infile.stream)
        values = [int(f) if str(f).isdigit() else str(f) for f in data]
        keys = ["processed","accepted","errorEvents","fname","size","eolField1","eolField2","fm_date","ls","stream"]
        document = dict(zip(keys, values))
        document['id'] = infile.name
        document['_parent']= self.runnumber
        documents = [document]
        self.es.bulk_index(index_name,'minimerge',documents)

#used when connection to central ES fails
class elasticBandDummy():

    def __init__(self):
        return

    def elasticize_modulelegend(self,fullpath):
        return

    def elasticize_pathlegend(self,fullpath):
        return

    def elasticize_runend_time(self,endtime):
        return

    def elasticize_box(self,infile):
        return

    def elasticize_eols(self,infile):
        return

    def elasticize_minimerge(self,infile):
        return



class elasticCollectorBU():

    
    def __init__(self, inMonDir, inRunDir, watchdir, rn):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.inputMonDir = inMonDir
        

        self.insertedModuleLegend = False
        self.insertedPathLegend = False
        self.eorCheckPath = inRunDir + '/run' +  str(rn).zfill(conf.run_number_padding) + '_ls0000_EoR.jsn'
        #self.endingFilePath = watchdir + '/eor' + str(rn)
        
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
                    #create endingXXXXXX file to signal main process to look for completition in appliance ES cluster
                    #endingFile = open(self.endingFilePath, 'w+')
                    #close(endingFile)
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
            elif filetype == OUTPUT:
                #mini-merged json file on BU
                self.logger.info(self.infile.basename)
                es.elasticize_minimerge(self.infile)
                self.infile.deleteFile()


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
        self.stopping = False

        try:
            threading.Thread.__init__(self)
            self.threadEvent = threading.Event()

            boxesDir =  os.path.join(ramdisk,'appliance/boxes')
            boxesMask = inotify.IN_CLOSE_WRITE 
            self.logger.info("starting elastic for "+boxesDir)
    
            self.eventQueue = Queue.Queue()
            self.mr = MonitorRanger()
            self.mr.setEventQueue(self.eventQueue)
            self.mr.register_inotify_path(boxesDir,boxesMask)

        except Exception,ex:
            self.logger.exception(ex)

    def run(self):
        try:
            while self.stopping==False:
              try:
                if conf.elastic_bu_test is not None:
                    self.es = elasticBandBU('http://localhost:9200',0,'',False)
                    break;
                else:
                    self.es = elasticBandBU(conf.elastic_runindex_url,0,'',False)
                    break;
              except:
                self.threadEvent.wait(60)

            if self.stopping:return

            self.ec = elasticBoxCollectorBU(self.es)
            self.ec.setSource(self.eventQueue)

            self.mr.start_inotify()
            self.ec.start()
        except Exception,ex:
            self.logger.exception(ex)

    def stop(self):
        try:
            self.stopping=True
            self.threadEvent.set()
            if self.mr is not None:
                self.mr.stop_inotify()
            if self.ec is not None:
                self.ec.stop()
            self.join()
        except RuntimeError,ex:
            pass
        except Exception,ex:
            self.logger.exception(ex)

class RunCompletedChecker(threading.Thread):

    def __init__(self,mode,nr,nresources):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.mode = mode
        self.nr = nr
        self.nresources = nresources
        self.eorCheckPath = conf.watch_directory +'/run'+ str(nr).zfill(conf.run_number_padding) + '/run' +  str(nr).zfill(conf.run_number_padding) + '_ls0000_EoR.jsn'
        self.url = 'http://localhost:9200/run'+str(nr).zfill(conf.run_number_padding)+'*/fu-complete/_count'
        self.urlclose = 'http://localhost:9200/run'+str(nr).zfill(conf.run_number_padding)+'*/_close'
        self.logurlclose = 'http://localhost:9200/log_run'+str(nr).zfill(conf.run_number_padding)+'*/_close'
        self.stop = False
        self.threadEvent = threading.Event()
        try:
            threading.Thread.__init__(self)

        except Exception,ex:
            self.logger.exception(ex)

    def run(self):

        self.threadEvent.wait(10)
        while self.stop == False:
            self.threadEvent.wait(5)
            if self.stop: return#giving up
            if os.path.exists(self.eorCheckPath):
                break

        #start another loop where we check that no FU contains current run
        #code checks if the file content is consistent
        dir = conf.resource_base+'/boxes/'
        if self.mode == 2:
            while True:
                files = os.listdir(dir)
                endAllowed=True
                runFound=False
                for file in files:
                    if file != os.uname()[1]:
                        f = open(dir+file,'r')
                        lines = f.readlines()
                        #test that we are not reading incomplete file
                        try:
                            if lines[-1].startswith('entriesComplete'):pass
                            else:
                                endAllowed=False
                                break
                        except:
                            endAllowed=False
                            break
                        firstCopy=None
                        for l in lines:
                            if l.startswith('activeRuns='):
                                if firstCopy==None:
                                    firstCopy=l
                                    continue
                                else:
                                    if firstCopy!=l:
                                        endAllowed=False
                                        break
                                runstring = l.split('=')
                                try:
                                    runs = runstring[1].strip('\n ').split(',')
                                    for run in runs:
                                        if run.isdigit()==False:continue
                                        if int(run)==int(self.nr):
                                            runFound=True
                                            break
                                except:
                                    endAllowed=False
                                break
                        if firstCopy==None:endAllowed=False
                        if runFound==True:break
                        if endAllowed==False:break

                if endAllowed==True and runFound==False: break
                else: self.threadEvent.wait(5)

            try:
                time.sleep(10)
                resp = requests.post(self.urlclose)
                self.logger.info('closed appliance ES index for run '+str(self.nr))

            except Exception,ex:
                self.logger.error('Error in run completition check')
                self.logger.exception(ex)



        elif self.mode == 1:
            try:
                totalElapsed=0
                while self.stop == False:
                    resp = requests.post(self.url, '')
                    data = json.loads(resp.content)
                    if int(data['count']) >= len(self.nresources):
                        #all hosts are finished, close the index
                        #wait a bit for indexing and querying to complete
                        time.sleep(10)
                        resp = requests.post(self.urlclose)
                        self.logger.info('closed appliance ES index for run '+str(self.nr))
                        break
                    else:
                        time.sleep(5)
                        totalElapsed+=5
                        if totalElapsed>600:
                            self.logger.error('run index complete flag was not written by all FUs, giving up after 10 minutes.')
                            break
                    #TODO:write completition time to global ES index
            except Exception,ex:
                self.logger.error('Error in run completition check')
                self.logger.exception(ex)

    def stop(self):
        self.stop = True
        self.threadEvent.set() 



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
    outputdir = sys.argv[2]
    runnumber = sys.argv[3]
    watchdir = conf.watch_directory
    dt=os.path.getctime(dirname)
    startTime = datetime.datetime.utcfromtimestamp(dt).isoformat()
    index_name = conf.elastic_runindex_name
    
    #EoR file path to watch for

    mainDir = dirname
    mainMask = inotify.IN_CLOSE_WRITE |  inotify.IN_MOVED_TO
    monDir = os.path.join(dirname,"mon")
    monMask = inotify.IN_CLOSE_WRITE |  inotify.IN_MOVED_TO
    outMonDir = os.path.join(outputdir,"mon")
    outMonMask = inotify.IN_CLOSE_WRITE |  inotify.IN_MOVED_TO

    logger.info("starting elastic for "+mainDir)
    logger.info("starting elastic for "+monDir)

    try:
        logger.info("try create input mon dir " + monDir)
        os.makedirs(monDir)
    except OSError,ex:
        logger.info(ex)
        pass

    try:
        logger.info("try create output mon dir " + outMonDir)
        os.makedirs(outMonDir)
    except OSError,ex:
        logger.info(ex)
        pass

    mr = None
    try:
        #starting inotify thread
        mr = MonitorRanger()
        mr.setEventQueue(eventQueue)
        mr.register_inotify_path(monDir,monMask)
        mr.register_inotify_path(outMonDir,outMonMask)
        mr.register_inotify_path(mainDir,mainMask)

        mr.start_inotify()

        try:
            if conf.elastic_bu_test is not None:
                es = elasticBandBU('http://localhost:9200',runnumber,startTime)
            else:
                es = elasticBandBU(conf.elastic_runindex_url,runnumber,startTime)
        except:
            try:
                es = elasticBandBU(conf.elastic_runindex_url,runnumber,startTime)

            except ConnectionError,ex:
                logger.exception(ex)
                logger.info('falling back to dummy ES mode (no connections to central server in this run)')
                es = elasticBandDummy()
                

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

