#!/bin/env python

import sys,traceback
import os
import time
import shutil

import filecmp
from inotifywrapper import InotifyWrapper
import _inotify as inotify
import threading
import Queue
import json
import logging
import collections

from pyelasticsearch.client import ElasticSearch
from pyelasticsearch.client import IndexAlreadyExistsError
from pyelasticsearch.client import ElasticHttpError

from hltdconf import *

terminate = False
#message type
MLMSG,EXCEPTION,EVENTLOG,UNFORMATTED = range(4)
#message severity
DEBUGLEVEL,INFOLEVEL,WARNINGLEVEL,ERRORLEVEL,FATALLEVEL = range(5)

typeStr=['MessageLogger','Exception','EventLog','Unformatted']
severityStr=['DEBUG','INFO','WARNING','ERROR','FATAL']

#defaults
READBULK=20
history = 5
saveHistory = False
logThreshold = 1 #(INFO)

#datetime_fmt = "30-Apr-2014 16:50:32 CEST"
datetime_fmt = "%d-%M-%Y %H:%M:%S %Z"

hostname = os.uname()[1]

class CMSSWLogEvent(object):

    def __init__(self,pid,type,severity,firstLine,findTimestamp=True):

        self.pid = pid
        self.type = type
        self.severity = severity
        self.document = {}
        self.message = [firstLine]
        #maybe should be skipped if we don't store these events
        if findTimestamp:
          self.document['timestamp'] = datetime.datetime.utcnow().isoformat()
      
    def append(self,line):
        self.message.append(line)

    def fillCommon(self):
        self.document['host']=hostname
        self.document['pid']=self.pid
        self.document['type']=typeStr[self.type]
        self.document['severity']=severityStr[self.severity]

    def decode(self):
        self.fillComon()
        self.document['message']=self.message[0]

             
class CMSSWLogEventML(CMSSWLogEvent):

    def __init__(self,pid,severity,firstLine):
        self.CMSSWLogEvent(pid,MLMSG,severity,firstLine,False)

    def decode(self):
        self.fillComon()
        #parse log header
        headerInfo = self.message[0].split(' ')
        self.document['category'] = headerInfo[1]
        self.document['module'] = headerInfo[2]
        self.document['fwkStage'] = headerInfo[6]
        #parse date and time and convert to UTC
        dt = datetime.datetime.strptime(headerInfo[3]+' '+headerInfo[4]+' '+headerInfo[5],datetime_fmt)
        self.document['timestamp'] = datetime.datetime.utcfromtimestamp(dt).isoformat()
        self.document['message'] = ""
        if len(self.message>1):
            for i in range(1,len(message)-1):
                self.document['message'].append(self.message[i])
#                if i<len(message)-1:
#                    self.document['message'].append('\n')


class CMSSWLogEventException(CMSSWLogEvent):

    def __init__(self,pid,firstLine):
        self.CMSSWLogEvent(pid,EXCEPTION,FATALLEVEL,severity,firstLine,False)

    def decode(self):
        self.fillComon()
        #parse log header
        headerInfo = self.message[0].split(' ')
        dt = datetime.datetime.strptime(headerInfo[4]+' '+headerInfo[5]+' '+headerInfo[6].split('-')[0],datetime_fmt)
        self.document['timestamp'] = datetime.datetime.utcfromtimestamp(dt).isoformat()

        if len(self.message>1):
            line2 = self.message[1].split(' ')
            self.document['category'] = line2[4]
        if len(self.message>2):
            line3 = self.message[2].strip().split(' ')
            self.document['fwkStage']=line3[2]
        if len(self.message>4):
            for i in range(4,len(message)-1):
                self.document['message'].append(self.message[i])
#                if i<len(message)-1:
#                    self.document['message'].append('\n')


class CMSSWLogParser(threading.Thread):

    def __init__(self,path,pid,queue):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.es = ElasticSearch('http://localhost:9200')
        self.pid = pid
        self.mainQueue = queue

        self.abort = False
        self.closed = False
        self.currentEvent = None
        self.threadEvent = threading.Event()

        self.historyFifo = collections.deque(history*[0], history)
        #self.localQueue = Queue.Queue()

    def run():
        #open and parse file, look for stop boolean
        #decode run number and pid from file name

        f = open(path,'r')

        #seenHeader = False
        while self.abort == False:
            buf = f.readlines(READBULK)
            if len(buf)>0:
                self.process(buf)
            else:
                if self.closed:
                    #finish reading
                    time.sleep(0.1)
                    buf = f.readlines()
                    if len(buf)>0:
                         self.process(buf)
                    break
                if self.abort == False:
                    self.threadEvent.wait(2)
        close(f)

        #if self.abort: logwriter.stop()
        #else: logwriter.finish()
        #logwriter.join()

    def process(self,buf,offset=0):
        max = len(buf)
        pos = offset
        if not self.currentEvent:
            #check lines to ignore / count etc.
            while pos < max:
                if buf[pos].startswith('%MSG-d'):
                    type = MLMSG
                    severity = DEBUGLEVEL
                    self.currentEvent = CMSSWLogEventML(self.pid,DEBUGLEVEL,buf[pos])
                elif buf[pos].startswith('%MSG-i'):
                    self.currentEvent = CMSSWLogEventML(self.pid,INFOLEVEL,buf[pos])
                elif buf[pos].startswith('%MSG-w'):
                    self.currentEvent = CMSSWLogEventML(self.pid,WARNINGLEVEL,buf[pos])
                elif buf[pos].startswith('%MSG-e'):
                    self.currentEvent = CMSSWLogEventML(self.pid,ERRORLEVEL,buf[pos])
                elif buf[pos].startswith('----- Begin Fatal Exception'):
                    self.currentEvent = CMSSWLogEventML(self.pid,buf[pos])
                elif buf[pos].startswith('----- Begin Processing'):
                    self.putInQueue(CMSSWLogEvent(pid,EVENTLOG,DEBUGLEVEL,buf[pos]))
                    pos+=1
                    continue
                else:
                    self.putInQueue(CMSSWLogEvent(pid,UNFORMATTED,DEBUGLEVEL,buf[pos]))
                    pos+=1
                    continue
                pos+=1
                if pos<max:
                    #recursive
                    self.process(buf,pos)
        else:
            while pos < max:
                if self.currentEvent.type == MLMSG and buf[pos]=='%MSG':
                    #close event
                    self.putInQueue(self.currentEvent)
                    self.currentEvent = None
                    pos+=1
                    if pos<max: 
                        process(buf,pos)
                    break
                elif self.currentEvent.type == EXCEPTION and buf[pos].startswith('----- End Fatal Exception'):
                    self.putInQueue(self.currentEvent)
                    self.currentEvent = None
                    pos+=1
                    if pos<max:
                        process(buf,pos)
                    break
                else:
                   #append to event
                   self.currentEvent.append(buf[pos])
                   pos+=1

    def putInQueue(event):
        if event.severity >= logThreshold:

            #store N logs before the problematic one
            if saveHistory and event.severity >= WARNINGLEVEL:
                while historyFIFO.count():
                    e = historyFIFO.popleft()
                    e.decode()
                    mainQueue.put(e)

            event.decode()
            mainQueue.put(event)
        elif saveHistory:
            historyFIFO.append(event)
 
    def stop(self):
        self.abort = True
        self.threadEvent.set()


class CMSSWLogESWriter(threading.Thread):

    def __init__(self,run):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.es = ElasticSearch('http://localhost:9200')
        self.index_name = index_name
        self.queue = Queue.Queue()
        self.stop = False
        self.threadEvent = threading.Event()

        self.parsers = {}
        self.numParsers=0

        #try to create elasticsearch index for run logging
        index_name = 'cmsswlog_run'+str(self.run)+'_'+conf.elastic_cluster
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
                'number_of_replicas' : 16
            },
        }
        #todo:close index entry
        self.run_mapping = {
            'cmsswlog' : {

                '_timestamp' : { 
                    'enabled'   : True,
                    'store'     : "yes",
                    "path"      : "timestamp"
                    },
#                '_ttl'       : { 'enabled' : True,                             
#                                 'default' :  '15d'} 
#                },
                'properties' : {
                    'timestamp' : {'type' : 'date'},
                    'host'      : {'type' : 'string'},
                    'pid'       : {'type' : 'string'},
                    'type'      : {'type' : 'string'},
                    'severity'  : {'type' : 'string'},
                    'category'  : {'type' : 'string'},
                    'module'    : {'type' : 'string'},
                    'fwkStage'  : {'type' : 'string'},
                    'message'   : {'type' : 'string'}
#                    'context'   : {'type' : 'string'}
                 }
            }
        }

        try:
            self.logger.info('writing to elastic index '+index_name)
            self.es.create_index(index_name, settings={ 'settings': self.settings, 'mappings': self.run_mapping })
        except ElasticHttpError as ex:
            self.logger.info(ex)
            pass

    def run(self):
        while self.abort == False:
            if queue.qsize>6:#rule of the thumb
                documents = []
                while self.abort == False:
                    try:
                        evt = queue.get(False)
                        documents.append(evt.document)
                    except Empty:
                        break
                if len(documents)>0:
                    self.es.bulk_index(self.index_name,'cmsswlog',documents)
            elif queue.qsize>0:
                    while self.abort == False:
                        try:
                            evt = queue.get(False)
                            self.es.index(self.index_name,'cmsswlog',evt.document)
                        except Empty:
                            break
            else:
                if self.stop == False and self.abort == False:
                    self.threadEvent.wait(2)
                else: break 


    def stop(self):
        for key in parsers:
            parsers[key].stop()
        for key in parsers:
            parsers[key].join()
        self.abort = True
        self.threadEvent.set()
        self.join()

    def finish(self):
        self.stop = True
        self.join()

    def addParser(self,path,pid):
        if self.stop or self.abort: return
        parsers[path] =  CMSSWLogParser(path,pid,queue)
        parsers[path].start()
        self.numParsers+=1

    def removeParser(self,path):
        try:
            parsers[path].closed=True
            parsers[path].join()
            self.numParsers-=1
        except Exception,ex:
            logger.warn('problem closing parser',str(ex))


class CMSSWLogCollector(object):

    def __init__(self,logger):
        self.logger = logger
        self.inotifyWrapper = InotifyWrapper(self,False)
        self.indices = {}
        self.stop = False

        #start another queue to fill events into elasticsearch

    def register_inotify_path(self,path,mask):
        self.inotifyWrapper.registerPath(path,mask)

    def start_inotify(self):
        self.inotifyWrapper.start()

    def stop_inotify(self,abort = False):
        self.stop = True
        logging.info("MonitorRanger: Stop inotify wrapper")
        self.inotifyWrapper.stop()
        logging.info("MonitorRanger: Join inotify wrapper")
        self.inotifyWrapper.join()
        logging.info("MonitorRanger: Inotify wrapper returned")
        for key in indices:
            indices[key].stop()

    def process_IN_CREATE(self, event):
        if self.stop: return
        self.logger.info("new cmssw log file found: %s on: %s" %(str(event.mask),event.fullpath))
        #find run number and pid

        run,pid = parseFileName(event.fullpath)
        if run and int(run) > 0 and pid:
            if not indices[run]:
                indices[run] = CMSSWLogESWriter(run)
                indices[run].start()
            indices[run].addParser(event.fullpath,pid)

    def process_IN_CLOSE_WRITE(self, event):

        run,pid = parseFileName(event.fullpath)
        if run and int(run) > 0:
            try:
                parsers[str(run)].removeParser(event.fullpath)
            except Exception,ex:
                logger.warn('log file for unknown run was closed',ex)

    def process_default(self, event):
        return

    def parseFileName(self,name):
        run = None
        pid = None
        try:
            elements = os.path.splitext(path).split('_')
            for e in elements:
               if e.startswith('run'):
                   run = int(e[3:])
            if e.startswith('pid'):
                   pid = int(e[3:])
            return run,pid
        except Exception,ex:
            logger.warn('problem parsing log file name',str(ex))
            return None,None
 

def signalHandler():
    global terminate
    terminate = True
    threadEventRef.set()

if __name__ == "__main__":
    logging.basicConfig(filename=os.path.join(conf.log_dir,"logcollector.log"),
                    level=logging.INFO,
                    format='%(levelname)s:%(asctime)s - %(funcName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))

    #STDOUT AND ERR REDIRECTIONS
    sys.stderr = stdErrorLog()
    sys.stdout = stdOutLog()

    self.threadEvent = threading.Event()
    global threadEventRef
    threadEventRef = self.threadEvent

    signal.signal(signal.SIGINT, signalHandler)

    #TODO
    hltdlogdir = '/var/log/hltd'
    hltdlogs = ['hltd.log','anelastic.log','elastic.log','elasticbu.log']

    cmsswlogdir = ['/var/log/hltd/pid']
    mask = inotify.IN_CREATE | inotify.IN_CLOSE_WRITE  # cmssw log files
    logger.info("starting CMSSW log collector for "+cmsswlogdir)
    mr = None
    try:

        #starting inotify thread
        mr = MonitorRanger(logger)
        mr.register_inotify_path(cmsswlogdir,mask)
        mr.start_inotify()

    except Exception,e:
        logger.exception("error: "+str(e))
        sys.exit(1)

    while terminate == False:
        self.threadEvent.wait(5)

    logging.info("Closing notifier")
    if mr is not None:
        mr.stop_inotify()

    logging.info("Quit")
    sys.exit(0)

