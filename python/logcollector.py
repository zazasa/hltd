#!/bin/env python

import sys,traceback
import os
import time
import datetime
import pytz
import shutil
import signal

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
from aUtils import stdOutLog,stdErrorLog

terminate = False
threadEventRef = None
#message type
MLMSG,EXCEPTION,EVENTLOG,UNFORMATTED,STACKTRACE = range(5)
#message severity
DEBUGLEVEL,INFOLEVEL,WARNINGLEVEL,ERRORLEVEL,FATALLEVEL = range(5)

typeStr=['messagelogger','exception','eventlog','unformatted','stacktrace']
severityStr=['DEBUG','INFO','WARNING','ERROR','FATAL']

#test defaults
readonce=32
bulkinsertMin = 8
history = 8
saveHistory = False
logThreshold = 1 #(INFO)
contextLogThreshold = 0 #(DEBUG)

#cmssw date and time: "30-Apr-2014 16:50:32 CEST"
#datetime_fmt = "%d-%b-%Y %H:%M:%S %Z"

hostname = os.uname()[1]

class CMSSWLogEvent(object):

    def __init__(self,pid,type,severity,firstLine):

        self.pid = pid
        self.type = type
        self.severity = severity
        self.document = {}
        self.message = [firstLine]
      
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
        CMSSWLogEvent.__init__(self,pid,MLMSG,severity,firstLine)

    def decode(self):
        CMSSWLogEvent.fillCommon(self)
        headerInfo = filter(None,self.message[0].split(' '))
        self.document['category'] = headerInfo[1].rstrip(':')
        self.document['info1'] = headerInfo[2]
        self.document['info2'] = headerInfo[6].rstrip(' :\n')
        #capture lumi info if present
        if headerInfo[6]=='Run':
            try:
                self.document['info2']=headerInfo[8]+headerInfo[9].rstrip(' :\n')
            except:
                pass
        self.document['msgtime']=headerInfo[3]+' '+headerInfo[4]
        if len(self.message)>1:
            for i in range(1,len(self.message)):
                if i==len(self.message)-1: 
                    self.message[i]=self.message[i].rstrip('\n')
                if 'message' in self.document:
                    self.document['message']+=self.message[i]
                else:
                    self.document['message'] = self.message[i]


class CMSSWLogEventException(CMSSWLogEvent):

    def __init__(self,pid,firstLine):
        CMSSWLogEvent.__init__(self,pid,EXCEPTION,FATALLEVEL,firstLine)
        self.documentclass = 'cmssw'

    def decode(self):
        CMSSWLogEvent.fillCommon(self)
        headerInfo = filter(None,self.message[0].split(' '))
        zone = headerInfo[6].rstrip('-')
        if zone == 'CEST':zone='CET'
        self.document['msgtime']=headerInfo[4]+' '+headerInfo[5]
        if len(self.message)>1:
            line2 = filter(None,self.message[1].split(' '))
            self.document['category'] = line2[4]
        if len(self.message)>2:
            line3 = filter(None,self.message[2].strip().split(' '))
            self.document['info2'] = line3[2].rstrip(' :\n')
        if len(self.message)>4:
            for i in range(4,len(self.message)):
                if i==len(self.message)-1: 
                    self.message[i]=self.message[i].rstrip('\n')
                if 'message' in self.document:
                    self.document['message']+=self.message[i]
                else:
                    self.document['message'] = self.message[i]


class CMSSWLogEventStackTrace(CMSSWLogEvent):

    def __init__(self,pid,firstLine):
        CMSSWLogEvent.__init__(self,pid,STACKTRACE,FATALLEVEL,firstLine)

    def decode(self):
        CMSSWLogEvent.fillCommon(self)
        #collect all lines
        if len(self.message)>1:
            for i in range(1,len(self.message)):
                if i==len(self.message)-1: 
                    self.message[i]=self.message[i].rstrip('\n')
                self.document['message']+=self.message[i]


class CMSSWLogParser(threading.Thread):

    def __init__(self,path,pid,queue):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.es = ElasticSearch('http://localhost:9200')
        self.path = path
        self.pid = pid
        self.mainQueue = queue

        self.abort = False
        self.closed = False
        self.currentEvent = None
        self.threadEvent = threading.Event()

        self.historyFifo = collections.deque(history*[0], history)

    def run(self):
        #decode run number and pid from file name
        f = open(self.path,'r')
        pidcheck = 3
        checkedOnce=False

        while self.abort == False:
            buf = f.readlines(readonce)
            if len(buf)>0:
                pidcheck=3
                self.process(buf)
            else:
                if self.abort == False:
                    pidcheck-=1
                    self.threadEvent.wait(2)
                    if pidcheck<=0:
                       try:
                           if os.kill(self.pid,0):
                               if checkedOnce==True:break
                               checkedOnce=True
                       except OSError:
                           if checkedOnce==True:break
                           checkedOnce=True
                           

        #consider last event finished and queue if not completed
        if self.abort == False and self.currentEvent:
            self.putInQueue(self.currentEvent)
        if self.abort == False:
            logger.info('detected termination of the CMSSW process '+str(self.pid)+', finishing.')
        f.close()
        self.closed=True
        #prepend file with 'old_' prefix so that it can be deleted later
        fpath, fname = os.path.split(self.path)
        os.rename(self.path,fpath+'/old_'+fname)

    def process(self,buf,offset=0):
        max = len(buf)
        pos = offset
        while pos < max:
            if not self.currentEvent:
            #check lines to ignore / count etc.
                if len(buf[pos])==0:
                    pass
                elif buf[pos].startswith('----- Begin Processing'):
                    self.putInQueue(CMSSWLogEvent(self.pid,EVENTLOG,DEBUGLEVEL,buf[pos]))
                elif buf[pos].startswith('%MSG-d'):
                    self.currentEvent = CMSSWLogEventML(self.pid,DEBUGLEVEL,buf[pos])
                elif buf[pos].startswith('%MSG-i'):
                    self.currentEvent = CMSSWLogEventML(self.pid,INFOLEVEL,buf[pos])
                elif buf[pos].startswith('%MSG-w'):
                    self.currentEvent = CMSSWLogEventML(self.pid,WARNINGLEVEL,buf[pos])
                elif buf[pos].startswith('%MSG-e'):
                    self.currentEvent = CMSSWLogEventML(self.pid,ERRORLEVEL,buf[pos])
                elif buf[pos].startswith('%MSG-d'):
                    self.currentEvent = CMSSWLogEventML(self.pid,DEBUGLEVEL,buf[pos])
                elif buf[pos].startswith('----- Begin Fatal Exception'): 
                    #TODO:should enable stack trace below (also with MSG-e for fpe,illegal instr.,sigsegv and bus error)
                    #and disable opening other multilines
                    #sometimes looks like: %MSG-e FatalSystemSignal:  ExceptionGenerator:a
                    #or %MSG-e Root_Error:  ExceptionGenerator:a  ...... floating point exception
                    #TODO:catch assertion
                    self.currentEvent = CMSSWLogEventException(self.pid,buf[pos])
                #else signals: -8, ... or -
                elif buf[pos].startswith('There was a crash.') \
                    or buf[pos].startswith('A fatal system signal') \
                    or buf[pos].startswith('Aborted (core dumped)'):
                    #or buf[pos].startswith('A fatal signal') # 

                    #these are usually caused by intentionally sent signal:
                    #or buf[pos].startswith('Trace/breakpoint trap (core dumped)') #-4
                    #or buf[pos].startswith('Hangup') #-1
                    #or buf[pos].startswith('Quit') #-2
                    #or buf[pos].startswith('User defined signal 1') #-10
                    #or buf[pos].startswith('Terminated') #-15
                    #will be completed when file closes
                    self.currentEvent = CMSSWLogEventStackTrace(self.pid,buf[pos])
                elif buf[pos]=='\n':
                    pass
                else:
                    self.putInQueue(CMSSWLogEvent(self.pid,UNFORMATTED,DEBUGLEVEL,buf[pos]))
                pos+=1
            else:
                if self.currentEvent.type == MLMSG and (buf[pos]=='%MSG' or buf[pos]=='%MSG\n') :
                    #close event
                    self.putInQueue(self.currentEvent)
                    self.currentEvent = None
                elif self.currentEvent.type == EXCEPTION and buf[pos].startswith('----- End Fatal Exception'):
                    self.putInQueue(self.currentEvent)
                    self.currentEvent = None
                else:
                   #append message line to event
                   self.currentEvent.append(buf[pos])
                pos+=1

    def putInQueue(self,event):
        if event.severity >= logThreshold:

            #store N logs before the problematic one
            if saveHistory and event.severity >= WARNINGLEVEL:
                while historyFIFO.count():
                    e = historyFIFO.popleft()
                    e.decode()
                    mainQueue.put(e)

            event.decode()
            self.mainQueue.put(event)
        elif saveHistory and event.severity>=contextLogThreshold:
            self.historyFIFO.append(event)
 
    def stop(self):
        self.abort = True
        self.threadEvent.set()


class CMSSWLogESWriter(threading.Thread):

    def __init__(self,rn):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.es = ElasticSearch('http://localhost:9200')
        self.queue = Queue.Queue()
        self.parsers = {}
        self.numParsers=0
        self.doStop = False
        self.threadEvent = threading.Event()
        self.rn = rn
        self.abort = False

        #try to create elasticsearch index for run logging
        if not conf.elastic_cluster:
            self.index_name = 'log_run'+str(self.rn).zfill(6)
        else:
            self.index_name = 'log_run'+str(self.rn).zfill(6)+'_'+conf.elastic_cluster
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
#                'number_of_shards' : 1,
#                'number_of_replicas' : 1
                'number_of_shards' : 16,
                'number_of_replicas' : 1
            }
        }
        #todo:close index entry, id & parent-child for context logs?
        self.run_mapping = {
            'cmsswlog' : {

#                '_timestamp' : { 
#                    'enabled'   : True,
#                    'store'     : "yes",
#                    "path"      : "timestamp"
#                    },
#                '_ttl'       : { 'enabled' : True,                             
#                                 'default' :  '15d'} 
#                },
                'properties' : {
                    'host'      : {'type' : 'string'},
                    'pid'       : {'type' : 'integer'},
                    'type'      : {'type' : 'string'},
                    'severity'  : {'type' : 'string'},
                    'category'  : {'type' : 'string'},
                    'info1'     : {'type' : 'string'},
                    'info2'     : {'type' : 'string'},
                    'message'   : {'type' : 'string'},
                    'msgtime' : {'type' : 'date','format':'dd-MMM-YYYY HH:mm:ss'}
#                    'context'   : {'type' : 'string'}
                 }
            }
        }

        #try to create index if not already created from other nodes
        try:
            self.logger.info('writing to elastic index '+self.index_name)
            self.es.create_index(self.index_name, settings={ 'settings': self.settings, 'mappings': self.run_mapping })
        except ElasticHttpError as ex:
            self.logger.info(ex)
            pass

    def run(self):
        while self.abort == False:
            if self.queue.qsize()>bulkinsertMin:
                documents = []
                while self.abort == False:
                    try:
                        evt = self.queue.get(False)
                        documents.append(evt.document)
                    except Queue.Empty:
                        break
                if len(documents)>0:
                    try:
                        self.es.bulk_index(self.index_name,'cmsswlog',documents)
                    except Exception,ex:
                        logger.error("es bulk index:"+str(ex))
            elif self.queue.qsize()>0:
                    while self.abort == False:
                        try:
                            evt = self.queue.get(False)
                            try:
                                self.es.index(self.index_name,'cmsswlog',evt.document)
                            except Exception,ex: 
                                logger.error("es index:"+str(ex))
                        except Queue.Empty:
                            break
            else:
                if self.doStop == False and self.abort == False:
                    self.threadEvent.wait(1)
                else: break 


    def stop(self):
        for key in self.parsers.keys():
            self.parsers[key].stop()
        for key in self.parsers.keys():
            self.parsers[key].join()
        self.abort = True
        self.threadEvent.set()
        self.join()

    def clearFinished(self):
        aliveCount=0
        for key in self.parsers.keys():
            aliveCount+=1
            if self.parsers[key].closed:
                self.parsers[key].join()
                del self.parsers[key]
                aliveCount-=1
        return aliveCount

    def addParser(self,path,pid):
        if self.doStop or self.abort: return
        self.parsers[path] =  CMSSWLogParser(path,pid,self.queue)
        self.parsers[path].start()
        self.numParsers+=1

    def removeParser(self,path):
        try:
            self.parsers[path].join()
            self.numParsers-=1
        except Exception,ex:
            logger.warn('problem closing parser: '+str(ex))


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
        for rn in self.indices.keys():
                self.indices[rn].stop()

    def process_IN_CREATE(self, event):
        if self.stop: return
        if event.fullpath.startswith('old_'): return
        self.logger.info("new cmssw log file found: "+event.fullpath)
        #find run number and pid

        rn,pid = self.parseFileName(event.fullpath)
        if rn and rn > 0 and pid:
            if rn not in self.indices:
                self.indices[rn] = CMSSWLogESWriter(rn)
                self.indices[rn].start()
            self.indices[rn].addParser(event.fullpath,pid)

        #cleanup
        for rn in self.indices.keys():
                alive = self.indices[rn].clearFinished()
                if alive == 0:
                    logger.info('removing old run'+str(rn)+' from the list')
                    del self.indices[rn]

    def process_default(self, event):
        return

    def parseFileName(self,name):
        rn = None
        pid = None
        try:
            elements = os.path.splitext(name)[0].split('_')
            for e in elements:
               if e.startswith('run'):
                   rn = int(e[3:])
               if e.startswith('pid'):
                   pid = int(e[3:])
            return rn,pid
        except Exception,ex:
            logger.warn('problem parsing log file name: '+str(ex))
            return None,None
 

def signalHandler(p1,p2):
    global terminate
    global threadEventRef
    terminate = True
    if threadEventRef:
        threadEventRef.set()

def registerSignal(eventRef):
    global threadEventRef
    threadEventRef = threadEvent
    signal.signal(signal.SIGINT, signalHandler)
    signal.signal(signal.SIGTERM, signalHandler)
    

if __name__ == "__main__":
    logging.basicConfig(filename=os.path.join(conf.log_dir,"logcollector.log"),
                    level=logging.INFO,
                    format='%(levelname)s:%(asctime)s - %(funcName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))

    #STDOUT AND ERR REDIRECTIONS
    sys.stderr = stdErrorLog()
    sys.stdout = stdOutLog()

    threadEvent = threading.Event()
    registerSignal(threadEvent)

    #TODO:hltd log parser
    hltdlogdir = '/var/log/hltd'
    hltdlog = 'hltd.log'
    hltdrunlogs = ['hltd.log','anelastic.log','elastic.log','elasticbu.log']

    cmsswlogdir = '/var/log/hltd/pid'

    #TODO:do this check on run based intervals
    #existing_cmsswlogs = os.listdir(hltdlogdir)
    #current_dt = datetime.datetime.now() 
    #for file in existing_cmsswlogs:
    #   if file.startswith('old_'):
    #       try:
    #           file_dt = os.path.getmtime(file)
    #           if (current_dt - file_dt).totalHours > 92:
    #               #delete file if not modified for more than 4 days
    #               os.remove(file)
    #       except:
    #           #maybe permissions were insufficient
    #           pass

    mask = inotify.IN_CREATE # | inotify.IN_CLOSE_WRITE  # cmssw log files
    logger.info("starting CMSSW log collector for "+cmsswlogdir)
    clc = None
    try:

        #starting inotify thread
        clc = CMSSWLogCollector(logger)
        clc.register_inotify_path(cmsswlogdir,mask)
        clc.start_inotify()

    except Exception,e:
        logger.exception("error: "+str(e))
        sys.exit(1)

    while terminate == False:
        threadEvent.wait(5)

    logger.info("Closing notifier")
    if clc is not None:
        clc.stop_inotify()

    logger.info("Quit")
    sys.exit(0)

