#!/bin/env python

import sys,traceback
import os
import time
import datetime
#import pytz
import shutil
import signal
import re
import zlib

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
from elasticBand import elasticBand
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
saveHistory = False #experimental
logThreshold = 1 #(INFO)
contextLogThreshold = 0 #(DEBUG)
STRMAX=80
#cmssw date and time: "30-Apr-2014 16:50:32 CEST"
#datetime_fmt = "%d-%b-%Y %H:%M:%S %Z"

hostname = os.uname()[1]

def calculateLexicalId(string):

    pos = string.find('-:')
    strlen = len(string)
    if (pos==-1 and strlen>STRMAX) or pos>STRMAX:
        pos=80
        if strlen<pos:
          pos=strlen
    return zlib.adler32(re.sub("[0-9\+\- ]", "",string[:pos]))

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
        self.document['severityVal']=self.severity

    def decode(self):
        self.fillComon()
        self.document['message']=self.message[0]
        self.document['lexicalId']=calculateLexicalId(self.message[0])

             
class CMSSWLogEventML(CMSSWLogEvent):

    def __init__(self,pid,severity,firstLine):
        CMSSWLogEvent.__init__(self,pid,MLMSG,severity,firstLine)

    def parseSubInfo(self):
        if self.info1.startswith('(NoMod'):
            self.document['module']=self.category
        elif self.info1.startswith('AfterMod'):
            self.document['module']=self.category
        else:
            #module in some cases
            tokens = self.info1.split('@')
            tokens2 = tokens[0].split(':')
            self.document['module'] = tokens2[0]
            if len(tokens2)>1:
                self.document['moduleInstance'] = tokens2[1]
            if len(tokens)>1:
                self.document['moduleCall'] = tokens[1]

    def decode(self):
        CMSSWLogEvent.fillCommon(self)

        #parse various header formats
        headerInfo = filter(None,self.message[0].split(' '))
        self.category =  headerInfo[1].rstrip(':')

        #capture special case MSG-e (Root signal handler piped to ML)
        if self.severity>=ERRORLEVEL:
            while len(headerInfo)>3 and headerInfo[3][:2].isdigit()==False:
                if 'moduleCall' not in self.document.keys():
                    self.document['moduleCall']=headerInfo[3]
                else:
                    self.document['moduleCall']+=headerInfo[3]
                headerInfo.pop(3)

        self.document['category'] = self.category
        self.info1 =  headerInfo[2]

        self.info2 =  headerInfo[6].rstrip(':\n')

        #try to extract module and fwk state information from the inconsistent mess of MessageLogger output
        if self.info2=='pre-events':
            self.parseSubInfo()
        elif self.info2.startswith('Post') or self.info2.startswith('Pre'):
            self.document['fwkState']=self.info2
            if self.info2!=self.info1:
                if self.info1.startswith('PostProcessPath'):
                    self.document['module']=self.category
                else:
                    self.parseSubInfo()
        elif self.info1.startswith('Pre') or self.info1.startswith('Post'):
            self.document['fwkState']=self.info1
            try:
              if headerInfo[6] == 'Run:':
                if len(headerInfo)>=10:
                    if headerInfo[8]=='Lumi:':
                        istr = int(headerInfo[9].rstrip('\n'))
                        self.document['lumi']=int(istr)
                    elif headerInfo[8]=='Event:':
                        istr = int(headerInfo[9].rstrip('\n'))
                        self.document['eventInPrc']=int(istr)
            except:
              pass

        #time parsing
        self.document['msgtime']=headerInfo[3]+' '+headerInfo[4]
        self.document['msgtimezone']=headerInfo[5]

        #message payload processing
        if len(self.message)>1:
            for i in range(1,len(self.message)):
                if i==1:
                    self.document['lexicalId']=calculateLexicalId(self.message[i])
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
        zone = headerInfo[6].rstrip('-\n')
        self.document['msgtime']=headerInfo[4]+' '+headerInfo[5]
        self.document['msgtimezone']=zone
        if len(self.message)>1:
            line2 = filter(None,self.message[1].split(' '))
            self.document['category'] = line2[4].strip('\'')

        procl=2
        foundState=False
        while len(self.message)>procl:
            line3 = filter(None,self.message[procl].strip().split(' '))
            if line3[0].strip().startswith('[') and foundState==False:
                self.document['fwkState'] = line3[-1].rstrip(':\n')
                if self.document['fwkState']=='EventProcessor':
                    self.document['fwkState']+=':'+line3[1]
                foundState=True
                procl+=1
            else:
                break
        procl+=1

        if len(self.message)>procl:
            for i in range(procl,len(self.message)):
                if i==procl:
                    self.document['lexicalId']=calculateLexicalId(self.message[i])
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
        self.document['message']=self.message[0]
        self.document['lexicalId']=calculateLexicalId(self.message[0])
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

        self.historyFIFO = collections.deque(history*[0], history)

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
            self.logger.info('detected termination of the CMSSW process '+str(self.pid)+', finishing.')
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
                elif buf[pos].startswith('Current states'):#FastMonitoringService
                    pass
                elif buf[pos].startswith('%MSG-d'):
                    self.currentEvent = CMSSWLogEventML(self.pid,DEBUGLEVEL,buf[pos])

                elif buf[pos].startswith('%MSG-i'):
                    self.currentEvent = CMSSWLogEventML(self.pid,INFOLEVEL,buf[pos])

                elif buf[pos].startswith('%MSG-w'):
                    self.currentEvent = CMSSWLogEventML(self.pid,WARNINGLEVEL,buf[pos])

                elif buf[pos].startswith('%MSG-e'):
                    self.currentEvent = CMSSWLogEventML(self.pid,ERRORLEVEL,buf[pos])

                elif buf[pos].startswith('%MSG-d'):
                    #should not be present in production
                    self.currentEvent = CMSSWLogEventML(self.pid,DEBUGLEVEL,buf[pos])

                elif buf[pos].startswith('----- Begin Fatal Exception'):
                    self.currentEvent = CMSSWLogEventException(self.pid,buf[pos])

                #signals not caught as exception (and libc assertion)
                elif buf[pos].startswith('There was a crash.') \
                    or buf[pos].startswith('A fatal system signal') \
                    or (buf[pos].startswith('cmsRun:') and  buf[pos].endswith('failed.\n')) \
                    or buf[pos].startswith('Aborted (core dumped)'):

                    #we don't care to catch these:
                    #or buf[pos].startswith('Killed') #9
                    #or buf[pos].startswith('Stack fault'):#16
                    #or buf[pos].startswith('CPU time limit exceeded'):#24
                    #or buf[pos].startswith('A fatal signal') # ?
                    #or buf[pos].startswith('Trace/breakpoint trap (core dumped)') #4
                    #or buf[pos].startswith('Hangup') #1
                    #or buf[pos].startswith('Quit') #3
                    #or buf[pos].startswith('User defined signal 1') #10
                    #or buf[pos].startswith('Terminated') #15
                    #or buf[pos].startswith('Virtual timer expired') #26
                    #or buf[pos].startswith('Profiling timer expired') #27
                    #or buf[pos].startswith('I/O possible') #29
                    #or buf[pos].startswith('Power failure') #30

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
                elif self.currentEvent.type == STACKTRACE:
                   if buf[pos].startswith('Current states')==False:#FastMonitoringService
                       self.currentEvent.append(buf[pos])
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
                    try:
                        e.decode()
                        mainQueue.put(e)
                    except Exception,ex:
                        self.logger.error('failed to parse log, exception: ' + str(ex))
                        self.logger.error('on message content: '+str(e.message))
            try:
                event.decode()
                self.mainQueue.put(event)

            except Exception,ex:
                self.logger.error('failed to parse log, exception: ' + str(ex))
                self.logger.error('on message content: '+str(event.message))

        elif saveHistory and event.severity>=contextLogThreshold:
            self.historyFIFO.append(event)
 
    def stop(self):
        self.abort = True
        self.threadEvent.set()


class CMSSWLogESWriter(threading.Thread):

    def __init__(self,rn):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.queue = Queue.Queue()
        self.parsers = {}
        self.numParsers=0
        self.doStop = False
        self.threadEvent = threading.Event()
        self.rn = rn
        self.abort = False

        #try to create elasticsearch index for run logging
        #if not conf.elastic_cluster:
        #    self.index_name = 'log_run'+str(self.rn).zfill(conf.run_number_padding)
        #else:
        self.index_runstring = 'run'+str(self.rn).zfill(conf.run_number_padding)
        self.index_suffix = conf.elastic_cluster
        self.eb = elasticBand('http://localhost:9200',self.index_runstring,self.index_suffix,0,0)

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
                        self.eb.es.bulk_index(self.eb.indexName,'cmsswlog',documents)
                    except Exception,ex:
                        self.logger.error("es bulk index:"+str(ex))
            elif self.queue.qsize()>0:
                    while self.abort == False:
                        try:
                            evt = self.queue.get(False)
                            try:
                                self.eb.es.index(self.eb.indexName,'cmsswlog',evt.document)
                            except Exception,ex: 
                                self.logger.error("es index:"+str(ex))
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
            self.logger.warn('problem closing parser: '+str(ex))


class CMSSWLogCollector(object):

    def __init__(self,dir,loglevel):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.inotifyWrapper = InotifyWrapper(self,False)
        self.indices = {}
        self.stop = False
        self.dir = dir

        global logThreshold
        logThreshold = loglevel

        #start another queue to fill events into elasticsearch

    def register_inotify_path(self,path,mask):
        self.inotifyWrapper.registerPath(path,mask)

    def start_inotify(self):
        self.inotifyWrapper.start()

    def stop_inotify(self,abort = False):
        self.stop = True
        self.logger.info("MonitorRanger: Stop inotify wrapper")
        self.inotifyWrapper.stop()
        self.logger.info("MonitorRanger: Join inotify wrapper")
        self.inotifyWrapper.join()
        self.logger.info("MonitorRanger: Inotify wrapper returned")
        for rn in self.indices.keys():
                self.indices[rn].stop()

    def process_IN_CREATE(self, event):
        if self.stop: return
        if event.fullpath.startswith('old_') or not event.fullpath.endswith('.log'):
            return
        self.logger.info("new cmssw log file found: "+event.fullpath)
        #find run number and pid

        rn,pid = self.parseFileName(event.fullpath)
        if rn and rn > 0 and pid:
            if rn not in self.indices:
                self.indices[rn] = CMSSWLogESWriter(rn)
                self.indices[rn].start()
                #self.deleteOldLogs()#not deleting for now
            self.indices[rn].addParser(event.fullpath,pid)

        #cleanup
        for rn in self.indices.keys():
                alive = self.indices[rn].clearFinished()
                if alive == 0:
                    self.logger.info('removing old run'+str(rn)+' from the list')
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
            self.logger.warn('problem parsing log file name: '+str(ex))
            return None,None

 
    def deleteOldLogs(self):

        existing_cmsswlogs = os.listdir(self.dir)
        current_dt = datetime.datetime.now() 
        for file in existing_cmsswlogs:
           if file.startswith('old_'):
               try:
                   file_dt = os.path.getmtime(file)
                   if (current_dt - file_dt).totalHours > 48:
                       #delete file if not modified for more than 4 days
                       os.remove(file)
               except:
                   #maybe permissions were insufficient
                   pass



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

    cmsswloglevel = 1
    try:
        cmsswloglevel_name = conf.es_cmssw_log_level.upper().strip()
        if cmsswloglevel_name == 'DISABLED':
            cmsswloglevel = -1
        else:
            cmsswloglevel = [i for i,x in enumerate(severityStr) if x == cmsswloglevel_name][0]
    except:
        logger.info("No valid es_cmssw_log_level configuration. Quit")
        sys.exit(0)

    threadEvent = threading.Event()
    registerSignal(threadEvent)

    #TODO:hltd log parser
    hltdlogdir = '/var/log/hltd'
    hltdlog = 'hltd.log'
    hltdrunlogs = ['hltd.log','anelastic.log','elastic.log','elasticbu.log']
    cmsswlogdir = '/var/log/hltd/pid'

    mask = inotify.IN_CREATE # | inotify.IN_CLOSE_WRITE  # cmssw log files
    logger.info("starting CMSSW log collector for "+cmsswlogdir)
    clc = None

    if cmsswloglevel>=0:
      try:

        #starting inotify thread
        clc = CMSSWLogCollector(cmsswlogdir,cmsswloglevel)
        clc.register_inotify_path(cmsswlogdir,mask)
        clc.start_inotify()

      except Exception,e:
        logger.exception("error: "+str(e))
        sys.exit(1)

    else:
        logger.info('CMSSW logging is disabled')

    while terminate == False:
        threadEvent.wait(5)

    logger.info("Closing notifier")
    if clc is not None:
        clc.stop_inotify()

    logger.info("Quit")
    sys.exit(0)

