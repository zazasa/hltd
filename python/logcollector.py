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
import subprocess

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
line_limit=1000
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
        #line limit
        if len(self.message)>line_limit: return
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
                        self.logger.error('failed to parse message contentent: ' + str(e.message))
                        self.logger.exception(ex)
            try:
                event.decode()
                self.mainQueue.put(event)

            except Exception,ex:
                self.logger.error('failed to parse message contentent: ' + str(e.message))
                self.logger.exception(ex)

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
            self.logger.warn('problem closing parser')
            self.logger.exception(ex)


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
                #clean old log files if size is excessive
                if self.getDirSize(event.fullpath[:event.fullpath.rfind('/')])>33554432: #32G in kbytes
                    self.deleteOldLogs()
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
            self.logger.exception(ex)
            return None,None

 
    def deleteOldLogs(self,maxAgeHours=0):

        existing_cmsswlogs = os.listdir(self.dir)
        current_dt = datetime.datetime.now() 
        for file in existing_cmsswlogs:
           if file.startswith('old_'):
               try:
                   if maxAgeHours>0:
                       file_dt = os.path.getmtime(file)
                       if (current_dt - file_dt).totalHours > maxAgeHours:
                           #delete file
                           os.remove(file)
                   else:
                       os.remove(file)
               except Exception,ex:
                   #maybe permissions were insufficient
                   self.logger.error("could not delete log file")
                   self.logger.exception(ex)
                   pass

    def getDirSize(self,dir):
        try:
            p = subprocess.Popen("du -s " + str(dir), shell=True, stdout=subprocess.PIPE)
            p.wait()
            std_out=p.stdout.read()
            out = std_out.split('\t')[0]
            self.logger.info("size of directory "+str(dir)+" is "+str(out)+ " kB")
            return int(out)
        except Exception,ex:
            self.logger.error("Could not check directory size")
            self.logger.exception(ex)
            return 0


class HLTDLogIndex():

    def __init__(self,es_server_url):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.host = os.uname()[1]

        if 'localhost' in es_server_url:
            nshards = 16
            self.index_name = 'hltdlogs'
        else:
            nshards=1
            index_suffix = conf.elastic_cluster
            if index_suffix.startswith('runindex_'):
                index_suffix=index_suffix[index_suffix.find('_'):]
            elif index_suffix.startswith('runindex'):
                index_suffix='_'+index_suffix[8:]
            self.index_name = 'hltdlogs'+index_suffix
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
                'number_of_shards' : nshards,
                'number_of_replicas' : 1
            }
        }
        self.mapping = {
            'hltdlog' : {
                '_timestamp' : { 
                    'enabled'   : True,
                    'store'     : "yes"
                },
                #'_ttl'       : { 'enabled' : True,
                #              'default' :  '30d'}
                #,
                'properties' : {
                    'host'      : {'type' : 'string'},
                    'type'      : {'type' : 'string',"index" : "not_analyzed"},
                    'severity'  : {'type' : 'string',"index" : "not_analyzed"},
                    'severityVal'  : {'type' : 'integer'},
                    'message'   : {'type' : 'string'},#,"index" : "not_analyzed"},
                    'lexicalId' : {'type' : 'string',"index" : "not_analyzed"},
                    'msgtime' : {'type' : 'date','format':'YYYY-mm-dd HH:mm:ss'},
                 }
            }
        }
        while True:
            if self.abort:break
            try:
                self.logger.info('writing to elastic index '+self.index_name)
                ip_url=getURLwithIP(es_server_url)
                self.es = ElasticSearch(ip_url)
                self.es.create_index(self.index_name, settings={ 'settings': self.settings, 'mappings': self.mapping })
                break
            except ElasticHttpError as ex:
                #this is normally fine as the index gets created somewhere across the cluster
                self.logger.info(ex)
                break
            except ConnectionError as ex:
                #try to reconnect with different IP from DNS load balancing
                self.threadEvent.wait(2)
                continue

    def elasticize_log(self,type,severity,timestamp,msg):
        document= {}
        document['host']=self.host
        document['type']=type
        document['severity']=severityStr[severity]
        document['severityVal']=severity
        document['message']=''
        if len(msg):

            #filter cgi "error" messages
            if "HTTP/1.1\" 200" in msg[0]: return

            for line_index, line in enumerate(msg):
                if line_index==len(msg)-1:
                    document['message']+=line.strip('\n')
                else:
                    document['message']+=line
                
            document['lexicalId']=calculateLexicalId(msg[0])
        else:
            document['lexicalId']=0
        document['msgtime']=timestamp
        try:
            self.es.index(self.index_name,'hltdlog',document)
        except:
            try:
                #retry with new ip adddress in case of a problem
                ip_url=getURLwithIP(self.es_server_url)
                self.es = ElasticSearch(ip_url)
                self.es.index(self.index_name,'hltdlog',document)
            except:
                logger.warning('failed connection attempts to ' + self.es_server_url)
 
class HLTDLogParser(threading.Thread):
    def __init__(self,dir,file,loglevel,esHandler,skipToEnd):
        self.logger = logging.getLogger(self.__class__.__name__)
        threading.Thread.__init__(self)
        self.dir = dir
        self.filename = file
        self.loglevel = loglevel
        self.esHandler = esHandler
        self.abort=False
        self.threadEvent = threading.Event()
        self.skipToEnd=skipToEnd

        self.type=-1
	if 'hltd.log' in file: self.type=0
        if 'anelastic.log' in file: self.type=1
        if 'elastic.log' in file: self.type=2
        if 'elasticbu.log' in file: self.type=3

        #message info
        self.logOpen = False
        self.msglevel = -1
        self.timestamp = None
        self.msg = []

    def parseEntry(self,level,line,openNew=True):
        if self.logOpen:
            #ship previous
            self.esHandler.elasticize_log(self.type,self.msglevel,self.timestamp,self.msg)
            self.logOpen=False

        if openNew:
            begin = line.find(':')+1
            end = line.find(':')+20
            msgbegin = line.find(':')+23
            self.msglevel=level
            self.timestamp = line[begin:end]
            self.msg = [line[msgbegin:]]
            self.logOpen=True

    def stop(self):
        self.abort=True

    def run(self):
        #open file and rewind to the end
        fullpath = os.path.join(self.dir,self.filename)
        startpos = os.stat(fullpath).st_size
        f = open(fullpath)
        if self.skipToEnd:
            f.seek(startpos)
        else:
            startpos=0

        line_counter = 0
        truncatecheck=3
        while self.abort == False:
            buf = f.readlines(readonce)
            buflen = len(buf)
            if buflen>0:
                line_counter+=buflen
                truncatecheck=3
            else:
                if self.abort == False:
                    truncatecheck-=1
                    self.threadEvent.wait(2)
                    if truncatecheck<=0:
                        #close existing message if any
                        self.parseEntry(0,'',False)
                        try:
                            #if number of lines + previous size is > file size, it safe to assume it got truncated
                            if os.stat(fullpath).st_size<line_counter+startpos:
                                #reopen
                                line_counter=0
                                startpos=0
                                f.close()
                                f.open(self.fullpath)
                                self.logger.info('reopened file '+self.filename)
                        except Exception,ex:
                            self.logger.info('problem reopening file')
                            self.logger.exception(ex)
                            pass
                    continue
                else:break

            for  line in buf:
                    if line.startswith('INFO:'):
                        if self.loglevel<2:
                            currentEvent = self.parseEntry(1,line)
                        else:continue
                    if line.startswith('DEBUG:'):
                        if self.loglevel<1:
                            currentEvent = self.parseEntry(0,line)
                        else:continue
                    if line.startswith('WARNING:'):
                        if self.loglevel<3:
                            currentEvent = self.parseEntry(2,line)
                        else:continue
                    if line.startswith('ERROR:'):
                        if self.loglevel<4:
                            currentEvent = self.parseEntry(3,line)
                        else:continue
                    if line.startswith('CRITICAL:'):
                            currentEvent = self.parseEntry(4,line)
                    if line.startswith('Traceback'):
                            if self.logOpen:
                                selg.msg.append(line)

        f.close()



class HLTDLogCollector():

    def __init__(self,dir,files,loglevel):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.dir = dir
        self.files=files
        self.loglevel=loglevel
        self.activeFiles=[]
        self.handlers = []
        self.esurl = conf.elastic_runindex_url
        self.esHandler = HLTDLogIndex(esurl)
        self.firstScan=True
    
    def scanForFiles(self):
        #if found ne
        if len(self.files)==0: return
        found = os.listdir(self.dir)
        for f in found:
            if f.endswith('.log') and f in self.files and f not in self.activeFiles:
                self.logger.info('starting parser... file: '+f)
                #new file found
                self.files.remove(f)
                self.activeFiles.append(f)
                self.handlers.append(HLTDLogParser(self.dir,f,self.loglevel,self.esHandler,self.firstScan))
                self.handlers[-1].start()
        #if file was not found first time, it is assumed to be created in the next iteration
        self.firstScan=False

    def setStop(self):
        for h in self.handlers:h.stop()

    def stop(self):
        for h in self.handlers:h.stop()
        for h in self.handlers:h.join()


def signalHandler(p1,p2):
    global terminate
    global threadEventRef
    terminate = True
    if threadEventRef:
        threadEventRef.set()
    if hlc:hlc.setStop()

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

    hltdloglevel = 1
    try:
        hltdloglevel_name = conf.es_hltd_log_level.upper().strip()
        if hltdloglevel_name == 'DISABLED':
            hltdloglevel = -1
        else:
            hltdloglevel = [i for i,x in enumerate(severityStr) if x == hltdloglevel_name][0]
    except:
        logger.info("No valid es_cmssw_log_level configuration. Quit")
        sys.exit(0)



    threadEvent = threading.Event()
    registerSignal(threadEvent)

    hltdlogdir = '/var/log/hltd'
    hltdlogs = ['hltd.log','anelastic.log','elastic.log','elasticbu.log']
    cmsswlogdir = '/var/log/hltd/pid'

    mask = inotify.IN_CREATE
    logger.info("starting CMSSW log collector for "+cmsswlogdir)
    clc = None

    if cmsswloglevel>=0:
      try:
          #starting inotify thread
          clc = CMSSWLogCollector(cmsswlogdir,cmsswloglevel)
          clc.register_inotify_path(cmsswlogdir,mask)
          clc.start_inotify()
      except Exception,e:
          logger.error('exception starting cmssw log monitor')
          logger.exception(e)
    else:
        logger.info('CMSSW log collection is disabled')

    if hltdloglevel>=0:
      try:
          hlc = HLTDLogCollector(hltdlogdir,hltdlogs,hltdloglevel)

      except Exception,e:
          hlc = None
          logger.error('exception starting hltd log monitor')
          logger.exception(e)
    else:
        logger.info('hltd log collection is disabled')

    if cmsswloglevel or hltdloglevel:
        doEvery=10
        counter=0
        while terminate == False:
            counter+=1
            if hltdloglevel>=0:
                if hlc:
                    hlc.scanForFiles()
                else:
                    #retry connection to central ES if it was unavailable
                    try:
                         if counter%doEvery==0:
                             hlc = HLTDLogCollector(hltdlogdir,hltdlogs,hltdloglevel)
                    except:
                         hlc=None
                         pass

            threadEvent.wait(5)
        if hlc:hlc.stop()


    logger.info("Closing notifier")
    if clc is not None:
        clc.stop_inotify()

    logger.info("Quit")
    sys.exit(0)

