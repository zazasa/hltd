#!/bin/env python

import sys,traceback
import os
import time
import shutil

import filecmp
import pyinotify
import threading
import Queue
import json
import logging
import hltdconf

import itertools as itools

import signal


UNKNOWN,STREAM,INDEX,FAST,SLOW,OUTPUT,INI,EOLS,EOR,DAT,CRASH = range(11)            #file types :

ES_DIR_NAME = "TEMP_ES_DIRECTORY"
TO_ELASTICIZE = [STREAM,INDEX,OUTPUT,EOR]

#Output redirection class
class stdOutLog:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)    
    def write(self, message):
        self.logger.debug(message)
class stdErrorLog:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    def write(self, message):
        self.logger.error(message)

    #on notify, put the event file in a queue
class MonitorRanger(pyinotify.ProcessEvent):

    def __init__(self):
        super(MonitorRanger, self).__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.eventQueue = False

    def process_default(self, event):
        self.logger.debug("event: %s on: %s" %(event.maskname,event.pathname))
        if self.eventQueue:
            self.eventQueue.put(event)

    def setEventQueue(self,queue):
        self.eventQueue = queue

class fileHandler(object):
    def __eq__(self,other):
        return self.filepath == other.filepath

    def __getattr__(self,name):
        if name not in self.__dict__: 
            if name in ["dir","ext","basename","name"]: self.getFileInfo() 
            elif name in ["fileType"]: self.fileType = self.getFileType();
            elif name in ["run","ls","stream","index","pid"]: self.getFileHeaders()
            elif name in ["data"]: self.data = self.getJsonData(); 
            elif name in ["jsdfile","definitions"]: self.getDefinitions()
            elif name in ["host"]: self.host = os.uname()[1];
            elif name in ["outfilepath"]: self.calcOutfilepath()
        return self.__dict__[name]

    def __init__(self,filepath,runNumber,outputDir):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.filepath = filepath
        self.run = runNumber
        self.outputDir = outputDir
        
    def getFileInfo(self):
        self.dir = os.path.dirname(self.filepath)
        self.basename = os.path.basename(self.filepath)
        self.name,self.ext = os.path.splitext(self.basename)

    def getFileType(self,filepath = None):
        if not filepath: filepath = self.filepath
        filename = self.basename
        name,ext = self.name,self.ext
        name = name.upper()
        if "mon" not in filepath:
            if ext == ".dat" and "PID" not in name: return DAT
            if ext == ".ini" and "PID" in name: return INI
            if ext == ".jsn":
                if "STREAM" in name and "PID" in name: return STREAM
                if "STREAM" in name and "PID" not in name: return OUTPUT
                elif "INDEX" in name and  "PID" in name: return INDEX
                elif "CRASH" in name and "PID" in name: return CRASH
                elif "EOLS" in name: return EOLS
                elif "EOR" in name: return EOR
        if ".fast" in filename: return FAST
        if "slow" in filename: return SLOW
        return UNKNOWN


    def getFileHeaders(self):
        fileType = self.fileType
        name,ext = self.name,self.ext
        splitname = name.split("_")
        if fileType in [STREAM,OUTPUT,DAT,CRASH]: self.run,self.ls,self.stream,self.pid = splitname
        elif fileType == INDEX: self.run,self.ls,self.index,self.pid = splitname
        elif fileType == EOLS: self.ls = "ls"+splitname[1]
        elif fileType == INI: self.run,self.stream,self.pid = splitname
        else: 
            self.logger.warning("Bad filetype: %s" %self.filepath)
            self.run,self.ls,self.stream = [None]*3


        #get data from json file
    def getJsonData(self,filepath = None):
        if not filepath: filepath = self.filepath
        try:
            with open(filepath) as fi:
                data = json.load(fi)
        except StandardError,e:
            self.logger.error(e)
            data = {}
        return data

        #get definitions from jsd file
    def getDefinitions(self):
        data = self.data
        if data: 
            self.jsdfile = self.data["definition"]
            self.definitions = self.getJsonData(self.jsdfile)["legend"]
        else:
            self.jsdfile,self.definitions = "",{}


    def deleteFile(self):
        filepath = self.filepath
        self.logger.info(filepath)
        if os.path.isfile(filepath):
            try:
                self.esCopy()
                os.remove(filepath)
            except Exception,e:
                self.logger.error(e)
                return False
        return True

    def moveFile(self,newpath = None,copy = False):
        oldpath = self.filepath
        if not os.path.exists(oldpath): return False

        run = self.run
        filename = self.basename
        runDir = os.path.join(self.outputDir,run)
        if not newpath: newpath = os.path.join(runDir,filename)

        self.logger.info("%s -> %s" %(oldpath,newpath))
        try:
            if not os.path.isdir(runDir): os.makedirs(runDir)
            if copy: shutil.copy(oldpath,newpath)
            else: 
                self.esCopy()
                shutil.move(oldpath,newpath)
        except OSError,e:
            self.logger(e)
            return False
        self.filepath = newpath
        self.getFileInfo()
        return True   

        #generate the name of the output file
    def calcOutfilepath(self):
        filename = "_".join([self.run,self.ls,self.stream,self.host])+".jsn"
        filename = os.path.join(self.dir,filename)
        self.outfilepath = filename
        return True

    def getOutfile(self):
        return self.__class__(self.outfilepath,self.run,self.outputDir)

    def exists(self):
        return os.path.exists(self.filepath)

        #write self.outputData in json self.outputFile
    def writeout(self):
        filepath = self.filepath
        outputData = self.data
        self.logger.info(filepath)
        try:
            with open(filepath,"w") as fi: 
                json.dump(outputData,fi)
        except Exception,e:
            self.logger.error(e)
            return False
        return True

    def esCopy(self):
        if self.fileType in TO_ELASTICIZE:
            esDir = os.path.join(self.dir,ES_DIR_NAME)
            self.logger.debug(esDir)
            if os.path.isdir(esDir):
                newpath = os.path.join(esDir,self.basename)
                shutil.copy(self.filepath,newpath)


class LumiSectionRanger():
    stoprequest = threading.Event()
    emptyQueue = threading.Event()
    source = False
    eventType = False
    infile = False
    runNumber = None
    outputDir = None

    def __init__(self,runNumber,tempDir,outputDir):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("runNumber: %s, tempfolder: %s, outputfolder: %s" %(runNumber,tempDir,outputDir))

        self.host = os.uname()[1]        
        self.outputDir = outputDir
        self.tempDir = tempDir
        self.runNumber = runNumber

        self.LSHandlerList = {} # {(run,ls): LumiSectionHandler()}
        self.activeStreams = [] # updated by the ini files

    def join(self, stop=False, timeout=None):
        if stop: self.stop()
        super(LumiSectionRanger, self).join(timeout)

        #remove for threading
    def start(self):
        self.run()

    def stop(self):
        self.stoprequest.set()

    def setSource(self,source):
        self.source = source

    def run(self):
        self.logger.info("Start main loop") 
        while not self.stoprequest.isSet() or not self.emptyQueue.isSet() :
            if self.source:
                try:
                    event = self.source.get(True,0.5) #blocking with timeout
                    self.eventType = event.maskname
                    self.infile = fileHandler(event.pathname,self.runNumber,self.outputDir)
                    self.emptyQueue.clear()
                    self.process()  
                except (KeyboardInterrupt,Queue.Empty) as e:
                    self.emptyQueue.set() 
            else:
                time.sleep(0.5)
        self.logger.info("Stop main loop")


        #send the fileEvent to the proper LShandlerand remove closed LSs, or process INI and EOR files
    def process(self):
        self.logger.debug("RECEIVED FILE: %s " %(self.infile.basename))
        self.logger.debug("LSHandlerList %s" %repr(self.LSHandlerList))

        fileType = self.infile.fileType
        eventType = self.eventType

        if eventType == "IN_CLOSE_WRITE":
            if fileType in [STREAM,INDEX,EOLS,DAT]:
                run,ls = (self.infile.run,self.infile.ls)
                key = (run,ls)
                if key not in self.LSHandlerList:
                    self.LSHandlerList[key] = LumiSectionHandler(run,ls,self.activeStreams)
                self.LSHandlerList[key].processFile(self.infile)
                if self.LSHandlerList[key].closed.isSet():
                    self.LSHandlerList.pop(key,None)
            elif fileType == CRASH:
                self.processCRASHfile()
            elif fileType == INI:
                self.processINIfile()
            elif fileType == EOR:
                self.processEORFile()
    
    def processCRASHfile(self):
        lsList = self.LSHandlerList
        basename = self.infile.basename
        pid = self.infile.pid
        dirname = self.infile.dir
        errcode = self.infile.data["errorCode"]

        self.logger.info("%r with errcode: %r" %(basename,errcode))

        #find ls with hung process       
        hungKeyList = filter(lambda x: lsList[x].checkHungPid(pid),lsList.keys())

        self.logger.info(hungKeyList)
        self.logger.info(lsList.keys())


        #merge ouput data for each key found
        for key in hungKeyList:
            eventsNum = lsList[key].pidEvents(pid)
            run,ls = key

            errFilename = "_".join([run,ls,"error"])+".jsn"
            errFilepath = os.path.join(dirname,errFilename)
            outfile = fileHandler(errFilepath,run,dirname)
            definitions = [ { "name":"notProcessed",  "operation":"sum",  "type":"integer"},
                            { "name":"errorCodes",    "operation":"cat",  "type":"string" }]

            newData = [str(eventsNum),str(errcode)]
            oldData = outfile.data["data"][:] if outfile.exists() else [None]

            result=Aggregator(definitions,newData,oldData).output()
            outfile.data = {"data":result}
            outfile.writeout()


    def processINIfile(self):
            #get file information
        self.logger.info(self.infile.basename)
        filepath = self.infile.filepath
        path = self.infile.dir
        basename = self.infile.basename
        name,ext = self.infile.name,self.infile.ext
        run = self.infile.run
        stream = self.infile.stream
            #calc generic local ini path
        localfilename = "_".join([run,stream,self.host])+ext
        localfilepath = os.path.join(path,localfilename)
            #check and move/delete ini file
        if not os.path.exists(localfilepath):
            if stream not in self.activeStreams: self.activeStreams.append(stream)
            self.infile.moveFile(newpath = localfilepath)
            self.infile.moveFile(copy = True)
        else:
            self.logger.debug("compare %s , %s " %(localfilepath,filepath))
            if not filecmp.cmp(localfilepath,filepath,False):
                        # Where shall this exception be handled?
                self.logger.warning("Found a bad ini file %s" %filepath)
            else:
                self.infile.deleteFile()

    def processEORFile(self):
        self.logger.info("CLOSING RUN")
        self.checkClosure()
        if self.infile.deleteFile():
            self.stop()

    def checkClosure(self):
        for key in self.LSHandlerList.keys():
            if not self.LSHandlerList[key].closed.isSet():
                self.logger.warning("%r not closed " %repr(key))



class LumiSectionHandler():
    def __init__(self,run,ls,activeStreams):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.run = run
        self.ls = ls
        self.activeStreams = activeStreams
        self.closedStreams = []


        self.totalEvent = 0

        self.outFileList = {}       # {"filepath":eventCounter}
        self.datFileList = []
        self.indexFileList = []
        self.pidList = {}           # {"pid":{"numEvents":num,"streamList":[streamA,streamB]}


        self.EOLS = threading.Event()
        self.closed = threading.Event() #True if all files are closed/moved
        self.logger.info("%r with %r" %(self.ls,self.activeStreams))


    def processFile(self,infile):
        self.infile = infile
        fileType = self.infile.fileType

        self.logger.info("START PROCESS FILE: %s filetype: %s" %(self.infile.basename,fileType))

        if fileType == STREAM: self.processStreamFile()
        elif fileType == INDEX: self.processIndexFile()
        elif fileType == EOLS: self.processEOLSFile()
        elif fileType == DAT: self.processDATFile()

        self.logger.info(self.pidList)


    def processStreamFile(self):
        self.logger.info(self.infile.basename)
        ls = self.infile.ls
        pid = self.infile.pid
        stream = self.infile.stream
        if self.closed.isSet(): self.closed.clear()
        if self.getData():
            #update pidlist
            if stream not in self.pidList[pid]["streamList"]: self.pidList[pid]["streamList"].append(stream)

            #merging and update counters
            if self.merge():
                if self.outfile.basename in self.outFileList:
                    self.outFileList[self.outfile.basename] += self.counterValue
                else: 
                    self.outFileList[self.outfile.basename] = self.counterValue

                self.logger.info("ls: %s - events %s / %s " %(ls,self.outFileList[self.outfile.basename],self.totalEvent))
                self.infile.deleteFile()
                self.closeStream()
                return True
        return False

        #get info from Stream type json file
    def getData(self):
        data = self.infile.data.copy()
        if data:
            self.buffer =  data["data"]
            self.counterValue = int(self.buffer[0])
            return True
        return False

    def merge(self):
        definitions = self.infile.definitions
        if not definitions: return False
        self.outfile = self.infile.getOutfile()

        host = self.infile.host
        jsdfile = self.infile.jsdfile
        outfile = self.outfile

        newData = self.buffer
        oldData = outfile.data["data"][:] if outfile.exists() else [None]

        result=Aggregator(definitions,newData,oldData).output()

        document = {}
        document["definition"] = jsdfile
        document["data"] = result
        document["source"] = host
        outfile.data = document
        return outfile.writeout()


    def closeStream(self):
        basename = self.outfile.basename
        stream = self.outfile.stream

        if self.EOLS.isSet() and self.outFileList[basename] == self.totalEvent:
            self.logger.info("%r for ls %r" %(stream,self.ls))
                #move output file in rundir
            if self.outfile.moveFile():
                self.outFileList.pop(basename,None)
                self.closedStreams.append(stream)
                #move all dat files in rundir
            for item in self.datFileList:
                if item.stream == stream: item.moveFile()
                
            if not self.outFileList and sorted(self.closedStreams) == sorted(self.activeStreams):
                #delete all index files
                for item in self.indexFileList:
                    item.deleteFile()
                #close lumisection if all streams are closed
                self.closed.set()


    def processDATFile(self):
        stream = self.infile.stream
        if self.infile not in self.datFileList:
            self.datFileList.append(self.infile)


    def processIndexFile(self):
        self.logger.info(self.infile.basename)
        if self.getData():

            #update pidlist
            pid = self.infile.pid
            if pid not in self.pidList: self.pidList[pid] = {"numEvents": 0, "streamList": []}
            self.pidList[pid]["numEvents"]+=self.counterValue

            #update counters and indexfilelist
            self.totalEvent+=self.counterValue
            if self.infile not in self.indexFileList:
                self.indexFileList.append(self.infile)
            return True
        return False

    def processEOLSFile(self):
        self.logger.info(self.infile.basename)
        ls = self.infile.ls
        if self.EOLS.isSet():
            self.logger.warning("LS %s already closed" %repr(ls))
            return False
        self.EOLS.set()
        #self.infile.deleteFile()   #cmsRUN create another eols if it will be delete too early
        return True 


        # return TRUE if the streamList of the pid doesnt present all activeStreams
    def checkHungPid(self,pid):
        self.logger.info("%r in activeStreams %r" %(pid,self.activeStreams))
        if pid in self.pidList: return not sorted(self.pidList[pid]["streamList"]) == sorted(self.activeStreams)
        else: return False

    def pidEvents(self,pid):
        return self.pidList[pid]["numEvents"]




class Aggregator(object):
    def __init__(self,definitions,newData,oldData):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.definitions = definitions
        self.newData = newData
        self.oldData = oldData

    def output(self):
        self.result = map(self.action,self.definitions,self.newData,self.oldData)
        return self.result

    def action(self,definition,data1,data2=None):
        actionName = "action_"+definition["operation"]        
        try:
            if data2 : 
                return getattr(self,actionName)(data1,data2)
            else: 
                return getattr(self,actionName)(data1)
        except AttributeError,e:
            self.logger.error(e)
            return None

    def action_sum(self,data1,data2 = 0):
        try:
            res =  int(data1) + int(data2)
        except TypeError,e:
            self.logger.error(e)
            res = 0
        return str(res)

    def action_same(self,data1,data2 = None):
        if not data2: data2 = data1
        if str(data1) == str(data2):
            return str(data1)
        else:
            return "N/A"
        
    def action_cat(self,data1,data2 = ""):
        return str(data1)+","+str(data2)


if __name__ == "__main__":
    logging.basicConfig(filename="/tmp/anelastic.log",
                    level=logging.INFO,
                    format='%(levelname)s:%(asctime)s-%(name)s.%(funcName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))

    #STDOUT AND ERR REDIRECTIONS
    sys.stderr = stdErrorLog()
    sys.stdout = stdOutLog()

    eventQueue = Queue.Queue()
    conf=hltdconf.hltdConf('/etc/hltd.conf')
    dirname = sys.argv[1]
    dirname = os.path.basename(os.path.normpath(dirname))
    watchDir = os.path.join(conf.watch_directory,dirname)
    outputDir = conf.micromerge_output

    mask = pyinotify.IN_CLOSE_WRITE   # watched events
    logger.info("starting anelastic for "+dirname)
    try:
        #starting inotify thread
        wm = pyinotify.WatchManager()
        mr = MonitorRanger()
        mr.setEventQueue(eventQueue)
        notifier = pyinotify.ThreadedNotifier(wm, mr)
        notifier.start()
        wdd = wm.add_watch(watchDir, mask, rec=False)


        #starting lsRanger thread
        ls = LumiSectionRanger(dirname,watchDir,outputDir)
        ls.setSource(eventQueue)
        ls.start()
    except Exception,e:
        logger.exception("error: ")
        sys.exit(1)

    

#    while not ls.stoprequest.isSet():  
#        try:
#            time.sleep(0.5)
#        except KeyboardInterrupt:
#            logging.info("Closing LumiSectionRanger")
#            ls.join(True,0.5)
#            break
#    ls.join()

    logging.info("Closing notifier")
    notifier.stop()

    logging.info("Quit")
    sys.exit(0)


    

    