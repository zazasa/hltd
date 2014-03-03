#!/bin/env python

import sys
import os
import time
import shutil


import pyinotify
import threading
import Queue
import json
import logging
import hltdconf

import itertools as itools


UNKNOWN,STREAM,INDEX,EOLS,EOR = 0,1,2,3,4             #file types 
RUN,LS,STR,PID,DATA = 0,1,2,3,4                      #file infos runnumber,ls number,stream name,pid number, json data


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

class genericException(Exception):
    pass


    #on notify, put the event file in a queue
class MonitorRanger(pyinotify.ProcessEvent):

    def __init__(self):
        super(MonitorRanger, self).__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.fileQueue = False

    def process_IN_CREATE(self, event):
        pass

    def process_IN_CLOSE_WRITE(self, event):
        self.logger.info("filepath: %s" %event.pathname) 
        if self.fileQueue:
            self.fileQueue.put(event.pathname) 

    def process_default(self, event):
        pass

    def setFileQueue(self,queue):
        self.fileQueue = queue


class LumiSectionHandler(object):
    def __init__(self,run,ls,temp_folder,output_folder):
        super(LumiSectionHandler, self).__init__()
        self.logger = logging.getLogger(self.__class__.__name__)

        self.output_folder = output_folder
        self.temp_folder = temp_folder


        self.host = os.uname()[1]
        self.run = run
        self.ls = ls
        self.buffer = {}
        self.totalEvent = 0
        self.infilepath = ""
        self.filename = ""
        self.fileExt = ""
        self.fileType = -1
        self.EOLS = threading.Event()
        self.outputFile = ""
        self.jsdfilePath = ""
        self.jsd = {}
        self.tempdata = {}

        self.outputFileList = {} #{"filepath":eventCounter}

        self.closed = threading.Event() #True if all files are closed/moved
    
        self.istanceDump()

    def istanceDump(self):
        self.logger.debug("self.run: %s" %repr(self.run))
        self.logger.debug("self.ls: %s" %repr(self.ls))
        self.logger.debug("self.outputFile: %s" %repr(self.outputFile))
        self.logger.debug("elf.closed: %s" %repr(self.closed.isSet()))

    def processDump(self):
        if not self.fileType == UNKNOWN:

            self.logger.debug("self.buffer: %s" %repr(self.buffer))
            self.logger.debug("self.outputFileList: %s" %repr(self.outputFileList))
            self.logger.debug("self.counterList: %s" %repr(self.totalEvent))
            self.logger.debug("self.EOLS: %s" %repr(self.EOLS.isSet()))


    def processFile(self,filepath,fileType):

        self.fileType = fileType
        self.infilepath = filepath
        self.filebasename = os.path.basename(self.infilepath)
        self.filename,self.fileExt = os.path.splitext(self.filebasename)

        self.logger.info("START PROCESS FILE: %s filetype: %s" %(self.filebasename,self.fileType))
        self.processDump()

        filetype = self.fileType
        if fileType == STREAM: self.processStreamFile()
        elif fileType == INDEX: self.processIndexFile()
        elif fileType == EOLS: self.processEOLSFile()
        elif fileType == EOR: self.processEORFile()


        self.logger.info("STOP PROCESS FILE: %s filetype: %s" %(self.filebasename,self.fileType))
        self.processDump()


    def processIndexFile(self):
        self.logger.info("%s" %self.filebasename)
        if self.getIndexFileInfo():
            try:
                self.totalEvent+=int(self.counterValue)
            except Exception,e:
                self.logger.error("error: %s" %e)
                return False
        return False 

        #get info from index type json file
    def getIndexFileInfo(self):
        name = self.filename
        splitFile = name.split("_")
        run     = splitFile[0]
        ls      = splitFile[1]
        index   = splitFile[2]
        pid     = splitFile[3]

        if self.getJsonData(self.infilepath):
            self.buffer =  (run,ls,index,pid,self.tempdata)
            self.counterValue = self.tempdata["data"][0]
            return True
        return False


    def processEOLSFile(self):
        self.logger.info("%s" %self.infilepath)
        if self.EOLS.isSet():
            self.logger.error("LS %s already closed" %repr(key))
            return False
        self.EOLS.set()
        return True 


    def processStreamFile(self):
        self.logger.info("%s" %self.infilepath)

        if self.closed.isSet(): self.closed.clear()

        if self.getStreamFileInfo():
            if self.getDefinitions():
                self.calcOutFilePath()

                self.merge()

                if self.writeout():

                    if self.outputFile in self.outputFileList:
                        self.outputFileList[self.outputFile] += self.counterValue
                    else: 
                        self.outputFileList[self.outputFile] = self.counterValue


                    self.closeFile()

                    return True
        return False


        #get info from Stream type json file
    def getStreamFileInfo(self):
        name = self.filename
        splitFile = name.split("_")
        run     = splitFile[0]
        ls      = splitFile[1]
        stream  = splitFile[2]
        pid     = splitFile[3]

        if self.getJsonData(self.infilepath):
            self.buffer =  (run,ls,stream,pid,self.tempdata)
            self.counterValue = int(self.tempdata["data"][0])
            return True
        return False


        #generate the name of the output file
    def calcOutFilePath(self):
        filename = "_".join([self.buffer[RUN],self.buffer[LS],self.buffer[STR],self.host])
        filepath = os.path.join(self.temp_folder,filename+".jsn")
        self.outputFile = filepath

        #get definitions from jsd file
    def getDefinitions(self):
        self.jsdfilePath = self.buffer[DATA]["definition"]
        if self.getJsonData(self.jsdfilePath):
            self.jsd = self.tempdata["legend"]
            return True
        return False

        #get data from json file
    def getJsonData(self,filepath):
        try:
            with open(filepath) as fi:
                self.tempdata = json.load(fi)
        except StandardError,e:
            self.logger.error("error: %s with filepath: %s" %(e,filepath))
            self.tempdata = {}
            return False
        return True

    def merge(self):
        newData = self.buffer[DATA]["data"]
        oldData = [None]
        if os.path.isfile(self.outputFile):
            if self.getJsonData(self.outputFile):
                oldData = self.tempdata["data"]
        self.result=Aggregator(self.jsd,newData,oldData).output()

        document = {}
        document["definition"] = self.jsdfilePath
        document["data"] = self.result
        document["source"] = self.host
        self.outputData = document
        return True

        #write self.outputData in json self.outputFile
    def writeout(self):
        filepath = self.outputFile
        outputData = self.outputData
        try:
            with open(filepath,"w") as fi: 
                json.dump(outputData,fi)
        except Exception,e:
            self.logger.error("error: %s with filepath: %s" %(e,filepath))
            return False
        return True


    def closeFile(self):
        if self.EOLS.isSet() and self.outputFileList[self.outputFile] == self.totalEvent:
            if self.moveFile():
                self.outputFileList.pop(self.outputFile,None)
                if not self.outputFileList:
                    self.closed.set()

    def moveFile(self):
        self.istanceDump()
        run = self.run
        oldpath = self.outputFile
        filename = os.path.basename(self.outputFile)
        runfolder = os.path.join(self.output_folder,run)
        newpath = os.path.join(runfolder,filename)
        self.logger.info("moving file from : %s to: %s" %(oldpath,newpath))
        try:
            if not os.path.isdir(runfolder): os.makedirs(runfolder)
            shutil.move(oldpath,newpath)
        except OSError:
            raise genericException("moveFile error creating folder: %s" %runfolder)
            return False
        return True            


class LumiSectionRanger(threading.Thread):

    def __init__(self,runNumber,temp_folder,output_folder):
        super(LumiSectionRanger, self).__init__()
        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.debug("init with runNumber: %s, tempfolder: %s, outputfolder: %s" %(runNumber,temp_folder,output_folder))

        self.stoprequest = threading.Event()
        self.emptyQueue = threading.Event()

        
        self.output_folder = output_folder
        self.temp_folder = temp_folder
        self.source = False

        self.runNumber = runNumber

        self.LSHandlerList = {} #{(run,ls): LumiSectionHandler()}

    def join(self, stop=False, timeout=None):
        if stop: self.stop()
        super(LumiSectionRanger, self).join(timeout)

    def stop(self):
        self.stoprequest.set()

    def setSource(self,source):
        self.source = source

    def run(self):
        self.logger.info("Start main loop") 
        while not self.stoprequest.isSet() or not self.emptyQueue.isSet() :
            if self.source:
                try:
                    self.infilepath = self.source.get(True,0.5) #blocking with timeout
                    self.emptyQueue.clear()
                    self.process()  
                except Queue.Empty as e:
                    self.emptyQueue.set() 
            else:
                time.sleep(0.5)
        self.logger.info("Stop main loop")


    def process(self):
        self.logger.debug("RECEIVED FILE: %s " %(self.infilepath))
        filebasename = os.path.basename(self.infilepath)
        fileType = self.getFileType(filebasename)
        if fileType in [STREAM,INDEX,EOLS]:
            run,ls = self.getRUNandLS(filebasename,fileType)
            key = (run,ls)
            if key not in self.LSHandlerList:
                self.LSHandlerList[key] = LumiSectionHandler(run,ls,self.temp_folder,self.output_folder)
            self.LSHandlerList[key].processFile(self.infilepath,fileType)
        elif fileType == EOR:
            self.processEORFile()


    def getFileType(self,filebasename):
        name,ext = os.path.splitext(filebasename)
        name = name.upper()
        if ext == ".jsn":
            if "STREAM" in name and "PID" in name: return STREAM
            elif "INDEX" in name and  "PID" in name: return INDEX
            elif "EOLS" in name: return EOLS
            elif "EOR" in name: return EOR
            else: return UNKNOWN

    def getRUNandLS(self,filebasename,fileType):
        name,ext = os.path.splitext(filebasename)
        splitname = name.split("_")
        if fileType in [STREAM,INDEX]:
            run,ls = splitname[0],splitname[1]
        elif fileType == EOLS:
            run,ls = self.runNumber,"ls"+splitname[1]
        return run,ls


    def processEORFile(self):
        self.logger.info("CLOSING RUN")
        self.checkClosure()
        self.stop()
        

    def checkClosure(self):
        for key in self.LSHandlerList.keys():
            if not self.LSHandlerList[key].closed.isSet():
                self.logger.warning("%s not closed " %repr(key))


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
            self.logger.error("error: %s performing action: %s" %(e,actionName))
            return None

    def action_sum(self,data1,data2 = 0):
        try:
            res =  int(data1) + int(data2)
        except TypeError,e:
            self.logger.error("error %s with data1,data2 : %s" %(e,data1,data2))
            return 0
        return res

    def action_same(self,data1,data2 = None):
        if not data2: data2 = data1
        if str(data1) == str(data2):
            return str(data1)
        else:
            return "N/A"
        
    def action_cat(self,data1,data2 = ""):
        return str(data1)+","+str(data2)




logging.basicConfig(filename="/tmp/anelastic.log",
                    level=logging.INFO,
                    format='%(levelname)s:%(asctime)s-%(name)s.%(funcName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


fileQueue = Queue.Queue()

if __name__ == "__main__":

    logger = logging.getLogger(__name__)

    #STDOUT AND ERR REDIRECTIONS
    #sys.stderr = stdErrorLog()
    #sys.stdout = stdOutLog()

    conf=hltdconf.hltdConf('/etc/hltd.conf')
    dirname = sys.argv[1]
    dirname = dirname[dirname.rfind("/")+1:]
    watch_folder = conf.watch_directory+'/'+dirname
    output_folder = conf.micromerge_output

    #watch_folder = "data" #for testing
    #output_folder = "data/output"
    #dirname = "run000002"

    try:
        #starting inotify thread
        mask = pyinotify.IN_CLOSE_WRITE | pyinotify.IN_CREATE   # watched events

        wm = pyinotify.WatchManager()
        mr = MonitorRanger()
        mr.setFileQueue(fileQueue)
        notifier = pyinotify.ThreadedNotifier(wm, mr)
        notifier.start()

        wdd = wm.add_watch(watch_folder, mask, rec=True)


        #starting lsRanger thread
        ls = LumiSectionRanger(dirname,watch_folder,output_folder)
        ls.setSource(fileQueue)
        ls.start()
    except Exception,e:
        logging.error("error: %s" %e)
        sys.exit(1)

    

#    while not ls.stoprequest.isSet():  
#        try:
#            time.sleep(0.5)
#        except KeyboardInterrupt:
#            logging.info("Closing LumiSectionRanger")
#            ls.join(True,0.5)
#            break

    ls.join()

    logging.info("Closing notifier")
    notifier.stop()

    logging.info("Quit")
    sys.exit(0)


    

    