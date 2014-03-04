#!/bin/env python

import sys
import os
import glob
import filecmp
import time
import shutil
import json

import pyinotify
import threading

import elasticBand
import hltdconf
import collate
import logging

from anelastic import *



class BadIniFile(Exception):
    pass


class elasticCollector(LumiSectionRanger):
    
    def __init__(self,runNumber, outputDir):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.run = runNumber
        self.outputDir = outputDir

        self.source = False
        self.completeLS = []
        self.LSfileList = {}    #{(run,ls):[file1,file2,...]}

    def deleteFile(self,filepath):
        pass

    def moveFile(self,oldpath):
        run = self.run
        filename = os.path.basename(oldpath)
        runDir = os.path.join(self.outputDir,run)
        newpath = os.path.join(runDir,filename)
        self.logger.info("moving file from : %s to: %s" %(oldpath,newpath))
        try:
            if not os.path.isdir(runDir): os.makedirs(runDir)
            shutil.move(oldpath,newpath)
        except OSError,e:
            self.logger(e)
            return False
        return True          

    def elasticize(self,filepath,fileType):
        path = os.path.dirname(filepath)
        name = os.path.basename(filepath)
        if es and os.path.isfile(filepath):
            if fileType == "FAST": es.elasticize_prc_istate(path,name)
            elif fileType == "SLOW": es.elasticize_prc_sstate(path,name) 
            elif fileType == "INDEX": 
                self.logger.info(name+" going into prc-in")
                es.elasticize_prc_in(path,name)
            elif fileType == "STREAM"
                self.logger.info(name+" going into prc-out")
                es.elasticize_prc_out(path,name)
            elif fileType == "OUTPUT"
                self.logger.info(name+" going into prc-out")
                es.elasticize_fu_out(path,name)

    def cleanLS(self,key):
        pass

    def process(self):
        self.logger.debug("RECEIVED FILE: %s " %(self.infilepath))
        fileType = self.getFileType(self.infilepath)
        eventType = self.eventType

        if fileType in [STREAM,INDEX]:
            self.elasticize(self.infilepath,fileType)
            run,ls = self.getRUNandLS(filebasename,fileType)
            key = (run,ls)
            if key in self.completeLS:
                self.deleteFile(self.infilepath)
            else:
                self.LSfileList.setDefault(key, list).append(self.infilepath)

        elif fileType in [EOLS] and eventType == "IN_DELETE":
            run,ls = self.getRUNandLS(filebasename,fileType)
            key = (run,ls)
            self.completeLS.append(key)
            self.cleanLS(key)

        elif fileType in [FAST,SLOW]:
            self.elasticize(self.infilepath,fileType)
            self.moveFile(self.infilepath)

        elif fileType in [OUTPUT]:
            run,ls = self.getRUNandLS(filebasename,fileType)
            key = (run,ls)
            if key in self.completeLS:
                self.elasticize(self.infilepath,fileType)
                self.moveFile(self.infilepath)
            else:
                self.LSfileList.setDefault(key, list).append(self.infilepath)                

        elif fileType in [DAT,INI]:
            self.moveFile(self.infilepath)





if __name__ == "__main__":
    #STDOUT AND ERR REDIRECTIONS
    sys.stderr = stdErrorLog()
    sys.stdout = stdOutLog()

    logger = logging.getLogger(__name__)
    #signal.signal(signal.SIGINT, signalHandler)
    
    eventQueue = Queue.Queue()

    conf=hltdconf.hltdConf('/etc/hltd.conf')
    dirname = sys.argv[1]
    dirname = dirname[dirname.rfind("/")+1:]
    watchDir = conf.watch_directory+'/'+dirname
    outputDir = conf.micromerge_output

    es = elasticBand.elasticBand('http://localhost:9200',dirname)

    mask = pyinotify.IN_CLOSE_WRITE | pyinotify.IN_MOVED_TO | pyinotify.IN_DELETE
    logger.info("starting elastic for "+dirname)



    try:
        wm = pyinotify.WatchManager()
        mr = MonitorRanger()
        mr.setEventQueue(eventQueue)
        notifier = pyinotify.ThreadedNotifier(wm, mr)
        notifier.start()
        wdd = wm.add_watch(watchDir, mask, rec=True, auto_add =True)


    except Exception as ex:
        logger.exception(ex)
        logger.error("when processing files from directory "+dirname)
