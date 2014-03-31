#!/bin/env python

import sys,traceback
import os

import logging
import _inotify as inotify
import threading
import Queue

import elasticBand

from hltdconf import *
from aUtils import *


class BadIniFile(Exception):
    pass

class elasticCollector():
    stoprequest = threading.Event()
    emptyQueue = threading.Event()
    source = False
    infile = False
    
    def __init__(self, esDir, inMonDir):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.esDirName = esDir
        self.inputMonDir = inMonDir
        self.movedModuleLegend = False
        self.movedPathLegend = False

    def start(self):
        self.run()

    def stop(self):
        self.stoprequest.set()

    def run(self):
        self.logger.info("Start main loop") 
        while not (self.stoprequest.isSet() and self.emptyQueue.isSet()) :
            if self.source:
                try:
                    event = self.source.get(True,0.5) #blocking with timeout
                    self.eventtype = event.mask
                    self.infile = fileHandler(event.fullpath)
                    self.emptyQueue.clear()
                    self.process() 
                except (KeyboardInterrupt,Queue.Empty) as e:
                    self.emptyQueue.set() 
            else:
                time.sleep(0.5)

        es.flushAll()
        self.logger.info("Stop main loop")


    def setSource(self,source):
        self.source = source

    def process(self):
        self.logger.debug("RECEIVED FILE: %s " %(self.infile.basename))
        infile = self.infile
        filetype = infile.filetype
        eventtype = self.eventtype    
        if eventtype & inotify.IN_CLOSE_WRITE:
            if filetype in [FAST,SLOW]:
                self.elasticize()
            elif self.esDirName in infile.dir:
                if filetype in [INDEX,STREAM,OUTPUT]:self.elasticize()
                elif filetype in [EOLS]:self.elasticizeLS()
                elif filetype in [EOR]: 
                    self.stop()
                    self.infile.deleteFile()
            elif filetype in [MODULELEGEND] and self.movedModuleLegend == False:
                try:
                    if not os.path.exists(self.inputMonDir+'/microstatelegend.leg'):
                        self.infile.moveFile(self.inputMonDir+'/microstatelegend.leg')
                except Exception,ex:
                    logger.error(ex)
                    pass
                self.movedModuleLegend = True
            elif filetype in [PATHLEGEND] and self.movedPathLegend == False:
                try:
                    if not os.path.exists(self.inputMonDir+'/pathlegend.leg'):
                        self.infile.moveFile(self.inputMonDir+'/pathlegend.leg')
                except Exception,ex:
                    logger.error(ex)
                    pass
                self.movedPathLegend = True

    def elasticize(self):
        infile = self.infile
        filetype = infile.filetype
        name = infile.name
        if es and os.path.isfile(infile.filepath):
            if filetype == FAST: 
                es.elasticize_prc_istate(infile)
                self.logger.debug(name+" going into prc-istate")
            elif filetype == SLOW: 
                es.elasticize_prc_sstate(infile)      
                self.logger.debug(name+" going into prc-sstate")     
                self.infile.deleteFile()  
            elif filetype == INDEX: 
                self.logger.info(name+" going into prc-in")
                es.elasticize_prc_in(infile)
                self.infile.deleteFile()
            elif filetype == STREAM:
                self.logger.info(name+" going into prc-out")
                es.elasticize_prc_out(infile)
                self.infile.deleteFile()
            elif filetype == OUTPUT:
                self.logger.info(name+" going into fu-out")
                es.elasticize_fu_out(infile)
                self.infile.deleteFile()

    def elasticizeLS(self):
        ls = self.infile.ls
        es.flushLS(ls)
        self.infile.deleteFile()



if __name__ == "__main__":
    logging.basicConfig(filename=os.path.join(conf.log_dir,"elastic.log"),
                    level=logging.INFO,
                    format='%(levelname)s:%(asctime)s - %(funcName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))

    #STDOUT AND ERR REDIRECTIONS
    sys.stderr = stdErrorLog()
    sys.stdout = stdOutLog()


    #signal.signal(signal.SIGINT, signalHandler)
    
    eventQueue = Queue.Queue()

    dirname = sys.argv[1]
    inmondir = sys.argv[2]
    dirname = os.path.basename(os.path.normpath(dirname))
    watchDir = os.path.join(conf.watch_directory,dirname)
    outputDir = conf.micromerge_output
    monDir = os.path.join(watchDir,"mon")
    tempDir = os.path.join(watchDir,ES_DIR_NAME)

    mask = inotify.IN_CLOSE_WRITE | inotify.IN_MOVED_TO
    monMask = inotify.IN_CLOSE_WRITE
    tempMask = inotify.IN_CLOSE_WRITE

    logger.info("starting elastic for "+dirname)

    try:
        os.makedirs(monDir)
    except OSError:
        pass
    try:
        os.makedirs(tempDir)
    except OSError:
        pass

    mr = None
    try:
        #starting inotify thread
        mr = MonitorRanger()
        mr.setEventQueue(eventQueue)
        #mr.register_inotify_path(watchDir,mask)
        mr.register_inotify_path(monDir,monMask)
        mr.register_inotify_path(tempDir,tempMask)
        mr.start_inotify()

        es = elasticBand.elasticBand('http://localhost:9200',dirname)

        #starting elasticCollector thread
        ec = elasticCollector(ES_DIR_NAME,inmondir)
        ec.setSource(eventQueue)
        ec.start()

    except Exception as e:
        logger.exception(e)
        print traceback.format_exc()
        logger.error("when processing files from directory "+dirname)

    logging.info("Closing notifier")
    if mr is not None:
      mr.stop_inotify()

    logging.info("Quit")
    sys.exit(0)
