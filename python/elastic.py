#!/bin/env python

import sys,traceback
import os

import logging
import _inotify as inotify
import threading
import Queue

import elasticBand
import hltdconf

from anelastic import *
from aUtils import *



class BadIniFile(Exception):
    pass

class elasticCollector():
    stoprequest = threading.Event()
    emptyQueue = threading.Event()
    source = False
    infile = False
    
    def __init__(self, esDir):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.esDirName = esDir

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

        self.logger.info("Stop main loop")


    def setSource(self,source):
        self.source = source

    def process(self):
        self.logger.debug("RECEIVED FILE: %s " %(self.infile.basename))
        filepath = self.infile.filepath
        filetype = self.infile.filetype
        eventtype = self.eventtype
        if eventtype & inotify.IN_CLOSE_WRITE:
            if self.esDirName in self.infile.dir:
                if filetype in [INDEX,STREAM,OUTPUT]:   self.elasticize(filepath,filetype)
                if filetype in [EOR]: self.stop()
                self.infile.deleteFile()
            elif filetype in [FAST,SLOW]:
                self.elasticize(filepath,filetype)


    def elasticize(self,filepath,filetype):
        self.logger.debug(filepath)
        path = os.path.dirname(filepath)
        name = os.path.basename(filepath)
        if es and os.path.isfile(filepath):
            if filetype == FAST: 
                es.elasticize_prc_istate(path,name)
                self.logger.debug(name+" going into prc-istate")
            elif filetype == SLOW: 
                es.elasticize_prc_sstate(path,name)      
                self.logger.debug(name+" going into prc-sstate")       
            elif filetype == INDEX: 
                self.logger.info(name+" going into prc-in")
                es.elasticize_prc_in(path,name)
            elif filetype == STREAM:
                self.logger.info(name+" going into prc-out")
                es.elasticize_prc_out(path,name)
            elif filetype == OUTPUT:
                self.logger.info(name+" going into fu-out")
                es.elasticize_fu_out(path,name)

if __name__ == "__main__":
    logging.basicConfig(filename="/tmp/elastic.log",
                    level=logging.INFO,
                    format='%(levelname)s:%(asctime)s-%(name)s.%(funcName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))

    #STDOUT AND ERR REDIRECTIONS
    sys.stderr = stdErrorLog()
    sys.stdout = stdOutLog()


    #signal.signal(signal.SIGINT, signalHandler)
    
    eventQueue = Queue.Queue()

    conf=hltdconf.hltdConf('/etc/hltd.conf')
    dirname = sys.argv[1]
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
        ec = elasticCollector(ES_DIR_NAME)
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
