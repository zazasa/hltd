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


from hltdconf import *
from aUtils import *



class LumiSectionRanger():
    host = os.uname()[1]        
    def __init__(self,tempdir,outdir,run_number):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.stoprequest = threading.Event()
        self.emptyQueue = threading.Event()  
        self.firstStream = threading.Event()  
        self.errIniFile = threading.Event()  
        self.LSHandlerList = {}  # {(run,ls): LumiSectionHandler()}
        self.activeStreams = [] # updated by the ini files
        self.streamCounters = {} # extended by ini files, updated by the lumi handlers
        self.source = None
        self.eventtype = None
        self.infile = None
        self.EOR = None  #EORfile Object
        self.complete = None  #complete file Object
        self.run_number = run_number 
        self.outdir = outdir
        self.tempdir = tempdir
        self.jsdfile = None
        self.buffer = []        # file list before the first stream file



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
        endTimeout=-1
        while not (self.stoprequest.isSet() and self.emptyQueue.isSet() and self.checkClosure()):
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
            #allow timeout in case 'complete' file is received and lumi is not closed
            if self.stoprequest.isSet() and self.emptyQueue.isSet() and self.checkClosure()==False:
                if endTimeout<=-1: endTimeout=10
                if endTimeout==0: break
                endTimeout-=1

        if self.checkClosure()==False:
            self.logger.error('not all lumisections were closed on exit!')

        self.complete.esCopy()
        #generate and move EoR completition file
        self.createOutputEoR()

        self.logger.info("Stop main loop")

    def flushBuffer(self):
        self.firstStream.set()
        for self.infile in self.buffer:
            self.process()

        #send the fileEvent to the proper LShandlerand remove closed LSs, or process INI and EOR files
    
    def process(self):
        
        filetype = self.infile.filetype
        eventtype = self.eventtype

        if eventtype & inotify.IN_CLOSE_WRITE:
            if filetype == JSD and not self.jsdfile: self.jsdfile=self.infile.filepath
            elif filetype == COMPLETE:
                self.processCompleteFile()
            elif filetype == INI: self.processINIfile()
            elif not self.firstStream.isSet():
                self.buffer.append(self.infile)
                if filetype == STREAM: self.flushBuffer()
            elif filetype in [STREAM,INDEX,EOLS,DAT]:
                run,ls = (self.infile.run,self.infile.ls)
                key = (run,ls)
                if key not in self.LSHandlerList and not filetype == EOLS :
                    self.LSHandlerList[key] = LumiSectionHandler(run,ls,self.activeStreams,self.streamCounters,self.tempdir,self.outdir,self.jsdfile)
                if key in self.LSHandlerList:
                    self.LSHandlerList[key].processFile(self.infile)
                    if self.LSHandlerList[key].closed.isSet():
                        self.LSHandlerList.pop(key,None)
            elif filetype == CRASH:
                self.processCRASHfile()
            elif filetype == EOR:
                self.processEORFile()
        elif eventtype & inotify.IN_MOVED_TO:
           if filetype == JSD and not self.jsdfile: self.jsdfile=self.infile.filepath
    
    def processCRASHfile(self):
        #send CRASHfile to every LSHandler
        lsList = self.LSHandlerList
        basename = self.infile.basename
        errCode = self.infile.data["errorCode"]
        self.logger.info("%r with errcode: %r" %(basename,errCode))
        for item in lsList.values():
            item.processFile(self.infile)
    
    def createErrIniFile(self):
        if self.errIniFile.isSet(): return 

        runname = 'run'+self.run_number.zfill(conf.run_number_padding)
        ls = ZEROLS
        stream = STREAMERRORNAME
        ext = ".ini"

        filename = "_".join([runname,ls,stream,self.host])+ext
        filepath = os.path.join(self.outdir,runname,filename)
        infile = fileHandler(filepath)
        infile.data = ""
        infile.writeout(True)
        self.errIniFile.set()

        self.logger.info("created error ini file")



    def processINIfile(self):

        self.logger.info(self.infile.basename)
        infile = self.infile 

        localdir,name,ext,filepath = infile.dir,infile.name,infile.ext,infile.filepath
        run,ls,stream = infile.run,infile.ls,infile.stream

            #calc generic local ini path
        filename = "_".join([run,ls,stream,self.host])+ext
        localfilepath = os.path.join(localdir,filename)
        remotefilepath = os.path.join(self.outdir,run,filename)
            #check and move/delete ini file
        if not os.path.exists(localfilepath):
            if stream not in self.activeStreams:
                self.activeStreams.append(stream)
                self.streamCounters[stream]=0
            self.infile.moveFile(newpath = localfilepath)
            self.infile.moveFile(newpath = remotefilepath,copy = True)
        else:
            self.logger.debug("compare %s , %s " %(localfilepath,filepath))
            if not filecmp.cmp(localfilepath,filepath,False):
                        # Where shall this exception be handled?
                self.logger.warning("Found a bad ini file %s" %filepath)
            else:
                self.infile.deleteFile()

        self.createErrIniFile()



    def processEORFile(self):
        self.logger.info(self.infile.basename)
        self.EOR = self.infile
        self.EOR.esCopy()

    def processCompleteFile(self):
        self.logger.info("received run complete file")
        self.complete = self.infile
        self.stop()

    def checkClosure(self):
        for key in self.LSHandlerList.keys():
            if not self.LSHandlerList[key].closed.isSet():
                return False
        return True

    def createOutputEoR(self):

        #make json and moveFile
        totalCount=-1
        #namePrefix = "/run"+str(self.run_number).zfill(conf.run_number_padding)+"_ls0000_"
        eorname = 'run'+self.run_number.zfill(conf.run_number_padding)+"_ls0000_EoR_"+os.uname()[1]+".jsn"
        runname = 'run'+self.run_number.zfill(conf.run_number_padding)
        srcName = os.path.join(conf.watch_directory,runname,eorname)
        destName = os.path.join(conf.micromerge_output,runname,eorname)
        document = {"data":[str(0)]}

        for stream in self.streamCounters.keys():
            document = {"data":[str(self.streamCounters[stream])]}
            break
        try:
            with open(srcName,"w") as fi:
                json.dump(document,fi)
        except: logging.exception("unable to create %r" %srcName)

        f = fileHandler(srcName)
        f.moveFile(destName)
        self.logger.info('created local EoR file for '+stream)



class LumiSectionHandler():
    host = os.uname()[1]
    def __init__(self,run,ls,activeStreams,streamCounters,tempdir,outdir,jsdfile):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(ls)

        self.activeStreams = activeStreams      
        self.streamCounters = streamCounters
        self.ls = ls
        self.run = run
        self.outdir = outdir
        self.tempdir = tempdir   
        self.jsdfile = jsdfile 
        
        self.outfileList = []
        self.streamErrorFile = ""
        self.datfileList = []
        self.indexfileList = []
        self.pidList = {}           # {"pid":{"numEvents":num,"streamList":[streamA,streamB]}    
        self.EOLS = None               #EOLS file
        self.closed = threading.Event() #True if all files are closed/moved
        self.totalEvent = 0
        self.totalFiles = 0
        
        self.initOutFiles()
        self.initErrFiles()

    def initOutFiles(self):
        activeStreams,run,ls,tempdir = self.activeStreams,self.run,self.ls,self.tempdir
        ext = ".jsn"
        if not os.path.exists(self.jsdfile):
            self.logger.error("JSD file not found %r" %self.jsdfile)
            return False

        for stream in self.activeStreams:
            outfilename = "_".join([run,ls,stream,self.host])+ext
            outfilepath = os.path.join(tempdir,outfilename)
            outfile = fileHandler(outfilepath)
            outfile.setJsdfile(self.jsdfile)
            self.outfileList.append(outfile)

    def initErrFiles(self):
        run,ls,tempdir = self.run,self.ls,self.tempdir
        ext = ".jsn"
        if not os.path.exists(self.jsdfile):
            self.logger.error("JSD file not found %r" %self.jsdfile)
            return False

        stream = STREAMERRORNAME
        errfilename = "_".join([run,ls,stream,self.host])+ext
        errfilepath = os.path.join(tempdir,errfilename)
        errfile = fileHandler(errfilepath)
        errfile.setJsdfile(self.jsdfile)
        self.streamErrorFile = errfile



    def processFile(self,infile):
        self.infile = infile
        filetype = self.infile.filetype

        if filetype == STREAM: self.processStreamFile()
        elif filetype == INDEX: self.processIndexFile()
        elif filetype == EOLS: self.processEOLSFile()
        elif filetype == DAT: self.processDATFile()
        elif filetype == CRASH: self.processCRASHFile()

        self.checkClosure()

    def processStreamFile(self):
        self.logger.info(self.infile.basename)
        
        self.infile.checkSources()
        infile = self.infile
        ls,stream,pid = infile.ls,infile.stream,infile.pid
        outdir = self.outdir

        #if self.closed.isSet(): self.closed.clear()
        if infile.data:
            #update pidlist
            if stream not in self.pidList[pid]["streamList"]: self.pidList[pid]["streamList"].append(stream)

            #update output files
            outfile = next((outfile for outfile in self.outfileList if outfile.stream == stream),False)
            if outfile:
                outfile.merge(infile)
                processed = outfile.getFieldByName("Processed")
                self.logger.info("ls,stream: %r,%r - events %r / %r " %(ls,stream,processed,self.totalEvent))
                infile.esCopy()
                infile.deleteFile()
                return True
        return False

    def processIndexFile(self):
        self.logger.info(self.infile.basename)
        infile = self.infile
        ls,pid = infile.ls,infile.pid

        #if self.closed.isSet(): self.closed.clear()
        if infile.data:
            numEvents = int(infile.data["data"][0])
            self.totalEvent+=numEvents
            self.totalFiles+=1
            
            #update pidlist
            if pid not in self.pidList:
                self.pidList[pid] = {"numEvents": 0, "streamList": [], "indexFileList" : []}
            self.pidList[pid]["numEvents"]+=numEvents
            self.pidList[pid]["indexFileList"].append(infile)

            if infile not in self.indexfileList:
                self.indexfileList.append(infile)
                infile.esCopy()
            return True
        #else: TODO:delete raw file in ramdisk if we receive malformed index (process probably crashed while writing it)
        return False
 
    def processCRASHFile(self):
        #self.logger.info("LS: " + self.ls + " CHECKING ...... ")
        if self.infile.pid not in self.pidList: return True
      
        
        #self.logger.info("LS: "+self.ls+" pid: " + self.infile.pid)
        infile = self.infile
        pid = infile.pid
        data  = infile.data.copy()
        numEvents = self.pidList[pid]["numEvents"]
        errCode = data["errorCode"]

        file2merge = fileHandler(infile.filepath)
        file2merge.setJsdfile(self.jsdfile)
        file2merge.setFieldByName("ErrorEvents",numEvents)
        file2merge.setFieldByName("ReturnCodeMask",errCode)
        
        streamDiff = list(set(self.activeStreams)-set(self.pidList[pid]["streamList"]))
        for outfile in self.outfileList:
            if outfile.stream in streamDiff:
                outfile.merge(file2merge)

        #add crash infos to the streamError output file (only if no streams are merged yet for lumi)
        if len(streamDiff)==len(self.activeStreams):
            inputFileList = [item.name[:item.name.find('_pid')]+".raw" for item in self.pidList[pid]["indexFileList"]]
            inputFileEvents = [int(item.data["data"][0]) for item in self.pidList[pid]["indexFileList"]]
            errorRawFiles=[]
            rawErrorEvents=0
            for index,rawFile in enumerate(inputFileList):
               try:
                 os.stat(os.path.join(rawinputdir,rawFile))
                 errorRawFiles.append(rawFile)
                 rawErrorEvents+=inputFileEvents[index]
               except OSError:
                 self.logger.info('error stream input file '+rawFile+' is gone, possibly already deleted by the process')
                 pass
            file2merge.setFieldByName("ErrorEvents",rawErrorEvents)
            inputFileList = ",".join(errorRawFiles)
            self.logger.info("inputFileList: " + inputFileList)
            file2merge.setFieldByName("InputFiles",inputFileList)
            self.streamErrorFile.merge(file2merge)

    def processDATFile(self):
        self.logger.info(self.infile.basename)
        stream = self.infile.stream
        if self.infile not in self.datfileList:
            self.datfileList.append(self.infile)

    def processEOLSFile(self):
        self.logger.info(self.infile.basename)
        ls = self.infile.ls
#        try:
#            if os.stat(infile.filepath).st_size>0:
#                #self-triggered inotify event, this lumihandler should be deleted
#                self.closed.set()
#                return False
#        except:
#            pass
        if self.EOLS:
            self.logger.warning("LS %s already closed" %repr(ls))
            return False
        self.EOLS = self.infile
        #self.infile.deleteFile()   #cmsRUN create another EOLS if it will be delete too early
        return True 

    def checkClosure(self):
        if not self.EOLS: return False
        outfilelist = self.outfileList[:]
        for outfile in outfilelist:
            stream = outfile.stream
            processed = outfile.getFieldByName("Processed")+outfile.getFieldByName("ErrorEvents")
            if processed == self.totalEvent:
                self.streamCounters[stream]+=processed
                self.logger.info("%r,%r complete" %(self.ls,outfile.stream))

                #create BoLS file in output dir
                bols_file = str(self.run)+"_"+self.ls+"_"+stream+"_BoLS.jsn"
                bols_path =  os.path.join(self.outdir,self.run,bols_file)
                try:
                    open(bols_path,'a').close()
                except:
                    time.sleep(0.1)
                    try:open(bols_path,'a').close()
                    except:
                        self.logger.warning('unable to create BoLS file for ls ', self.ls)
                logger.info("bols file "+ str(bols_path) + " is created in the output")

                #move all dat files in rundir
                datfilelist = self.datfileList[:]
                for datfile in datfilelist:
                    if datfile.stream == stream:
                        newfilepath = os.path.join(self.outdir,datfile.run,datfile.basename)
                        datfile.moveFile(newfilepath)
                        self.datfileList.remove(datfile)

                #move output file in rundir
                newfilepath = os.path.join(self.outdir,outfile.run,outfile.basename)
                outfile.esCopy()
                if outfile.moveFile(newfilepath):
                    self.outfileList.remove(outfile)
 
                
        if not self.outfileList and not self.closed.isSet():
            #self.EOLS.deleteFile()

            #delete all index files
            for item in self.indexfileList:
                item.deleteFile()


            #moving streamError file
            self.logger.info("Writing streamError file ")
            errfile = self.streamErrorFile
            #create BoLS file in output dir
            bols_file = str(self.run)+"_"+self.ls+"_"+errfile.stream+"_BoLS.jsn"
            bols_path =  os.path.join(self.outdir,self.run,bols_file)
            try:
                open(bols_path,'a').close()
            except:
                time.sleep(0.1)
                try:open(bols_path,'a').close()
                except:
                    self.logger.warning('unable to create BoLS file for ls ', self.ls)
            logger.info("bols file "+ str(bols_path) + " is created in the output")


            numErr = errfile.getFieldByName("ErrorEvents") or 0
            total = self.totalEvent
            errfile.setFieldByName("Processed", str(total - numErr) )
            errfile.writeout()
            newfilepath = os.path.join(self.outdir,errfile.run,errfile.basename)
            ##@SM: initially disabled moving error stream json!
            errfile.moveFile(newfilepath)




            #close lumisection if all streams are closed
            self.logger.info("closing %r" %self.ls)
            self.EOLS.esCopy()
            #self.writeLumiInfo()
            self.closed.set()
            #update EOLS file with event processing statistics

    def writeLumiInfo(self):
        #populating EoL information back into empty EoLS file (disabled)
        document = { 'data':[str(self.totalEvent),str(self.totalFiles),str(self.totalEvent)],
                     'definition':'',
                     'source':os.uname()[1] }
        try:
            if os.stat(self.EOLS.filepath).st_size==0:
                with open(self.EOLS.filepath,"w+") as fi:
                    json.dump(document,fi,sort_keys=True)
        except: logging.exception("unable to write to " %self.EOLS.filepath)
             

if __name__ == "__main__":
    logging.basicConfig(filename=os.path.join(conf.log_dir,"anelastic.log"),
                    level=logging.INFO,
                    format='%(levelname)s:%(asctime)s - %(funcName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.basename(__file__))

    #STDOUT AND ERR REDIRECTIONS
    sys.stderr = stdErrorLog()
    sys.stdout = stdOutLog()

    eventQueue = Queue.Queue()
    
    dirname = sys.argv[1]
    run_number = sys.argv[2]
    rawinputdir = sys.argv[3]
    dirname = os.path.basename(os.path.normpath(dirname))
    watchDir = os.path.join(conf.watch_directory,dirname)
    outputDir = conf.micromerge_output

    mask = inotify.IN_CLOSE_WRITE | inotify.IN_MOVED_TO  # watched events
    logger.info("starting anelastic for "+dirname)
    mr = None
    try:

        #starting inotify thread
        mr = MonitorRanger()
        mr.setEventQueue(eventQueue)
        mr.register_inotify_path(watchDir,mask)
        mr.start_inotify()

        #starting lsRanger thread
        ls = LumiSectionRanger(watchDir,outputDir,run_number)
        ls.setSource(eventQueue)
        ls.start()

    except Exception,e:
        logger.exception("error: ")
        sys.exit(1)

    #make temp dir if we are here before elastic.py
    try:
        os.makedirs(os.path.join(watchDir,ES_DIR_NAME))
    except OSError:
        pass


    logging.info("Closing notifier")
    if mr is not None:
        mr.stop_inotify()

    logging.info("Quit")
    sys.exit(0)


    

    
