#!/bin/env python

import sys
import os
import time
import shutil
import json

import pyinotify
import threading

import elasticBand
import hltdconf
import collate
import logging

logging.basicConfig(filename='/tmp/elastic2.log',
                    level=logging.ERROR,
                    format='%(levelname)s:%(asctime)s-%(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('pyelasticsearch').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.ERROR)

lslogger = logging.getLogger('LumiSectionRanger')
mologger = logging.getLogger('MonitorRanger')
mainlogger = logging.getLogger('elastic')
lslogger.setLevel(logging.DEBUG)
mologger.setLevel(logging.DEBUG)
mainlogger.setLevel(logging.DEBUG)

es = None

conf=hltdconf.hltdConf('/etc/hltd.conf')
es_url = 'http://localhost:9200'

class LumiSectionRanger(threading.Thread):
    def __init__(self,es_url,run_index,ls):
        threading.Thread.__init__(self)
        self.coll = collate.Collation(es_url)
        self.target = 0
        self.index = run_index
        self.ls=ls

    def filestem(self,stream):
        return self.index+'_ls'+str(self.ls).zfill(4)+'_'+stream+'_'+os.uname()[1]
    def inpath(self):
        return conf.watch_directory+'/'+self.index+'/'
    
    def outpath(self):
        return conf.micromerge_output+'/'+self.index+'/'

    def writeout(self,res,stream):
        stem = self.filestem(stream)
        outname = stem+'.dat'
        fp = open(self.inpath()+stem+'.jsn','w')
        document = {}
        document['data'] = [res['data']['in'],res['data']['out'],outname]
        document['definition'] = ""
        document['source'] = os.uname()[1]
        document['infiles'] = res['data']['file']
        lslogger.info(self.index+" Writing document "+self.inpath()+stem+'.jsn')
        json.dump(document,fp)
        fp.close()

    def filemover(self,stream):
        #do the ini file if it isn't already there
        
        if not os.path.exists(self.outpath()+stream+'.ini'): 
            infile=self.inpath()+stream+'.ini'
            outfile=self.outpath()+stream+'.ini'
            if os.path.exists(self.inpath()+stream+'.ini'): 
                if not os.path.exists(self.outpath()):
                    os.makedirs(self.outpath())
                shutil.move(infile,outfile)
        #check once more that files are there
        datafile = self.inpath()+self.filestem(stream)+'.dat'
        jsonfile = self.inpath()+self.filestem(stream)+'.jsn'
        if not os.path.exists(datafile): return False
        if not os.path.exists(jsonfile): return False
        datadest = self.outpath()+self.filestem(stream)+'.dat'
        jsondest = self.outpath()+self.filestem(stream)+'.jsn'
        if not os.path.exists(self.outpath()):
            os.makedirs(self.outpath())
            
        shutil.move(datafile,datadest)
        shutil.move(jsonfile,jsondest)
        return True

    def run(self):
        #find all streams
        complete = False
        prev_in = 0
        iterations = 0
        total_out = {}
        complete_streams = []
        try:
            while not complete:
                if iterations%4==0:
                    self.coll.refresh(self.index)
                iterations+=1
                lslogger.info(self.index+" ls "+str(self.ls)+" running ")
                time.sleep(2.)
                cres = self.coll.collate(self.index,'prc-in',self.ls)
                lslogger.info("collate of prc-in for ls "+str(self.ls)+" returned "+str(cres))
                total_in = cres['data']['out']
                
                if prev_in != total_in:
                    lslogger.info(self.index+" "+str(self.ls)+'/still updating...'+str(total_in)+
                              '/'+str(prev_in)+" iterations:",str(iterations))
                    prev_in = total_in
                else:
                    if total_in == None: break;
                    complete = True

                    res = self.coll.search(self.index,'prc-out',self.ls)
                    lslogger.info("search for prc-out for ls "+str(self.ls)+" returned "+str(res))
                    streams = list(set([res['hits']['hits'][i]['_source']['stream'] for i in range(len(res['hits']['hits']))]))
                    lslogger.info(self.index+" ls "+str(self.ls)+" streams found "+
                                  str(streams)+" iterations "+str(iterations))
                    if len(streams) != 0:
                        for x in streams:
                            if x not in complete_streams:
                                res = self.coll.collate(self.index,'prc-out',self.ls,x)
                                lslogger.info("collate of prc-out for ls "+str(self.ls)+" returned "+str(res))
                                total_out[x] = res['data']['in']
                                lslogger.info(self.index+" "+str(self.ls)+'/'+x+
                                          "********totals "+ str(total_out[x]) + ","+str(total_in))
                                if total_out[x] == total_in:
                                    lslogger.info(self.index+" "+str(self.ls)+" going to write for "+x )
                                    self.writeout(res,x)
                                    self.coll.stash(self.index,'fu-out',res)
                                    self.filemover(x)
                                    complete_streams.append(x)
                                else:
                                    complete = False
                    else:
                        complete = False

                    if iterations > 60: 
                        lslogger.error(self.index+" lumisection "+str(self.ls)+" timed out")
                        break

        except Exception as ex:
            lslogger.exception(ex)
        lslogger.info("Collation of run "+self.index+" ls "+ str(self.ls)+" completed")

class MonitorRanger(pyinotify.ProcessEvent):
    
    def __init__(self,s,dirname):
        pyinotify.ProcessEvent.__init__(self,s)
#        print 'MonitorRanger constructor'
        self.thread_history = {}
        self.dirname = dirname

    def process_IN_MOVED_TO(self, event):
        if 'open' in event.pathname:
            return
        if '.dat' in event.pathname:
            return

#        print 'MonitorRanger-MOVEDTO: event '+event.pathname
        if es:
            if 'open' in event.pathname: 
                return
            if '.dat' in event.pathname:
                return
            try:
                path=event.pathname[0:event.pathname.rfind("/")+1]
                name=event.pathname[event.pathname.rfind("/")+1:]
                if '.jsn' in name and 'stream' not in name:
                    mologger.info(name+" going into prc-in")
                    es.elasticize_prc_in(path,name)
                elif ('stream' in name) and not ('pid' in name):
                    es.elasticize_fu_out(path,name)
                
            except Exception as ex:
                mologger.exception(ex)

    def process_IN_CLOSE_WRITE(self, event):

        if 'open' in event.pathname:
            return
        if '.dat' in event.pathname:
            return
        if 'complete' in event.pathname:
            mologger.info("exiting because run "+self.dirname+" is complete")
            sys.exit(0) 
#        print 'MonitorRanger-CLOSE_WRITE: event '+event.pathname
        if es:

            try:
                path=event.pathname[0:event.pathname.rfind("/")+1]
                name=event.pathname[event.pathname.rfind("/")+1:]
                if '.fast' in name:
                    es.elasticize_prc_istate(path,name)
                elif 'slow' in name:
                    es.elasticize_prc_sstate(path,name) 
                if '.jsn' in name and 'mon' not in path:
                    if 'stream' not in name and 'EoLS' not in name :
                        mologger.info(name+" going into prc-in")
                        es.elasticize_prc_in(path,name)
                    elif 'EoLS' in name:
                        mologger.info(self.dirname+" "+name+" seen")
                        ls = int(name.split('.')[0][name.split('.')[0].rfind('_')+1:])
                        if ls not in self.thread_history:
                            self.thread_history[ls] = LumiSectionRanger(es_url,
                                                                        dirname,
                                                                        ls)
                            self.thread_history[ls].start()

                    elif ('stream' in name) and ('pid' in name):
                        mologger.info(name+" going into prc-out")
                        ls = es.elasticize_prc_out(path,name)
            except Exception as ex:
                mologger.exception(ex)
                mologger.error("when processing event "+name)
            
    def process_default(self, event):
#        print 'MonitorRanger: event '+event.pathname+' type '+event.maskname
        filename=event.pathname[event.pathname.rfind("/")+1:]



if __name__ == "__main__":
    dirname = sys.argv[1]
    dirname = dirname[dirname.rfind("/")+1:]
    es = elasticBand.elasticBand('http://localhost:9200',dirname)
    mainlogger.info("starting elastic for "+dirname)
    try:
        wm3 = pyinotify.WatchManager()
        s3 = pyinotify.Stats() # Stats is a subclass of ProcessEvent
        monranger = MonitorRanger(s3,dirname)
        notifier = pyinotify.ThreadedNotifier(wm3, default_proc_fun=monranger)
        notifier.start()
        wm3.add_watch(conf.watch_directory+'/'+dirname,
                           pyinotify.IN_CLOSE_WRITE | pyinotify.IN_MOVED_TO,
                           rec=True,
                           auto_add=True)
    except Exception as ex:
        mainlogger.exception(ex)
        mainlogger.error("when processing files from directory "+dirname)
