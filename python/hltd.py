#!/bin/env python
import os,sys
sys.path.append('/opt/hltd/python')
sys.path.append('/opt/hltd/lib')

import time
from datetime import datetime
import logging
import subprocess
from signal import SIGKILL
from signal import SIGINT
import json
#import SOAPpy
import threading
import fcntl
import CGIHTTPServer
import BaseHTTPServer
import cgitb
import httplib
import demote
import re

#modules distributed with hltd
import prctl

#modules which are part of hltd
from daemon2 import Daemon2
from hltdconf import *
from inotifywrapper import InotifyWrapper
import _inotify as inotify


idles = conf.resource_base+'/idle/'
used = conf.resource_base+'/online/'
broken = conf.resource_base+'/except/'
quarantined = conf.resource_base+'/quarantined/'
nthreads = None
expected_processes = None
run_list=[]
bu_disk_list=[]

logging.basicConfig(filename=os.path.join(conf.log_dir,"hltd.log"),
                    level=conf.service_log_level,
                    format='%(levelname)s:%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

conf.dump()

def preexec_function():
    dem = demote.demote(conf.user)
    dem()
    prctl.set_pdeathsig(SIGKILL)
    #    os.setpgrp()

def cleanup_resources():

    dirlist = os.listdir(broken)
    for cpu in dirlist:
        os.rename(broken+cpu,idles+cpu)
    dirlist = os.listdir(used)
    for cpu in dirlist:
        os.rename(used+cpu,idles+cpu)
    dirlist = os.listdir(quarantined)
    for cpu in dirlist:
        os.rename(quarantined+cpu,idles+cpu)

def cleanup_mountpoints():
    bu_disk_list[:] = []
    if conf.bu_base_dir[0] == '/':
        bu_disk_list[:] = [conf.bu_base_dir]
        return
    try:
        process = subprocess.Popen(['mount'],stdout=subprocess.PIPE)
        out = process.communicate()[0]
        mounts = re.findall('/'+conf.bu_base_dir+'[0-9]+',out)
        logging.info("cleanup_mountpoints: found following mount points ")
        logging.info(mounts)
        for point in mounts:
            logging.error("trying umount of "+point)
            try:
                subprocess.check_call(['umount','/'+point])
            except subprocess.CalledProcessError, err1:
                logging.error("Error calling umount in cleanup_mountpoints")
                logging.error(str(err1.returncode))
            os.rmdir('/'+point)
        i = 0
        if os.path.exists(conf.resource_base+'/bus.config'):
            for line in open(conf.resource_base+'/bus.config'):
                logging.info("found BU to mount at "+line.strip())
                try:
                    os.makedirs('/'+conf.bu_base_dir+str(i))
                except OSError:
                    pass
                if os.system("ping -c 1 "+line.strip())==0:
                    logging.info("trying to mount "+line.strip()+':/ '+'/'+conf.bu_base_dir+str(i))
                    try:
                        subprocess.check_call(
                            [conf.mount_command,
                             '-t',
                             conf.mount_type,
                             '-o',
                             conf.mount_options,
                             line.strip()+':/',
                             '/'+conf.bu_base_dir+str(i)]
                            )
                        bu_disk_list.append('/'+conf.bu_base_dir+str(i))
                    except subprocess.CalledProcessError, err2:
                        logging.error("Error calling mount in cleanup_mountpoints for "+line.strip()+':/',
                             '/'+conf.bu_base_dir+str(i))
                        logging.error(str(err2.returncode))
                else:
                    logging.error("Cannot ping BU "+line.strip())

                i+=1
    except Exception as ex:
        logging.error("Exception in cleanup_mountpoints")
        logging.error(ex)

def calculate_threadnumber():
    global nthreads
    global expected_processes
    idlecount = len(os.listdir(idles))
    if conf.cmssw_threads_autosplit>0:
        nthreads = idlecount/conf.cmssw_threads_autosplit
        if nthreads*conf.cmssw_threads_autosplit != nthreads:
            logging.error("idle cores can not be evenly split to cmssw threads")
    else:
        nthreads = conf.cmssw_threads
    expected_processes = idlecount/nthreads

class system_monitor(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.running = True
        self.hostname = os.uname()[1]
        self.directory = []
        self.file = []
        self.rehash()
        self.threadEvent = threading.Event()

    def rehash(self):
        if conf.role == 'fu':
            self.directory = ['/'+x+'/'+conf.ramdisk_subdirectory+'/appliance/boxes/' for x in bu_disk_list]
        else:
            self.directory = [conf.watch_directory+'/appliance/boxes/']
        self.file = [x+self.hostname for x in self.directory]
        for dir in self.directory:
            try:
                os.makedirs(dir)
            except OSError:
                pass
        logging.info("system_monitor: rehash found the following BU disks")
        for disk in self.file:
            logging.info(disk)

    def run(self):
        try:
            logging.debug('entered system monitor thread ')
            while self.running:
#                logging.info('system monitor - running '+str(self.running))
                self.threadEvent.wait(5)

                tstring = datetime.utcfromtimestamp(time.time()).isoformat()

                fp = None
                for mfile in self.file:
                    if conf.role == 'fu':
                        dirstat = os.statvfs(conf.watch_directory)
                        fp=open(mfile,'w+')
                        fp.write('fm_date='+tstring+'\n')
                        fp.write('idles='+str(len(os.listdir(idles)))+'\n')
                        fp.write('used='+str(len(os.listdir(used)))+'\n')
                        fp.write('broken='+str(len(os.listdir(broken)))+'\n')
                        fp.write('quarantined='+str(len(os.listdir(quarantined)))+'\n')
                        fp.write('usedDataDir='+str((dirstat.f_blocks - dirstat.f_bavail)*dirstat.f_bsize)+'\n')
                        fp.write('totalDataDir='+str(dirstat.f_blocks*dirstat.f_bsize)+'\n')
                        fp.close()
                    if conf.role == 'bu':
                        ramdisk = os.statvfs(conf.watch_directory)
                        outdir = os.statvfs('/fff/output')
                        fp=open(mfile,'w+')

                        fp.write('idles=0')
                        fp.write('used=0')
                        fp.write('broken=0')
                        fp.write('quarantined=0')
                        fp.write('usedRamdisk='+str((ramdisk.f_blocks - ramdisk.f_bavail)*ramdisk.f_bsize)+'\n')
                        fp.write('totalRamdisk='+str(ramdisk.f_blocks*ramdisk.f_bsize)+'\n')
                        fp.write('usedOutput='+str((outdir.f_blocks - outdir.f_bavail)*outdir.f_bsize)+'\n')
                        fp.write('totalOutput='+str(outdir.f_blocks*outdir.f_bsize)+'\n')
                        fp.close()
                        
                if conf.role == 'bu':
                    mfile = conf.resource_base+'/disk.jsn'
                    stat=[]
                    if not os.path.exists(mfile):
                        fp=open(mfile,'w+')
                    else:
                        fp=open(mfile,'r+')
                        stat = json.load(fp)
                    if len(stat)>100:
                        stat[:] = stat[1:]
                    res=os.statvfs(mfile)

                    stat.append([int(time.time()*1000),float(res.f_bfree)/float(res.f_blocks)])
                    fp.seek(0)
                    fp.truncate()
                    json.dump(stat,fp)
                    fp.close()
        except Exception as ex:
            logging.error(ex)

        for mfile in self.file:
            try:
                os.remove(mfile)
            except OSError:
                pass

        logging.debug('exiting system monitor thread ')

    def stop(self):
        logging.debug("system_monitor: request to stop")
        self.running = False
        self.threadEvent.set()

class BUEmu:
    def __init__(self):
        self.process=None
        self.runnumber = None

    def startNewRun(self,nr):
        if self.runnumber:
            logging.error("Another BU emulator run "+str(self.runnumber)+" is already ongoing")
            return
        self.runnumber = nr
        configtouse = conf.test_bu_config
        destination_base = None
        if role == 'fu':
            destination_base = bu_disk_list[startindex%len(bu_disk_list)]+'/'+conf.ramdisk_subdirectory
        else:
            destination_base = conf.watch_directory
            

        new_run_args = [conf.cmssw_script_location+'/startRun.sh',
                        conf.cmssw_base,
                        conf.cmssw_arch,
                        conf.cmssw_default_version,
                        conf.exec_directory,
                        configtouse,
                        str(nr),
                        '/tmp', #input dir is not needed
                        destination_base,
                        '1']
        try:
            self.process = subprocess.Popen(new_run_args,
                                            preexec_fn=preexec_function,
                                            close_fds=True
                                            )
        except Exception as ex:
            logging.error("Error in forking BU emulator process")
            logging.error(ex)

    def stop(self):
        os.kill(self.process.pid,SIGINT)
        self.process.wait()
        self.runnumber=None

bu_emulator=BUEmu()

class OnlineResource:

    def __init__(self,resourcenames,lock):
        self.hoststate = 0 #@@MO what is this used for?
        self.cpu = resourcenames
        self.process = None
        self.processstate = None
        self.watchdog = None
        self.runnumber = None
        self.associateddir = None
        self.lock = lock
        self.retry_attempts = 0;

    def ping(self):
        if conf.role == 'bu':
            if not os.system("ping -c 1 "+self.cpu[0])==0: self.hoststate = 0

    def NotifyNewRun(self,runnumber):
        self.runnumber = runnumber
        logging.info("calling start of run on "+self.cpu[0]);
        connection = httplib.HTTPConnection(self.cpu[0], 8000)
        connection.request("GET",'cgi-bin/start_cgi.py?run='+str(runnumber))
        response = connection.getresponse()
        #do something intelligent with the response code
        logging.error("response was "+str(response.status))
        if response.status > 300: self.hoststate = 1
        else:
            logging.info(response.read())

    def NotifyShutdown(self):
        connection = httplib.HTTPConnection(self.cpu[0], 8000)
        connection.request("GET",'cgi-bin/stop_cgi.py?run='+str(self.runnumber))
        response = connection.getresponse()
#do something intelligent with the response code
        if response.status > 300: self.hoststate = 0

    def StartNewProcess(self ,runnumber, startindex, arch, version, menu,num_threads):
        logging.debug("OnlineResource: StartNewProcess called")
        self.runnumber = runnumber

        """
        this is just a trick to be able to use two
        independent mounts of the BU - it should not be necessary in due course
        IFF it is necessary, it should address "any" number of mounts, not just 2
        """
        input_disk = bu_disk_list[startindex%len(bu_disk_list)]+'/'+conf.ramdisk_subdirectory
        #run_dir = input_disk + '/run' + str(self.runnumber).zfill(conf.run_number_padding)

        logging.info("starting process with "+version+" and run number "+str(runnumber))

        new_run_args = [conf.cmssw_script_location+'/startRun.sh',
                        conf.cmssw_base,
                        arch,
                        version,
                        conf.exec_directory,
                        menu,
                        str(runnumber),
                        input_disk,
                        conf.watch_directory,
                        str(num_threads)]
        logging.info("arg array "+str(new_run_args).translate(None, "'"))
        try:
#            dem = demote.demote(conf.user)
            self.process = subprocess.Popen(new_run_args,
                                            preexec_fn=preexec_function,
                                            close_fds=True
                                            )
            self.processstate = 100
            logging.info("started process "+str(self.process.pid))
#            time.sleep(1.)
            if self.watchdog==None:
                self.watchdog = ProcessWatchdog(self,self.lock)
                self.watchdog.start()
                logging.debug("watchdog thread for "+str(self.process.pid)+" is alive "
                             + str(self.watchdog.is_alive()))
            else:
                self.watchdog.join()
                self.watchdog = ProcessWatchdog(self,self.lock)
                self.watchdog.start()
                logging.debug("watchdog thread restarted for "+str(self.process.pid)+" is alive "
                              + str(self.watchdog.is_alive()))
        except Exception as ex:
            logging.info("OnlineResource: exception encountered in forking hlt slave")
            logging.info(ex)

    def join(self):
        logging.debug('calling join on thread ' +self.watchdog.name)
        self.watchdog.join()

    def disableRestart(self):
        logging.debug("OnlineResource "+str(self.cpu)+" restart is now disabled")
        if self.watchdog:
            self.watchdog.disableRestart()

class ProcessWatchdog(threading.Thread):
    def __init__(self,resource,lock):
        threading.Thread.__init__(self)
        self.resource = resource
        self.lock = lock
        self.retry_limit = conf.process_restart_limit
        self.retry_delay = conf.process_restart_delay_sec
        self.retry_enabled = True
    def run(self):
        try:
            monfile = self.resource.associateddir+'hltd.jsn'
            logging.info('watchdog for process '+str(self.resource.process.pid))
            self.resource.process.wait()
            returncode = self.resource.process.returncode
            pid = self.resource.process.pid

            #update json process monitoring file
            self.resource.processstate=returncode
            logging.debug('ProcessWatchdog: acquire lock thread '+str(pid))
            self.lock.acquire()
            logging.debug('ProcessWatchdog: acquired lock thread '+str(pid))

            try:
                fp=open(monfile,'r+')

                stat=json.load(fp)

                stat=[[x[0],x[1],returncode]
                      if x[0]==self.resource.cpu else [x[0],x[1],x[2]] for x in stat]
                fp.seek(0)
                fp.truncate()
                json.dump(stat,fp)

                fp.flush()
                fp.close()
            except IOError,ex:
                logging.error(str(ex))

            logging.debug('ProcessWatchdog: release lock thread '+str(pid))
            self.lock.release()
            logging.debug('ProcessWatchdog: released lock thread '+str(pid))

            #cleanup actions- remove process from list and
            # attempt restart on same resource

            if returncode < 0:
                logging.error("process "+str(pid)
                              +" for run "+str(self.resource.runnumber)
                              +" on resource(s) " + str(self.resource.cpu)
                              +" exited with signal "
                              +str(returncode)
                              +" restart is enabled ? "
                              +str(self.retry_enabled)
                              )


                #generate crashed pid json file like: run000001_ls0000_crash_pid12345.jsn
                oldpid = "pid"+str(pid).zfill(5)
                outdir = os.path.dirname(self.resource.associateddir[:-1])
                runnumber = "run"+str(self.resource.runnumber).zfill(conf.run_number_padding)
                ls = "ls0000"
                filename = "_".join([runnumber,ls,"crash",oldpid])+".jsn"
                filepath = os.path.join(outdir,filename)
                document = {"errorCode":returncode}
                try: 
                    with open(filepath,"w") as fi: 
                        json.dump(document,fi)
                except: logging.exception("unable to create %r" %filename)
                logging.info("pid crash file: %r" %filename)




                if self.resource.retry_attempts < self.retry_limit:
                    """
                    sleep a configurable amount of seconds before
                    trying a restart. This is to avoid 'crash storms'
                    """
                    time.sleep(self.retry_delay)

                    self.resource.process = None
                    self.resource.retry_attempts += 1

                    logging.info("try to restart process for resource(s) "
                                 +str(self.resource.cpu)
                                 +" attempt "
                                 + str(self.resource.retry_attempts))
                    for cpu in self.resource.cpu:
                      os.rename(used+cpu,broken+cpu)
                    logging.debug("resource(s) " +str(self.resource.cpu)+
                                  " successfully moved to except")
                elif self.resource.retry_attempts >= self.retry_limit:
                    logging.error("process for run "
                                  +str(self.resource.runnumber)
                                  +" on resources " + str(self.resource.cpu)
                                  +" reached max retry limit "
                                  )
                    for cpu in self.resource.cpu:
                        os.rename(used+cpu,quarantined+cpu)

            #successful end= release resource
            elif returncode == 0:

                logging.info('releasing resource, exit0 meaning end of run '+str(self.resource.cpu))
                # generate an end-of-run marker if it isn't already there - it will be picked up by the RunRanger
                endmarker = conf.watch_directory+'/'+conf.watch_end_prefix+str(self.resource.runnumber)
                stoppingmarker = self.resource.associateddir+'/'+Run.STOPPING
                completemarker = self.resource.associateddir+'/'+Run.COMPLETE
                if not os.path.exists(endmarker):
                    fp = open(endmarker,'w+')
                    fp.close()
                # wait until the request to end has been handled
                while not os.path.exists(stoppingmarker):
                    if os.path.exists(completemarker): break
                    time.sleep(.1)
                # move back the resource now that it's safe since the run is marked as ended
                for cpu in self.resource.cpu:
                  os.rename(used+cpu,idles+cpu)
                #self.resource.process=None

            #        logging.info('exiting thread '+str(self.resource.process.pid))

        except Exception as ex:
            logging.info("OnlineResource watchdog: exception")
            logging.exception(ex)
        return

    def disableRestart(self):
        self.retry_enabled = False

class Run:

    STARTING = 'starting'
    ACTIVE = 'active'
    STOPPING = 'stopping'
    ABORTED = 'aborted'
    COMPLETE = 'complete'

    VALID_MARKERS = [STARTING,ACTIVE,STOPPING,COMPLETE,ABORTED]

    def __init__(self,nr,dirname,bu_dir):
        self.runnumber = nr
        self.dirname = dirname
        self.online_resource_list = []
        self.is_active_run = False
        self.anelastic_monitor = None
        self.elastic_monitor = None   
        self.elastic_test = None   
        
        self.arch = None
        self.version = None
        self.menu = None
        self.waitForEndThread = None
        if conf.role == 'fu':
            self.changeMarkerMaybe(Run.STARTING)
        self.menu_directory = bu_dir+'/'+conf.menu_directory
        if os.path.exists(self.menu_directory):
            self.menu = self.menu_directory+'/'+conf.menu_name
            if os.path.exists(self.menu_directory+'/'+conf.arch_file):
                fp = open(self.menu_directory+'/'+conf.arch_file,'r')
                self.arch = fp.readline().strip()
                fp.close()
            if os.path.exists(self.menu_directory+'/'+conf.version_file):
                fp = open(self.menu_directory+'/'+conf.version_file,'r')
                self.version = fp.readline().strip()
                fp.close()
            logging.info("Run "+str(self.runnumber)+" uses "+self.version+" ("+self.arch+") with "+self.menu)
        else:
            self.arch = conf.cmssw_arch
            self.version = conf.cmssw_default_version
            self.menu = conf.test_hlt_config1
            logging.warn("Using default values for run "+str(self.runnumber)+": "+self.version+" ("+self.arch+") with "+self.menu)

        self.rawinputdir = None
        if conf.role == "bu":
            try:
                self.rawinputdir = conf.watch_directory+'/run'+str(self.runnumber).zfill(conf.run_number_padding)
                os.makedirs(mondir+'/mon')
            except Exception, ex:
                logging.error("could not create mon dir inside the run input directory")
        else:
            self.rawinputdir= bu_disk_list[0]+'/'+conf.ramdisk_subdirectory+'/run' + str(self.runnumber).zfill(conf.run_number_padding)

        self.lock = threading.Lock()
        #conf.use_elasticsearch = False
            #note: start elastic.py first!
        if conf.use_elasticsearch:
            try:
                if conf.elastic_bu_test is not None:
                    logging.info("starting elasticbu.py testing mode with arguments:"+self.dirname)
                    elastic_args = ['/opt/hltd/python/elasticbu.py',self.rawinputdir,str(self.runnumber)]
                elif conf.role == "bu":
                    logging.info("starting elasticbu.py with arguments:"+self.dirname)
                    elastic_args = ['/opt/hltd/python/elasticbu.py',self.dirname,str(self.runnumber)]
                else:
                    logging.info("starting elastic.py with arguments:"+self.dirname)
                    elastic_args = ['/opt/hltd/python/elastic.py',self.dirname,self.rawinputdir+'/mon',str(expected_processes),conf.elastic_cluster]

                self.elastic_monitor = subprocess.Popen(elastic_args,
                                                        preexec_fn=preexec_function,
                                                        close_fds=True
                                                        )

            except OSError as ex:
                logging.error("failed to start elasticsearch client")
                logging.error(ex)
        if conf.role == "fu":
            try:
                logging.info("starting anelastic.py with arguments:"+self.dirname)
                elastic_args = ['/opt/hltd/python/anelastic.py',self.dirname]
                self.anelastic_monitor = subprocess.Popen(elastic_args,
                                                    preexec_fn=preexec_function,
                                                    close_fds=True
                                                    )
            except OSError as ex:
                logging.error("failed to start elasticsearch client: " + str(ex))




    def AcquireResource(self,resourcenames,fromstate):
        idles = conf.resource_base+'/'+fromstate+'/'
        try:
            logging.debug("Trying to acquire resource "
                          +str(resourcenames)
                          +" from "+fromstate)

            for resourcename in resourcenames:
              os.rename(idles+resourcename,used+resourcename)
            if not filter(lambda x: x.cpu==resourcenames,self.online_resource_list):
                logging.debug("resource(s) "+str(resourcenames)
                              +" not found in online_resource_list, creating new")
                self.online_resource_list.append(OnlineResource(resourcenames,self.lock))#
                return self.online_resource_list[-1]
            logging.debug("resource(s) "+str(resourcenames)
                          +" found in online_resource_list")
            return filter(lambda x: x.cpu==resourcenames,self.online_resource_list)[0]
        except Exception as ex:
            logging.info("exception encountered in looking for resources")
            logging.info(ex)

    def ContactResource(self,resourcename):
        self.online_resource_list.append(OnlineResource(resourcename,self.lock))
        self.online_resource_list[-1].ping() #@@MO this is not doing anything useful, afaikt

    def ReleaseResource(self,res):
        self.online_resource_list.remove(res)

    def AcquireResources(self,mode):
        logging.info("acquiring resources from "+conf.resource_base)
        idles = conf.resource_base
        idles += '/idle/' if conf.role == 'fu' else '/boxes/'
        try:
            dirlist = os.listdir(idles)
        except Exception as ex:
            logging.info("exception encountered in looking for resources")
            logging.info(ex)
        logging.info(dirlist)
        current_time = time.time()
        count = 0
        cpu_group=[]
        #self.lock.acquire()
        for cpu in dirlist:
            count = count+1
            cpu_group.append(cpu)
            age = current_time - os.path.getmtime(idles+cpu)
            logging.info("found resource "+cpu+" which is "+str(age)+" seconds old")
            if conf.role == 'fu':
                if count == nthreads:
                  self.AcquireResource(cpu_group,'idle')
                  cpu_group=[]
                  count=0
            else:
                if age < 10:
                    cpus = [cpu]
                    self.ContactResource(cpus)
        #self.lock.release()

    def Start(self):
        self.is_active_run = True
        for resource in self.online_resource_list:
            logging.info('start run '+str(self.runnumber)+' on cpu(s) '+str(resource.cpu))
            if conf.role == 'fu': self.StartOnResource(resource)
            else: resource.NotifyNewRun(self.runnumber)
        if conf.role == 'fu':
            self.changeMarkerMaybe(Run.ACTIVE)

    def StartOnResource(self, resource):
        mondir = conf.watch_directory+'/run'+str(self.runnumber).zfill(conf.run_number_padding)+'/mon/'
        logging.debug("StartOnResource called")
        resource.associateddir=mondir
        resource.StartNewProcess(self.runnumber,
                                 self.online_resource_list.index(resource),
                                 self.arch,
                                 self.version,
                                 self.menu,
                                 len(resource.cpu))
        logging.debug("StartOnResource process started")
        logging.debug("StartOnResource going to acquire lock")
        self.lock.acquire()
        logging.debug("StartOnResource lock acquired")
        try:
            os.makedirs(mondir)
        except OSError:
            pass
        monfile = mondir+'hltd.jsn'

        fp=None
        stat = []
        if not os.path.exists(monfile):
            logging.debug("No log file "+monfile+" found, creating one")
            fp=open(monfile,'w+')
            stat.append([resource.cpu,resource.process.pid,resource.processstate])

        else:
            logging.debug("Updating existing log file "+monfile)
            fp=open(monfile,'r+')
            stat=json.load(fp)
            me = filter(lambda x: x[0]==resource.cpu, stat)
            if me:
                me[0][1]=resource.process.pid
                me[0][2]=resource.processstate
            else:
                stat.append([resource.cpu,resource.process.pid,resource.processstate])
        fp.seek(0)
        fp.truncate()
        json.dump(stat,fp)

        fp.flush()
        fp.close()
        self.lock.release()
        logging.debug("StartOnResource lock released")

    def Shutdown(self):
        logging.debug("Run:Shutdown called")
        self.is_active_run = False
        if conf.role == 'fu':
            self.changeMarkerMaybe(Run.ABORTED)

        try:
            for resource in self.online_resource_list:
                resource.disableRestart()
            for resource in self.online_resource_list:
                if conf.role == 'fu':
                    if resource.processstate==100:
                        logging.info('terminating process '+str(resource.process.pid)+
                                     ' in state '+str(resource.processstate))

                        resource.process.terminate()
                        logging.info('process '+str(resource.process.pid)+' join watchdog thread')
                        #                    time.sleep(.1)
                        resource.join()
                        logging.info('process '+str(resource.process.pid)+' terminated')
                    logging.info('releasing resource(s) '+str(resource.cpu))
                    for cpu in resource.cpu:
                      os.rename(used+cpu,idles+cpu)
                    resource.process=None
                elif conf.role == 'bu':
                    resource.NotifyShutdown()

            self.online_resource_list = []
            try:
                if self.anelastic_monitor:
                    self.anelastic_monitor.terminate()
            except Exception as ex:
                logging.info("exception encountered in shutting down anelastic.py " + str(ex))
            if conf.use_elasticsearch:
                try:
                    if self.elastic_monitor:
                        self.elastic_monitor.terminate()
                except Exception as ex:
                    logging.info("exception encountered in shutting down elastic.py " + str(ex))
            if self.waitForEndThread is not none:
                self.waitForEndThread.join()
        except Exception as ex:
            logging.info("exception encountered in shutting down resources")
            logging.info(ex)
        logging.info('Shutdown of run '+str(self.runnumber).zfill(conf.run_number_padding)+' completed')

    def StartWaitForEnd(self):
        self.is_active_run = False
        if conf.role == 'fu':
            self.changeMarkerMaybe(Run.STOPPING)
        try:
            self.waitForEndThread = threading.Thread(target = self.WaitForEnd)
            self.waitForEndThread.start()
        except Exception as ex:
            logging.info("exception encountered in starting run end thread")
            logging.info(ex)

    def WaitForEnd(self):
        print "wait for end thread!"
        try:
            for resource in self.online_resource_list:
                resource.disableRestart()
            for resource in self.online_resource_list:
                if resource.processstate==100:
                    logging.info('waiting for process '+str(resource.process.pid)+
                                 ' in state '+str(resource.processstate) +
                                 ' to complete ')
                    resource.join()
                    logging.info('process '+str(resource.process.pid)+' completed')
#                os.rename(used+resource.cpu,idles+resource.cpu)
                resource.process=None
            self.online_resource_list = []
            if conf.role == 'fu':
                self.changeMarkerMaybe(Run.COMPLETE)
                self.anelastic_monitor.wait()
            if conf.use_elasticsearch:
                self.elastic_monitor.wait()

        except Exception as ex:
            logging.info("exception encountered in ending run")
            logging.info(ex)

    def changeMarkerMaybe(self,marker):
        mondir = self.dirname+'/mon'
        try:
            os.makedirs(mondir)
        except OSError:
            pass

        current = filter(lambda x: x in Run.VALID_MARKERS, os.listdir(mondir))
        if (len(current)==1 and current[0] != marker) or len(current)==0:
            if len(current)==1: os.remove(mondir+'/'+current[0])
            fp = open(mondir+'/'+marker,'w+')
            fp.close()
        else:
            logging.error("There are more than one markers for run "
                          +str(self.runnumber))
            return


class RunRanger:

    def __init__(self):
        self.inotifyWrapper = InotifyWrapper(self)

    def register_inotify_path(self,path,mask):
        self.inotifyWrapper.registerPath(path,mask)

    def start_inotify(self):
        self.inotifyWrapper.start()

    def stop_inotify(self):
        logging.info("RunRanger: Stop inotify wrapper")
        self.inotifyWrapper.stop()
        logging.info("RunRanger: Join inotify wrapper")
        self.inotifyWrapper.join()
        logging.info("RunRanger: Inotify wrapper returned")

    def process_IN_CREATE(self, event):
        nr=0
        logging.info('RunRanger: event '+event.fullpath)
        dirname=event.fullpath[event.fullpath.rfind("/")+1:]
        #@@EM
        logging.info('RunRanger: new filename '+dirname)
        if dirname.startswith(conf.watch_prefix):
            nr=int(dirname[3:])
            if nr!=0:
                try:
                    logging.info('new run '+str(nr))
                    if conf.role == 'fu':
                        bu_dir = bu_disk_list[0]+'/'+conf.ramdisk_subdirectory+'/'+dirname
                        os.symlink(bu_dir+'/jsd',event.fullpath+'/jsd')
                    else:
                        bu_dir = ''
                    run_list.append(Run(nr,event.fullpath,bu_dir)) #@@MO in case of the BU, the run_list grows until the hltd is stopped
                    run_list[-1].AcquireResources(mode='greedy')
                    run_list[-1].Start()
                except OSError as ex:
                    logging.error("RunRanger: "+str(ex)+" "+ex.filename)
                except Exception as ex:
                    logging.exception("RunRanger: unexpected exception encountered in forking hlt slave")
                    logging.error(ex)

        elif dirname.startswith(conf.watch_emu_prefix):
            nr=int(dirname[3:])
            if nr!=0:
                try:
                    """
                    start a new BU emulator run here - this will trigger the start of the HLT run
                    """
                    bu_emulator.startNewRun(nr)

                except Exception as ex:
                    logging.info("exception encountered in starting BU emulator run")
                    logging.info(ex)

                os.remove(event.fullpath)

        elif dirname.startswith(conf.watch_end_prefix):
            # need to check is stripped name is actually an integer to serve
            # as run number
            if dirname[3:].isdigit():
                nr=int(dirname[3:])
                if nr!=0:
                    try:
                        runtoend = filter(lambda x: x.runnumber==nr,run_list)
                        if len(runtoend)==1:
                            logging.info('end run '+str(nr))
                            if conf.role == 'fu':
                                runtoend[0].StartWaitForEnd()
                            if bu_emulator and bu_emulator.runnumber != None:
                                bu_emulator.stop()

                            logging.info('run '+str(nr)+' removing end-of-run marker')
                            os.remove(event.fullpath)
                            run_list.remove(runtoend[0])
                        elif len(runtoend)==0:
                            logging.error('request to end run '+str(nr)
                                          +' which does not exist')
                        else:
                            logging.error('request to end run '+str(nr)
                                          +' has more than one run object - this should '
                                          +'*never* happen')

                    except Exception as ex:
                        logging.info("exception encountered when waiting hltrun to end")
                        logging.info(ex)
                else:
                    logging.error('request to end run '+str(nr)
                                  +' which is an invalid run number - this should '
                                  +'*never* happen')
            else:
                logging.error('request to end run '+str(nr)
                              +' which is NOT a run number - this should '
                              +'*never* happen')

        elif dirname.startswith('herod') and conf.role == 'fu':
            os.remove(event.fullpath)
            logging.info("killing all child processes")
            for run in run_list:
                for resource in run.online_resource_list:
                    os.kill(resource.process.pid, SIGKILL)
            logging.info("killed all child processes")

        elif dirname.startswith('populationcontrol'):
            logging.info("terminating all ongoing runs")
            for run in run_list:
                run.Shutdown()
            run_list[:] = []
            logging.info("terminated all ongoing runs via cgi interface")
            os.remove(event.fullpath)

        elif dirname.startswith('harakiri') and conf.role == 'fu':
            os.remove(event.fullpath)
            pid=os.getpid()
            logging.info('asked to commit seppuku:'+str(pid))
            try:
                logging.info('sending signal '+str(SIGKILL)+' to myself:'+str(pid))
                retval = os.kill(pid, SIGKILL)
                logging.info('sent SIGINT to myself:'+str(pid))
                logging.info('got return '+str(retval)+'waiting to die...and hope for the best')
            except Exception as ex:
                logging.error("exception in committing harakiri - the blade is not sharp enough...")
                logging.error(ex)

        logging.debug("RunRanger completed handling of event "
                      +event.fullpath)

    def process_default(self, event):
        logging.info('RunRanger: event '+event.fullpath+' type '+str(event.mask))
        filename=event.fullpath[event.fullpath.rfind("/")+1:]

class ResourceRanger:

    def __init__(self):
        self.inotifyWrapper = InotifyWrapper(self)

        self.managed_monitor = system_monitor()
        self.managed_monitor.start()

    def register_inotify_path(self,path,mask):
        self.inotifyWrapper.registerPath(path,mask)

    def start_inotify(self):
        self.inotifyWrapper.start()

    def stop_managed_monitor(self):
        logging.info("ResourceRanger: Stop managed monitor")
        self.managed_monitor.stop()
        logging.info("ResourceRanger: Join managed monitor")
        self.managed_monitor.join()
        logging.info("ResourceRanger: managed monitor returned")

    def stop_inotify(self):
        logging.info("ResourceRanger: Stop inotify wrapper")
        self.inotifyWrapper.stop()
        logging.info("ResourceRanger: Join inotify wrapper")
        self.inotifyWrapper.join()
        logging.info("ResourceRanger: Inotify wrapper returned")

    def process_IN_MOVED_TO(self, event):
        logging.debug('ResourceRanger-MOVEDTO: event '+event.fullpath)
        try:
            resourcepath=event.fullpath[1:event.fullpath.rfind("/")]
            resourcestate=resourcepath[resourcepath.rfind("/")+1:]
            resourcename=event.fullpath[event.fullpath.rfind("/")+1:]
            if not (resourcestate == 'online' or resourcestate == 'offline'
                    or resourcestate == 'quarantined'):
                logging.debug('ResourceNotifier: new resource '
                              +resourcename
                              +' in '
                              +resourcepath
                              +' state '
                              +resourcestate
                              )
                activeruns = filter(lambda x: x.is_active_run==True,run_list)
                if activeruns:
                    activerun = activeruns[0]
                    logging.info("ResourceRanger: found active run "+str(activerun.runnumber))
                    """grab resources that become available
                    #@@EM implement threaded acquisition of resources here
                    """
                    #find all idle cores
                    #activerun.lock.acquire()

                    idlesdir = '/'+resourcepath
		    try:
                        reslist = os.listdir(idlesdir)
                    except Exception as ex:
                        logging.info("exception encountered in looking for resources")
                        logging.info(ex)
                    #put inotify-ed resource as the first item
                    for resindex,resname in enumerate(reslist):
                        fileFound=False
                        if resname == resourcename:
                            fileFound=True
                            if resindex != 0:
                                firstitem = reslist[0]
                                reslist[0] = resourcename
                                reslist[resindex] = firstitem
                        break
                        if fileFound==False:
                            #inotified file was already moved earlier
                            return                    
                    #acquire sufficient cores for a multithreaded process start 
                    resourcenames = []
                    for resname in reslist:
                        if len(resourcenames) < nthreads:
                            resourcenames.append(resname)
                        else:
                            break

                    acquired_sufficient = False
                    if len(resourcenames) == nthreads:
                        acquired_sufficient = True
                        res = activerun.AcquireResource(resourcenames,resourcestate)
                    #activerun.lock.release()

                    if acquired_sufficient:
                        logging.info("ResourceRanger: acquired resource(s) "+str(res.cpu))
                        activerun.StartOnResource(res)
                        logging.info("ResourceRanger: started process on resource "
                                     +str(res.cpu))
                else:
                    #if no run is active, move (x N threads) files from except to idle to be picked up for the next run
                    #TODO:debug,write test
                    if resourcestate == 'except':
                        idlesdir = '/'+resourcepath
		        try:
                            reslist = os.listdir(idlesdir)
                            #put inotify-ed resource as the first item
                            fileFound=False
                            for resindex,resname in enumerate(reslist):
                                if resname == resourcename:
                                    fileFound=True
                                    if resindex != 0:
                                        firstitem = reslist[0]
                                        reslist[0] = resourcename
                                        reslist[resindex] = firstitem
                                    break
                                if fileFound==False:
                                    #inotified file was already moved earlier
                                    return
                            resourcenames = []
                            for resname in reslist:
                                if len(resourcenames) < nthreads:
                                    resourcenames.append(resname)
                                else:
                                    break
                            if len(resourcenames) == nthreads:
                                for resname in resourcenames:
                                    os.rename(broken+resname,idles+resname)

                        except Exception as ex:
                            logging.info("exception encountered in looking for resources in except")
                            logging.info(ex)

        except Exception as ex:
            logging.error("exception in ResourceRanger")
            logging.error(ex)

    def process_IN_MODIFY(self, event):

        logging.debug('ResourceRanger-MODIFY: event '+event.fullpath)
        try:
            if event.fullpath == conf.resource_base+'/bus.config':
                if self.managed_monitor:
                    self.managed_monitor.stop()
                    self.managed_monitor.join()
                cleanup_mountpoints()
                if self.managed_monitor:
                    self.managed_monitor = system_monitor()
                    self.managed_monitor.start()
                    logging.info("ResouceRanger: managed monitor is "+str(self.managed_monitor))
        except Exception as ex:
            logging.error("exception in ResourceRanger")
            logging.error(ex)

    def process_default(self, event):
        logging.debug('ResourceRanger: event '+event.fullpath +' type '+ str(event.mask))
        filename=event.fullpath[event.fullpath.rfind("/")+1:]


class hltd(Daemon2,object):
    def __init__(self, pidfile):
        Daemon2.__init__(self,pidfile)

    def stop(self):
        if self.silentStatus():
            try:
                if os.path.exists(conf.watch_directory+'/populationcontrol'):
                    os.remove(conf.watch_directory+'/populationcontrol')
                fp = open(conf.watch_directory+'/populationcontrol','w+')
                fp.close()
                while 1:
                    os.stat(conf.watch_directory+'/populationcontrol')
                    sys.stdout.write('o.o')
                    sys.stdout.flush()
                    time.sleep(1.)
            except OSError, err:
                pass
        super(hltd,self).stop()

    def run(self):
        """
        if role is not defined in the configuration (which it shouldn't)
        infer it from the name of the machine
        """

        if conf.role == 'fu':

            """
            cleanup resources
            """

            cleanup_resources()
            """
            recheck mount points
            this is done at start and whenever the file /etc/appliance/resources/bus.config is modified
            mount points depend on configuration which may be updated (by runcontrol)
            (notice that hltd does not NEED to be restarted since it is watching the file all the time)
            """

            cleanup_mountpoints()

            calculate_threadnumber()

        """
        the line below is a VERY DIRTY trick to address the fact that
        BU resources are dynamic hence they should not be under /etc
        """
        conf.resource_base = conf.watch_directory+'/appliance' if conf.role == 'bu' else conf.resource_base

        watch_directory = os.readlink(conf.watch_directory) if os.path.islink(conf.watch_directory) else conf.watch_directory
        resource_base = os.readlink(conf.resource_base) if os.path.islink(conf.resource_base) else conf.resource_base

        runRanger = RunRanger()
        runRanger.register_inotify_path(watch_directory,inotify.IN_CREATE)
        runRanger.start_inotify()
        logging.info("started RunRanger  - watch_directory " + watch_directory)

        rr = ResourceRanger()
        try:
            imask  = inotify.IN_MOVED_TO | inotify.IN_CREATE | inotify.IN_DELETE | inotify.IN_MODIFY
            if conf.role == 'bu':
                #currently does nothing on bu
                rr.register_inotify_path(resource_base, imask)
                rr.register_inotify_path(resource_base+'/boxes', imask)
            else:
                rr.register_inotify_path(resource_base, imask)
                rr.register_inotify_path(resource_base+'/idle', imask)
                rr.register_inotify_path(resource_base+'/except', imask)
            rr.start_inotify()
            logging.info("started ResourceRanger - watch_directory "+resource_base)
        except Exception as ex:
            logging.error("Exception caught in starting notifier2")
            logging.error(ex)

        try:
            cgitb.enable(display=0, logdir="/tmp")
            handler = CGIHTTPServer.CGIHTTPRequestHandler
            # the following allows the base directory of the http
            # server to be 'conf.watch_directory, which is writeable
            # to everybody
            if os.path.exists(conf.watch_directory+'/cgi-bin'):
                os.remove(conf.watch_directory+'/cgi-bin')
            os.symlink('/opt/hltd/cgi',conf.watch_directory+'/cgi-bin')

            handler.cgi_directories = ['/cgi-bin']
            logging.info("starting http server on port "+str(conf.cgi_port))
            httpd = BaseHTTPServer.HTTPServer(("", conf.cgi_port), handler)

            logging.info("hltd serving at port "+str(conf.cgi_port)+" with role "+conf.role)
            os.chdir(conf.watch_directory)
            httpd.serve_forever()
        except KeyboardInterrupt:
            logging.info("terminating all ongoing runs")
            for run in run_list:
                run.Shutdown()
            logging.info("terminated all ongoing runs")
            logging.info("stopping run ranger inotify helper")
            runRanger.stop_inotify()
            logging.info("stopping resource ranger inotify helper")
            rr.stop_inotify()
            logging.info("stopping system monitor")
            rr.stop_managed_monitor()
            logging.info("closing httpd socket")
            httpd.socket.close()
            logging.info(threading.enumerate())
            logging.info("shutdown of service completed")
        except Exception as ex:
            logging.info("exception encountered in operating hltd")
            logging.info(ex)
            runRanger.stop_inotify()
            rr.stop_inotify()
            rr.stop_managed_monitor()
            raise


if __name__ == "__main__":
    daemon = hltd('/var/run/hltd.pid')
    daemon.start()
