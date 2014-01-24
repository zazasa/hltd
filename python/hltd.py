#!/bin/env python
import os,sys
sys.path.append('/opt/hltd/python')
sys.path.append('/opt/hltd/lib')

import time
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
import pyinotify
import prctl

#modules which are part of hltd
from daemon2 import Daemon2
import hltdconf

conf=hltdconf.hltdConf('/etc/hltd.conf')


#put this in the configuration !!!
RUNNUMBER_PADDING=conf.run_number_padding

idles = conf.resource_base+'/idle/'
used = conf.resource_base+'/online/'
abused = conf.resource_base+'/except/'
quarantined = conf.resource_base+'/quarantined/'

run_list=[]
bu_disk_list=[]

logging.basicConfig(filename=conf.service_log,
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

    dirlist = os.listdir(abused)
    for cpu in dirlist:
        os.rename(abused+cpu,idles+cpu)
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
                if not os.path.exists('/'+conf.bu_base_dir+str(i)):
                    os.makedirs('/'+conf.bu_base_dir+str(i))
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

                i+=1
    except Exception as ex:
        logging.error("Exception in cleanup_mountpoints")
        logging.error(ex)

class system_monitor(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.running = True
        self.hostname = os.uname()[1]
        self.directory = []
        self.file = []
        self.rehash()

    def rehash(self):
        self.directory = ['/'+x+'/ramdisk/appliance/boxes/' for x in bu_disk_list]
        self.file = [x+self.hostname for x in self.directory]
        for dir in self.directory:
            if not os.path.exists(dir):
                os.makedirs(dir)
        logging.info("system_monitor: rehash found the following BU disks")
        for disk in self.file:
            logging.info(disk)

    def run(self):
        try:
            logging.debug('entered system monitor thread ')
            while self.running:
#                logging.info('system monitor - running '+str(self.running))
                time.sleep(5.)
                tstring = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                fp = None
                for mfile in self.file:
                        fp=open(mfile,'w+')
                        fp.write(tstring)
                        fp.write('\n')
                        fp.write('ires='+str(len(os.listdir(idles))))
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
        logging.debug('exiting system monitor thread ')

    def stop(self):
        logging.debug("system_monitor: request to stop")
        self.running = False

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

        new_run_args = [conf.cmssw_script_location+'/startRun.sh',
                        conf.cmssw_base,
                        conf.cmssw_arch,
                        conf.cmssw_default_version,
                        conf.exec_directory,
                        configtouse,
                        str(nr),
                        ' ']
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

    def __init__(self,resourcename,lock):
        self.hoststate = 0
        self.cpu = resourcename
        self.process = None
        self.processstate = None
        self.watchdog = None
        self.runnumber = None
        self.associateddir = None
        self.lock = lock
        self.retry_attempts = 0;

    def ping(self):
        if conf.role == 'bu':
            if not os.system("ping -c 1 "+self.cpu)==0: self.hoststate = 0

    def NotifyNewRun(self,runnumber):
        self.runnumber = runnumber
        logging.info("calling start of run on "+self.cpu);
        connection = httplib.HTTPConnection(self.cpu, 8000)
        connection.request("GET",'cgi-bin/start_cgi.py?run='+str(runnumber))
        response = connection.getresponse()
        #do something intelligent with the response code
        logging.error("response was "+str(response.status))
        if response.status > 300: self.hoststate = 1
        else:
            logging.info(response.read())

    def NotifyShutdown(self):
        connection = httplib.HTTPConnection(self.cpu, 8000)
        connection.request("GET",'cgi-bin/stop_cgi.py?run='+str(self.runnumber))
        response = connection.getresponse()
#do something intelligent with the response code
        if response.status > 300: self.hoststate = 0

    def StartNewProcess(self ,runnumber, startindex, arch, version, menu):
        logging.debug("OnlineResource: StartNewProcess called")
        self.runnumber = runnumber
        """
        @@EM here - get the config and cmssw version from the BU run directory
        fall back to test config only if those are not found
        """
        pass

        """
        this is just a trick to be able to use two
        independent mounts of the BU - it should not be necessary in due course
        IFF it is necessary, it should address "any" number of mounts, not just 2
        """

        input_disk = bu_disk_list[startindex%len(bu_disk_list)]+'/ramdisk'

        logging.info("starting process with "+version+" and run number "+str(runnumber))

        new_run_args = [conf.cmssw_script_location+'/startRun.sh',
                        conf.cmssw_base,
                        arch,
                        version,
                        conf.exec_directory,
                        menu,
                        str(runnumber),
                        input_disk]
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
        logging.debug("OnlineResource "+self.cpu+" restart is now disabled")
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
            idles = conf.resource_base+'/idle/'
            used = conf.resource_base+'/online/'
            broken = conf.resource_base+'/except/'
            quarantined = conf.resource_base+'/quarantined/'

            #update json process monitoring file
            self.resource.processstate=returncode
            logging.debug('ProcessWatchdog: acquire lock thread '+str(pid))
            self.lock.acquire()
            logging.debug('ProcessWatchdog: acquired lock thread '+str(pid))
            fp=open(monfile,'r+')

            stat=json.load(fp)

            stat=[[x[0],x[1],returncode]
                  if x[0]==self.resource.cpu else [x[0],x[1],x[2]] for x in stat]
            fp.seek(0)
            fp.truncate()
            json.dump(stat,fp)

            fp.flush()
            fp.close()
            logging.debug('ProcessWatchdog: release lock thread '+str(pid))
            self.lock.release()
            logging.debug('ProcessWatchdog: released lock thread '+str(pid))

            #cleanup actions- remove process from list and
            # attempt restart on same resource

            if returncode < 0:
                logging.error("process "+str(pid)
                              +" for run "+str(self.resource.runnumber)
                              +" on resource " + self.resource.cpu
                              +" exited with signal "
                              +str(returncode)
                              +" restart is enabled ? "
                              +str(self.retry_enabled)
                              )

                oldpid = pid

                if self.resource.retry_attempts < self.retry_limit and self.retry_enabled:
                    """
                    sleep a configurable amount of seconds before
                    trying a restart. This is to avoid 'crash storms'
                    """
                    time.sleep(self.retry_delay)

                    self.resource.process = None
                    self.resource.retry_attempts += 1

                    logging.info("try to restart process for resource "
                                 +self.resource.cpu
                                 +" attempt "
                                 + str(self.resource.retry_attempts))
                    os.rename(used+self.resource.cpu,broken+self.resource.cpu)
                    logging.debug("resource " +self.resource.cpu+
                                  " successfully moved to except")
                elif self.resource.retry_attempts >= self.retry_limit:
                    logging.error("process for run "
                                  +str(self.resource.runnumber)
                                  +" on resource " + self.resource.cpu
                                  +" reached max retry limit "
                                  )
                    os.rename(used+self.resource.cpu,quarantined+self.resource.cpu)
            #successful end= release resource
            elif returncode == 0:

                logging.info('releasing resource, exit0 meaning end of run '+self.resource.cpu)
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
                os.rename(used+self.resource.cpu,idles+self.resource.cpu)
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

    def __init__(self,nr,dirname):
        self.runnumber = nr
        self.dirname = dirname
        self.online_resource_list = []
        self.is_active_run = True
        self.managed_monitor = None
        self.arch = None
        self.version = None
        self.menu = None
        if conf.role == 'fu':
            self.changeMarkerMaybe(Run.STARTING)
        self.menu_directory = dirname+conf.menu_directory
        if os.path.exists(self.menu_directory):
            self.menu = self.menu_directory+'/'+conf.menu_name
            if os.path.exists(self.menu_directory+'/'+conf.arch_file):
                fp = open(self.menu_directory+'/'+conf.arch_file,'r')
                self.arch = fp.readline()
                fp.close()
            if os.path.exists(self.menu_directory+'/'+conf.version_file):
                fp = open(self.menu_directory+'/'+conf.version_file,'r')
                self.version = fp.readline()
                fp.close()
        else:
            self.arch = conf.cmssw_arch
            self.version = conf.cmssw_default_version
            self.menu = conf.test_hlt_config1

        self.lock = threading.Lock()
        if conf.use_elasticsearch:
            try:
                logging.info("starting elastic.py with arguments:"+self.dirname)
                elastic_args = ['/opt/hltd/python/elastic.py',self.dirname]
                self.managed_monitor = subprocess.Popen(elastic_args,
                                                        preexec_fn=preexec_function,
                                                        close_fds=True
                                                        )
            except OSError as ex:
                logging.error("failed to start elasticsearch client")
                logging.error(ex)




    def AcquireResource(self,resourcename,fromstate):
        idles = conf.resource_base+'/'+fromstate+'/'
        used = conf.resource_base+'/online/'
        try:
            logging.debug("Trying to acquire resource "
                          +resourcename
                          +" from "+fromstate)

            os.rename(idles+resourcename,used+resourcename)
            if not filter(lambda x: x.cpu==resourcename,self.online_resource_list):
                logging.debug("resource "+resourcename
                              +" not found in online_resource_list, creating new")
                self.online_resource_list.append(OnlineResource(resourcename,self.lock))
                return self.online_resource_list[-1]
            logging.debug("resource "+resourcename
                          +" found in online_resource_list")
            return filter(lambda x: x.cpu==resourcename,self.online_resource_list)[0]
        except Exception as ex:
            logging.info("exception encountered in looking for resources")
            logging.info(ex)

    def ContactResource(self,resourcename):
        self.online_resource_list.append(OnlineResource(resourcename,self.lock))
        self.online_resource_list[-1].ping()

    def ReleaseResource(self,res):
        idles = conf.resource_base+'/idle/'
        used = conf.resource_base+'/online/'
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
        for cpu in dirlist:
            age = current_time - os.path.getmtime(idles+cpu)
            logging.info("found resource "+cpu+" which is "+str(age)+" seconds old")
            if conf.role == 'fu':
                self.AcquireResource(cpu,'idle')
            else:
                if age < 10:
                    self.ContactResource(cpu)

    def Start(self):
        for resource in self.online_resource_list:
            logging.info('start run '+str(self.runnumber)+' on cpu '+resource.cpu)
            if conf.role == 'fu': self.StartOnResource(resource)
            else: resource.NotifyNewRun(self.runnumber)
        if conf.role == 'fu':
            self.changeMarkerMaybe(Run.ACTIVE)

    def StartOnResource(self, resource):
        mondir = conf.watch_directory+'/run'+str(self.runnumber).zfill(RUNNUMBER_PADDING)+'/mon/'
        logging.debug("StartOnResource called")
        resource.associateddir=mondir
        resource.StartNewProcess(self.runnumber,
                                 self.online_resource_list.index(resource),
                                 self.arch,
                                 self.version,
                                 self.menu)
        logging.debug("StartOnResource process started")
        logging.debug("StartOnResource going to acquire lock")
        self.lock.acquire()
        logging.debug("StartOnResource lock acquired")
        if not os.path.exists(mondir):
            os.makedirs(mondir)
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
        idles = conf.resource_base+'/idle/'
        used = conf.resource_base+'/online/'

        try:
            for resource in self.online_resource_list:
                resource.disableRestart()
                if conf.role == 'fu':
                    if resource.processstate==100:
                        logging.info('terminating process '+str(resource.process.pid)+
                                     ' in state '+str(resource.processstate))

                        resource.process.terminate()
                        logging.info('process '+str(resource.process.pid)+' join watchdog thread')
                        #                    time.sleep(.1)
                        resource.join()
                        logging.info('process '+str(resource.process.pid)+' terminated')
                    logging.info('releasing resource '+resource.cpu)
                    os.rename(used+resource.cpu,idles+resource.cpu)
                    resource.process=None
                elif conf.role == 'bu':
                    resource.NotifyShutdown()

            self.online_resource_list = []
            if conf.use_elasticsearch:
                if self.managed_monitor:
                    self.managed_monitor.terminate()
        except Exception as ex:
            logging.info("exception encountered in shutting down resources")
            logging.info(ex)
        logging.info('Shutdown of run '+str(self.runnumber).zfill(RUNNUMBER_PADDING)+' completed')

    def WaitForEnd(self):
        self.is_active_run = False
        if conf.role == 'fu':
            self.changeMarkerMaybe(Run.STOPPING)

        idles = conf.resource_base+'/idle/'
        used = conf.resource_base+'/online/'

        try:
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
            if conf.use_elasticsearch:
                self.managed_monitor.wait()

        except Exception as ex:
            logging.info("exception encountered in ending run")
            logging.info(ex)

    def changeMarkerMaybe(self,marker):
        mondir = self.dirname+'/mon'
        if not os.path.exists(mondir):
            os.makedirs(mondir)

        current = filter(lambda x: x in Run.VALID_MARKERS, os.listdir(mondir))
        if (len(current)==1 and current[0] != marker) or len(current)==0:
            if len(current)==1: os.remove(mondir+'/'+current[0])
            fp = open(mondir+'/'+marker,'w+')
            fp.close()
        else:
            logging.error("There are more than one markers for run "
                          +str(self.runnumber))
            return


class RunRanger(pyinotify.ProcessEvent):
    def process_IN_CREATE(self, event):
        nr=0
        logging.info('RunRanger: event '+event.pathname)
        dirname=event.pathname[event.pathname.rfind("/")+1:]
        #@@EM
        logging.info('RunRanger: new filename '+dirname)
        if dirname.startswith(conf.watch_prefix):
            nr=int(dirname[3:])
            if nr!=0:
                try:
                    logging.info('new run '+str(nr))
                    if len(bu_disk_list):
                        os.symlink(bu_disk_list[0]+'/ramdisk/'+dirname+'/jsd',event.pathname+'/jsd')
                    run_list.append(Run(nr,event.pathname))
                    run_list[-1].AcquireResources(mode='greedy')
                    run_list[-1].Start()
                except OSError as ex:
                    logging.error("RunRanger: "+str(ex)+" "+ex.filename)
                except Exception as ex:
                    logging.exception("RunRanger: unexpected exception encountered in forking hlt slave")
                    logging.error(ex)

        if dirname.startswith(conf.watch_emu_prefix):
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

                os.remove(event.pathname)

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
                                runtoend[0].WaitForEnd()
                            elif bu_emulator:
                                bu_emulator.stop()

                            logging.info('run '+str(nr)+' removing end-of-run marker')
                            os.remove(event.pathname)
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
            os.remove(event.pathname)
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
            os.remove(event.pathname)

        elif dirname.startswith('harakiri') and conf.role == 'fu':
            os.remove(event.pathname)
            pid=os.getpid()
            logging.info('asked to commit seppuku:'+str(pid))
            try:
                logging.info('sending signal '+str(SIGKILL)+' to myself:'+str(pid))
                retval = os.kill(pid, SIGKILL)
                logging.info('sent SIGINT to myself:'+str(pid))
                logging.info('got return '+str(retval)+'waiting to die...and hope for the best')
            except Exception as ex:
                logging.error("exception in committing harakiri - the blade is not sharp enough...")
                msg = str(ex)
                logging.error(msg)

        logging.debug("RunRanger completed handling of event "
                      +event.pathname)

    def process_default(self, event):
        logging.info('RunRanger: event '+event.pathname+' type '+event.maskname)
        filename=event.pathname[event.pathname.rfind("/")+1:]


class ResourceRanger(pyinotify.ProcessEvent):

    def __init__(self,s):
        pyinotify.ProcessEvent.__init__(self,s)
        self.managed_monitor = system_monitor()
        self.managed_monitor.start()

    def stop_managed_monitor(self):
        logging.info("ResourceRanger: Stop managed monitor")
        self.managed_monitor.stop()
        logging.info("ResourceRanger: Join managed monitor")
        self.managed_monitor.join()
        logging.info("ResourceRanger: managed monitor returned")

    def process_IN_MOVED_TO(self, event):
        logging.debug('ResourceRanger-MOVEDTO: event '+event.pathname)
        try:
            resourcepath=event.pathname[1:event.pathname.rfind("/")]
            resourcestate=resourcepath[resourcepath.rfind("/")+1:]
            resourcename=event.pathname[event.pathname.rfind("/")+1:]
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
                    res = activerun.AcquireResource(resourcename,resourcestate)
                    logging.info("ResourceRanger: acquired resource "+res.cpu)
                    activerun.StartOnResource(res)
                    logging.info("ResourceRanger: started process on resource "
                                 +res.cpu)

        except Exception as ex:
            logging.error("exception in ResourceRanger")
            msg = str(ex)
            logging.error(msg)

    def process_IN_MODIFY(self, event):

        logging.debug('ResourceRanger-MODIFY: event '+event.pathname)
        try:
            if event.pathname == conf.resource_base+'/bus.config':
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
            msg = str(ex)


    def process_default(self, event):
        logging.debug('ResourceRanger: event '+event.pathname+' type '+event.maskname)
        filename=event.pathname[event.pathname.rfind("/")+1:]





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

        if not conf.role and 'bu' in os.uname()[1]:
            conf.role = 'bu'
        else:
            conf.role = 'fu'

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

        """
        the line below is a VERY DIRTY trick to address the fact that
        BU resources are dynamic hence they should not be under /etc
        """
        conf.resource_base = conf.watch_directory+'/appliance' if conf.role == 'bu' else conf.resource_base

        watch_directory = os.readlink(conf.watch_directory) if os.path.islink(conf.watch_directory) else conf.watch_directory
        resource_base = os.readlink(conf.resource_base) if os.path.islink(conf.resource_base) else conf.resource_base
        wm1 = pyinotify.WatchManager()
        s1 = pyinotify.Stats() # Stats is a subclass of ProcessEvent
        notifier1 = pyinotify.ThreadedNotifier(wm1, default_proc_fun=RunRanger(s1))
        logging.info("starting notifier - watch_directory "+watch_directory)
        notifier1.start()
        wm1.add_watch(watch_directory,
                      pyinotify.IN_CREATE,
                      rec=False,
                      auto_add=False)
        wm2 = pyinotify.WatchManager()

        s2 = pyinotify.Stats() # Stats is a subclass of ProcessEvent

        try:
            rr = ResourceRanger(s2)
            notifier2 = pyinotify.ThreadedNotifier(wm2, default_proc_fun=rr)
            logging.info("starting notifier - watch_directory "+resource_base)
            notifier2.start()
            wm2.add_watch(resource_base,
                          pyinotify.IN_CREATE | pyinotify.IN_DELETE | pyinotify.IN_MODIFY | pyinotify.IN_MOVED_TO,
                          rec=True,
                          auto_add=True)
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
            logging.info("terminating notifier 1")
            notifier1.stop()
            logging.info("terminating notifier 2")
            notifier2.stop()
            logging.info("stopping system monitor")
            rr.stop_managed_monitor()
            logging.info("closing httpd socket")
            httpd.socket.close()
            logging.info(threading.enumerate())
            logging.info("shutdown of service completed")
        except Exception as ex:
            logging.info("exception encountered in operating hltd")
            logging.info(ex)
            notifier1.stop()
            notifier2.stop()
            rr.stop_managed_monitor()
            raise


if __name__ == "__main__":
    daemon = hltd('/tmp/hltd.pid')
    daemon.start()
