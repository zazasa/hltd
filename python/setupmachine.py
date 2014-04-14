#!/bin/env python

import os,sys
import shutil
sys.path.append('/opt/hltd/python')
#from fillresources import *

#for testing enviroment
try:
    import cx_Oracle
except ImportError:
    pass

import socket

backup_dir = '/usr/share/fff'

hltdconf = '/etc/hltd.conf'
busconfig = '/etc/appliance/resources/bus.config'
elasticsysconf = '/etc/sysconfig/elasticsearch'
elasticconf = '/etc/elasticsearch/elasticsearch.yml'

dbpwd = 'empty'
dbhost = 'empty'

def removeResources():
    try:
        shutil.rmtree('/etc/appliance/online/*')
    except:
        pass
    try:
        shutil.rmtree('/etc/appliance/offline/*')
    except:
        pass
    try:
        shutil.rmtree('/etc/appliance/except/*')
    except:
        pass
    try:
        shutil.rmtree('/etc/appliance/quarantined/*')
    except:
        pass

def countCPUs():
    fp=open('/proc/cpuinfo','r')
    resource_count = 0
    for line in fp:
        if line.startswith('processor'):
            resource_count+=1
    return resource_count

def getmachinetype():
    myhost = os.uname()[1]
    print "running on host ",myhost
    if   myhost.startswith('dvrubu-') : return 'daqval','fu'
    elif myhost.startswith('dvbu-') : return 'daqval','bu'
    elif myhost.startswith('bu-') : return 'prod','bu'
    elif myhost.startswith('fu-') : return 'prod','fu'
    elif myhost.startswith('cmsdaq-401b28') : return 'test','fu'
    elif myhost.startswith('dvfu-') : return 'test','fu'
    else: 
       print "debug"
       return 'unknown','unknown'

def getIPs(hostname):
    try:
        ips = socket.gethostbyname_ex(hostname)
    except socket.gaierror:
        ips=[]
    return ips

def checkModifiedConfigInFile(file):

    f = open(file)
    lines = f.readlines(2)#read first 2
    f.close()
    for l in lines:
        if l.strip().startswith("#edited by fff meta rpm"):
            return True
    return False
    


def checkModifiedConfig(lines):
    for l in lines:
        if l.strip().startswith("#edited by fff meta rpm"):
            return True
    return False
    

#daqval
def getDaqvalBUAddr(hostname):

    con = cx_Oracle.connect('CMS_DAQ2_TEST_HW_CONF_W/'+dbpwd+'@'+dbhost+':10121/int2r_lb.cern.ch',
                        cclass="FFFSETUP",purity = cx_Oracle.ATTR_PURITY_SELF)
    #print con.version

    cur = con.cursor()

    myDAQ_EQCFG_EQSET1 = '/daq2val/eq_140325_attributes'
    myDAQ_EQCFG_EQSET = 'DAQ_EQCFG_EQSET'

    qstring=  "select attr_name, attr_value from \
                DAQ_EQCFG_HOST_ATTRIBUTE ha, \
                DAQ_EQCFG_HOST_NIC hn, \
                DAQ_EQCFG_DNSNAME d \
                where \
                ha.eqset_id=hn.eqset_id AND \
                hn.eqset_id=d.eqset_id AND \
                ha.host_id = hn.host_id AND \
                ha.attr_name like 'myBU%' AND \
                hn.nic_id = d.nic_id AND \
                d.dnsname = '" + hostname + "' \
                AND d.eqset_id = (select eqset_id from "+ myDAQ_EQCFG_EQSET +" \
                where tag='DAQ2VAL' AND \
                ctime = (SELECT MAX(CTIME) FROM " + myDAQ_EQCFG_EQSET + " WHERE tag='DAQ2VAL')) order by attr_name"

    cur.execute(qstring)
    retval = []
    for res in cur:
        retval.append(res)
    cur.close()
    return retval

class FileManager:
    def __init__(self,file,sep,edited,os1='',os2=''):
        self.name = file
        f = open(file,'r')
        self.lines = f.readlines()
        f.close()
        self.sep = sep
        self.regs = []
        self.remove = []
        self.edited = edited
        #for style
        self.os1=os1
        self.os2=os2

    def reg(self,key,val,section=None):
        self.regs.append([key,val,False,section])

    def removeEntry(self,key):
        self.remove.append(key)

    def commit(self):
        out = []
        if self.edited  == False:
            out.append('#edited by fff meta rpm\n')

        #first removing elements
        for rm in self.remove:
            for i,l in enumerate(self.lines):
                if l.strip().startswith(rm):
                    del self.lines[i]
                    break

        for i,l in enumerate(self.lines):
            lstrip = l.strip()
            if lstrip.startswith('#'):
                continue
                   
            try:
                key = lstrip.split(self.sep)[0].strip()
                for r in self.regs:
                    if r[0] == key:
                        self.lines[i] = r[0].strip()+self.os1+self.sep+self.os2+r[1].strip()+'\n'; #ref ?
                        r[2]= True
                        break
            except:
                continue
        for r in self.regs:
            if r[2] == False:
                toAdd = r[0]+self.os1+self.sep+self.os2+r[1]+'\n'
                insertionDone = False
                if r[3] is not None:
                    for idx,l in enumerate(self.lines):
                        if l.strip().startswith(r[3]):
                            try:
                                self.lines.insert(idx+1,toAdd)
                                insertionDone = True
                            except:
                                pass
                            break
                if insertionDone == False:
                    self.lines.append(toAdd)
        for l in self.lines:
            out.append(l)
        #print "file ",self.name,"\n\n"
        #for o in out: print o
        f = open(self.name,'w+')
        f.writelines(out)
        f.close()


def restoreFileMaybe(file):
    try:
        try:
            f = open(file,'r')
            lines = f.readlines()
            f.close()
            shouldNotCopy = checkModifiedConfig(lines)
        except:
            #backup also if file got deleted
            shouldNotCopy = False

        if not shouldNotCopy:
            backuppath = os.path.join(backup_dir,os.path.basename(file))
            f = open(backuppath)
            blines = f.readlines()
            f.close()
            if  checkModifiedConfig(blines) == False and len(blines)>0:
                shutil.move(backuppath,file)
    except:
        pass

#main function
if True:
    if not sys.argv[1]:
        print "selection of packages to set up (hltd and/or elastic) missing"
        sys.exit(1)
    selection = sys.argv[1]
    #print selection

    if selection == 'restore':
        restoreFileMaybe(hltdconf)
        restoreFileMaybe(elasticsysconf)
        restoreFileMaybe(elasticconf)
        try:
            os.remove(os.path.join(backup_dir,os.path.basename(busconfig)))
        except:
            pass

        sys.exit(0)

    if not sys.argv[2]:
        print "global elasticsearch hostname name missing"
        sys.exit(1)
    elastic_host = sys.argv[2]
    #http prefix is required here
    if not elastic_host.strip().startswith('http://'):
        elastic_host = 'http://'+ elastic_host.strip()


    if not sys.argv[3]:
        print "elasticsearch tribe hostname name missing"
        sys.exit(1)
    elastic_host2 = sys.argv[3]


    if not sys.argv[4]:
        print "CMSSW base missing"
        sys.exit(1)
    cmssw_base = sys.argv[4]


    if not sys.argv[5]:
        print "DB connection parameters missing"
        sys.exit(1)
    dbhost = sys.argv[5]


    if not sys.argv[6]:
        print "DB connection parameters missing"
        sys.exit(1)
    dbpwd = sys.argv[6]

    if not sys.argv[7]:
        print "Username parameter is missing"
        sys.exit(1)
    username = sys.argv[7]




    cluster,type = getmachinetype()
    cnhostname = os.uname()[1]+".cms"

    print "running configuration for machine ", cnhostname, " of type ", type, " in cluster", cluster

    buName = ''
    if type == 'fu':
        if cluster == 'daqval':
            addrList =  getDaqvalBUAddr(cnhostname)
            selectedAddr = False
            for addr in addrList:
                result = os.system("ping -c 1 "+ str(addr[1])+" >/dev/null")
                os.system("clear")
                print "debug ping result ", result
                if result == 0:
                    buDataAddr = addr[1]
                    if addr[1].find('.'):
                        buName = addr[1].split('.')[0]
                    else:
                        buName = addr[1]
                    selectedAddr=True
                    break
            #if none are pingable, first one is picked
            if selectedAddr==False:
                if len(addrList)>0:
                    addr = addrList[0]
                    buDataAddr = addr[1]
                    if addr[1].find('.'):
                        buName = addr[1].split('.')[0]
                    else:
                        buName = addr[1]
            if buName == '':
                print "no BU found for this FU in the dabatase"
                sys.exit(-1)
 
        elif cluster =='test':
            addrList = os.uname()[1]
            buName = os.uname()[1]
            buDataAddr = os.uname()[1]
        else:
            print "production cluster support not yet implemented !!"
            sys.exit(-2)

    elif type == 'bu':
       buName = os.uname()[1]
       addrList = buName

    print "detected address", addrList," and name ",buName

    if 'elasticsearch' in selection:

        print "will modify sysconfig elasticsearch configuration"
        #maybe backup vanilla versions
        essysEdited =  checkModifiedConfigInFile(elasticsysconf)
        if essysEdited == False:
          print "elasticsearch sysconfig configuration was not yet modified"
          shutil.copy(elasticsysconf,os.path.join(backup_dir,os.path.basename(elasticsysconf)))

        esEdited =  checkModifiedConfigInFile(elasticconf)
        if esEdited == False:
          shutil.copy(elasticconf,os.path.join(backup_dir,os.path.basename(elasticconf)))

        escfg = FileManager(elasticconf,':',esEdited,'',' ')

        clusterName='appliance_'+buName
        escfg.reg('cluster.name',clusterName)
        if type == 'fu':
            essyscfg = FileManager(elasticsysconf,'=',essysEdited)
            essyscfg.reg('ES_HEAP_SIZE','512M')
            escfg.reg('discovery.zen.ping.multicast.enabled','false')
            escfg.reg('discovery.zen.ping.unicast.hosts','[ \"'+buDataAddr+'\" ]')
            escfg.reg('transport.tcp.compress','true')
            if cluster != 'test':
                escfg.reg('node.master','false')
                escfg.reg('node.data','true')
            essyscfg.commit()
        if type == 'bu':
            escfg.reg('discovery.zen.ping.multicast.enabled','false')
            #escfg.reg('discovery.zen.ping.unicast.hosts','[ \"'+elastic_host2+'\" ]')
            escfg.reg('transport.tcp.compress','true')
            escfg.reg('node.master','true')
            escfg.reg('node.data','false')

        escfg.commit()

    if "hltd" in selection:
      #number of cmssw threads (if set)
      nthreads = 1
      try:
          nthreads = sys.argv[8]
      except:
          pass

      #first prepare bus.config file
      if type == 'fu':
        try:
          shutil.copy(busconfig,os.path.join(backup_dir,os.path.basename(busconfig)))
          os.remove(busconfig)
        except Exception,ex:
          print "problem with copying bus.config? ",ex
          pass

      #write bu ip address
        f = open(busconfig,'w+')
        f.writelines(getIPs(buDataAddr)[0])
        f.close()

      hltdEdited = checkModifiedConfigInFile(hltdconf)
      print "was modified?",hltdEdited
      if hltdEdited == False:
        shutil.copy(hltdconf,os.path.join(backup_dir,os.path.basename(hltdconf)))
      hltdcfg = FileManager(hltdconf,'=',hltdEdited,' ',' ')

      if type=='bu':
      
          #get needed info here
          hltdcfg.reg('user',sys.argv[7],'[General]')
          hltdcfg.reg('elastic_runindex_url',sys.argv[2],'[Monitoring]')
          hltdcfg.removeEntry('watch_directory')
          hltdcfg.commit() 


      if type=='fu':

          #max_cores_done = False
          #do_max_cores = True
          #num_max_cores = countCPUs()

          #num_threads_done = False
          #do_num_threads = True
          #num_threads = nthreads 
 
          hltdcfg.reg('user',sys.argv[7],'[General]')
          hltdcfg.reg('role','fu','[General]')
          hltdcfg.reg('cmssw_base',cmssw_base,'[CMSSW]')
          hltdcfg.removeEntry('watch_directory')
          hltdcfg.commit()
          #get customized info here

          #removeResources()
          #fillResources(num_max_cores)

    

#/opt/hltd/python/fillresources.py

