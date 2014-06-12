#!/bin/env python

import os
import shutil
import hltdconf

conf=hltdconf.hltdConf('/etc/hltd.conf')

role=None

if conf.role==None:
    if 'bu' in os.uname()[1]: role='bu'
    elif 'fu' in os.uname()[1]: role='fu'
else: role = conf.role

if role=='fu':

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


    fp=open('/proc/cpuinfo','r')
    resource_count = 0
    for line in fp:
        if line.startswith('processor'):
            open(conf.resource_base+'/idle/core'+str(resource_count),'a').close()
            resource_count+=1

    try:
        os.umask(0)
        os.makedirs(conf.watch_directory)
    except OSError:
        try: 
            os.chmod(conf.watch_directory,0777)
        except:
            pass

elif role=='bu':

    try:
        os.umask(0)
        os.makedirs(conf.watch_directory+'/appliance')
    except OSError:
        try:
            os.chmod(conf.watch_directory+'/appliance',0777)
        except:
            pass

