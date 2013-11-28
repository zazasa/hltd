#!/bin/env python

import os
import hltdconf
import subprocess

conf=hltdconf.hltdConf('/etc/hltd.conf')

role=None


if not conf.role and 'bu' in os.uname()[1]: role='bu'
else:
    role='fu'

if not os.path.exists(conf.output_directory): os.makedirs(conf.output_directory)

if role=='fu':

    fp=open('/proc/cpuinfo','r')
    resource_count = 0
    for line in fp:
        if line.startswith('processor'):
            open(conf.resource_base+'/idle/core'+str(resource_count),'a').close()
            resource_count+=1

    if not os.path.exists(conf.watch_directory): os.makedirs(conf.watch_directory)

elif role=='bu':

    if not os.path.exists(conf.watch_directory):
        os.symlink('/dev/shm',conf.watch_directory)
    if not os.path.exists(conf.watch_directory+'/appliance'):
        os.makedirs(conf.watch_directory+'/appliance')

    
