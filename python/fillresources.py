#!/bin/env python

import os
import hltdconf
import subprocess

conf=hltdconf.hltdConf('/etc/hltd.conf')

role=None


if not conf.role and 'bu' in os.uname()[1]: role='bu'
else:
    role='fu'

if role=='fu':

    fp=open('/proc/cpuinfo','r')
    resource_count = 0
    for line in fp:
        if line.startswith('processor'):
            open(conf.resource_base+'/idle/core'+str(resource_count),'a').close()
            resource_count+=1

    try:
        os.makedirs(conf.watch_directory)
    except OSError:
        pass

elif role=='bu':

    try:
        os.makedirs(conf.watch_directory+'/appliance')
    except OSError:
        pass

