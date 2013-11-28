#!/bin/env python2.6

import hltdconf
import demote
import subprocess
conf=hltdconf.hltdConf('/etc/hltd.conf')
new_run_args = ['watch','-n 1', 'ls']
dem = demote.demote(conf.user)
subprocess.Popen(new_run_args,preexec_fn=dem,close_fds=True)
