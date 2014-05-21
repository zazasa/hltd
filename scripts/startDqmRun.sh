#!/bin/env /bin/bash

set -x
logname="/var/log/hltd/pid/dqm_run$2_pid$$.log"

echo -e "\n************************************************************" >> $logname 2>&1
echo -e "This is the CMSSW simulation run \n"                            >> $logname 2>&1
echo -e "Running on machine: $(hostname) \n"                             >> $logname 2>&1
echo -e "Host details: "                                                 >> $logname 2>&1
echo -e "\tkernel-name: $(uname --kernel-name)"                          >> $logname 2>&1
echo -e "\tkernel-release: $(uname --kernel-release)"                    >> $logname 2>&1
echo -e "\tkernel-version: $(uname --kernel-version)"                    >> $logname 2>&1
echo -e "\tmachine: $(uname --machine)"                                  >> $logname 2>&1
echo -e "\thardware-platform: $(uname --hardware-platform) \n"           >> $logname 2>&1
echo -e "PID: $$"                                                        >> $logname 2>&1
echo -e "Run Number: $2 \n\n"                                            >> $logname 2>&1
echo -e "The CMSSW will be started like this: CMSSW $1 \n"               >> $logname 2>&1
echo -e "************************************************************\n" >> $logname 2>&1
