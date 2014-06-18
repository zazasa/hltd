#!/bin/env /bin/bash
set -x #echo on
TODAY=$(date)
logname="/var/log/hltd/pid/hlt_run$6_pid$$.log"
#override the noclobber option by using >| operator for redirection - then keep appending to log
echo startDqmRun invoked $TODAY with arguments $1 $2 $3 $4 $5 $6 $7 $8 >| $logname
export SCRAM_ARCH=$2
source $1/cmsset_default.sh >> $logname
cd $5;
pwd >> $logname 2>&1
eval `scram runtime -sh`;
cd $4;
pwd >> $logname 2>&1
exec cmsRun `readlink $8` runInputDir=$7 runNumber=$6 >> $logname 2>&1
