#!/bin/env /bin/bash
set -x #echo on
TODAY=$(date)
logname="/tmp/hlt$$.log"
#override the noclobber option by using >| operator for redirection - then keep appending to log
echo startRun invoked $TODAY with arguments $1 $2 $3 $4 $5 $6 $7 >| $logname
dir=$1
export SCRAM_ARCH=$2
source $dir/cmsset_default.sh >> $logname
dir+=/$2/cms/cmssw/$3/src
#dir+=/$3/src
cd $dir;
pwd >> $logname 2>&1
eval `scram runtime -sh`;
cd $4;
logname="/tmp/hlt$$.log"
exec cmsRun $5 "runNumber="$6 "buBaseDir="$7 >> $logname 2>&1
