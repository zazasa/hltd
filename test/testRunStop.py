#!/bin/env python
import os,sys,shutil,time

def lineSearch(logfile,*args):
  searchTimeout = 10.
  searchTimeAccum = 0.
  doLoop = True
  while doLoop:
    line = logfile.readline()
    if not line:
      time.sleep(0.1)
      searchTimeAccum += 0.1
      continue
    foundAll = True
    for arg in args:
      if line.find(arg) == -1:
        foundAll = False
        break
    if foundAll == True:
      return line
    if searchTimeAccum >= searchTimeout:
      print "Error: 10s timeout waiting hlt to log a line containing all of: " + str(args)
      sys.exit(-1)


testBuDir = '/fff/BU/ramdisk'
testDataDir = '/fff/data'
testCMSSWcfg = 'testEmptySource.py'
cmssw_version = 'CMSSW_7_0_0'
scram_arch = 'slc6_amd64_gcc481'

testRunNumber = 999998
testRunNumber2 = 999999
RunDirPrefix = 'run'
EoRDirPrefix = 'end'

menu_dir = testBuDir+'/hlt'
cmssw_config = menu_dir+'/HltConfig.py'

print "test script 1! killing all cmssw and restarting hltd"
os.system("killall hltd")
os.system("killall cmsRun")
time.sleep(1)
os.system("/etc/init.d/hltd restart")

try:
  shutil.rmtree(menu_dir)
except OSError as oserror:
  print "no old dir to delete.OK"

os.mkdir(menu_dir)

shutil.copy(testCMSSWcfg,cmssw_config)

fcmssw = open(menu_dir+'/CMSSW_VERSION','w')
fcmssw.write(cmssw_version)
fcmssw.close()

fscram = open(menu_dir+'/SCRAM_ARCH','w')
fscram.write(scram_arch)
fscram.close()

#create input run dir (empty)
try:
  shutil.rmtree(testBuDir+'/'+RunDirPrefix+str(testRunNumber))
except OSError as oserror:
  print "no old dir to delete.OK"
try:
  shutil.rmtree(testBuDir+'/'+RunDirPrefix+str(testRunNumber2))
except OSError as oserror:
  print "no old dir to delete.OK"

try:
  os.remove(testDataDir+'/'+EoRDirPrefix+str(testRunNumber))
except OSError as oserror:
  print "no old file to delete.OK"

try:
  os.remove(testDataDir+'/'+EoRDirPrefix+str(testRunNumber2))
except OSError as oserror:
  print "no old file to delete.OK"



os.mkdir(testBuDir+'/'+RunDirPrefix+str(testRunNumber))


#open hltd log for reading
logfile = open("/var/log/hltd.log","rt")
logfile.seek(0,2)#goto file end
#while True:
#  line = logfile.readline()
#  if not line:
#    break

#fire up a new run by creating a data dir (avoiding cgi here)
print "starting run for hltd"
try:
  shutil.rmtree(testDataDir+'/'+RunDirPrefix+str(testRunNumber))
except OSError as oserror:
  print "no old dir to delete.OK"

time.sleep(0.5)
os.mkdir(testDataDir+'/'+RunDirPrefix+str(testRunNumber))

#wait until hltd reacts
time.sleep(0.5)

print "created data dir"

#look at the hltd log
retval = lineSearch(logfile,"started process")
print "hltd printed line: " + retval

time.sleep(1)
#signal EoR
print "writing EoR file"
eorFile = open(testDataDir+'/'+EoRDirPrefix+str(testRunNumber),"w")
eorFile.close()

time.sleep(0.3)

retval = lineSearch(logfile,"end run")
print "hltd printed line: " + retval

time.sleep(2)


#second set of test run dirs
os.mkdir(testBuDir+'/'+RunDirPrefix+str(testRunNumber2))
print "starting run " + str(testRunNumber2) +" for hltd"
try:
  shutil.rmtree(testDataDir+'/'+RunDirPrefix+str(testRunNumber2))
except OSError as oserror:
  print "no old dir to delete.OK"
time.sleep(0.1)
os.mkdir(testDataDir+'/'+RunDirPrefix+str(testRunNumber2))

time.sleep(1)
logfile.seek(0,2)

print "running killall cmsRun"
os.system("killall cmsRun")

time.sleep(1)

print "waiting for next..."
retval = lineSearch(logfile,"started process")
print "hltd printed line: " + retval


############################

sys.exit(0) 
