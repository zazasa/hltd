#!/bin/env python

import requests
import simplejson as json
import sys
import datetime
import time

url =  'http://localhost:9200/run*/cmsswlog/_search'

#resp = requests.post(url, query)

addSpace=False
repeatsMax=100
suppressionMap = {}
alreadySuppressed = {}

q1 = '{ \
  "fields": [ \
    "_timestamp", \
    "_source" \
  ], \
  "size":100, \
  "query": { \
    "filtered": { \
      "query": { \
        "match_all": {} \
      }, \
      "filter": { \
        "range": { \
          "_timestamp": {'

#            "from": "2014-05-12T12:08:41", \
#            "to": "2014-05-12T12:09:41" \
q2 = '          } \
        } \
      } \
    } \
  }, \
  "sort": { "_timestamp": { "order": "asc" }} \
}'

qdoc = { 
  "fields": [ 
    "_timestamp", 
    "_source" 
  ], 
  "size":100, 
  "query": { 
    "filtered": { 
      "query": { 
        "match_all": {} 
      }, 
      "filter": { 
        "range": { 
          "_timestamp": {
            "from": "", 
            "to": "" 
          } 
        } 
      } 
    } 
  }, 
  "sort": { "_timestamp": { "order": "asc" }} 
}

sleept = 0.5

init = True
tnow =  datetime.datetime.utcnow().isoformat()
time.sleep(sleept)
tfuture =  datetime.datetime.utcnow().isoformat()
time.sleep(sleept)
lastEmpty=True

counter=0

while True:

    counter+=1
    tbefore = tnow
    tnow = tfuture
    tfuture = datetime.datetime.utcnow().isoformat()

    if lastEmpty==False:
        print "\n================\n"

    time.sleep(sleept)

    qdoc['query']['filtered']['filter']['range']['_timestamp']['from']=tbefore
    qdoc['query']['filtered']['filter']['range']['_timestamp']['to']=tnow

    #q = q1 + '"from" :"'+tbefore+'",to:"'+tnow+'"'+q2
    q = json.dumps(qdoc)


    resp = requests.post(url, q)
    data = json.loads(resp.content)
    lastEmpty==True
    maxhostlen=0
   
    #allow suppressed messages every 60 seconds
    if counter%(60/sleept)==0:
        for k in suppressionMap.keys():
            if suppressionMap[k]>100:
                suppressionMap[k]=99
            elif suppressionMap[k]>0:
                suppressionMap[k]-=1

    try:
      if data['hits']['total']!=0:
        lastEmpty==False

        arr = data['hits']['hits']

        for e in arr:


            runstring = ''
            try:
	        runidx = e['_index'].split('_')
                if runidx[0].startswith('log_'):
                    rn = runidx[1]
                else:
                    rn = runidx[0]
                if rn[3:].isdigit():
                    runstring = 'Run: '+str(int(rn[3:]))+' '
            except:
                pass



            ev = e['_source']
            severity = ev['severity']

            repeats=0
            try:
                rep = suppressionMap[ev['lexicalId']]
                rep+=1
                suppressionMap[ev['lexicalId']]=rep
                repeats=rep
                if repeats>repeatsMax and (severity=="INFO" or severity=="DEBUG"):
                    alreadySuppressed[ev['lexicalId']]=True
                    continue
            except:
                try:
                    suppressionMap[ev['lexicalId']]=1
                    repeats=1
                except:pass

            severity = ev['severity']
            sevorig=severity
            if severity=='INFO':
                #severity = '\x1b[32m INFO \x1b[0m'
                severity = '\x1b[37;1;42m INFO \x1b[0m   '
            if severity=='WARNING':
                severity = '\x1b[37;1;43m WARNING \x1b[0m'
            if severity=='ERROR':
                severity = '\x1b[37;1;41m ERROR \x1b[0m  '
            if severity=='FATAL':
                severity = '\x1b[31;1;40m FATAL \x1b[0m  '

            #msg time/zone are not present in all messages...
            msgtime=''
            try:
                msgtime = '\x1b[1m'+ev['msgtime']
                try:
                    msgtime+=' '+ev['msgtimezone']
                except:
                    pass
                msgtime+='\x1b[0m'
            except:
                pass
            host = ev['host']
            hlen = len(host)
            if hlen<maxhostlen and maxhostlen<30:
                host = host.ljust(maxhostlen-hlen)
            else:
                maxhostlen=hlen
               
            pidstr=('(pid '+str(ev['pid'])+')').ljust(11)
            #while len(pidstr)<7:
            #    pidstr+=' '

            #parse optional entries
            cat=''
            str2='\x1b[1m'
            try:
                cat =ev['category']
                str2 +=cat.ljust(21)+' '
            except:
                pass

            mod=''
            try:
                mod = ev['module']
                if mod!=cat:
                    str2+=' '+mod
            except:pass
            try:
                str2+=' '+ev['moduleInstance']
            except:pass
            try:
                str2+=' '+ev['moduleCall']
            except:pass
            try:
                str2+=' '+ev['fwkState']
            except:pass
            try:
                str2+='   \x1b[0mLumi:'+ str(ev['lumi'])
            except:pass
            try:
                str2+='   \x1b[0mEvent:'+str( ev['eventInPrc'])
            except:pass

            #construct final log print:
            str2+='\x1b[0m'
            if sevorig=='INFO':
                print severity + runstring +' '+msgtime+' '+host+' '+pidstr+' : ' + str2 + '\n  \x1b[47m'+ev['message']+'\x1b[0m'
            elif sevorig=='WARNING':
                print severity + runstring +' '+msgtime+' '+host+' '+pidstr+' : ' + str2 + '\n  \x1b[37;1;45m'+ev['message']+'\x1b[0m'
            elif sevorig=='ERROR':
                print severity + runstring +' '+msgtime+' '+host+' '+pidstr+' : ' + str2 + '\n  \x1b[37;1;40m'+ev['message']+'\x1b[0m'
            else:
                print severity + runstring +' '+msgtime+' '+host+' '+pidstr+' : ' + str2 + '\n  \x1b[31;1;40m'+ev['message']+'\x1b[0m'

            if repeats==repeatsMax and (sevorig=="INFO" or sevorig=="DEBUG"):
                try:
                    suppressionStatus = alreadySuppressed[ev['lexicalId']]
                except:
                    print "\x1b[1;36;46m[Printed message has reached maximum repeat threshold (",str(repeatsMax),") and will be suppressed for this session!]\x1b[0m"
            if addSpace:
                print ""
        #print json.dumps(data['hits']['hits'],indent=1)
    except Exception,ex:
      #print ex
      pass
