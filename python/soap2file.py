#!/bin/env python

import os
import pwd
import sys
import SOAPpy

sys.path.append('/opt/hltd/python')
sys.path.append('/opt/hltd/lib')

import demote
import hltdconf
from daemon2 import Daemon2


def writeToFile(filename,content):
    try:
        with open(filename,'w') as file:
            file.write(content)
        return "Success"
    except IOError as ex:
        return "Failed to write data: "+str(ex)


class Soap2file(Daemon2):

    def __init__(self,pidfile):
        Daemon2.__init__(self,pidfile,'soap2file')
        #SOAPpy.Config.debug = 1
        self._conf=hltdconf.hltdConf('/etc/hltd.conf')
        self._hostname = os.uname()[1]

    def run(self):
        dem = demote.demote(self._conf.user)
        dem()

        server = SOAPpy.SOAPServer((self._hostname, self._conf.soap2file_port))
        server.registerFunction(writeToFile)
        server.serve_forever()


if __name__ == "__main__":

    pidfile = '/var/run/soap2file.pid'
    soap2file = Soap2file(pidfile)

    if len(sys.argv) == 2:

        if 'start' == sys.argv[1]:
            try:
                soap2file.start()
                if soap2file.silentStatus():
                    print '[OK]'
                else:
                    print '[Failed]'
            except:
                pass

        elif 'stop' == sys.argv[1]:
            if soap2file.status():
                soap2file.stop()
            elif os.path.exists(pidfile):
                soap2file.delpid()

        elif 'restart' == sys.argv[1]:
            soap2file.restart()

        elif 'status' == sys.argv[1]:
            soap2file.status()

        else:
            print "Unknown command"
            sys.exit(2)
            sys.exit(0)
    else:
        print "usage: %s start|stop|restart|status" % sys.argv[0]
        sys.exit(2)

