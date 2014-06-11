import sys, os, time, atexit
import procname
from signal import SIGINT
from aUtils import * #for stdout and stderr redirection


class Daemon2:
    """
    A generic daemon class.

    Usage: subclass the Daemon2 class and override the run() method

    reference:
    http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/

    attn: May change in the near future to use PEP daemon
    """

    def __init__(self, pidfile, processname, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
                      self.stdin = stdin
                      self.stdout = stdout
                      self.stderr = stderr
                      self.pidfile = pidfile
                      self.processname = processname

    def daemonize(self):

        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)
        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # redirect standard file descriptors


        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
        sys.stderr = stdErrorLog()
        sys.stdout = stdOutLog()

        #change process name
        procname.setprocname(self.processname)

        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'w+').write("%s\n" % pid)

    def delpid(self):
        os.remove(self.pidfile)
    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "pidfile %s already exists. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)
        # Start the daemon
        self.daemonize()
        self.run()

    def status(self):
        """
        Get the daemon status from the pid file and ps
        """
        retval = False
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        if not pid:
            message = self.processname+" not running no pidfile %s\n"
        else:
            try:
                os.kill(pid,0)
                message = self.processname+" is running with pidfile %s\n"
                retval = True
            except:
                message = self.processname+" pid exist in %s but process is not running\n"

        sys.stderr.write(message % self.pidfile)
        return retval

    def silentStatus(self):
        """
        Get the daemon status from the pid file and ps
        """
        retval = False
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        if not pid:
            message = self.processname+" not running no pidfile %s\n"
        else:
            try:
                os.kill(pid,0)
                retval = True
            except:
                pass

        return retval

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        # Try killing the daemon process
        try:
            # signal the daemon to stop
            timeout = 5.0 #kill timeout
            os.kill(pid, SIGINT)
            #Q: how is the while loop exited ???
            #A: os.kill throws an exception of type OSError
            #   when pid does not exist
            #C: not very elegant but it works
            while 1:
                if timeout <=0.:
                  sys.stdout.write("\nterminating with -9...")
                  os.kill(pid,9)
                  sys.stdout.write("\nterminated after 5 seconds\n")
                  time.sleep(0.5)
                os.kill(pid,0)
                sys.stdout.write('.')
                sys.stdout.flush()
                time.sleep(0.5)
                timeout-=0.5
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                #this handles the successful stopping of the daemon...
                if os.path.exists(self.pidfile):
                    print 'removing pidfile'
                    os.remove(self.pidfile)
                    sys.stdout.write('[OK]\n')
                    sys.stdout.flush()
            else:
                print str(err)
                sys.exit(1)
        sys.stdout.write('[OK]\n')

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()

    def run(self):
        """
        You should override this method when you subclass Daemon2. It will be called after the process has been
        daemonized by start() or restart().
        """
