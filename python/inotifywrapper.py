#
# wrapper around the python-inotify library
#

import threading
#from inotify import watcher
#import inotify
import watcher
import _inotify as inotify
#import select
#import sys
#from time import sleep

class InotifyWrapper(threading.Thread):

    def __init__(self, parent, logging):

        threading.Thread.__init__(self)

        self.parent_ = parent
        self.logging_ = logging
        self.w = watcher.AutoWatcher()
        self.quit = False

        self.threshold_ = None
        self.timeout_ = None
        self.delay_ = None

    def setThreshold(self,threshold,timeout,delay):
        #currently ignored
        self.threshold_ = threshold
        self.timeout_ = timeout
        self.delay_ = delay

    def registerPath(self,path,mask,recursive=False):
        try:
            if recursive:
                self.w.add_all(path, mask, reportError)
            else:
                self.w.add(path,mask)
        except OSError, err:
            self.logging.error("inotify wrapper exception: " + err.strerror)

    def reportError(self,err):
        self.logging.error("error registering inotify path " + err.strerror +", continuing")

    def run(self):
	while self.quit == False:
	    for event in self.w.read():
                #note:add more events in case needed
	        if event.mask & inotify.IN_CREATE:
	            self.parent_.process_IN_CREATE(event)
	        elif event.mask & inotify.IN_MODIFY:
	            self.parent_.process_IN_MODIFY(event)
	        elif event.mask & inotify.IN_MOVED_TO:
	            self.parent_.process_IN_MOVED_TO(event)
	        elif event.mask & inotify.IN_DELETE:
	            self.parent_.process_IN_DELETE(event)
	        elif event.mask & inotify.IN_CLOSE_WRITE:
	            self.parent_.process_IN_CLOSE_WRITE(event)
                else:
                    parent_.process_default(event)
                if self.quit == True:
                    break

    def stop(self):
        self.quit = True
        self.w.close()

#more advanced use
#    def run(self):
#        poll = select.poll()
#        poll.register(w, select.POLLIN)
#        thresholdObj = watcher.Threshold(w, threshold_)
#
#        while quit == False:
#            events = poll.poll(timeout)
#            sleep(0.01)#wait for events to accumulate
#            nread = 0
#            if thresholdObj() or not events:
#                #print 'reading,', threshold.readable(), 'bytes available'
#                for event in w.read(0):
#                     if event.type == xy:
#                         parent.process_IN_MOVE_TO(event)

