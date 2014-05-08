import ConfigParser
import logging
import os

class hltdConf:
    def __init__(self, conffile):
        #        logging.debug( 'opening config file '+conffile)
        #        print 'opening config file '+conffile
        #        f = file(conffile)
        #        print f
        cfg = ConfigParser.SafeConfigParser()
        cfg.read(conffile)

        self.role = None
        self.elastic_bu_test = None
        self.elastic_runindex_url = None
        self.elastic_runindex_name = 'runindex'
        self.watch_directory = None
        self.ramdisk_subdirectory = 'ramdisk'
        self.fastmon_insert_modulo = 1
        self.elastic_cluster = None
 
        for sec in cfg.sections():
            for item,value in cfg.items(sec):
                self.__dict__[item] = value

        self.run_number_padding = int(self.run_number_padding)
        self.use_elasticsearch = bool(self.use_elasticsearch=="True")
        self.cgi_port = int(self.cgi_port)
        self.process_restart_delay_sec = float(self.process_restart_delay_sec)
        self.process_restart_limit = int(self.process_restart_limit)
        self.cmssw_threads_autosplit = int(self.cmssw_threads_autosplit)
        self.cmssw_threads = int(self.cmssw_threads)
        self.service_log_level = getattr(logging,self.service_log_level)
        self.autodetect_parameters()

        #read cluster name from elastic search configuration file (used to specify index name)
        if not self.elastic_cluster and self.use_elasticsearch == True:
            f = None
            try:
                f=open('/etc/elasticsearch/elasticsearch.yml')
            except:
                pass
            if f is not None:
                lines = f.readlines()
                for line in lines:
                    sline = line.strip()
                    if line.startswith("cluster.name"):
                        self.elastic_cluster = line.split(':')[1].strip()
        if not self.elastic_cluster and self.use_elasticsearch == True and self.role != 'bu':
            logging.error("elasticsearch cluster name missing!")
      
    def dump(self):
        logging.info( 'self.exec_directory '+self.exec_directory)
        logging.info( 'self.user '+self.user)
        if conf.watch_directory:
            logging.info( 'self.watch_directory '+ self.watch_directory)
        logging.info( 'self.watch_prefix '+ self.watch_prefix)
        logging.info( 'self.watch_emu_prefix '+ self.watch_emu_prefix)
        logging.info( 'self.watch_end_prefix '+ self.watch_end_prefix)
        logging.info( 'self.bu_base_dir '+ self.bu_base_dir)
        logging.info( 'self.mount_command '+ self.mount_command)
#        logging.info( 'self.role '+ self.role)
        logging.info( 'self.resource_base '+ self.resource_base)
        logging.info( 'self.process_restart_delay_sec '+ str(self.process_restart_delay_sec))
        logging.info( 'self.process_restart_limit '+ str(self.process_restart_limit))
        logging.info( 'self.cmssw_base '+ self.cmssw_base)
        logging.info( 'self.cmssw_arch '+ self.cmssw_arch)
        logging.info( 'self.cmssw_default_version '+ self.cmssw_default_version)
        logging.info( 'self.cmssw_script_location '+ self.cmssw_script_location)
        logging.info( 'self.cmssw_threads_autosplit '+ str(self.cmssw_threads_autosplit))
        logging.info( 'self.cmssw_threads '+ str(self.cmssw_threads))
        logging.info( 'self.test_hlt_config '+ self.test_hlt_config1)
        logging.info( 'self.test_bu_config '+ self.test_bu_config)
        logging.info( 'self.service_log_level '+str(self.service_log_level))

    def autodetect_parameters(self):
        if not self.role and 'bu' in os.uname()[1]:
            self.role = 'bu'
        elif not self.role:
            self.role = 'fu'
        if not self.watch_directory:
            if self.role == 'bu': self.watch_directory='/fff/ramdisk'
            if self.role == 'fu': self.watch_directory='/fff/data'


conf = hltdConf('/etc/hltd.conf')
