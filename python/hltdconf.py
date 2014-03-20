import ConfigParser
import logging

class hltdConf:
    def __init__(self, conffile):
        #        logging.debug( 'opening config file '+conffile)
        #        print 'opening config file '+conffile
        #        f = file(conffile)
        #        print f
        cfg = ConfigParser.SafeConfigParser()
        cfg.read(conffile)
        #        cfg.optionxform = str
        #        print cfg.sections()
        #logging.debug(cfg.sections())
        self.exec_directory = cfg.get('General','exec_directory')
        self.user = cfg.get('General','user')
        self.watch_directory = cfg.get('General','watch_directory')
        self.micromerge_output = cfg.get('General','micromerge_output')
        self.watch_prefix = cfg.get('General','watch_prefix')
        self.watch_emu_prefix = cfg.get('General','watch_emu_prefix')
        self.watch_end_prefix = cfg.get('General','watch_end_prefix')
        self.bu_base_dir = cfg.get('General','bu_base_dir')
        self.run_number_padding = cfg.getint('General','run_number_padding')

        self.mount_command = cfg.get('General','mount_command')
        self.mount_type = cfg.get('General','mount_type')
        self.mount_options = cfg.get('General','mount_options')

        self.role = None
        if cfg.has_option('General','role'):
            self.role = cfg.get('General','role')

        self.use_elasticsearch = cfg.getboolean('Monitoring','use_elasticsearch')
        self.elastic_log = cfg.get('Monitoring','elastic_log')

        self.cgi_port = cfg.getint('Web','cgi_port')


        self.resource_base = cfg.get('Resources','resource_base')

        self.process_restart_delay_sec = cfg.getfloat('Recovery','process_restart_delay_sec')
        self.process_restart_limit = cfg.getint('Recovery','process_restart_limit')

        self.cmssw_base = cfg.get('CMSSW','cmssw_base')
        self.cmssw_arch = cfg.get('CMSSW','cmssw_arch')
        self.cmssw_default_version = cfg.get('CMSSW','cmssw_default_version')
        self.cmssw_threads = cfg.getint('CMSSW','cmssw_threads')
        self.cmssw_script_location = cfg.get('CMSSW','cmssw_script_location')
        self.test_hlt_config1 = cfg.get('CMSSW','test_hlt_config1')
        self.test_hlt_config2 = cfg.get('CMSSW','test_hlt_config2')
        self.test_bu_config = cfg.get('CMSSW','test_bu_config')

        self.menu_directory = cfg.get('HLT','menu_directory')
        self.menu_name = cfg.get('HLT','menu_name')
        self.arch_file = cfg.get('HLT','arch_file')
        self.version_file = cfg.get('HLT','version_file')

        self.service_log_level = getattr(logging,cfg.get('Logs','service_log_level'))
        self.service_log = cfg.get('Logs','service_log')
        self.hlt_log = cfg.get('Logs','hlt_log')

    def dump(self):

        logging.info( 'self.exec_directory '+self.exec_directory)
        logging.info( 'self.user '+self.user)
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
        logging.info( 'self.test_hlt_config '+ self.test_hlt_config1)
        logging.info( 'self.test_bu_config '+ self.test_bu_config)
        logging.info( 'self.service_log_level '+str(self.service_log_level))
        logging.info( 'self.service_log '+self.service_log)
        logging.info( 'self.hlt_log '+self.hlt_log)
