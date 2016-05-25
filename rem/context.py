import os
from ConfigParser import ConfigParser, NoOptionError
import rem_logging

class ConfigReader(ConfigParser):
    def safe_get(self, section, option, default=""):
        try:
            return self.get(section, option)
        except NoOptionError:
            return default

    def safe_getint(self, section, option, default=0):
        try:
            return self.getint(section, option)
        except NoOptionError:
            return default

    def safe_getboolean(self, section, option, default=False):
        try:
            return self.getboolean(section, option)
        except NoOptionError:
            return default

    def safe_getlist(self, section, option, default=None):
        try:
            value = self.get(section, option)
            return [item.strip() for item in value.split(",") if item.strip()]
        except NoOptionError:
            return default or []


class Context(object):
    @classmethod
    def prep_dir(cls, dir_name):
        dir_name = os.path.abspath(dir_name)
        if not os.path.isdir(dir_name):
            os.makedirs(dir_name)
        if not os.path.isdir(dir_name):
            raise RuntimeError("can't create directory: \"%s\"" % dir_name)
        return dir_name

    def __init__(self, config_file):
        config = ConfigReader()
        assert config_file in config.read(config_file), 'error in configuration file "%s"' % config_file
        self._init_config_options(config)

    def _init_config_options(self, config):
        self.log_directory = self.prep_dir(config.get("log", "dir"))
        self.log_filename = config.get("log", "filename")
        self.log_backup_count = config.getint("log", "rollcount")
        self.log_warn_level = config.get("log", "warnlevel")
        self.log_to_stderr = False
        self.packets_directory = self.prep_dir(config.get("store", "pck_dir"))
        self.backup_directory = self.prep_dir(config.get("store", "backup_dir"))
        self.backup_period = config.getint("store", "backup_period")
        self.backup_count = config.getint("store", "backup_count")
        self.backup_in_child = config.safe_getboolean("store", "backup_in_child", False)
        self.backup_child_max_working_time = config.getint("store", "backup_child_max_working_time")
        self.backup_fork_lock_friendly_timeout = config.safe_getint("store", "backup_fork_lock_friendly_timeout", None)
        self.backups_enabled = config.safe_getboolean("store", "backups_enabled", True)
        self.use_ekrokhalev_server_process_title = config.safe_getboolean("run", "use_ekrokhalev_server_process_title", True)
        self.journal_lifetime = config.getint("store", "journal_lifetime")
        self.binary_directory = self.prep_dir(config.get("store", "binary_dir"))
        self.binary_lifetime = config.getint("store", "binary_lifetime")
        self.errored_packet_lifetime = config.getint("store", "error_packet_lifetime")
        self.successfull_packet_lifetime = config.getint("store", "success_packet_lifetime")
        self.tags_db_file = config.get("store", "tags_db_file")
        self.recent_tags_file = config.get("store", "recent_tags_file")
        self.remote_tags_db_file = config.safe_get("store", "remote_tags_db_file")
        self.fix_bin_links_at_startup = config.safe_getboolean("store", "fix_bin_links_at_startup", True)
        self.working_job_max_count = config.getint("run", "poolsize")
        self.subprocsrv_runner_count = config.safe_getint("run", "subprocsrv_runner_count", 0)
        self.xmlrpc_pool_size = config.safe_getint("run", "xmlrpc_poolsize", 1)
        self.readonly_xmlrpc_pool_size = config.safe_getint("run", "readonly_xmlrpc_pool_size", 1)
        self.pgrpguard_binary = config.safe_get("run", "pgrpguard_binary", None)
        self.pgrpguard_binary = config.safe_get("run", "process_wrapper", self.pgrpguard_binary)
        self.sandbox_api_url = config.safe_get("run", "sandbox_api_url", None)
        self.sandbox_api_token = config.safe_get("run", "sandbox_api_token", None)
        self.sandbox_task_owner = config.safe_get("run", "sandbox_task_owner", None)
        self.sandbox_task_max_count = config.safe_getint("run", "sandbox_task_max_count", 50)
        self.cloud_tags_server = config.safe_get("store", "cloud_tags_server", None)
        self.cloud_tags_masks = config.safe_get("store", "cloud_tags_masks", None)
        self.cloud_tags_masks_reload_interval = config.safe_getint("store", "cloud_tags_masks_reload_interval", 300)
        self.cloud_tags_release_delay = 7200
        self.tags_random_cloudiness = config.safe_getboolean("store", "tags_random_cloudiness", False)
        self.all_tags_in_cloud = config.safe_getboolean("store", "all_tags_in_cloud", False)
        self.allow_startup_tags_conversion = config.safe_getboolean("store", "allow_startup_tags_conversion", True)
        self.manager_port = config.getint("server", "port")
        self.manager_readonly_port = config.safe_getint("server", "readonly_port")
        self.system_port = config.safe_getint("server", "system_port")
        self.network_topology = config.safe_get("server", "network_topology")
        self.network_name = config.safe_get("server", "network_hostname")
        self.send_emails = config.getboolean("server", "send_emails")
        self.send_emergency_emails = config.safe_getboolean("server", "send_emergency_emails")
        self.mailer_thread_count = config.safe_getint("server", "mailer_thread_count", 1)
        self.use_memory_profiler = config.getboolean("server", "use_memory_profiler")
        self.max_remotetags_resend_delay = config.safe_getint("server", "max_remotetags_resend_delay", 300)
        self.allow_backup_rpc_method = config.safe_getboolean("server", "allow_backup_rpc_method", False)
        self.register_objects_creation = False

    def send_email_async(self, rcpt, msg):
        self.Scheduler.send_email_async(rcpt, msg)

    def registerScheduler(self, scheduler):
        if getattr(self, "Scheduler", None):
            raise RuntimeError("can't relocate scheduler for context object")
        self.Scheduler = scheduler
