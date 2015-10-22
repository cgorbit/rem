#!/usr/bin/env python

import logging
import os
import subprocess
import shutil
import sys
import tempfile
import unittest
import json
from ConfigParser import ConfigParser

sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), "client"))
import remclient
import testdir


class ClientInfo(object):
    def __init__(self, name, projectDir, hostname):
        self.name = name
        self.projectDir = projectDir.split("://", 1)[-1]
        configPath = os.path.join(projectDir, 'rem.cfg')
        tmp_dir = tempfile.mkdtemp(dir=".", prefix="configuration-")
        try:
            cp = self.LoadConfiguration(configPath, tmp_dir)
        finally:
            if os.path.isdir(tmp_dir):
                shutil.rmtree(tmp_dir)
        self.binDir = cp.get('store', 'binary_dir')
        self.url = "http://%s:%d" % (hostname, cp.getint("server", "port"))
        self.admin_url = "http://%s:%d" % (hostname, cp.getint("server", "system_port"))
        self.readonly_url = "http://%s:%d" % (hostname, cp.getint("server", "readonly_port"))
        self.connector = remclient.Connector(self.url, verbose=True, packet_name_policy=remclient.IGNORE_DUPLICATE_NAMES_POLICY)
        self.admin_connector = remclient.AdminConnector(self.admin_url, verbose=True)
        self.readonly_connector = remclient.Connector(self.readonly_url, verbose=True, packet_name_policy=remclient.IGNORE_DUPLICATE_NAMES_POLICY)

    def LoadConfiguration(self, config_path, tmpdir):
        if config_path.startswith("svn+ssh://"):
            config_temporary_path = os.path.join(tmpdir, os.path.basename(config_path))
            subprocess.check_call(
                ["svn", "export", "--force", "--non-interactive", "-q", config_path, config_temporary_path])
        elif config_path.startswith("local://"):
            config_temporary_path = config_path[8:]
            self.path = os.path.dirname(config_temporary_path)
        elif os.path.isfile(config_path):
            config_temporary_path = config_path
        else:
            raise RuntimeError("not implemented scheme type for location %s" % config_path)
        cp = ConfigParser()
        assert config_temporary_path in cp.read(config_temporary_path)
        return cp


class Configuration(object):
    @staticmethod
    def __get_notify_email():
        def _load_config(filename):
            with open(filename) as in_:
                return json.load(in_)

        email = os.environ.get('REM_TESTS_NOTIFY_EMAIL', None)
        if email is not None:
            return email

        home = os.environ.get('HOME', None)
        if home is not None:
            config_filename = home + '/.remrc'
            if os.path.exists(config_filename):
                return _load_config(config_filename).get('tests', {}).get('notify_email', None)

    @classmethod
    def GetLocalConfig(cls):
        config = cls()
        config.server1 = ClientInfo("local-01", "local://./local-01/", "localhost")
        config.server2 = ClientInfo("local-02", "local://./local-02/", "localhost")
        config.notify_email = cls.__get_notify_email()
        if not config.notify_email:
            raise RuntimeError("Can't determine notify_email for tests")
        return config

    @staticmethod
    def __sync_dir(srcdir, dstdir, paths):
        for p in paths:
            srcp, dstp = os.path.join(srcdir, p), os.path.join(dstdir, p)
            try:
                if os.path.isdir(dstp) and not os.path.islink(dstp):
                    shutil.rmtree(dstp)
                else:
                    os.unlink(dstp)
            except OSError:
                logging.exception("rem servers synchronization")
            if os.path.islink(srcp):
                os.symlink(os.readlink(srcp), dstp)
            elif os.path.isfile(srcp):
                shutil.copy2(srcp, dstp)
            elif os.path.isdir(srcp):
                shutil.copytree(srcp, dstp)
            else:
                raise RuntimeError("syncdir unexpected situation")

    def setUp(self):
        path1 = getattr(getattr(self, "server1", None), "path", None)
        path2 = getattr(getattr(self, "server2", None), "path", None)
        if path1 and path2:
            testdir.common.RestartService(path1)
            with testdir.common.ServiceTemporaryShutdown(path2):
                self.__sync_dir(path1, path2, ["client", "rem", "rem-server.py", "start-stop-daemon.py", "setup_env.sh",
                                               "network_topology.cfg"])


if __name__ == "__main__":
    config = Configuration.GetLocalConfig()
    testdir.setUp(config, "userdata")
    unittest.TestProgram(module=testdir)
