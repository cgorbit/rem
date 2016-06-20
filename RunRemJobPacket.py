import os
import json
import re
import types
import shutil
import logging
import base64
from pprint import pformat
import glob
import sys

from common.types.client import Tag

from projects import resource_types as rt
from sandboxsdk.task import SandboxTask
from sandboxsdk.process import run_process
from sandboxsdk import parameters
#from sandboxsdk.errors import SandboxTaskFailureError


class RuntimeErrorWithCode(RuntimeError):
    pass


def reraise(code):
    t, e, tb = sys.exc_info()
    if isinstance(e, RuntimeErrorWithCode):
        raise
    raise RuntimeErrorWithCode, RuntimeErrorWithCode(code, e), tb


class RunRemJobPacket(SandboxTask):
    '''REM job-packet executor'''

    type = 'RUN_REM_JOBPACKET'

    client_tags = Tag.GENERIC & Tag.LINUX_PRECISE & ~Tag.OXYGEN

    class Executor(parameters.ResourceSelector):
        name = "executor_resource"
        description = "executor_resource"
        resource_type = rt.REM_JOBPACKET_EXECUTOR
        required = True

    class ExecutionSnapshotData(parameters.SandboxStringParameter):
        name = "snapshot_data"
        description = "snapshot_data"
        required = False
        multiline = True

    class ExecutionSnapshotResource(parameters.ResourceSelector):
        name = "snapshot_resource_id"
        description = "snapshot_resource_id"
        resource_type = rt.REM_JOBPACKET_EXECUTION_SNAPSHOT
        required = False

    class RemServerAddr(parameters.SandboxStringParameter):
        name = "rem_server_addr"
        description = "rem_server_addr"
        required = True

    class CustomResourcesDescr(parameters.SandboxStringParameter):
        name = "custom_resources"
        multiline = True
        description = "custom resources"
        required = False

    # For cache-locality of resources
    class CustomResources(parameters.ResourceSelector):
        name = "custom_resources_locality_enforcer"
        resource_type = None
        description = "custom resources"
        multiple = True
        required = False

    class PythonVirtualEnvironment(parameters.ResourceSelector):
        name = "python_resource"
        description = "python"
        resource_type = rt.REM_JOBPACKET_PYTHON
        required = True

    input_parameters = [
        RemServerAddr,
        Executor,
        ExecutionSnapshotData,
        ExecutionSnapshotResource,
        CustomResources,
        CustomResourcesDescr,
        PythonVirtualEnvironment,
    ]

    def arcadia_info(self):
        return '', None, 1

    @classmethod
    def __parse_custom_resources_any(cls, resources):
        if isinstance(resources, types.StringTypes):
            resources = resources.strip()

            if resources:
                if resources[0] == '=':
                    resources = resources[1:]
                return list(cls.__parse_custom_resources(json.loads(resources)))
            else:
                return None

        elif isinstance(resources, dict):
            return list(cls.__parse_custom_resources(resources))

        else:
            return None

    @staticmethod
    def __parse_custom_resources(raw_resources):
        for local_name, full_path in raw_resources.items():
            m = re.match('^(?:sbx:)(\d+)(/.*)?$', full_path)
            if not m:
                raise RuntimeErrorWithCode('E_MALFORMED_RESOURCE_PATH', None)

            resource_id, in_resource_path = m.groups()

            yield int(resource_id), \
                  in_resource_path or None, \
                  local_name
                  #None if in_resource_path and '*' in in_resource_path else local_name

    def __sync_custom_resources(self, file_map):
        logging.debug('__sync_custom_resources(%s)' % file_map)

        custom_resources_ids = {
            resource_id
                for resource_id, in_content_path, local_name in file_map
        }

        logging.debug('resource_ids: %s' % custom_resources_ids)

        resources = {}

        for resource_id in custom_resources_ids:
            try:
                resources[resource_id] = self.sync_resource(resource_id)
            except:
                reraise('E_CUSTOM_RESOURCE_SYNC_FAILED')

        logging.debug('resource: %s' % resources)

        landing_folder = 'custom_resources'
        target_prefix = 'work/root'

        symlinks = set()

        def symlink(target, name):
            if os.path.lexists(name):
                os.unlink(name) # TODO Be more strict: raise

            logging.debug('os.user.symlink(%s, %s)' % (target, name))
            os.symlink(target, name)
            symlinks.add(name)

        for resource_id, in_content_path, local_name in file_map:
            resource_path = resources[resource_id]
            logging.debug('iter -> %s' % ((resource_id, in_content_path, local_name),))

            if in_content_path:
                if os.path.isdir(resource_path):
                    content_path = landing_folder + '/%s' % resource_id

                    logging.debug('treat as folder with content')
                    logging.debug('os.symlink(%s, %s)' % (resource_path, content_path))

                    if not os.path.lexists(content_path):
                        os.symlink(resource_path, content_path)

                elif resource_path.endswith('.tar') \
                        or resource_path.endswith('.tar.gz'):

                    content_path = landing_folder + '/%s_x' % resource_id

                    logging.debug('treat as archive with content')
                    logging.debug('os.mkdir(%s)' % content_path)

                    if not os.path.exists(content_path):
                        os.mkdir(content_path)
                        run_process(['tar', '-C', content_path, '-xf', resource_path])

                full_path = content_path + in_content_path

                files = glob.glob(full_path)
                logging.debug('glob(%s) => %s' % (full_path, files))

                if os.path.exists(full_path) or os.path.lexists(full_path):
                    symlink('../../' + full_path, target_prefix + '/' + local_name)

                elif files:
                    for file in files:
                        symlink('../../' + file, target_prefix + '/' + os.path.basename(file))

                else:
                    raise RuntimeErrorWithCode(
                        'E_NO_FILE_IN_RESOURCE',
                        "No '%s' file in resource %d" % (in_content_path, resource_id))

            else:
                landing_path = landing_folder + '/%d' % resource_id
                if not os.path.lexists(landing_path):
                    os.symlink(resource_path, landing_path)

                target_path = target_prefix + '/' + local_name
                symlink('../../' + landing_path, target_path)

            #logging.debug('symlink(%s, %s)' % (target, name))
        self.ctx['_created_symlinks'] = list(symlinks)

    @property
    def __custom_resources(self):
        return self.ctx['_custom_resources_parsed']

    def __do_on_execute(self):
        logging.debug("on_execute work dir: %s" % os.getcwd())

        logging.debug(pformat(self.ctx))
        logging.debug(type(self.ctx['custom_resources']))

        try:
            python_virtual_env_path = self.sync_resource(int(self.ctx['python_resource']))
        except:
            reraise('E_PYTHON_RESOURCE_SYNC_FAILED')

        os.mkdir('python')
        run_process(['tar', '-C', 'python', '-xf', python_virtual_env_path])
        python_virtual_env_bin_directory = os.path.abspath('./python/bin')

        os.mkdir('custom_resources')
        os.mkdir('work')

        prev_packet_snapshot_file = None
        if self.ctx['snapshot_resource_id']:
            try:
                prev_snapshot_path = self.sync_resource(int(self.ctx['snapshot_resource_id']))
            except:
                reraise('E_SNAPSHOT_RESOURCE_SYNC_FAILED')

            if False:
                for subdir in ['io', 'root']:
                    shutil.copytree(
                        prev_snapshot_path + '/' + subdir,
                        'work/' + subdir,
                        symlinks=True
                    )
                prev_packet_snapshot_file = prev_snapshot_path + '/' + 'packet.pickle'

            else:
                run_process(['tar', '-C', 'work', '-xf', prev_snapshot_path])
                prev_packet_pickle_basename = 'prev_packet.pickle'
                os.rename('work/packet.pickle', prev_packet_pickle_basename)
                prev_packet_snapshot_file = prev_packet_pickle_basename

        # snapshot_data may be E2BIG for execve
        elif self.ctx['snapshot_data']:
        # TODO DRY
            prev_packet_snapshot_file = 'initial_snapshot.pickle'

            with open(prev_packet_snapshot_file, 'w') as out:
                out.write(
                    base64.b64decode(
                        self.ctx['snapshot_data'].replace('\n', '')))


            os.mkdir('work/io')
            os.mkdir('work/root')

        else:
            raise RuntimeErrorWithCode('E_NO_PREV_SNAPSHOT_SETUP', None)

        try:
            executor_path = self.sync_resource(int(self.ctx['executor_resource']))
        except:
            reraise('E_EXECUTOR_RESOURCE_SYNC_FAILED')

        os.mkdir('executor')
        run_process(['tar', '-C', 'executor', '-xf', executor_path])

        if '_custom_resources_parsed' not in self.ctx:
            try:
                self.ctx['_custom_resources_parsed'] \
                    = self.__parse_custom_resources_any(self.ctx['custom_resources'])
            except:
                reraise('E_MALFORMED_CUSTOM_RESOURCES')

        custom_resources = self.__custom_resources
        if custom_resources:
            try:
                self.__sync_custom_resources(custom_resources)
            except:
                reraise('E_CUSTOM_RESOURCES_SETUP_FAILED')

        packet_snapshot_file = 'work/packet.pickle'
        last_update_message_file = 'last_update_message.pickle'

        argv = [
            './executor/run_sandbox_packet.py',
                '--work-dir', 'work/root',
                '--io-dir',   'work/io',
                '--task-id', str(self.id),
                '--rem-server-addr', self.ctx['rem_server_addr'],
                '--result-snapshot-file', packet_snapshot_file,
                '--last-update-message-file', last_update_message_file,
                # FIXME This option was to ensure that
                #'--result-status-file=result.json',
        ]

    # FIXME What for?
        #if custom_resources:
            #orig = self.ctx['custom_resources']
            #if orig[0] == '=':
                #orig = orig[1:]
            #argv.extend(['--custom-resources', json.dumps(orig)])

        #if prev_packet_snapshot_file:
        argv.extend(['--snapshot-file', prev_packet_snapshot_file])

        #elif self.ctx['snapshot_data']:
            #argv.extend(['--snapshot-data', self.ctx['snapshot_data'].replace('\n', '')])

        #else:
            #raise SandboxTaskFailureError()

        env = os.environ.copy()
        env['PATH'] = python_virtual_env_bin_directory + ':' + env.get('PATH', '')
        env['PYTHONPATH'] = env.get('PYTHONPATH', '') \
            + ':' + os.path.abspath('executor') + '/client'

        run_process(argv, environment=env, log_prefix='executor')

        if '_created_symlinks' in self.ctx:
            for name in self.ctx['_created_symlinks']:
                try:
                    os.unlink(name)
                except:
                    pass

        # TODO XXX Checks (at least for snapshot_file existence)

        # TODO ttl

        run_process(['tar', '-C', 'work', '-cf', 'work.tar', './'])
        self.create_resource(
            '',
            'work.tar',
            rt.REM_JOBPACKET_EXECUTION_SNAPSHOT)

        self.create_resource(
            '',
            last_update_message_file,
            rt.REM_JOBPACKET_GRAPH_UPDATE)

        #rt.REM_JOBPACKET_EXECUTION_RESULTS

    def on_execute(self):
        self.__run(self.__do_on_execute)

    def __run(self, f):
        try:
            return f()
        except RuntimeErrorWithCode:
            t, e, tb = sys.exc_info()
            code, orig_error = e.args
            self.ctx['__last_rem_error'] = (code, str(orig_error))
            if isinstance(orig_error, Exception):
                raise type(orig_error), orig_error, tb
            else:
                raise t, e, tb
        except Exception as e:
            self.ctx['__last_rem_error'] = ('E_UNKNOWN', str(e))
            raise

__Task__ = RunRemJobPacket
