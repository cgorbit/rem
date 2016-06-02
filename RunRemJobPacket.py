import os
import json
import re
import types
import shutil
import logging
import base64

from projects import resource_types as rt
from sandboxsdk.task import SandboxTask
from sandboxsdk.process import run_process
from sandboxsdk import parameters
from sandboxsdk.errors import SandboxTaskFailureError


class RunRemJobPacket(SandboxTask):
    '''REM job-packet executor'''

    type = 'RUN_REM_JOBPACKET'

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
        PythonVirtualEnvironment, # TODO
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
                raise SandboxTaskFailureError()

            resource_id, in_resource_path = m.groups()

            yield int(resource_id), in_resource_path, local_name

    def __sync_custom_resources(self, resources):
        logging.debug('__sync_custom_resources(%s)' % resources)

        custom_resources_ids = set()

        for resource_id, in_resource_path, local_name in resources:
            custom_resources_ids.add(resource_id)

            target = '../../custom_resources/%d/%s' % (resource_id, in_resource_path)
            name = 'work/root/%s' % local_name

            logging.debug('symlink(%s, %s)' % (target, name))
            os.symlink(target, name)

        logging.debug('resource_ids: %s' % custom_resources_ids)

        for resource_id in custom_resources_ids:
            res_real_path = self.sync_resource(resource_id)
            os.symlink(res_real_path, 'custom_resources/%d' % resource_id)

    def __unlink_custom_resources(self, resources):
        for resource_id, in_resource_path, local_name in resources:
            try:
                os.unlink('work/root/%s' % local_name)
            except:
                pass

    @property
    def __custom_resources(self):
        return self.ctx['custom_resources_parsed']

    def on_execute(self):
        logging.debug("on_execute work dir: %s" % os.getcwd())
        from pprint import pformat

        logging.debug(pformat(self.ctx))
        logging.debug(type(self.ctx['custom_resources']))

        python_virtual_env_path = self.sync_resource(int(self.ctx['python_resource']))
        os.mkdir('python')
        run_process(['tar', '-C', 'python', '-zxf', python_virtual_env_path])
        python_virtual_env_bin_directory = os.path.abspath('./python/bin')

        os.mkdir('custom_resources')
        os.mkdir('work')

        prev_packet_snapshot_file = None
        if self.ctx['snapshot_resource_id']:
            prev_snapshot_path = self.sync_resource(int(self.ctx['snapshot_resource_id']))

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
# TODO DRY
# TODO DRY
            prev_packet_snapshot_file = 'initial_snapshot.pickle'

            with open(prev_packet_snapshot_file, 'w') as out:
                out.write(
                    base64.b64decode(
                        self.ctx['snapshot_data'].replace('\n', '')))


            os.mkdir('work/io')
            os.mkdir('work/root')

        executor_path = self.sync_resource(int(self.ctx['executor_resource']))
        if False:
            os.symlink(executor_path, 'executor')
        else:
            os.mkdir('executor')
            run_process(['tar', '-C', 'executor', '-xf', executor_path])

        if 'custom_resources_parsed' not in self.ctx:
            self.ctx['custom_resources_parsed'] \
                = self.__parse_custom_resources_any(self.ctx['custom_resources'])

        custom_resources = self.__custom_resources
        if custom_resources:
            self.__sync_custom_resources(custom_resources)

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

        if custom_resources:
            orig = self.ctx['custom_resources']
            if orig[0] == '=':
                orig = orig[1:]
            argv.extend(['--custom-resources', json.dumps(orig)])

        if prev_packet_snapshot_file:
            argv.extend(['--snapshot-file', prev_packet_snapshot_file])

        #elif self.ctx['snapshot_data']:
            #argv.extend(['--snapshot-data', self.ctx['snapshot_data'].replace('\n', '')])

        else:
            raise SandboxTaskFailureError()

        env = os.environ.copy()
        env['PATH'] = python_virtual_env_bin_directory + ':' + env.get('PATH', '')
        run_process(argv, environment=env, log_prefix='executor')

        # This actually not needed for tar-archive-resource, only for raw file-tree
        if custom_resources:
            self.__unlink_custom_resources(custom_resources)

        # TODO XXX Checks (at least for snapshot_file existence)

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


__Task__ = RunRemJobPacket
