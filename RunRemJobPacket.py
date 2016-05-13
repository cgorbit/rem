import os
import sys
import json
import re
import types
import logging
import subprocess

from projects import resource_types as rt
from sandboxsdk.channel import channel
from sandboxsdk.task import SandboxTask
from sandboxsdk.process import run_process
from sandboxsdk import parameters


class RunRemJobPacket(SandboxTask):
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
        name = "snapshot_resource"
        description = "snapshot_resource"
        resource_type = rt.REM_JOBPACKET_EXECUTION_SNAPSHOT
        required = False

    class RemServerAddr(parameters.SandboxStringParameter):
        name = "rem_server_addr"
        description = "rem_server_addr"
        required = True

    #class CustomResources(parameters.ListRepeater, parameters.SandboxStringParameter):
    #class CustomResources(parameters.DictRepeater, parameters.SandboxStringParameter):
    class CustomResources(parameters.SandboxStringParameter):
        name = "custom_resources"
        description = "custom resources"
        multiline = True
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
        #PythonVirtualEnvironment, # TODO UNCOMMENT
    ]

    def arcadia_info(self):
        return '', None, 1

    def __init_custom_resources_param(self):
        custom_resources = self.ctx['custom_resources']

        if isinstance(custom_resources, types.StringTypes):
            custom_resources = custom_resources.strip()

            self.ctx['custom_resources'] = json.loads(custom_resources) \
                if custom_resources else None

    def __sync_custom_resources(self, custom_resources):
        custom_resources_ids = set()

        for name, full_path in custom_resources.items():
            m = re.match('^(?:sbx:)(\d+)(/.*)?$', full_path)
            if not m:
                raise SandboxTaskFailureError()
            resource_id, in_resource_path = m.groups()
            resource_id = int(resource_id)

            custom_resources_ids.add(resource_id)

            os.symlink(
                '../custom_resources/%d/%s' % (resource_id, in_resource_path),
                'work/%s' % name)

        for resource_id in custom_resources_ids:
            res_real_path = self.sync_resource(resource_id)
            os.symlink(res_real_path, 'custom_resources/%d' % resource_id)

    def on_execute(self):
        logging.debug("on_execute work dir: %s" % os.getcwd())

        os.mkdir('work')
        os.mkdir('python')
        os.mkdir('io')
        os.mkdir('custom_resources')

        executor_path = self.sync_resource(int(self.ctx['executor_resource']))
        if False:
            os.symlink(executor_path, 'executor')
        else:
            os.mkdir('executor')
            run_process(['tar', '-C', 'executor', '-xf', executor_path])

        self.__init_custom_resources_param()

        custom_resources = self.ctx['custom_resources']
        if custom_resources:
            self.__sync_custom_resources(custom_resources)

        argv = [
            './executor/sbx_run_packet.py',
                '--work-dir', 'work',
                '--io-dir',   'io',
                '--task-id', self.id, # FIXME
                '--rem-server-addr', self.ctx['rem_server_addr'],
                '--result-snapshot-file=result-snapshot',
                #'--result-file=result.json', # FIXME Don't remember
        ]

        if custom_resources:
            argv.extend(['--custom-resources', json.dumps(custom_resources)])

        if self.ctx['snapshot_resource']:
            snapshot_resource_path = self.sync_resource(int(self.ctx['snapshot_resource']))
            argv.extend(['--snapshot-file', snapshot_resource_path])

        elif self.ctx['snapshot_data']:
            argv.extend(['--snapshot-data', self.ctx['snapshot_data'].replace('\n', '')])

        else:
            raise SandboxTaskFailureError()

        run_process(argv)

        #REM_JOBPACKET_EXECUTION_SNAPSHOT
        #REM_JOBPACKET_EXECUTION_RESULTS


__Task__ = RunRemJobPacket
