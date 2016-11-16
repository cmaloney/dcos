import os.path
import uuid
from collections import namedtuple

from ssh.ssh_runner import Node
from pkgpanda.util import load_json, write_json
from dcos_installer.action_lib import cli_steps, web_steps

"""Code to manage / maintain the state of all the hosts in the deployment.

The state is updated asynchronously by the active ssh runners. Actual host state
can always be inspected by asking a host
 - There is a file which marks the current stage of an agent
 - There is a file which indicates what stages have executed so far on an agent
 - All this is in the "installer tmp dir" (/opt/dcos_install_tmp) by default,
   along with a wrapper which will update the states so we can do one ssh
   execute command to do a step.
 - the "work dir" inside that is cleaned up automatically between stages
 - After install is complete, then all the installer tmp dir should be removed.

On the deploy / bootstrap host, there is a state file which tracks
 - The set of hosts being deployed to
 - What state was last seen for every host
 - Marking whether or not to keep trying to do new steps on a host

State updates can be dispatched realtime (used at console for status) or they
can be fetched in a group. Web could eventually use a websocket or something
similar.
"""

# TODO(cmaloney): need to log stdout / stderr / commands run on every host into
# files for easier debugging.
HostState = namedtuple('HostState', ['stage', 'step', 'skip', 'tags', 'node'])

run_mode_dict = {
    'web': web_steps,
    'cli': cli_steps
}

# TODO(cmaloney): assert no stage or stage step has the same name
pre_marker = '_pre'
post_marker = '_post'


class InstallStatus():
    """Gathers host results"""

    def __init__(self, json_dump_filename, run_mode):
        self._json_dump_filename = json_dump_filename
        self._run_mode = run_mode
        self._hosts = dict()
        self._current_stage = None
        self._run_id = None

        if os.path.exists(self._json_dump_filename):
            self._load()

    def initialize(self, master_list, agent_list, agent_public_list, path):
        if os.path.exists(self._json_dump_filename):
            raise Exception(
                "Can't start a new install, existing install progress found. "
                "To start a new install remove {}. To resume the in-progress "
                "install run the {} installer".format(
                    self._json_dump_filename,
                    self._run_mode))

        def add_all(host_list, tags):
            for host_id in host_list:
                # The repeat hosts should be checked by the validation
                # master_list, agent_list, and agent_public_list all have
                # distinct hosts in config validation.
                assert host_id not in self._hosts, "no repeat hosts allowed"
                self._hosts[host_id] = HostState(pre_marker, pre_marker, False, tags, Node(host_id, tags))

        add_all(master_list, {'role': 'master'})
        add_all(agent_list, {'role': 'slave'})
        add_all(agent_public_list, {'role', 'slave_public'})
        self._current_stage = pre_marker
        self._run_id = uuid.uuid4().hex

        self.checkpoint()

    def update(self, host_id, stage, step):
        assert host_id in self._hosts

        self._hosts[host_id].stage = stage
        self._hosts[host_id].step = step

    def mark_skip(self, host_id, skip_state):
        assert host_id in self._hosts

        host = self._hosts[host_id]

        if host.skip == skip_state:
            # Already at the desired state
            return

        if host.stage != self._current_stage:
            raise Exception("Host cannot be changed because it is in install "
                            "stage {} which isn't the current stage: {}".format(
                                host.stage, self._current_stage))

        host.skip = skip_state

    def get_active_nodes(self):
        active_nodes = list()
        for state in self._hosts.values():
            if state.skip:
                pass
            active_nodes.append(state.node)
        return active_nodes

    def _load(self):
        json_state = load_json(self._json_dump_filename)

        if json_state['run_mode'] != self._run_mode:
            raise Exception(
                "Hosts must be installed entirely using a single install "
                "method / method can't be changed halfway through. Install "
                "was started with `{}` but the current installer is `{}`"
                .format(json_state['run_mode'], self._run_mode))

        if self._run_id is None:
            self._run_id = json_state['run_id']
        else:
            if self._run_id != json_state['run_id']:
                raise Exception(
                    "Loaded data from a different installer run than expected. "
                    "Expected `{}` but got `{}`. Are two copies of the "
                    "installer running?".format(self._run_id, json_state['run_id']))

        self._current_stage = json_state['current_stage']

        for host in json_state['hosts']:
            self._hosts[host['id']] = HostState(host['stage'], host['step'], host['skip'],
                                                host['tags'], Node(host['id'], host['tags']))

    def checkpoint(self):
        json_state = {
            'run_mode': self._run_mode,
            'current_stage': self._current_stage,
            'run_id': self._run_id,
            'hosts': list(),
        }
        for id_, state in self._hosts.items():
            json_state['hosts'].append({
                'id': id_,
                'stage': state.stage,
                'step': state.step,
                'skip': state.skip,
                'tags': state.tags})

        write_json(self._json_dump_filename, json_state)


class Runner():
    """Runs actions from action_lib, storing results into the collector"""
    pass
