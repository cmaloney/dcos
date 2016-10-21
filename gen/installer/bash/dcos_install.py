# NOTE: avoid things that pull in lots of non-core python deps here to keep the install script
# minimal in size.
import argparse
import copy
import json
import logging
import os
import os.path
import shutil
import subprocess
import sys
from collections import namedtuple
from contextlib import contextmanager
from functools import partial

import pkgutil

log = logging.getLogger(__name__)

color_map = None

fail_color = ['red', 'bold']


def use_colors(yes):
    global color_map

    assert color_map is None

    if yes:
        color_map = {
            'red': '\e[1;31m',
            'bold': '\e[1m',
            'normal': '\e[0m'
        }
    else:
        color_map = {'red': '', 'bold': '', 'normal': ''}


def check_call(cmd, *args, **kwargs):
    print(' '.join(cmd))
    subprocess.check_call(cmd, *args, **kwargs)


def print_color(colors, msg, *args, **kwargs):
    assert color_map is not None
    if isinstance(colors, str):
        colors = [colors]

    color_setup = "".join(colors[color] for color in colors)
    print("{}{}{}".format(color_setup, msg.format(*args, **kwargs), colors['normal']))


def print_section(msg):
    print_color('bold', msg)


def print_fail(msg, *args, **kwargs):
    print_color(['bold', 'red'], msg, *args, **kwargs)


def read_used_ports(proc_filename) -> set:
    ports = set()
    with open(proc_filename) as f:
        # TODO(cmaloney): Make all this safer / error more cleanly.
        connection_iter = iter(f)
        header = connection_iter.next().split(' ')

        if header[1] != 'local_address':
            raise NotImplementedError(
                "{} has the second field as something other than local_address".format(proc_filename))

        for connection_str in connection_iter:
            local_addr = connection_str.split(' ')[1]
            [host, port_hex_str] = local_addr.rsplit(':', 1)
            ports.add(int(port_hex_str, 16))


def get_used_ports() -> set:
    # TODO(cmaloney): Support optionally enabled ipv6 (/proc/net/{tcp6,udp6})
    used_ports = read_used_ports('/proc/net/tcp')
    used_ports |= read_used_ports('/proc/net/udp')

    return used_ports


# Maps from port number -> service name
# TODO(cmaloney): Component name should match systemd service name
common_ports = {
    53: 'spartan',
    61420: 'epmd',
    61421: 'minuteman',
    62053: 'spartan',
    62080: 'navstar'}

agent_ports = {
    5051: 'mesos-agent',
    34451: 'navstar',
    39851: 'spartan',
    43995: 'minuteman',
    61001: 'agent-adminrouter'}

master_ports = {
    80: 'adminrouter',
    443: 'adminrouter',
    1050: '3dt',
    2181: 'zookeeper',
    5050: 'mesos-master',
    7070: 'cosmos',
    8080: 'marathon',
    8101: 'dcos-oauth',
    8123: 'mesos-dns',
    8181: 'exhibitor',
    9000: 'metronome',
    9942: 'metronome',
    9990: 'cosmos',
    15055: 'dcos-history',
    33107: 'navstar',
    36771: 'marathon',
    41281: 'zookeeper',
    42819: 'spartan',
    43911: 'minuteman',
    46839: 'metronome',
    61053: 'mesos-dns'}

CheckResult = namedtuple('CheckResult', ['passed', 'message', 'details'])


def fail(msg, **arguments):
    return CheckResult(False, msg.format(**arguments), arguments)


def success(msg, **arguments):
    return CheckResult(True, msg.format(**arguments), arguments)


def check_ports(is_agent):
    component_ports = copy.copy(common_ports)
    component_ports.update(agent_ports if is_agent else master_ports)

    used_ports = get_used_ports()

    # Find all common ports, return an error for every common port
    in_use_component_ports = set(component_ports.keys()) & used_ports

    for port in in_use_component_ports:
        yield fail('port {port} is required by DC/OS Component {component} but is in use',
                   port=port, component=component_ports[port])


def check_preexisting_dcos():
    # TODO(cmaloney): Check for /var/lib/dcos
    dcos_installed_files = [
        '/etc/systemd/system/dcos.target',
        '/etc/systemd/system/dcos.target.wants',
        '/opt/mesosphere',
        '/var/lib/dcos',
        '/run/dcos']

    for file in dcos_installed_files:
        if os.path.exists(file):
            yield success('No such folder', file)
        else:
            yield fail('Found folder from previous install: {}', file)


def check_selinux():
    enabled = subprocess.check_output('getenforce').decode().strip()
    if enabled == 'Enforcing':
        return fail('SELinux is currently Enforcing, must be disabled for DC/OS to operate')


def check_binary_in_path(name):
    # TODO(cmaloney): Need a better way to say "Checking X: <late-binding-status>".
    # Probably a context manager.
    path = shutil.which(name)

    if path is None:
        return fail('Missing dependency {}', name)
    else:
        return success('Found binary for dependecy {}'.format(name))


def check_binaries_in_path():
    programs = ['curl', 'bash', 'ping', 'tar', 'xz', 'unzip', 'ipset', 'systemd-notify']

    for program in programs:
        yield check_binary_in_path(program, raise_on_error=False)


def check_version(name, minimum, actual):
    if minimum <= actual:
        return

    return fail('{} has a version lower than the minimum supported ({}). Installed version: {}'.format(
        name, minimum, actual))


def check_systemd_version():
    # TODO(cmaloney): Make it easier to trampoline fast exit (Switch to EAFP?)
    check_binary_in_path('systemctl')

    systemctl_first_line = subprocess.check_output(['systemctl', '--version']).decode().lines()[0]
    # split out the version (first line is of the format `systemd NNNN`)
    version = systemctl_first_line.split(' ')[1]
    check_version('systemctl', minimum=200, actual=version)


def check_docker_version():
    raise NotImplementedError()


def check_nogroup():
    check_binary_in_path('getent')

    try:
        check_call(['getent', 'group', 'nogroup'])
    except subprocess.CalledProcessError:
        return


roles_agent = ['slave', 'slave_public']
roles_master = ['master']
roles_all = roles_agent + roles_master


def check_role(role):
    if role not in roles_all:
        return fail("Invalid role for host `{role}`. Allowed roles are: {all_roles}".format(role, roles_all))


class CheckCollector:

    def __init__(self, message):
        raise NotImplementedError()

    def add_result():
        raise NotImplementedError()

    def __enter__(self):
        return self


class CheckBlock:

    class _FailFast(BaseException):
        pass

    def __init__(self, collector, message):
        self.collector = collector
        self.message = message
        self.results = list()
        self.fail_fast = False

    @contextmanager
    def subblock(self, message):
        block = CheckCollector(message)
        self.results.append(block)

        yield block

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, exception_traceback):
        # No exception the no-op
        if isinstance(exception, None):
            return

        # Fail fast exception, consume the exception and return
        if isinstance(exception, CheckBlock._FailFast):
            return True

        # Log the exception as a failure
        # CheckResult -> just one normal result.
        # Other standard exceptions logged
        # Things not inherited by Exception mean exit / just pass by.
        if isinstance(exception_type, CheckResult):
            self.results.append(exception_type)
        elif isinstance(exception, Exception):
            self.results.append(fail('Error tyring to determine result: {}'.format(exception)))
            log.debug("Unexpected exception closing CheckBlock", exc_info=True)

    def has_failed(self):
        return any([result.passed for result in self.results])

    def log_result(self, result):
        self.collector.log_result()
        # check() may `return` a CheckResult, None, or be an iterable of CheckResults (yield CheckResult).
        # They may also throw a single CheckResult to 'fail early' / for control flow.
        if isinstance(result, CheckResult):
            self.results.append(result)
            if self.fail_fast:
                if not result.passed:
                    raise CheckBlock._FailFast
        else:
            new_results = list(result)
            assert all(map(lambda x: isinstance(x, CheckResult), new_results)), \
                "All check_*() functions should return, yield, or throw CheckResult. {}".format(new_results)

            self.results += new_results


class CheckCollector:

    def __init__(self, name, show_all, output_filename):
        self.show_all = show_all
        self.output_filename = output_filename

    @contextmanager
    def subblock(self, name):
        block = CheckBlock(self, name)
        yield block

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, exception_traceback):
        return



def do_preflight(role, show_all, output_file):
    print_section("Running preflight checks")

    checks = [
        (check_preexisting_dcos, 'DC/OS should not already be installed'),
        (check_selinux, 'SELinux should be disabled'),
        (check_binaries_in_path, 'Binaries depended upon by DC/OS should be installed'),
        (check_systemd_version, 'Systemd must be installed and above version 200'),
        (check_docker_version, 'Docker must be installed and above version 1.7'),
        (check_nogroup, 'The group `nogroup` should exist'),
        (partial(check_role, role), 'Role should be a valid DC/OS role'),
        (partial(check_ports, role in roles_agent), 'Ports required by components should not be in use'),
    ]

    # Run all checks and collect their error messages.
    check_block = CheckBlock('DC/OS Node Pre-Flight', show_all=show_all, output_file=output_file)
    with check_block:
        for check, message in checks:
            with check_binary_in_path.subblock(message) as collector:
                collector.log_result(check())

    # TODO(cmaloney): Print out results to screen, as well as logging to a json
    # file for machine processing
    raise NotImplementedError()

    if check_block.has_failed():
        sys.exit(1)


def get_data_json(filename):
    return json.loads(pkgutil.get_data('config', filename).decode())


def make_file(filename, content, mode, owner=None, group=None):
    check_call(['mkdir', '-p', os.path.dirname(filename)])
    print("Filling {} with contents".format(filename))
    with open(filename) as f:
        f.write(content)
    check_call(['chmod', mode, filename])

    if owner is None:
        assert group is None, "chown group but not owner is unimplemented"
        return

    chown_tgt = '{}{}'.format(owner, ':{}'.format(group) if group is not None else '')
    check_call(['chown', chown_tgt, filename])


def do_install(role, no_block):
    print_section('Starting DC/OS Install Process')
    # setup_directories
    print('Creating directories under /etc/mesosphere')
    check_call(['mkdir', '-p', '/etc/mesosphere/roles', '/etc/mesosphere/setup-flags'])

    # setup_dcos_roles
    check_call(['touch', '/etc/mesosphere/roles/{}'.format(role)])

    # configure_dcos: Extract and write setup_flags
    print_section('Setting flags for bootstrapping DC/OS onto host')
    for file_info in get_data_json('setup_flags.json'):
        make_file(**file_info)

    # setup_and_start_services
    print_section('Triggering DC/OS bootstrap')
    services = get_data_json('dcos_services.json')
    for service in services:
        # If no content, service is assumed to already exist
        if 'content' not in service:
            continue

        make_file(
            filename='/etc/systemd/system/{}'.format(service['name']),
            content=service['content'],
            mode='0644',
            owner='root',
            group='root')

    no_block_args = ['--no-block'] if no_block else []
    # Start, enable services which request it.
    for service in services:
        # Remove .service extension
        assert service['name'].endswith('.service')
        name = service['name'][:-8]

        if service.get('enable'):
            check_call(['systemctl', 'enable', service])
        if 'command' in service:
            cmd = ['systemctl', service['command'], name]
            if service.get('no_block'):
                cmd += no_block_args
            check_call(cmd)


def main():
    parser = argparse.ArgumentParser(description='Install DC/OS on a node')
    preflight_group = parser.add_mutually_exclusive_group()
    parser.add_argument('role', help='DC/OS Role for node')
    preflight_group.add_argument('-d', '--disable-preflight', action='store_true')
    preflight_group.add_argument('-p', '--preflight-only', action='store_true')
    parser.add_argument('--no-block-dcos-setup', action='store_true')
    parser.add_argument('--colors', choices=['yes', 'no', 'auto'], default='auto')

    args = parser.parse_args()

    if args.colors == 'auto':
        use_colors(sys.stdout.isatty())
    elif args.colors == 'yes':
        use_colors(True)
    elif args.colors == 'no':
        use_colors(False)

    if not os.geteuid() == 0:
        print_fail("Must be run as root")
        sys.exit(1)

    # Early exit if preflight only
    if args.preflight_only:
        do_preflight(args.role)
        sys.exit(0)

    # Run preflight unless it's been disabled
    if not args.disable_preflight:
        do_preflight(args.role)

    # Do the installl
    do_install()


if __name__ == '__main__':
    main()
