# NOTE: avoid things that pull in lots of non-core python deps here to keep the install script
# minimal in size.
import argparse
import copy
import json
import os
import os.path
import shutil
import subprocess
import sys
from collections import namedtuple
from functools import partial

import pkgutil

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

CheckFail = namedtuple('check_fail', ['message', 'details'])


def fail(msg, **arguments):
    return CheckFail(msg.format(**arguments), arguments)


# Add a structured error message to the set of messages,
def add_error(msg, **kwargs):
    global errors
    errors.append({'message': msg.format(**kwargs), 'details': kwargs})


def check_ports(is_agent):
    print('Checking no ports required by DC/OS Components are in use')
    component_ports = copy.copy(common_ports)
    component_ports.update(agent_ports if is_agent else master_ports)

    used_ports = get_used_ports()

    # Find all common ports, return an error for every common port
    in_use_component_ports = set(component_ports.keys()) & used_ports

    for port in in_use_component_ports:
        yield fail('port {port} is required by DC/OS Component {component} but is in use',
                   port=port, component=component_ports[port])


def check_preexisting_dcos():
    print('Checking if DC/OS is already on this host or is partially on the host still')

    # TODO(cmaloney): Check for /var/lib/dcos
    dcos_installed_files = [
        '/etc/systemd/system/dcos.target',
        '/etc/systemd/system/dcos.target.wants',
        '/opt/mesosphere']

    found_files = list(filter(map(os.path.exists, dcos_installed_files)))

    if found_files:
        return fail('DC/OS Already installed on host. Found the following files: {found_files}', found_files)


def check_selinux():
    print('Checking if SELinux is disabled')
    enabled = subprocess.check_output('getenforce').decode().strip()
    if enabled == 'Enforcing':
        return fail('SELinux is currently Enforcing, must be disabled for DC/OS to operate')


def check_binary_in_path(name):
    # TODO(cmaloney): Need a better way to say "Checking X: <late-binding-status>".
    # Probably a context manager.
    print('Checking if {} is installed and in PATH').format(name)
    path = shutil.which(name)

    if path is None:
        raise fail('{} not found', name)


def check_binaries_in_path():
    programs = ['curl', 'bash', 'ping', 'tar', 'xz', 'unzip', 'ipset', 'systemd-notify']

    for program in programs:
        try:
            check_binary_in_path(program)
        except CheckFail as check_fail:
            yield check_fail


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


def do_preflight(role):
    print_section("Running preflight checks")

    checks = [
        check_preexisting_dcos,
        check_selinux,
        check_binaries_in_path,
        check_systemd_version,
        check_docker_version,
        check_nogroup,
        partial(check_role, role),
        partial(check_ports, role in roles_agent),
    ]

    # Run all checks and collect their error messages.
    errors = list()
    for check in checks:
        # check() may `return` a CheckFail, None, or be an iterable of CheckFails (yield CheckFail).
        # They may also throw a single CheckFail to 'fail early' / for control flow.
        try:
            result = check()
            if isinstance(result, CheckFail) or isinstance(result, None):
                errors.append(result)
            else:
                errors += list(result)
        except CheckFail as result:
            errors.append(result)

    # Make sure everything remaining is a CheckFail
    assert all(map(lambda x: isinstance(x, CheckFail), errors)), \
        "All check_*() functions should return, yield, or throw CheckFail objects. {}".format(errors)

    # TODO(cmaloney): Change return between json and printing based on requesetd
    # options
    for error in errors:
        print_fail('FAIL: ' + error['msg'])

    if errors:
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
