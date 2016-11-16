import os
import stat
from collections import namedtuple

import gen
from gen.internals import Source, Target


def validate_ssh_key_path(ssh_key_path):
    assert os.path.isfile(ssh_key_path), 'could not find ssh private key: {}'.format(ssh_key_path)
    assert stat.S_IMODE(
        os.stat(ssh_key_path).st_mode) & (stat.S_IRWXG + stat.S_IRWXO) == 0, (
            'ssh_key_path must be only read / write / executable by the owner. It may not be read / write / executable '
            'by group, or other.')
    with open(ssh_key_path) as fh:
        assert 'ENCRYPTED' not in fh.read(), ('Encrypted SSH keys (which contain passphrases) '
                                              'are not allowed. Use a key without a passphrase.')


source = Source({
    'validate': [
        lambda ssh_port: gen.calc.validate_int_in_range(ssh_port, 1, 32000),
        lambda ssh_parallelism: gen.calc.validate_int_in_range(ssh_parallelism, 1, 100),
        validate_ssh_key_path
    ],
    'default': {
        'ssh_key_path': 'genconf/ssh_key',
        'ssh_port': '22',
        'ssh_process_timeout': '120',
        'ssh_parallelism': '20',
        'extra_ssh_options': '',
        'ssh_binary_path': '/usr/bin/ssh',
        'scp_binary_path': '/usr/bin/scp'
    }
})

# TODO(cmaloney): Convert to a "ssh_config" object which generates an ssh
# config file, then add a '-F' for all calls to that temporary config file
# rather than manually building up / adding the arguments in _get_base_args
# which is very error prone to get the formatting right. Should have just one
# host section which applies to all hosts, sets things like "user".
Config = namedtuple('Config', [
    'ssh_user',
    'ssh_port',
    'ssh_key_path',
    'ssh_parallelism',
    'process_timeout',
    'extra_ssh_options'])

# TODO(cmaloney): Should be able to ask the target to give back the namedtuple with the items.
config_target = Target.from_namedtuple(Config)


def make_config(config_target: Target):
    return Config(**config_target.arguments)
