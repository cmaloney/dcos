import copy
import logging
import os.path

import yaml

import gen
import ssh.config
from pkgpanda.util import write_string


log = logging.getLogger(__name__)

config_sample = """
---
# The name of your DC/OS cluster. Visable in the DC/OS user interface.
cluster_name: 'DC/OS'
master_discovery: static
exhibitor_storage_backend: 'static'
resolvers:
- 8.8.8.8
- 8.8.4.4
ssh_port: 22
process_timeout: 10000
bootstrap_url: file:///opt/dcos_install_tmp
"""


def compare_lists(first_json: str, second_json: str):
    first_list = gen.calc.validate_json_list(first_json)
    second_list = gen.calc.validate_json_list(second_json)
    dups = set(first_list) & set(second_list)
    assert not dups, 'master_list and agent_list cannot contain duplicates {}'.format(', '.join(dups))


def validate_agent_lists(agent_list, public_agent_list):
    compare_lists(agent_list, public_agent_list)


install_host_source = gen.internals.Source({
    'validate': [
        lambda agent_list: gen.calc.validate_ip_list(agent_list),
        lambda public_agent_list: gen.calc.validate_ip_list(public_agent_list),
        lambda master_list: gen.calc.validate_ip_list(master_list),
        # master list shouldn't contain anything in either agent lists
        lambda master_list, agent_list: compare_lists(master_list, agent_list),
        lambda master_list, public_agent_list: compare_lists(master_list, public_agent_list),
        # the agent lists shouldn't contain any common items
        lambda agent_list, public_agent_list: compare_lists(agent_list, public_agent_list),
    ],
    'default': {
        'agent_list': '[]',
        'public_agent_list': '[]',
    }
})


def get_installer_target():
    return gen.internals.Target({
        'master_list',
        'agent_list',
        'public_agent_list'
    })


def normalize_config_validation(messages):
    """Accepts Gen error message format and returns a flattened dictionary
    of validation messages.

    :param messages: Gen validation messages
    :type messages: dict | None
    """
    validation = {}
    if 'errors' in messages:
        for key, errors in messages['errors'].items():
            validation[key] = errors['message']

    if 'unset' in messages:
        for key in messages['unset']:
            validation[key] = 'Must set {}, no way to calculate value.'.format(key)

    return validation


extra_args = {'provider': 'onprem'}


def make_default_config_if_needed(config_path):
    if os.path.exists(config_path):
        return

    write_string(config_path, config_sample)


class NoConfigError(Exception):
    pass


class Config():

    def __init__(self, config_path):
        self.config_path = config_path

        # Create the config file iff allowed and there isn't one provided by the user.

        self._config = self._load_config()
        if not isinstance(self._config, dict):
            # FIXME
            raise NotImplementedError()

    def _load_config(self):
        if self.config_path is None:
            return {}

        try:
            with open(self.config_path) as f:
                return yaml.load(f)
        except FileNotFoundError as ex:
            raise NoConfigError(
                "No config file found at {}. See the DC/OS documentation for the "
                "available configuration options. You can also use the GUI web installer (--web),"
                "which provides a guided configuration and installation for simple "
                "deployments.".format(self.config_path)) from ex

    def update(self, updates):
        # TODO(cmaloney): check that the updates are all for valid keys, keep
        # any ones for valid keys and throw out any for invalid keys, returning
        # errors for the invalid keys.
        self._config.update(updates)

    # TODO(cmaloney): Figure out a way for the installer being generated (Advanced AWS CF templates vs.
    # bash) to automatically set this in gen.generate rather than having to merge itself.
    def as_gen_format(self):
        config = copy.copy(self._config)
        config.update({'provider': 'onprem'})
        return gen.stringify_configuration(config)

    def do_validate(self, include_ssh):
        user_arguments = self.as_gen_format()
        sources, targets, _ = gen.get_dcosconfig_source_target_and_templates(user_arguments, [])

        if include_ssh:
            sources.append(ssh.validate.source)
            targets.append(ssh.validate.config_target)

            # TODO(cmaloney): install_host_target should only apply if using the web / ssh installer.
            targets.append(install_host_target)

        messages = gen.internals.validate_configuration(sources, targets, user_arguments)
        # TODO(cmaloney): kill this function and make the API return the structured
        # results api as was always intended rather than the flattened / lossy other
        # format. This will be an  API incompatible change. The messages format was
        # specifically so that there wouldn't be this sort of API incompatibility.
        return normalize_config_validation(messages)

    def do_gen_configure(self):
        return gen.generate(self.as_gen_format())

    def get_yaml_str(self):
        return yaml.dump(self._config, default_flow_style=False, explicit_start=True)

    def write_config(self):
        assert self.config_path is not None

        write_string(self.config_path, self.get_yaml_str())

    def __getitem__(self, key: str):
        return self._config[key]

    def __contains__(self, key: str):
        return key in self._config

    # TODO(cmaloney): kill this, should use config target to set defaults. The config targets should
    # set these defaults.
    def hacky_default_get(self, *args, **kwargs):
        return self._config.get(*args, **kwargs)

    @property
    def config(self):
        return copy.copy(self._config)


def to_config(config_dict: dict):
    config = Config(None)
    config.update(config_dict)
    return config


# TODO(cmaloney): Work this API, callers until this result remapping is unnecessary
# and the couple places that need this can just make a trivial call directly.
def validate_ssh_config(user_arguments):
    user_arguments = gen.stringify_configuration(user_arguments)
    messages = gen.internals.validate_configuration([source], [target], user_arguments)
    if messages['status'] == 'ok':
        return {}

    # Re-format to the expected format
    # TODO(cmaloney): Make the unnecessary
    final_errors = dict()
    for name, message_blob in messages['errors'].items():
        final_errors[name] = message_blob['message']
    return final_errors
