    if not os.path.isfile(pf_script_path):
        log.error("genconf/serve/dcos_install.sh does not exist. Please run --genconf before executing preflight.")
        raise FileNotFoundError('genconf/serve/dcos_install.sh does not exist')


def _add_copy_dcos_install(chain, local_install_path='genconf/serve'):
    dcos_install_script = 'dcos_install.sh'
    local_install_path = os.path.join(local_install_path, dcos_install_script)
    remote_install_path = os.path.join(REMOTE_TEMP_DIR, dcos_install_script)
    chain.add_copy(local_install_path, remote_install_path, stage='Copying dcos_install.sh')



def _add_copy_packages(chain, local_pkg_base_path='genconf/serve'):
    if not os.path.isfile(CLUSTER_PACKAGES_FILE):
        err_msg = '{} not found'.format(CLUSTER_PACKAGES_FILE)
        log.error(err_msg)
        raise ExecuteException(err_msg)

    cluster_packages = pkgpanda.load_json(CLUSTER_PACKAGES_FILE)
    for package, params in cluster_packages.items():
        destination_package_dir = os.path.join(REMOTE_TEMP_DIR, 'packages', package)
        local_pkg_path = os.path.join(local_pkg_base_path, params['filename'])

        chain.add_execute(['mkdir', '-p', destination_package_dir], stage='Creating package directory')
        chain.add_copy(local_pkg_path, destination_package_dir,
                       stage='Copying packages')


def _add_copy_bootstap(chain, local_bs_path):
    remote_bs_path = REMOTE_TEMP_DIR + '/bootstrap'
    chain.add_execute(['mkdir', '-p', remote_bs_path], stage='Creating directory')
    chain.add_copy(local_bs_path, remote_bs_path,
                   stage='Copying bootstrap')


def _get_bootstrap_tarball(tarball_base_dir='genconf/serve/bootstrap'):
    '''
    Get a bootstrap tarball from a local filesystem
    :return: String, location of a tarball
    '''
    if 'BOOTSTRAP_ID' not in os.environ:
        err_msg = 'BOOTSTRAP_ID must be set'
        log.error(err_msg)
        raise ExecuteException(err_msg)

    tarball = os.path.join(tarball_base_dir, '{}.bootstrap.tar.xz'.format(os.environ['BOOTSTRAP_ID']))
    if not os.path.isfile(tarball):
        log.error('Ensure environment variable BOOTSTRAP_ID is set correctly')
        log.error('Ensure that the bootstrap tarball exists in '
                  'genconf/serve/bootstrap/[BOOTSTRAP_ID].bootstrap.tar.xz')
        log.error('You must run genconf.py before attempting Deploy.')
        raise ExecuteException('bootstrap tarball not found genconf/serve/bootstrap')
    return tarball


def _read_state_file(state_file):
    if not os.path.isfile(state_file):
        return {}

    with open(state_file) as fh:
        return json.load(fh)


def _remove_host(state_file, host):

    json_state = _read_state_file(state_file)

    if 'hosts' not in json_state or host not in json_state['hosts']:
        return False

    log.debug('removing host {} from {}'.format(host, state_file))
    try:
        del json_state['hosts'][host]
    except KeyError:
        return False

    with open(state_file, 'w') as fh:
        json.dump(json_state, fh)

    return True



def get_async_runner(config, hosts, async_delegate=None):
    # TODO(cmaloney): Delete these repeats. Use gen / expanded configuration to get all the values.
    process_timeout = config.hacky_default_get('process_timeout', 120)
    extra_ssh_options = config.hacky_default_get('extra_ssh_options', '')
    ssh_key_path = config.hacky_default_get('ssh_key_path', 'genconf/ssh_key')

    # if ssh_parallelism is not set, use 20 concurrent ssh sessions by default.
    parallelism = config.hacky_default_get('ssh_parallelism', 20)

    return ssh.ssh_runner.MultiRunner(hosts, ssh_user=config['ssh_user'], ssh_key_path=ssh_key_path,
                                      process_timeout=process_timeout, extra_opts=extra_ssh_options,
                                      async_delegate=async_delegate, parallelism=parallelism)


def add_pre_action(chain, ssh_user):
    # Do setup steps for a chain
    chain.add_execute(['sudo', 'mkdir', '-p', REMOTE_TEMP_DIR], stage='Creating temp directory')
    chain.add_execute(['sudo', 'chown', ssh_user, REMOTE_TEMP_DIR],
                      stage='Ensuring {} owns temporary directory'.format(ssh_user))


def add_post_action(chain):
    # Do cleanup steps for a chain
    chain.add_execute(['sudo', 'rm', '-rf', REMOTE_TEMP_DIR],
                      stage='Cleaning up temporary directory')

def nodes_count_by_type(config):
    total_agents_count = len(config.hacky_default_get('agent_list', [])) + \
        len(config.hacky_default_get('public_agent_list', []))
    return {
        'total_masters': len(config['master_list']),
        'total_agents': total_agents_count
    }


def get_full_nodes_list(config):
    def add_nodes(nodes, tag):
        return [Node(node, tag) for node in nodes]

    node_role_map = {
        'master_list': 'master',
        'agent_list': 'agent',
        'public_agent_list': 'public_agent'
    }
    full_target_list = []
    for config_field, role in node_role_map.items():
        if config_field in config:
            full_target_list += add_nodes(config[config_field], {'role': role})
    log.debug("full_target_list: {}".format(full_target_list))
    return full_target_list


    result = yield from runner.run_commands_chain_async(chains, block=block, state_json_dir=state_json_dir,
                                                        delegate_extra_params=delegate_extra_params)
    return result

    result = yield from runner.run_commands_chain_async([uninstall_chain], block=block, state_json_dir=state_json_dir)

    return result
