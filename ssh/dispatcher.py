
def parse_ip(ip):
    assert isinstance(ip, str), 'IP should be string, {} given'.format(ip)
    tmp = ip.split(':')
    if len(tmp) == 2:
        return {"ip": tmp[0], "port": int(tmp[1])}
    elif len(tmp) == 1:
        return {"ip": ip, "port": None}
    else:
        raise ValueError(
            "Expected a string of form <ip> or <ip>:<port> but found a string with more than one " +
            "colon in it. NOTE: IPv6 is not supported at this time. Got: {}".format(ip))


class Node():
    def __init__(self, host: str, tags: dict):
        assert isinstance(tags, dict)
        self.tags = tags
        self.host = parse_ip(host)
        self.ip = self.host['ip']
        self.port = self.host['port']

    def get_full_host(self):
        _host = self.host.copy()
        _host.update({'tags': self.tags})
        return _host

    def __repr__(self):
        return '{}:{} tags={}'.format(
            self.ip,
            self.port,
            ', '.join(['{}:{}'.format(k, v) for k, v in sorted(self.tags.items())]))


class CommandBuilder:
    def __init__(self, default_user, default_port, key_path, extra_options: list):
        self.default_user = default_user
        self.default_port = default_port
        self.key_path = key_path
        self.extra_options = extra_options
        self.remote_work_dir = '/opt/dcos_install_tmp'

    def get_port(self, node):
        port = node.port
        if port is None:
            port = self.default_port

        return port

    def _make_ssh(self, node, base_args, cmd, parameterized):

        if parameterized:
            cmd = [arg.format(**node.tags) for arg in cmd]

        return ['ssh'] + base_args + ['{}@{}'.format(self.default_user, node.ip)] + cmd

    # TODO(cmaloney): This and the current make_run should actually just call to
    # a common helper, rather than this depending on a bunch of special flags on
    # make_run (external_command).
    def make_run_external(self, node, base_args, cmd):
        return self._make_ssh(node, base_args, cmd)

    def _run_helper(self, node, cmd, description):
        return self.get_cmd(node, 'run_external', description, {'cmd': cmd})

    def yield_setup(self, node: Node):
        yield self._run_helper(
            node,
            ['sudo', 'mkdir', '-p', self.remote_work_dir],
            'Creating temporary work directory')

        yield self._run_helper(
            node,
            ['sudo', 'chown', self.default_user, self.remote_work_dir],
            'Ensuring {} owns temporary work directory'.format(self.default_user))

    def make_cleanup(self, node: Node):
        return self._run_helper(
            node,
            ['sudo', 'rm', '-rf', self.remote_work_dir],
            'Cleaning up temporary work directory')

    def make_copy(self, node, base_args, filename):
        remote_path = self.remote_work_dir + '/' + filename

        return ['scp', '-tt', '-P{}'.format(self.get_port(node))] + base_args + \
            [filename, '{}@{}:{}'.format(self.default_user, node.ip, remote_path)]

    def make_run(self, node, base_args, filename, args, parameterized):
        if args is None:
            args = list()

        return self._make_ssh(node, base_args, ['bash', self.remote_work_dir + '/' + filename] + args, parameterized)

    def get_cmd(self, node: Node, action: str, description: str, arguments: dict):
        # TODO(cmaloney): use the description somewhere....

        base_args = ['-oConnectTimeout=10', '-oStrictHostKeyChecking=no',
                     '-oUserKnownHostsFile=/dev/null', '-oBatchMode=yes', '-oPasswordAuthentication=no',
                     '-i', self.key_path] + self.extra_options

        return {
            'copy': self.make_copy,
            'run': self.make_run,
            'run_external': self.make_run_external
        }[action](node, base_args, **arguments)


class Dispatcher:
    def __init__(self, parallelism, process_timeout):
        self._paralleism = parallelism
        self._process_timeout = process_timeout

    @asyncio.coroutine
    def _run_cmd(self, cmd, host, namespace, future, stage):
        with make_slave_pty() as slave_pty:
            process = yield from asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=slave_pty,
                env={'TERM': 'linux'})
            stdout = b''
            stderr = b''
            try:
                stdout, stderr = yield from asyncio.wait_for(process.communicate(), self.process_timeout)
            except asyncio.TimeoutError:
                try:
                    process.terminate()
                except ProcessLookupError:
                    log.info('process with pid {} not found'.format(process.pid))
                log.error('timeout of {} sec reached. PID {} killed'.format(self.process_timeout, process.pid))

        # For each possible line in stderr, match from the beginning of the line for the
        # the confusing warning: "Warning: Permanently added ...". If the warning exists,
        # remove it from the string.
        err_arry = stderr.decode().split('\r')
        stderr = bytes('\n'.join([line for line in err_arry if not line.startswith(
            'Warning: Permanently added')]), 'utf-8')

        process_output = {
            '{}:{}'.format(host.ip, host.port): {
                "cmd": cmd,
                "stdout": stdout.decode().split('\n'),
                "stderr": stderr.decode().split('\n'),
                "returncode": process.returncode,
                "pid": process.pid,
                "stage": stage
            }
        }

        future.set_result((namespace, process_output, host))
        return process_output

    def dispatch(hosts, cmd_builder, chain, result_delegate):
        # Validate all sources / pre-requisites exist (copy sources, etc)
        # Launch all the subprocesses!
        # Should launch everything simultaneously, but limit the number of hosts
        # simultaneously running to parallelism.
        raise NotImplementedError()
