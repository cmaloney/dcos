import logging
import os
from collections import namedtuple

import pkgpanda

REMOTE_TEMP_DIR = '/opt/dcos_install_tmp'
CLUSTER_PACKAGES_FILE = 'genconf/cluster_packages.json'

log = logging.getLogger(__name__)

preflight_script = """#!/usr/bin/env bash
# setenforce is in this path
PATH=$PATH:/sbin

dist=$(cat /etc/os-release | sed -n 's@^ID="\(.*\)"$@\\1@p')

if ([ x$dist == 'xcoreos' ]); then
  echo "Detected CoreOS. All prerequisites already installed" >&2
  exit 0
fi

if ([ x$dist != 'xrhel' ] && [ x$dist != 'xcentos' ]); then
  echo "$dist is not supported. Only RHEL and CentOS are supported" >&2
  exit 0
fi

version=$(cat /etc/*-release | sed -n 's@^VERSION_ID="\([0-9]*\)\([0-9\.]*\)"$@\1@p')
if [ $version -lt 7 ]; then
  echo "$version is not supported. Only >= 7 version is supported" >&2
  exit 0
fi

if [ -f /opt/dcos-prereqs.installed ]; then
  echo "install_prereqs has been already executed on this host, exiting..."
  exit 0
fi

sudo setenforce 0 && \
sudo sed -i 's/^SELINUX=.*/SELINUX=disabled/g' /etc/sysconfig/selinux

sudo tee /etc/yum.repos.d/docker.repo <<-'EOF'
[dockerrepo]
name=Docker Repository
baseurl=https://yum.dockerproject.org/repo/main/centos/7
enabled=1
gpgcheck=1
gpgkey=https://yum.dockerproject.org/gpg
EOF

sudo yum -y update --exclude="docker-engine*"

sudo mkdir -p /etc/systemd/system/docker.service.d
sudo tee /etc/systemd/system/docker.service.d/override.conf <<- EOF
[Service]
Restart=always
StartLimitInterval=0
RestartSec=15
ExecStartPre=-/sbin/ip link del docker0
ExecStart=
ExecStart=/usr/bin/docker daemon --storage-driver=overlay -H fd://
EOF

sudo yum install -y docker-engine-1.11.2
sudo systemctl start docker
sudo systemctl enable docker

sudo yum install -y wget
sudo yum install -y git
sudo yum install -y unzip
sudo yum install -y curl
sudo yum install -y xz
sudo yum install -y ipset

sudo getent group nogroup || sudo groupadd nogroup
sudo touch /opt/dcos-prereqs.installed
"""

dcos_diag_script = """#!/usr/bin/env bash
# Run the DC/OS diagnostic script for up to 15 minutes (900 seconds) to ensure
# we do not return ERROR on a cluster that hasn't fully achieved quorum.
T=900
until OUT=$(sudo /opt/mesosphere/bin/./3dt -diag) || [[ T -eq 0 ]]; do
    sleep 1
    let T=T-1
done
RETCODE=$?
for value in $OUT; do
    echo $value
done
exit $RETCODE
"""


def add_prerequisites(chain, options):
    # TODO(cmaloney): Move preflight marker checking outside of the prereqs script?
    chain.copy_and_run(preflight_script)


def add_preflight(chain, options):
    chain.copy_and_run(
        filename='genconf/serve/dcos_install.sh',
        args=['{{role}}', '--preflight-only'],
        description='Running preflight checks',
        parameterized=True)

    # TODO(cmaloney): Copy back / gather preflight structured results file rather than parsing the text.


def add_install(chain, options):
    # TODO(cmaloney): Remove stale dcos bits?
    # TODO(cmaloney): pipe in bootstrap_id from config.
    chain.copy(filename='genconf/serve/bootstrap/{}.bootstrap.tar.xz'.format(os.getenv['BOOTSTRAP_ID']))
    chain.copy(filename="genconf/serve/dcos_install.sh")
    for package in pkgpanda.load_json("genconf/cluster_packages.json"):
        chain.copy(filename='packages/{}.tar.xz'.format(package))
    chain.copy_and_run(
        filename='genconf/serve/dcos_install.sh',
        args=['{{role}}'],
        description='Installing DC/OS on host',
        parameterized=True)
    chain.run_parameterized('genconf/serve/dcos_install.sh', args=["{{role}}"])


def add_postflight(chain, options):
    chain.copy_and_run(contents=dcos_diag_script, filename='dcos_diag.sh', description='Waiting for host to come up')

    # Cleanup leftover file from if we installed prereqs
    chain.run(
        filename='sudo',
        args=['rm', '-f', '/opt/dcos-prereqs.installed'],
        description='Removing prerequisites flag',
        external_command=True)


def add_preflight_web(chain, options):
    if options.offline:
        log.debug('Offline mode used. Do not install prerequisites on CentOS7, RHEL7 in web mode')
    else:
        add_prerequisites(chain)

    add_preflight(chain)


Stage = namedtuple('Stage', ['name', 'build_chain', 'optional'])


web_steps = [
    Stage('preflight', add_preflight_web, False),
    Stage('install', add_install, False),
    Stage('postflight', add_postflight, False)
]

cli_steps = [
    Stage('prerequisites', add_prerequisites, True),
    Stage('preflight', add_preflight, False),
    Stage('install', add_install, False),
    Stage('postflight', add_postflight, False),
]
