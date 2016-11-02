"""Fetch all dcos_generate_config.sh + the list of packages which make up the
version of DC/OS for later use

usage:
fetch_dcos channel [start_version-end_version|version]...  [--download-packages]
fetch_dcos exact <channel> <channel> <channel> [--download-packages]
  - Figures out the url to download from
  - fetches the dcos_generate_config.sh
  - Based on version in metadata.json, finds list of packages
  - Writes out the list of packages
  - Can fetch all the packages if asked.

Output layout
<version>/
    0/
        metadata.json
        packages/
            a-b.tar.gz
            b-c.tar.gz
            ...
        dcos_generate_config.sh
"""

import requests


class Version:

    @staticmethod
    def from_str(version_str: str):
        parts = version_str.split('-', 2)
        if len(parts) == 1:
            return Version(parts[0].split('.'), None)
        raise NotImplementedError()

    def __init__(self, components: list, tag):
        assert isinstance(components, list)


    def __lt__(self, other):
        raise NotImplementedError()

    def __gt__(self, other):
        raise NotImplementedError()

def get_base_url(version: Version):
    if version < Version.from_str("1.7"):
        return 'https://downloads.mesosphere.com/dcos'
    return 'https://downloads.dcos.io/dcos'


def fetch_dgc_and_packages(site, channel):
    requests.get()
  raise NotImplementedError()

"""
Download stuff
 - Make list of all packages
 - Be able to download all packages

if < 1.7
downloads.mesosphere.com/dcos
if > 1.7
downloads.mesosphere.com/dcos-enterprise
or
  downloads.dcos.io/dcos


Need a meta-indexer + on release promote update index

don't reveal enterprise urls / have enterprise



Need to make sure to have the package lists
need to have an index of all releases in a channel


"""
