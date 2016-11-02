"""Fetch all dcos_generate_config.sh + the list of packages which make up the
version of DC/OS for later use

usage:
fetch_dcos channel start_version end_version
fetch_dcos exact <channel> <channel> <channel>
  - Figures out the url to download from
  - fetches the dcos_generate_config.sh
  - Based on version in metadata.json, finds list of packages
  - Writes out the list of packages
  - Can fetch all the packages if asked.
  - TODO(cmaloney): Should it write how to interact / interface for doing a genconf?

Output layout
<version>/
    0/
        metadata.json
        packages/
        dcos_generate_config.sh
"""

import requests


def fetch_dgc_and_packages(channel):
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
