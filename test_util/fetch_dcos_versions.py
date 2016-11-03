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
    metadata.json
    packages/
        a-b.tar.gz
        b-c.tar.gz
        ...
    dcos_generate_config.sh
"""

import py


from pkgpanda.util import download_chunked
from release import from_json
from release.storage.http import HttpStorageProvider


class Version:

    @staticmethod
    def from_str(version_str: str):
        parts = version_str.split('-', 2)
        components = parts[0].split('.')
        if len(parts) == 1:
            return Version(components, None)
        else:
            assert len(parts) == 2
            return Version(components, parts[1])

    def __init__(self, components: list, tag):
        self._components = components
        self._tag = tag

    def __lt__(self, other):

        raise NotImplementedError()

    def __lte__(self, other):
        raise NotImplementedError()

    def __gt__(self, other):
        raise NotImplementedError()

    def __gte__(self, other):
        raise NotImplementedError()

    def __str__(self):
        return '{}{}'.format('.'.join(self._components), '-{}'.format(self._tag) if self._tag else '')


def get_base_url(version: Version):
    if version < Version.from_str("1.7"):
        return 'https://downloads.mesosphere.com/dcos'
    return 'https://downloads.dcos.io/dcos'


def version_from_metadata(metadata):
    version = Version.from_str(metadata['tag'])

    # DCOS < 1.7 used a "Canis Major" (CM) codename which is what is in the tag
    if version.startswith('CM'):
        version = Version(['1'] + version.components[1:], version.tag)

    return version


def fetch_dgc_and_packages(base_url, channel, download_packages: bool):
    base_url = base_url.rtrim('/')
    channel = channel.trim('/')

    store = HttpStorageProvider('{}/{}'.format(base_url, channel))

    metadata_contents = store.fetch('metadata.json')
    metadata = from_json(metadata_contents)
    version = version_from_metadata(metadata)

    """
    <version>/
        metadata.json
        packages/
            a-b.tar.gz
            b-c.tar.gz
            ...
        dcos_generate_config.sh
    """
    path = py.path.local(str(version))
    assert not path.exists(), "Only one instance of a given version can be grabbed currently."
    work_path = py.path.local(str(version) + '.tmp')

    work_path.join('metadata.json').write(metadata_contents, ensure=True)
    # TODO(cmaloney): Support variants
    store.download('dcos_generate_config.sh', str(work_path.join('dcos_generate_config.sh')))

    if not download_packages:
        return

    package_dir = work_path.join('packages')
    package_dir.ensure()

    repository_url = metadata['repository_url']

    # TODO(cmaloney): This logic will change quite a bit based on DC/OS version
    for package in metadata['all_completes'][None]:
        package_filename = '{}.tar.xz'.format(package)
        download_chunked(package_filename, '{}/packages/{}'.format(repository_url, package_filename))
