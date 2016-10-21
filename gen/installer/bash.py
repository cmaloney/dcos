"""Generates a bash script for installing by hand or light config management integration"""

import os
import subprocess
import tempfile

import pkg_resources
import py

import gen.installer.util as util
import gen.template
import pkgpanda.util


file_template = """mkdir -p `dirname {filename}`
cat <<'EOF' > "{filename}"
{content}
EOF
chmod {mode} {filename}

"""

systemctl_no_block_service = """
if (( $SYSTEMCTL_NO_BLOCK == 1 )); then
    systemctl {command} {name} --no-block
else
    systemctl {command} {name}
fi
"""


def generate(gen_out, output_dir):
    print("Generating Bash configuration files for DC/OS")
    make_bash(gen_out)
    util.do_bundle_onprem(['dcos_install.sh'], gen_out, output_dir)


def make_bash(gen_out):
    """
    Reformat the cloud-config into bash heredocs
    Assert the cloud-config is only write_files
    """
    setup_flags = ""
    cloud_config = gen_out.templates['cloud-config.yaml']
    assert len(cloud_config) == 1
    for file_dict in cloud_config['write_files']:
        # NOTE: setup-packages is explicitly disallowed. Should all be in extra
        # cluster packages.
        assert 'setup-packages' not in file_dict['path']
        setup_flags += file_template.format(
            filename=file_dict['path'],
            content=file_dict['content'],
            mode=file_dict.get('permissions', "0644"),
            owner=file_dict.get('owner', 'root'),
            group=file_dict.get('group', 'root'))

    # Reformat the DC/OS systemd units to be bash written and started.
    # Write out the units as files
    setup_services = ""
    for service in gen_out.templates['dcos-services.yaml']:
        # If no content, service is assumed to already exist
        if 'content' not in service:
            continue
        setup_services += file_template.format(
            filename='/etc/systemd/system/{}'.format(service['name']),
            content=service['content'],
            mode='0644',
            owner='root',
            group='root')

    setup_services += "\n"

    # Start, enable services which request it.
    for service in gen_out.templates['dcos-services.yaml']:
        assert service['name'].endswith('.service')
        name = service['name'][:-8]
        if service.get('enable'):
            setup_services += "systemctl enable {}\n".format(name)
        if 'command' in service:
            if service.get('no_block'):
                setup_services += systemctl_no_block_service.format(
                    command=service['command'],
                    name=name)
            else:
                setup_services += "systemctl {} {}\n".format(service['command'], name)

    # Populate in the bash script template
    bash_script = gen.template.parse_str(bash_template).render({
        'dcos_image_commit': util.dcos_image_commit,
        'generation_date': util.template_generation_date,
        'setup_flags': setup_flags,
        'setup_services': setup_services})

    # Output the dcos install script
    pkgpanda.util.write_string('dcos_install.sh', bash_script)

    return 'dcos_install.sh'


def make_installer_docker(variant, bootstrap_id, installer_bootstrap_id):
    assert len(bootstrap_id) > 0

    image_version = util.dcos_image_commit[:18] + '-' + bootstrap_id[:18]
    genconf_tar = "dcos-genconf." + image_version + ".tar"
    installer_filename = "packages/cache/dcos_generate_config." + pkgpanda.util.variant_prefix(variant) + "sh"
    bootstrap_filename = bootstrap_id + ".bootstrap.tar.xz"
    bootstrap_active_filename = bootstrap_id + ".active.json"
    installer_bootstrap_filename = installer_bootstrap_id + '.bootstrap.tar.xz'
    bootstrap_latest_filename = pkgpanda.util.variant_prefix(variant) + 'bootstrap.latest'
    latest_complete_filename = pkgpanda.util.variant_prefix(variant) + 'complete.latest.json'
    docker_image_name = 'mesosphere/dcos-genconf:' + image_version

    # TODO(cmaloney): All of this should use package_resources
    with tempfile.TemporaryDirectory() as build_dir:
        assert build_dir[-1] != '/'

        print("Setting up build environment")

        def dest_path(filename):
            return build_dir + '/' + filename

        def copy_to_build(src_prefix, filename):
            subprocess.check_call(['cp', os.getcwd() + '/' + src_prefix + '/' + filename, dest_path(filename)])

        def fill_template(base_name, format_args):
            pkgpanda.util.write_string(
                dest_path(base_name),
                pkg_resources.resource_string(__name__, 'bash/' + base_name + '.in').decode().format(**format_args))

        fill_template('Dockerfile', {
            'installer_bootstrap_filename': installer_bootstrap_filename,
            'bootstrap_filename': bootstrap_filename,
            'bootstrap_active_filename': bootstrap_active_filename,
            'bootstrap_latest_filename': bootstrap_latest_filename,
            'latest_complete_filename': latest_complete_filename})

        fill_template('installer_internal_wrapper', {
            'variant': pkgpanda.util.variant_str(variant),
            'bootstrap_id': bootstrap_id,
            'dcos_image_commit': util.dcos_image_commit})

        subprocess.check_call(['chmod', '+x', dest_path('installer_internal_wrapper')])

        # TODO(cmaloney) make this use make_bootstrap_artifacts / that set
        # rather than manually keeping everything in sync
        copy_to_build('packages/cache/bootstrap', bootstrap_filename)
        copy_to_build('packages/cache/bootstrap', installer_bootstrap_filename)
        copy_to_build('packages/cache/bootstrap', bootstrap_active_filename)
        copy_to_build('packages/cache/bootstrap', bootstrap_latest_filename)
        copy_to_build('packages/cache/complete', latest_complete_filename)

        # Copy across gen_extra if it exists
        if os.path.exists('gen_extra'):
            subprocess.check_call(['cp', '-r', 'gen_extra', dest_path('gen_extra')])
        else:
            subprocess.check_call(['mkdir', '-p', dest_path('gen_extra')])

        print("Building docker container in " + build_dir)
        subprocess.check_call(['docker', 'build', '-t', docker_image_name, build_dir])

        print("Building", installer_filename)
        pkgpanda.util.write_string(
            installer_filename,
            pkg_resources.resource_string(__name__, 'bash/dcos_generate_config.sh.in').decode().format(
                genconf_tar=genconf_tar,
                docker_image_name=docker_image_name,
                variant=variant) + '\n#EOF#\n')
        subprocess.check_call(
            ['docker', 'save', docker_image_name],
            stdout=open(genconf_tar, 'w'))
        subprocess.check_call(['tar', 'cvf', '-', genconf_tar], stdout=open(installer_filename, 'a'))
        subprocess.check_call(['chmod', '+x', installer_filename])

        # Cleanup
        subprocess.check_call(['rm', genconf_tar])

    return installer_filename


def make_dcos_launch():
    # NOTE: this needs to be kept in sync with build_dcos_launch.sh
    work_dir = py.path.local.mkdtemp()
    work_dir.join('dcos-launch.spec').write(pkg_resources.resource_string(__name__, 'bash/dcos-launch.spec'))
    work_dir.join('test_util').ensure(dir=True)
    work_dir.join('test_util').join('launch.py').write(pkg_resources.resource_string('test_util', 'launch.py'))
    with work_dir.as_cwd():
        subprocess.check_call(['pyinstaller', 'dcos-launch.spec'])
    subprocess.check_call(['mv', str(work_dir.join('dist').join('dcos-launch')), "dcos-launch"])
    work_dir.remove()

    return "dcos-launch"


def do_create(tag, build_name, reproducible_artifact_path, commit, variant_arguments, all_bootstraps):
    """Create a installer script for each variant in bootstrap_dict.

    Writes a dcos_generate_config.<variant>.sh for each variant in
    bootstrap_dict to the working directory, except for the default variant's
    script, which is written to dcos_generate_config.sh. Returns a dict mapping
    variants to (genconf_version, genconf_filename) tuples.

    Outputs the generated dcos_generate_config.sh as it's artifacts.
    """

    # TODO(cmaloney): Build installers in parallel.
    # Variants are sorted for stable ordering.
    for variant, bootstrap_info in sorted(variant_arguments.items(), key=lambda kv: pkgpanda.util.variant_str(kv[0])):
        print("Building installer for variant:", pkgpanda.util.variant_name(variant))
        bootstrap_installer_name = '{}installer'.format(pkgpanda.util.variant_prefix(variant))
        bootstrap_installer_id = all_bootstraps[bootstrap_installer_name]

        installer_filename = make_installer_docker(variant, bootstrap_info['bootstrap_id'], bootstrap_installer_id)

        yield {
            'channel_path': 'dcos_generate_config.{}sh'.format(pkgpanda.util.variant_prefix(variant)),
            'local_path': installer_filename
        }

    # Build dcos-launch
    # TODO(cmaloney): This really doesn't belong to here, but it's the best place it fits for now.
    #                 dcos-launch works many places which aren't bash / on-premise installers.
    #                 It also isn't dependent on the "reproducible" artifacts at all. Just the commit...
    yield {
        'channel_path': 'dcos-launch',
        'local_path': make_dcos_launch()
    }
