from setuptools import setup

setup(
    name='waiter',
    version='0.1',
    description='Tool to wait for components to come up before proceeding',
    url='https://dcos.io',
    author='Mesosphere, Inc.',
    author_email='help@dcos.io',
    license='apache2',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
    ],
    packages=['waiter'],
    entry_points={
        'console_scripts': ['waiter.py=waiter.cli:main']
    })
