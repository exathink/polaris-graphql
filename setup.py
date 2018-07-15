# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from setuptools import setup
from os import path

import polaris.graphql

here = path.abspath(path.dirname(__file__))


setup(
    name='polaris.graphql',
    version=polaris.graphql.__version__,
    packages=['polaris', 'polaris.graphql'],
    url='',
    license = 'Commercial',
    author='Krishna Kumar',
    author_email='kkumar@exathink.com',
    description='',
    long_description='',
    classifiers=[
        'Programming Language :: Python :: 3.5'
    ],
    # Run time dependencies - we will assume pytest is dependency of all packages.
    install_requires=[
        'pytest'
    ]
)
