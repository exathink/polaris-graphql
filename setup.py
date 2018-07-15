# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from setuptools import setup
from os import path

# Suppose the package you are build is called polaris.x.y.z

# First create a package directory called polaris/x/y/z with a valid
# __init__.py in it. This is a Python 3 Namespace Package.

# UNCOMMENT 'import' line and import the name of the package you are defining.

# import polaris.x.y.z

here = path.abspath(path.dirname(__file__))


setup(
    # --------------------------------------------------------------------------------
    # UNCOMMENT THE 'name' line and replace it with your package name.

    # name='polaris.x.y.z',

    # -------------------------------------------------------------------------------
    # UNCOMMENT 'version' and replace the version with the one from the right package.
    # Your packages __init__.py must have the __version__ property

    # version=polaris.x.y.z.__version__,

    # -------------------------------------------------------------------------------
    # UNCOMMENT THE 'packages' line and define the Python 3 namespace packages for this package.
    # This should specify a package for each prefix of your package name.

    # packages=['polaris', 'polaris.x', 'polaris.x.y', 'polaris.x.y.z'],

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
