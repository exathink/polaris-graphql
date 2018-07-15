#!/usr/bin/env bash
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
polaris-build/scripts/build_python_packages.sh ${THIS_DIR} "$@"
