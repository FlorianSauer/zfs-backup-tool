#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

bash "$SCRIPT_DIR/teardown_dev_zfs.sh"
bash "$SCRIPT_DIR/setup_dev_zfs.sh"
