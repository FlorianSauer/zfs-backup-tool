#!/usr/bin/env bash

set -e

zfs destroy devpool/dataset0@test-backup.initial
zfs destroy devpool/dataset0@test-backup.1
zfs destroy devpool/dataset0@test-backup.3
