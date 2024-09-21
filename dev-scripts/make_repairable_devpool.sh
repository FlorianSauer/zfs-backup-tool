#!/usr/bin/env bash

set -e

zfs destroy devpool/dataset0@test-backup.1

zfs destroy devpool/dataset1@test-backup.1
zfs destroy devpool/dataset1/dataset1-1@test-backup.1
zfs destroy devpool/dataset1/dataset1-1/dataset1-1-1@test-backup.1

zfs destroy -r devpool/dataset2/dataset2-1
