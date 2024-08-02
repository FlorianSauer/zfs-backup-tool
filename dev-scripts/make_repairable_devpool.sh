#!/usr/bin/env bash


zfs destroy -r devpool/dataset2/dataset2-1
zfs list -H -o name -t snapshot devpool/dataset0 | grep 'devpool/dataset0@test-backup.*' | xargs -n1 zfs destroy

zfs destroy devpool/dataset1@test-backup.1
zfs destroy devpool/dataset1/dataset1-1@test-backup.1
zfs destroy devpool/dataset1/dataset1-1/dataset1-1-1@test-backup.1
