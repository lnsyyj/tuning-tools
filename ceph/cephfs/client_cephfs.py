#!/usr/bin/python3
# -*- coding: UTF-8 -*-
"""
Copyright 2021 XCDATA, Inc
Author: Jiang Yu <lnsyyj@hotmail.com>
License: LGPLv2
"""

import cephfs
import datetime
import time
import functools

def run_time(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kw):
        start = time.time()
        res = fn(*args, **kw)
        print('%s (): run %f s' % (fn.__name__, time.time() - start))
        return res
    return wrapper

class ClientCephfs:

    def __init__(self, cephfs_root_dir, cephfs_conf_path, cephfs_name):
        self.cephfs_root_dir = cephfs_root_dir
        self.cephfs_conf_path = cephfs_conf_path
        self.cephfs_name = cephfs_name

        self.fs = cephfs.LibCephFS()
        self.fs.conf_read_file(cephfs_conf_path)
        self.fs.mount(cephfs_root_dir, cephfs_name)
        self.fd = -1

    @run_time
    def dir_mkdir(self, dir_name):
        self.fs.mkdir(dir_name, 777)

    @run_time
    def file_open(self, file_name):
        self.fd = self.fs.open(file_name, 'w')

    @run_time
    def file_close(self):
        self.fs.close(self.fd)

    @run_time
    def file_stat(self, file_name):
        # Get a file's extended statistics and attributes.
        # stat -> ceph_statx
        res = self.fs.stat(file_name)
        print(res)

    @run_time
    def file_statfs(self, file_name):
        # Perform a statfs on the ceph file system.  This call fills in file system wide statistics into the passed in buffer.
        # statfs -> ceph_statfs
        #   struct statvfs {
        #     unsigned long  f_bsize;    /* file system block size */
        #     unsigned long  f_frsize;   /* fragment size */
        #     fsblkcnt_t     f_blocks;   /* size of fs in f_frsize units */
        #     fsblkcnt_t     f_bfree;    /* # free blocks */
        #     fsblkcnt_t     f_bavail;   /* # free blocks for non-root */
        #     fsfilcnt_t     f_files;    /* # inodes */
        #     fsfilcnt_t     f_ffree;    /* # free inodes */
        #     fsfilcnt_t     f_favail;   /* # free inodes for non-root */
        #     unsigned long  f_fsid;     /* file system ID */
        #     unsigned long  f_flag;     /* mount flags */
        #     unsigned long  f_namemax;  /* maximum filename length */
        #   };
        # {'f_bsize': 4194304L, 'f_bavail': 13771L, 'f_fsid': 18446744073709551615L, 'f_favail': 18446744073709551615L, 'f_files': 21L, 'f_frsize': 4194304L, 'f_blocks': 13772L, 'f_ffree': 0L, 'f_bfree': 13771L, 'f_namemax': 255L, 'f_flag': 0L}
        res = self.fs.statfs(file_name)
        #print(res)

    @run_time
    def file_fstat(self):
        # Get an open file's extended statistics and attributes.
        # fstat -> ceph_fstatx
        res = self.fs.fstat(self.fd)
        print(res)

if __name__ == '__main__':
    r = ClientCephfs("/", "/etc/ceph/ceph.conf", "cephfs")
#    r.dir_mkdir("/mydir")
#    r.file_open("/mydir/test_file_1")
#    r.file_stat("/mydir/test_file_1")
#    r.file_fstat()
#    r.file_close()
    for i in range(100):
      r.file_statfs("/mydir/test_file_1")

