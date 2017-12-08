from ftplib import FTP
import os
from os.path import exists as path_exists
from os import makedirs
from os import listdir
from os.path import join as path_join
from shutil import rmtree, move
import filecmp
from datetime import datetime, timedelta

utcnow = datetime.utcnow

from pandas import DataFrame, read_csv, concat
from numpy import inf, nan
from .. import settings

import logging

logger = logging.getLogger(__name__)
DEBUG  = logger.debug
INFO   = logger.info
WARN   = logger.warn


class StorageEngine(object):

    def __init__(self):
        self._data_paths = []
        self._servers    = []
        self._tmp_dir    = 'storage_tmp'

    def add_server(self, addr):
        self._servers.append(addr)
        return self

    def add_data_item(self, data_path):
        self._data_paths.append(data_path)
        return self

    def run(self, asof=None, lookback=5):
        # Setup
        asof = asof or utcnow()
        root = settings.workspace_cryptoccy
        assert path_exists(root)

        pem_file = settings.svc_pem_file
        assert path_exists(pem_file)

        username = settings.svc_username

        # Main Loop
        for idx in reversed(range(lookback)):
            d = asof - timedelta(days=idx)
            for data_path in self._data_paths:
                data_path = data_path.replace('.', os.path.sep)
                files = self.download_data_scp(d, data_path, root, username, pem_file)
                if len(files) > 0:
                    self._merge(files, d, data_path, root)

        self._delete_temp_dir(root)

    def download_data_scp(self, asof, data_path, root, username, pem_file):
        """Pull data from server://.../archive/xxx/yyyymmdd.csv
        to local://.../storage_tmp/xxx/yyyymmdd.csv
        """
        ymd         = asof.strftime('%Y%m%d')
        files       = []

        for server in self._servers:
            remote_file = path_join('/data/crypto_ccy/archive', data_path, "{}.csv".format(ymd))

            local_path  = path_join(root, self._tmp_dir, data_path)
            makedirs(local_path, exist_ok=True)

            local_file = path_join(local_path, "{}_{}.csv".format(ymd, server))
            path_exists(local_file) and os.remove(local_file)

            cmd = 'scp -ri "{pem}" "{username}@{server}:{remote_file}" "{local_file}"'.format(
                    pem=pem_file, username=username, server=server, 
                    remote_file=remote_file, local_file=local_file
                )
            os.system(cmd)

            if path_exists(local_file):
                files.append(local_file)

        return files

    def _merge(self, files, asof, data_path, root):
        ymd      = asof.strftime('%Y%m%d')
        dst_path = path_join(root, 'storage', data_path)
        makedirs(dst_path, exist_ok=True)
        dst      = path_join(dst_path, ymd + '.csv')

        if len(files) == 1:
            src = files[0]
            move(src, dst)

        elif len(files) > 1:
            src_df = read_csv(files[0])
            for file in files[1:]:
                bk_df  = read_csv(file)
                src_df = concat([src_df,bk_df]).drop_duplicates().reset_index(drop=True)
            src_df.to_csv(dst, index=False, float_format='%g')
            
        else:
            raise FileNotFoundError("No file Found")

    def _delete_temp_dir(self, root):
        dname = path_join(root, self._tmp_dir)
        rmtree(dname, ignore_errors=True)
