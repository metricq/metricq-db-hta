# metricq
# Copyright (C) 2019 ZIH,
# Technische Universitaet Dresden,
# Federal Republic of Germany
#
# All rights reserved.
#
# This file is part of metricq.
#
# metricq is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# metricq is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with metricq.  If not, see <http://www.gnu.org/licenses/>.


import asyncio
import tempfile
import json
import os
import datetime

import click

from metricq.logging import get_logger
from metricq.types import Timestamp
from metricq import Client

import cloudant
import pymysql

logger = get_logger()

logger.setLevel('INFO')


# if you know what I mean...
class FakeAgent(Client):
    async def connect(self):
        await super().connect()

        # unfortunately, the manager will take some time, if the number of metrics is quite high,
        # so let's increase the timeout.
        logger.warn("Sending db.register to manager, the response may take some time. Stay a while and listen...")
        await self.rpc('db.register', timeout=360)
        await self.stop()


class ImportMetric(object):
    def __init__(self, metricq_name, import_name, sampling_rate=1, interval_factor=10,
                 interval_min=None, interval_max=None):
        self.metricq_name = metricq_name
        self.import_name = import_name

        self.interval_factor = interval_factor
        self.interval_min = interval_min
        self.interval_max = interval_max
        self.sampling_rate = sampling_rate

        if self.interval_min is None:
            self.interval_min = sampling_rate * 40 * 1e9
        if self.interval_max is None:
            self.interval_max = self._default_interval_max()

    def _default_interval_max(self):
        i = self.interval_min
        assert i > 0
        while True:
            if i * self.interval_factor >= 2.592e15:
                return i
            i *= self.interval_factor

    @property
    def config(self):
        return {
                "name": self.metricq_name,
                "mode": "RW",
                "interval_min": int(self.interval_min),
                "interval_max": int(self.interval_max),
                "interval_factor": self.interval_factor,
            }

    def __str__(self):
        return f'{self.import_name} => {self.metricq_name}, {self.interval_min:,}, {self.interval_max:,}, {self.interval_factor}'


class DataheapToHTAImporter(object):
    def __init__(self, rpc_url, token,
                 couchdb_url: str, couchdb_user: str, couchdb_password: str,
                 import_host: str, import_port: int,
                 import_user: str,  import_password: str, import_database: str,
                 num_workers=3):
        self.rpc_url = rpc_url
        self.token = token

        self.couchdb_client = cloudant.client.CouchDB(couchdb_user, couchdb_password,
                                                      url=couchdb_url, connect=True)
        self.couchdb_session = self.couchdb_client.session()
        self.couchdb_db_config = self.couchdb_client.create_database("config")

        self.import_host = import_host
        self.import_port = import_port
        self.import_user = import_user
        self.import_password = import_password
        self.import_database = import_database

        self.metrics = []
        self.failed_imports = []

        self.last_metric = None
        self.import_begin = None

        self.num_workers = num_workers

    def register(self, metricq_name, import_name, **kwargs):
        self.metrics.append(ImportMetric(metricq_name, import_name, **kwargs))

    @property
    def import_metrics(self):
        return [metric for metric in self.metrics if metric.import_name]

    def run(self):
        assert self.import_begin is None
        if not click.confirm(f'Please make sure the MetricQ db with the token '
                             f'{self.token}" is not running! Continue?'):
            return

        self.update_config()
        self.create_bindings()
        self.import_begin = Timestamp.now()
        self.run_import()

        if self.failed_imports:
            print('The following metrics have failed to import:')
            for metric in self.failed_imports:
                print(f' - {metric.metricq_name}')

    def dry_run(self, check_values=False):
        mysql = pymysql.connect(host=self.import_host, port=self.import_port,
                                user=self.import_user, passwd=self.import_password,
                                db=self.import_database)
        total_count = 0
        for metric in self.metrics:
            with mysql.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) as count, "
                               f"MIN(timestamp) as t_min, MAX(timestamp) as t_max"
                               f" FROM `{metric.import_name}`")
                count, t_min, t_max = cursor.fetchone()
                t_min /= 1e3
                t_max /= 1e3
                click.echo(f'{metric.metricq_name:30s} <== {metric.import_name:30s} with {count:12,} entries')
                click.echo(f'        intervals {metric.interval_min:,}, {metric.interval_max:,}, {metric.interval_factor}')

                dt_min = datetime.datetime.fromtimestamp(t_min)
                dt_max = datetime.datetime.fromtimestamp(t_max)
                interval_avg = (t_max - t_min) / count
                click.echo(f'        time range {dt_min} - {dt_max}, avg interval {interval_avg}')
                delta = datetime.datetime.now() - dt_max
                if not (datetime.timedelta() <
                        delta <
                        datetime.timedelta(hours=8)):
                    click.secho(f'suspicious max time {delta}', bg='red', bold=True)
                    click.confirm('continue?', abort=True)

                expected_interval = 1 / metric.sampling_rate
                tolerance = 1.5
                if not (expected_interval / tolerance < interval_avg < expected_interval * tolerance):
                    click.secho(f'suspicious interval, expected {expected_interval}', bg='red', bold=True)
                    click.confirm('continue?', abort=True)

                if check_values:
                    cursor.execute(f"MIN(value) as value_min, MAX(value) as value_max, "
                                   f" FROM `{metric.import_name}`")
                    value_min, value_max = cursor.fetchone()

                    click.echo(f'        value range {value_min} to {value_max}')
                    if not (-1e9 < value_min < value_max < 1e9):
                        click.secho('suspicious value range', bg='red', bold=True)
                        click.confirm('continue?', abort=True)

                total_count += count
        click.echo(f'Total count {total_count:,}, average {int(total_count/len(self.metrics)):,} per metric')

    def update_config(self):
        config_metrics = []
        for metric in self.metrics:
            config_metrics.append(metric.config)

        try:
            config_document = self.couchdb_db_config[self.token]
        except KeyError:
            raise KeyError(f"Please add a basic configuration in the CouchDB for the db '{self.token}'")

        config_document.fetch()
        current_config = dict(config_document)
        current_metrics = current_config["metrics"]
        current_metric_names = [metric["name"] for metric in current_metrics]
        conflicting_metrics = [metric for metric in self.metrics if metric.metricq_name in current_metric_names]

        if conflicting_metrics:
            print('The following metrics have already been defined in the database, but are in the import set:')
            for metric in conflicting_metrics:
                print(f' - {metric.metricq_name}')

            if not click.confirm('Overwrite old entries?'):
                raise RuntimeError('Please remove the config for the existing metrics or remove them from the import set')

            current_metrics = [metric for metric in current_metrics if metric["name"] not in [metric.metricq_name for metric in conflicting_metrics]]

        current_metrics = current_metrics + config_metrics

        config_document['metrics'] = current_metrics
        config_document.save()

        self.import_config = {
            'type': "file",
            'threads': 2,
            'path': current_config['path'],
            'import': {
                'host': self.import_host,
                'user': self.import_user,
                'password': self.import_password,
                'database': self.import_database
            }
        }

    def create_bindings(self):
        fake_agent = FakeAgent(self.token, self.rpc_url)
        fake_agent.run()

    def _show_last_metric(self, item):
        if self.last_metric:
            return self.last_metric.metricq_name
        return ''

    def run_import(self):
        # setup task queue
        self.queue = asyncio.Queue()
        for metric in self.import_metrics:
            self.queue.put_nowait(metric)
        self.num_import_metrics = self.queue.qsize()

        # run all pending import tasks
        with click.progressbar(length=self.num_import_metrics, label='Importing metrics', item_show_func=self._show_last_metric) as bar:
            asyncio.run(self.import_main(bar))

    async def import_worker(self, bar):
        while True:
            try:
                metric = self.queue.get_nowait()
                self.last_metric = metric
            except asyncio.QueueEmpty:
                return
            await self.import_metric(metric)
            bar.update(1)

    async def import_main(self, bar):
        workers = [self.import_worker(bar) for _ in range(self.num_workers)]
        await asyncio.wait(workers)

    async def import_metric(self, metric):
        # write config into a tmpfile
        conffile, conffile_name = tempfile.mkstemp(prefix='metricq-import-', suffix='.json', text=True)
        with open(conffile, 'w') as conf:
            config = self.import_config.copy()
            config['metrics'] = []
            config['metrics'].append(metric.config)
            json.dump(config, conf)

        try:
            process = await asyncio.create_subprocess_exec("hta_mysql_import",
                                                           '-m', metric.metricq_name,
                                                           '--import-metric', metric.import_name,
                                                           '-c', conffile_name,
                                                           '--max-timestamp', str(int(self.import_begin.posix_ms)))

            ret = await process.wait()
            if ret != 0:
                self.failed_imports.append(metric)
        except FileNotFoundError:
            logger.error('Make sure hta_mysql_import is in your PATH.')

        try:
            os.remove(conffile_name)
            pass
        except OSError:
            pass