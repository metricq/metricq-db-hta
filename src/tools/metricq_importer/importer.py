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


from itertools import chain

import asyncio

import tempfile
import json
import os

import click
import click_log
import click_completion

from metricq.logging import get_logger
from metricq.types import Timestamp
from metricq import Client

import cloudant

logger = get_logger()

click_log.basic_config(logger)
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
    def __init__(self, metricq_name, import_name, sampling_rate, interval_factor=10,
                 interval_min=None, interval_max=None):
        self.metricq_name = metricq_name
        self.import_name = import_name

        self.interval_factor = interval_factor
        self.interval_min = interval_min
        self.interval_max = interval_max

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


class DataheapToHTAImporter(object):
    def __init__(self, rpc_url, token,
                 couchdb_url, couchdb_user, couchdb_password,
                 import_host, import_user,  import_password, import_database,
                 num_workers=2):
        self.rpc_url = rpc_url
        self.token = token

        self.couchdb_client = cloudant.client.CouchDB(couchdb_user, couchdb_password,
                                                      url=couchdb_url, connect=True)
        self.couchdb_session = self.couchdb_client.session()
        self.couchdb_db_config = self.couchdb_client.create_database("config")

        self.import_host = import_host
        self.import_user = import_user
        self.import_password = import_password
        self.import_database = import_database

        self.metrics = []
        self.failed_imports = []

        self.last_metric = None
        self.import_begin = None

        self.num_workers = num_workers

    def register(self, metricq_name, import_name, sampling_rate, interval_factor=10,
                 interval_min=None, interval_max=None):
        self.metrics.append(ImportMetric(metricq_name, import_name, sampling_rate, interval_factor,
                                         interval_min, interval_max))

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
                                                           '--max-timestamp', str(int(self.import_begin.posix_ms)),
                                                           '--mysql-chunk-size', str(10000000))

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


click_completion.init()


@click.command(help="Start the import process into MetricQ")
@click.argument('rpc-url', default='amqp://localhost/')
@click.option('--db-token')
@click.option('--couchdb-url', default='http://127.0.0.1:5984')
@click.option('--couchdb-user', default='admin')
@click.option('--couchdb-password', default='admin', prompt=True)
@click.option('--import-host', default='127.0.0.1:3306')
@click.option('--import-user', default='admin')
@click.option('--import-password', default='admin', prompt=True)
@click.option('--import-database', default='db')
def importer_cmd(rpc_url, db_token,
                 couchdb_url, couchdb_user, couchdb_password,
                 import_host, import_user,  import_password, import_database):

    importer = DataheapToHTAImporter(rpc_url, db_token, couchdb_url, couchdb_user, couchdb_password,
                                     import_host, import_user,  import_password, import_database)
    for hostid in chain(range(4001, 4232+1), range(5001, 5612+1), range(6001, 6612+1)):
        hostname = f'taurusi{hostid}'
        importer.register(metricq_name=f'taurus.{hostname}.power', import_name=f'{hostname}_watts', sampling_rate=1.0)
        importer.register(metricq_name=f'taurus.{hostname}.cpu0.power', import_name=None, sampling_rate=1.0)
        importer.register(metricq_name=f'taurus.{hostname}.cpu1.power', import_name=None, sampling_rate=1.0)
    # importer = HDEEMImporter(rpc_url, db_token, couchdb_url, couchdb_user, couchdb_password)
    importer.run()
