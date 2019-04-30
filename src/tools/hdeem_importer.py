from itertools import chain

import click
import click_log
import click_completion

from metricq.logging import get_logger
from metricq_importer import DataheapToHTAImporter

logger = get_logger()

click_log.basic_config(logger)
logger.setLevel('INFO')

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
def hdeem_cmd(rpc_url, db_token,
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
