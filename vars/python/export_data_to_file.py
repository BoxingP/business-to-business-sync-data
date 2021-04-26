import datetime
import os

import yaml

from database import PostgresqlDatabase
from logger import Logger


def is_night():
    now = datetime.datetime.now().time().strftime('%H:%M:%S')
    return now >= '18:00:00' or now <= '06:00:00'


def main():
    logger = Logger(__name__)
    logger.info('Start to export data to file.')
    time_started = datetime.datetime.now()

    unix_epoch_str = '1970-01-01 00:00:00'
    last_export_time = datetime.datetime.strptime(unix_epoch_str, '%Y-%m-%d %H:%M:%S')
    if os.path.isfile('./export_data_status.yaml'):
        with open('./export_data_status.yaml', 'r') as yaml_file:
            data = yaml.safe_load(yaml_file.read())
        if not data['is_first_run']:
            last_export_time = data['last_run']

    local_db = PostgresqlDatabase('../postgresql/database_config.yaml')
    local_db.open_connection()
    local_db.export_data_to_csv(table='product', last_export_time=last_export_time, is_night=is_night())
    local_db.export_data_to_csv(table='product_action', last_export_time=last_export_time)
    local_db.export_data_to_csv(table='product_quote', last_export_time=last_export_time)
    local_db.export_data_to_csv(table='product_dummy_quote', last_export_time=last_export_time)
    local_db.export_data_to_csv(table='research_group_st_mapping', last_export_time=last_export_time)
    local_db.close_connection()

    time_ended = datetime.datetime.now()
    total_time = (time_ended - time_started).total_seconds()
    logger.info('The time of exporting data is %ss.' % round(total_time))

    data = dict(last_run=time_started, is_first_run=False)
    with open('./export_data_status.yaml', 'w') as yaml_file:
        yaml.dump(data, yaml_file, default_flow_style=False)


if __name__ == '__main__':
    main()
