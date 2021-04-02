import datetime

from database import PostgresqlDatabase
from logger import Logger


def is_night():
    now = datetime.datetime.now().time().strftime('%H:%M:%S')
    return now >= '18:00:00' or now <= '06:00:00'


def main():
    logger = Logger(__name__)
    logger.info('Start to export data to file.')
    time_started = datetime.datetime.utcnow()

    local_db = PostgresqlDatabase('../postgresql/database_config.yaml')
    local_db.open_connection()
    local_db.export_data_to_csv('product', is_night())
    local_db.export_data_to_csv('product_action')
    local_db.export_data_to_csv('product_quote')
    local_db.export_data_to_csv('product_shared_quote')
    local_db.export_data_to_csv('research_group_st_mapping')
    local_db.close_connection()

    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    logger.info('The time of exporting data is %ss.' % round(total_time))


if __name__ == '__main__':
    main()
