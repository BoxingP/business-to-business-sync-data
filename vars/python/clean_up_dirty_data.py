import datetime

from database import PostgresqlDatabase
from logger import Logger


def main():
    logger = Logger(__name__)
    logger.info('Start to clean up dirty data.')
    time_started = datetime.datetime.utcnow()
    local_db = PostgresqlDatabase('../postgresql/database_config.yaml')
    local_db.open_connection()

    total_number, products_discontinued = local_db.get_product(table='product',
                                                               fields=['product', 'product_type', 'business_unit'],
                                                               product_type='S', is_discontinued=True)

    for product_chunk in products_discontinued:
        if not product_chunk.empty:
            local_db.remove_discontinued_data_in_table(product_chunk, 'list_price')
            local_db.remove_discontinued_data_in_table(product_chunk, 'quote_price')
            local_db.remove_discontinued_data_in_table(product_chunk, 'dummy_quote_price')
            local_db.move_to_product_discontinued(product_chunk)
    local_db.remove_expired_data_in_table('quote_price')
    local_db.remove_expired_data_in_table('dummy_quote_price')
    local_db.remove_expired_data_in_table('list_price')
    local_db.move_to_product_action_backup()

    local_db.close_connection()
    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    logger.info('The time of cleaning up dirty data is %ss.' % round(total_time))


if __name__ == '__main__':
    main()
