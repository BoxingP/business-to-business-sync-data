import datetime

from database import OracleDatabase, PostgresqlDatabase
from logger import Logger


def main():
    e1_db = OracleDatabase('../oracle/database_config.yaml')
    local_db = PostgresqlDatabase('../postgresql/database_config.yaml')
    logger = Logger(__name__)
    local_db.open_connection()

    update_product_discontinued_status(e1_db, local_db, logger)

    update_product_list_price(e1_db, local_db, 'list_price', logger)

    total_number, products = local_db.get_product('../postgresql/get_product.sql')
    logger.info('The total number of non discontinued products is %s.' % total_number)
    total_number, sts = local_db.get_st()
    logger.info('The total number of STs is %s.' % total_number)
    logger.info('Start to update products quote price.')
    time_started = datetime.datetime.utcnow()
    for st_chunk in sts:
        update_product_quote_price(e1_db, local_db, st_chunk['st'].tolist(), 'quote_price', ['f', 'e', 'd', 'p'])
    logger.info('Start to update products dummy quote price')
    update_product_quote_price(e1_db, local_db, [90714], 'shared_quote_price', ['d', 'p'])
    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    logger.info('The time of updating products quote price is %ss.' % round(total_time))

    local_db.close_connection()


def update_product_quote_price(oracle, postgresql, st_list, table, quote_type):
    total_number, products = postgresql.get_product('../postgresql/get_product.sql')
    for product_chunk in products:
        product_list = product_chunk['sku'].tolist()
        oracle.open_connection()
        sku_quote_price, pl_quote_price = oracle.get_product_quote_price(product_list, st_list, quote_type)
        oracle.close_connection()
        if not all(df is None for df in (sku_quote_price, pl_quote_price)):
            postgresql.update_price(pl_quote_price, table)
            postgresql.update_price(sku_quote_price, table)
        postgresql.update_product_list(product_list, 'Updated Quote Price')


def update_product_list_price(oracle, postgresql, table, logger):
    logger.info('Start to update products list price.')
    time_started = datetime.datetime.utcnow()
    total_number, products = postgresql.get_product('../postgresql/get_product.sql')
    for product_chunk in products:
        product_list = product_chunk['sku'].tolist()
        oracle.open_connection()
        product_list_price = oracle.get_product_list_price(product_list)
        oracle.close_connection()
        if product_list_price is not None:
            postgresql.update_price(product_list_price, table)
        postgresql.update_product_list(product_list, 'Updated List Price')
    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    logger.info('The time of updating products list price is %ss.' % round(total_time))


def update_product_discontinued_status(oracle, postgresql, logger):
    total_number, products = postgresql.get_product('../postgresql/get_product.sql')
    logger.info('The total number of products is %s.' % total_number)
    logger.info('Start to update products discontinued status.')
    time_started = datetime.datetime.utcnow()
    for product_chunk in products:
        product_list = product_chunk['sku'].tolist()
        oracle.open_connection()
        product_discontinued_status = oracle.get_product_discontinued_status(product_list)
        oracle.close_connection()
        if not product_discontinued_status.empty:
            postgresql.update_discontinued_status(product_discontinued_status)
        postgresql.update_product_list(product_list, 'Updated Discontinued')
    total_number, products_discontinued = postgresql.get_product('../postgresql/get_product_discontinued.sql')
    logger.info('Check discontinued products status.')
    for product_chunk in products_discontinued:
        product_list = product_chunk['sku'].tolist()
        oracle.open_connection()
        product_discontinued_status = oracle.get_product_discontinued_status(product_list)
        oracle.close_connection()
        if not product_discontinued_status.empty:
            logger.info('Move non discontinued products back.')
            postgresql.move_non_discontinued_to_product_list(product_discontinued_status)
    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    logger.info('The time of updating products discontinued status is %ss.' % round(total_time))


if __name__ == '__main__':
    main()
