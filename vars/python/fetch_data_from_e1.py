import datetime
import os

import yaml

from database import OracleDatabase, PostgresqlDatabase, datetime_to_jde_julian_date
from logger import Logger


def main():
    start_point = datetime.datetime.now()
    e1_db = OracleDatabase('../oracle/database_config.yaml')
    local_db = PostgresqlDatabase('../postgresql/database_config.yaml')
    logger = Logger(__name__)
    local_db.open_connection()

    update_product_status(e1_db, local_db, logger)
    update_ship_to_status(e1_db, local_db, logger)

    update_product_list_price(e1_db, local_db, 'list_price', logger)

    total_number, products = local_db.get_product(table='product', product_type='S', is_discontinued=False)
    logger.info('The total number of non discontinued SKUs is %s.' % total_number)
    total_number, products = local_db.get_product(table='product', product_type='P', is_discontinued=False)
    logger.info('The total number of non discontinued PPLs is %s.' % total_number)
    total_number, sts = local_db.get_st('S')
    logger.info('The total number of active STs is %s.' % total_number)
    logger.info('Start to update products quote price.')
    time_started = datetime.datetime.utcnow()
    for st_chunk in sts:
        update_product_quote_price(e1_db, local_db, st_chunk['st'].tolist(), 'quote_price', 'P', ['F', 'E', 'D', 'P'])
        update_product_quote_price(e1_db, local_db, st_chunk['st'].tolist(), 'quote_price', 'S', ['F', 'E', 'D', 'P'])
    logger.info('Start to update products dummy quote price')
    update_product_quote_price(e1_db, local_db, [90714], 'dummy_quote_price', 'P', ['D', 'P'])
    update_product_quote_price(e1_db, local_db, [90714], 'dummy_quote_price', 'S', ['D', 'P'])
    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    logger.info('The time of updating products quote price is %ss.' % round(total_time))

    local_db.close_connection()

    if os.path.isfile('./fetch_data_status.yaml'):
        with open('./fetch_data_status.yaml', 'r') as yaml_file:
            data = yaml.safe_load(yaml_file.read())
        data['last_run_date'] = datetime_to_jde_julian_date(start_point)
        data['last_run_time'] = int(start_point.time().strftime('%H%M%S'))
        data['is_first_run'] = False
    else:
        data = dict(last_run_date=datetime_to_jde_julian_date(start_point),
                    last_run_time=int(start_point.time().strftime('%H%M%S')), is_first_run=False
                    )
    with open('./fetch_data_status.yaml', 'w') as yaml_file:
        yaml.dump(data, yaml_file, default_flow_style=False)


def update_ship_to_status(oracle, postgresql, logger):
    total_number, sts = postgresql.get_st()
    logger.info('The total number of STs is %s.' % total_number)
    logger.info('Start to update STs status.')
    time_started = datetime.datetime.utcnow()
    for st_chunk in sts:
        st_list = st_chunk['st'].tolist()
        oracle.open_connection()
        st_status = oracle.get_st_status(st_list)
        oracle.close_connection()
        if st_status is not None:
            postgresql.update_st_status(st_status)
    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    logger.info('The time of updating STs status is %ss.' % round(total_time))


def update_product_quote_price(oracle, postgresql, st_list, table, product_type, quote_type):
    total_number, products = postgresql.get_product(table='product', product_type=product_type, is_discontinued=False)
    for product_chunk in products:
        product_list = product_chunk['product_id'].tolist()
        oracle.open_connection()
        sku_quote_price, ppl_quote_price = oracle.get_product_quote_price(product_list, st_list, quote_type,
                                                                          product_type=product_type)
        oracle.close_connection()
        if not all(df is None for df in (sku_quote_price, ppl_quote_price)):
            postgresql.update_price(ppl_quote_price, table)
            postgresql.update_price(sku_quote_price, table)


def update_product_list_price(oracle, postgresql, table, logger):
    logger.info('Start to update products list price.')
    time_started = datetime.datetime.utcnow()
    total_number, products = postgresql.get_product(table='product', product_type='S', is_discontinued=False)
    for product_chunk in products:
        product_list = product_chunk['product_id'].tolist()
        oracle.open_connection()
        product_list_price = oracle.get_product_list_price(product_list)
        oracle.close_connection()
        if product_list_price is not None:
            postgresql.update_price(product_list_price, table)
    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    logger.info('The time of updating products list price is %ss.' % round(total_time))


def update_product_status(oracle, postgresql, logger):
    total_number, products = postgresql.get_product(table='product', product_type='S', is_discontinued=False)
    logger.info('The total number of products is %s.' % total_number)
    logger.info('Start to update products status.')
    time_started = datetime.datetime.utcnow()
    for product_chunk in products:
        product_list = product_chunk['product_id'].tolist()
        oracle.open_connection()
        product_discontinued_status = oracle.get_product_discontinued_status(product_list)
        oracle.close_connection()
        if not product_discontinued_status.empty:
            postgresql.update_discontinued_status(product_discontinued_status)
    total_number, products_discontinued = postgresql.get_product(table='product_discontinued')
    logger.info('Check discontinued products status.')
    for product_chunk in products_discontinued:
        product_list = product_chunk['product_id'].tolist()
        oracle.open_connection()
        product_discontinued_status = oracle.get_product_discontinued_status(product_list)
        oracle.close_connection()
        if not product_discontinued_status.empty:
            logger.info('Move non discontinued products back.')
            postgresql.move_non_discontinued_to_product(product_discontinued_status)
    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    logger.info('The time of updating products status is %ss.' % round(total_time))


if __name__ == '__main__':
    main()
