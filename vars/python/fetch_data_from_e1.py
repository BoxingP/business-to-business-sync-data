import datetime

from database import OracleDatabase, PostgresqlDatabase


def main():
    time_started = datetime.datetime.utcnow()

    e1_db = OracleDatabase('../oracle/database_config.yaml')
    local_db = PostgresqlDatabase('../postgresql/database_config.yaml')
    local_db.open_connection()

    update_product_discontinued_status(e1_db, local_db)
    update_product_list_price(e1_db, local_db, 'list_price')
    sts = local_db.get_st()
    for st_chunk in sts:
        update_product_quote_price(e1_db, local_db, st_chunk['st'].tolist(), 'quote_price', ['f', 'e', 'd', 'p'])
    update_product_quote_price(e1_db, local_db, [90714], 'shared_quote_price', ['d', 'p'])
    local_db.close_connection()
    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    print('Total time is %ss.' % round(total_time))


def update_product_quote_price(oracle, postgresql, st_list, table, quote_type):
    products = postgresql.get_product('../postgresql/get_product.sql')
    for product_chunk in products:
        product_list = product_chunk['sku'].tolist()
        oracle.open_connection()
        product_quote_price = oracle.get_product_quote_price(product_list, st_list, quote_type)
        oracle.close_connection()
        if not all(df is None for df in product_quote_price):
            postgresql.update_price(product_quote_price[1], table)
            postgresql.update_price(product_quote_price[0], table)
        postgresql.update_product_list(product_list, 'Updated Quote Price')


def update_product_list_price(oracle, postgresql, table):
    products = postgresql.get_product('../postgresql/get_product.sql')
    for product_chunk in products:
        product_list = product_chunk['sku'].tolist()
        oracle.open_connection()
        product_list_price = oracle.get_product_list_price(product_list)
        oracle.close_connection()
        if product_list_price is not None:
            postgresql.update_price(product_list_price, table)
        postgresql.update_product_list(product_list, 'Updated List Price')


def update_product_discontinued_status(oracle, postgresql):
    products = postgresql.get_product('../postgresql/get_product.sql')
    for product_chunk in products:
        product_list = product_chunk['sku'].tolist()
        oracle.open_connection()
        product_discontinued_status = oracle.get_product_discontinued_status(product_list)
        oracle.close_connection()
        if not product_discontinued_status.empty:
            postgresql.update_discontinued_status(product_discontinued_status)
        postgresql.update_product_list(product_list, 'Updated Discontinued')
    products_discontinued = postgresql.get_product('../postgresql/get_product_discontinued.sql')
    for product_chunk in products_discontinued:
        product_list = product_chunk['sku'].tolist()
        oracle.open_connection()
        product_discontinued_status = oracle.get_product_discontinued_status(product_list)
        oracle.close_connection()
        if not product_discontinued_status.empty:
            postgresql.move_non_discontinued_to_product_list(product_discontinued_status)


if __name__ == '__main__':
    main()
