import datetime

from database import OracleDatabase, PostgresqlDatabase


def main():
    time_started = datetime.datetime.utcnow()

    e1_db = OracleDatabase('../oracle/database_config.yaml')
    local_db = PostgresqlDatabase('../postgresql/database_config.yaml')
    local_db.open_connection()

    update_product_discontinued_status(e1_db, local_db)
    update_product_list_price(e1_db, local_db)
    update_product_quote_price(e1_db, local_db)

    local_db.close_connection()
    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    print('Total time is %ss.' % round(total_time))


def update_product_quote_price(oracle, postgresql):
    st_list = postgresql.get_st_list()
    while True:
        if postgresql.product_quote_price_is_updated():
            break
        product_list = postgresql.get_not_updated_quote_price_product_list(1000)
        oracle.open_connection()
        product_list = oracle.get_product_quote_price(product_list, st_list)
        oracle.close_connection()
        postgresql.update_quote_price(product_list)
        postgresql.update_product_list(product_list, 'Updated Quote Price')


def update_product_list_price(oracle, postgresql):
    while True:
        if postgresql.product_list_price_is_updated():
            break
        product_list = postgresql.get_not_updated_list_price_product_list(1000)
        oracle.open_connection()
        product_list = oracle.get_product_list_price(product_list)
        oracle.close_connection()
        postgresql.update_list_price(product_list)
        postgresql.update_product_list(product_list, 'Updated List Price')


def update_product_discontinued_status(oracle, postgresql):
    while True:
        if postgresql.product_discontinued_status_is_updated():
            break
        product_list = postgresql.get_not_updated_discontinued_status_product_list(1000)
        oracle.open_connection()
        product_list = oracle.get_product_discontinued_status(product_list)
        oracle.close_connection()
        postgresql.update_discontinued_status(product_list)
        postgresql.update_product_list(product_list, 'Updated Discontinued')
    product_discontinued_list = postgresql.get_product_discontinued_list()
    if product_discontinued_list:
        oracle.open_connection()
        product_discontinued_list = oracle.get_product_discontinued_status(product_discontinued_list)
        oracle.close_connection()
        postgresql.move_non_discontinued_to_product_list(product_discontinued_list)


if __name__ == '__main__':
    main()
