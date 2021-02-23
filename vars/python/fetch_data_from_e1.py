import datetime

from database import OracleDatabase, PostgresqlDatabase


def main():
    time_started = datetime.datetime.utcnow()

    e1_db = OracleDatabase('../oracle/database_config.yaml')
    local_db = PostgresqlDatabase('../postgresql/database_config.yaml')
    local_db.open_connection()

    while True:
        if local_db.product_discontinued_status_is_updated():
            break
        product_list = local_db.get_not_updated_discontinued_status_product_list(1000)
        e1_db.open_connection()
        product_list = e1_db.get_product_discontinued_status(product_list)
        e1_db.close_connection()
        local_db.update_product_list(product_list, 'Updated Discontinued')
    product_discontinued_list = local_db.get_product_discontinued_list()
    if product_discontinued_list:
        e1_db.open_connection()
        product_discontinued_list = e1_db.get_product_discontinued_status(product_discontinued_list)
        e1_db.close_connection()
        local_db.move_non_discontinued_to_product_list(product_discontinued_list)

    while True:
        if local_db.product_list_price_is_updated():
            break
        product_list = local_db.get_not_updated_list_price_product_list(1000)
        e1_db.open_connection()
        product_list = e1_db.get_product_list_price(product_list)
        e1_db.close_connection()
        local_db.update_list_price(product_list)
        local_db.update_product_list(product_list, 'Updated List Price')

    st_list = local_db.get_st_list()
    while True:
        if local_db.product_quote_price_is_updated():
            break
        product_list = local_db.get_not_updated_quote_price_product_list(1000)
        e1_db.open_connection()
        product_list = e1_db.get_product_quote_price(product_list, st_list)
        e1_db.close_connection()
        local_db.update_quote_price(product_list)
        local_db.update_product_list(product_list, 'Updated Quote Price')

    local_db.close_connection()
    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    print('Total time is %ss.' % round(total_time))


if __name__ == '__main__':
    main()
