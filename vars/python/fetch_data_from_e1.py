import datetime

from database import OracleDatabase, PostgresqlDatabase


def main():
    time_started = datetime.datetime.utcnow()

    e1_db = OracleDatabase('../oracle/db.json')
    local_db = PostgresqlDatabase('../postgresql/db.json')
    local_db.open_connection()

    st_list = local_db.run_sql('../postgresql/get_st_list.sql')
    st_list = st_list['st'].tolist()

    while not local_db.product_is_updated(8):
        sku_list = local_db.run_sql('../postgresql/get_sku_list.sql', 8, 100)
        sku_list = sku_list['sku'].tolist()

        e1_db.open_connection()
        for sku in sku_list:
            if e1_db.sku_is_discontinued(sku):
                local_db.update_sku_to_discontinued(sku)
        e1_db.close_connection()

        e1_db.open_connection()
        list_price_data = e1_db.export_list_price_data(sku_list)
        e1_db.close_connection()
        local_db.update_list_price(list_price_data)

        removed_discontinued_sku_list = local_db.remove_discontinued_sku(sku_list)
        if removed_discontinued_sku_list:
            e1_db.open_connection()
            e1_db.export_quote_price_data(removed_discontinued_sku_list, st_list, local_db)
            e1_db.close_connection()

        local_db.update_product_list_date(sku_list)

    local_db.close_connection()

    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    print('Total time is %ss.' % round(total_time))


if __name__ == '__main__':
    main()
