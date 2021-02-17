from database import PostgresqlDatabase


def main():
    local_db = PostgresqlDatabase('../postgresql/db.json')
    local_db.open_connection()

    discontinued_sku_list = local_db.run_sql('../postgresql/get_discontinued_sku_list.sql')
    discontinued_sku_list = discontinued_sku_list['sku'].tolist()
    for sku in discontinued_sku_list:
        local_db.remove_discontinued_quote_price(sku)
    for sku in discontinued_sku_list:
        local_db.move_to_product_discontinued_list(sku)
    local_db.remove_expired_quote_price()
    local_db.remove_expired_list_price()
    local_db.move_to_product_action_list_backup()

    local_db.close_connection()


if __name__ == '__main__':
    main()
