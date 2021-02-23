from database import PostgresqlDatabase


def main():
    local_db = PostgresqlDatabase('../postgresql/database_config.yaml')
    local_db.open_connection()

    discontinued_product_list = local_db.get_discontinued_product_list()
    if discontinued_product_list:
        local_db.remove_discontinued_data_in_table(discontinued_product_list, 'list_price')
        local_db.remove_discontinued_data_in_table(discontinued_product_list, 'quote_price')
        local_db.move_to_product_discontinued_list(discontinued_product_list)
    local_db.remove_expired_data_in_table('quote_price')
    local_db.remove_expired_data_in_table('list_price')
    local_db.move_to_product_action_list_backup()

    local_db.close_connection()


if __name__ == '__main__':
    main()
