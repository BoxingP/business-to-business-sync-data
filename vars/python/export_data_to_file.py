import datetime

from database import PostgresqlDatabase


def is_night():
    if datetime.datetime.now().time().strftime('%H:%M:%S') >= '18:00:00':
        return True
    else:
        return False


def main():
    local_db = PostgresqlDatabase('../postgresql/db.json')
    local_db.open_connection()
    if is_night():
        local_db.export_table_data('product', '*')
    else:
        local_db.export_table_data('product', 'sku, discontinued, business_unit')
    local_db.export_table_data('quote')
    local_db.export_table_data('product_action')
    local_db.export_table_data('group_st_mapping')
    local_db.close_connection()


if __name__ == '__main__':
    main()
