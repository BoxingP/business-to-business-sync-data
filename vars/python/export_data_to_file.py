import datetime

from database import PostgresqlDatabase


def is_night():
    if datetime.datetime.now().time().strftime('%H:%M:%S') >= '18:00:00':
        return True
    else:
        return False


def main():
    local_db = PostgresqlDatabase('../postgresql/database_config.yaml')
    local_db.open_connection()
    local_db.export_data_to_csv('product', is_night())
    local_db.export_data_to_csv('product_action')
    local_db.export_data_to_csv('product_quote')
    local_db.export_data_to_csv('research_group_st_mapping')
    local_db.close_connection()


if __name__ == '__main__':
    main()
