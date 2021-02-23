import datetime

import cx_Oracle
import pandas as pd
import psycopg2

import yaml


def jde_julian_date_to_datetime(jd, var='000000'):
    jd = str(jd)
    var = str(var)
    year = 1900 + int(jd[:1]) * 100 + int(jd[1:3])
    date = datetime.datetime(year, 1, 1) + datetime.timedelta(int(jd[3:]) - 1)
    clock = datetime.datetime.strptime(var, '%H%M%S').time()
    full_date = datetime.datetime.combine(date, clock)
    return full_date


def datetime_to_jde_julian_date(time):
    year = time.year
    day = time.timetuple().tm_yday
    first = str(int(str(year)[:2]) - 19)
    middle = str(year)[2:]
    last = str(day).zfill(3)
    julian_date = first + middle + last
    return int(julian_date)


class Database(object):
    def __init__(self, config_file):
        self.config = self.load_config(config_file)
        self.connection = None

    @staticmethod
    def load_config(config_file):
        with open(config_file, 'r', encoding='UTF-8') as file:
            config = yaml.load(file, Loader=yaml.BaseLoader)
        return config

    @staticmethod
    def generate_sql(sql, *args):
        return open(sql, 'r').read().format(*args)

    def run_sql(self, sql, *args):
        sql = self.generate_sql(sql, *args)
        return pd.read_sql(sql, con=self.connection)

    def close_connection(self):
        self.connection.close()


class OracleDatabase(Database):
    def __init__(self, config_file):
        super().__init__(config_file)

    def open_connection(self):
        login = self.config['oracle']
        connection = cx_Oracle.connect(login['username'], login['password'],
                                       login['host'] + ':' + str(login['port']) + '/' + login['schema'])
        self.connection = connection

    def get_product_discontinued_status(self, product_list):
        data = self.run_sql('../oracle/get_non_discontinued_sku.sql', str(product_list)[1:-1])
        non_discontinued_list = [product.strip() for product in data['SKU'].tolist()]
        discontinued_list = list(set(product_list) - set(non_discontinued_list))
        non_discontinued_df = pd.DataFrame(non_discontinued_list, columns=['SKU'])
        non_discontinued_df['DISCONTINUED'] = '0'
        discontinued_df = pd.DataFrame(discontinued_list, columns=['SKU'])
        discontinued_df['DISCONTINUED'] = '1'
        return non_discontinued_df.append(discontinued_df, ignore_index=True)

    def get_product_list_price(self, product_list):
        data = self.run_sql('../oracle/get_product_list_price.sql', str(product_list)[1:-1],
                            datetime_to_jde_julian_date(datetime.datetime.now()))
        data['SKU'] = data['SKU'].str.strip()
        data['EFFECTIVE_DATE'] = data['EFFECTIVE_DATE'].apply(jde_julian_date_to_datetime)
        data['EXPIRATION_DATE'] = data['EXPIRATION_DATE'].apply(jde_julian_date_to_datetime, var='235959')
        data['UPDATED_DATE'] = data['UPDATED_DATE'].apply(jde_julian_date_to_datetime)
        return data

    def get_product_quote_price(self, product_list, st_list):
        current_date = datetime_to_jde_julian_date(datetime.datetime.now())
        data = pd.DataFrame()
        for st in st_list:
            for product in product_list:
                f_quote_price_data = self.run_sql('../oracle/get_f_quote_price.sql', product, st, current_date)
                e_quote_price_data = self.run_sql('../oracle/get_e_quote_price.sql', product, st, current_date)
                d_quote_price_data = self.run_sql('../oracle/get_d_quote_price.sql', product, st, current_date)
                p_quote_price_data = self.run_sql('../oracle/get_p_quote_price.sql', product, st, current_date)
                result = pd.concat([f_quote_price_data, e_quote_price_data, d_quote_price_data, p_quote_price_data],
                                   ignore_index=True)
                data = data.append(result, ignore_index=True)
        data['EFFECTIVE_DATE'] = data['EFFECTIVE_DATE'].apply(jde_julian_date_to_datetime)
        data['EXPIRATION_DATE'] = data['EXPIRATION_DATE'].apply(jde_julian_date_to_datetime, var='235959')
        return data


class PostgresqlDatabase(Database):
    def __init__(self, config_file):
        super().__init__(config_file)

    def open_connection(self):
        login = self.config['postgresql']
        connection = psycopg2.connect(database=login['database'], user=login['username'], password=login['password'],
                                      host=login['host'], port=login['port'])
        self.connection = connection

    def product_discontinued_status_is_updated(self):
        hour = self.config['other']['table_needs_to_be_updated_in_hours']
        result = self.run_sql('../postgresql/check_product_discontinued_status_is_updated.sql', hour)
        if result.empty:
            return True
        else:
            return False

    def get_not_updated_discontinued_status_product_list(self, number):
        hour = self.config['other']['table_needs_to_be_updated_in_hours']
        product_list = self.run_sql('../postgresql/get_not_updated_discontinued_status_product_list.sql', hour, number)
        return product_list['sku'].tolist()

    def update_list_price(self, data):
        for index in range(0, data.shape[0]):
            sku_value = data.iloc[index, 0].strip()
            list_price_value = data.iloc[index, 1]
            effective_date_value = data.iloc[index, 2]
            expiration_date_value = data.iloc[index, 3]
            updated_date_value = data.iloc[index, 4]
            if self.list_price_is_exist(sku_value, effective_date_value, expiration_date_value):
                update_sql = self.generate_sql('../postgresql/update_list_price.sql',
                                               sku_value, list_price_value, effective_date_value, expiration_date_value,
                                               updated_date_value, 'System',
                                               datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            else:
                update_sql = self.generate_sql('../postgresql/insert_list_price.sql',
                                               sku_value, list_price_value, effective_date_value, expiration_date_value,
                                               updated_date_value, 'System',
                                               datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            cursor = self.connection.cursor()
            cursor.execute(update_sql)
            self.connection.commit()

    def update_product_list(self, data, operator='System', time='NOW()'):
        cursor = self.connection.cursor()
        update_sql = self.generate_sql('../postgresql/update_product_list.sql',
                                       str(data['SKU'].to_list())[1:-1], '0', operator, time)
        cursor.execute(update_sql)
        if 'DISCONTINUED' in data.columns:
            discontinued_df = data[data['DISCONTINUED'] == '1']
            if not discontinued_df.empty:
                update_discontinued_sql = self.generate_sql('../postgresql/update_product_list.sql',
                                                            str(discontinued_df['SKU'].to_list())[1:-1], '1', operator,
                                                            time)
                cursor.execute(update_discontinued_sql)
        self.connection.commit()

    def export_table_data(self, table, *args):
        data = self.run_sql('../postgresql/export_' + table + '_data.sql', *args)
        data.to_csv('../postgresql/' + table + '.csv', index=False)

    def list_price_is_exist(self, sku, effective_date, expiration_date):
        data = self.run_sql('../postgresql/check_list_price_exist.sql', sku, effective_date, expiration_date)
        if data.empty:
            return False
        else:
            return True

    def update_quote_price(self, data):
        for index in range(0, data.shape[0]):
            quote_type_value = data.iloc[index, 0]
            quote_number_value = data.iloc[index, 1]
            sku_value = data.iloc[index, 2]
            st_value = data.iloc[index, 3]
            min_order_quantity_value = data.iloc[index, 4]
            discount_value = data.iloc[index, 5]
            fixed_price_value = data.iloc[index, 6]
            effective_date_value = data.iloc[index, 7]
            expiration_date_value = data.iloc[index, 8]
            updated_time_value = jde_julian_date_to_datetime(data.iloc[index, 9], data.iloc[index, 10])

            if self.quote_price_is_exist(sku_value, st_value, quote_type_value, quote_number_value):
                update_sql = self.generate_sql('../postgresql/update_quote_price.sql', sku_value, st_value,
                                               quote_type_value, quote_number_value, min_order_quantity_value,
                                               discount_value, fixed_price_value, effective_date_value,
                                               expiration_date_value, updated_time_value,
                                               'System', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            else:
                update_sql = self.generate_sql('../postgresql/insert_quote_price.sql', discount_value,
                                               fixed_price_value, quote_type_value, quote_number_value,
                                               min_order_quantity_value, sku_value, st_value, effective_date_value,
                                               expiration_date_value, updated_time_value,
                                               'System', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            cursor = self.connection.cursor()
            cursor.execute(update_sql)
            self.connection.commit()

    def quote_price_is_exist(self, sku, st, quote_type, quote_number):
        data = self.run_sql('../postgresql/check_quote_price_exist.sql', sku, st, quote_type, quote_number)
        if data.empty:
            return False
        else:
            return True

    def get_product_discontinued_list(self):
        data = self.run_sql('../postgresql/get_product_discontinued_list.sql')
        return data['sku'].tolist()

    def move_non_discontinued_to_product_list(self, product_list):
        non_discontinued_df = product_list[product_list['DISCONTINUED'] == '0']
        if non_discontinued_df.empty:
            return None
        non_discontinued_list = [product.strip() for product in non_discontinued_df['SKU'].tolist()]
        cursor = self.connection.cursor()
        for product in non_discontinued_list:
            insert_sql = self.generate_sql('../postgresql/insert_product_list.sql', product)
            cursor.execute(insert_sql)
        delete_sql = self.generate_sql('../postgresql/delete_product_discontinued_list',
                                       str(non_discontinued_list)[1:-1])
        cursor.execute(delete_sql)
        self.connection.commit()

    def product_list_price_is_updated(self):
        result = self.run_sql('../postgresql/check_product_list_price_is_updated.sql', 'Updated Discontinued')
        if result.empty:
            return True
        else:
            return False

    def get_not_updated_list_price_product_list(self, number):
        product_list = self.run_sql('../postgresql/get_not_updated_list_price_product_list.sql', 'Updated Discontinued',
                                    number)
        return product_list['sku'].tolist()

    def get_st_list(self):
        data = self.run_sql('../postgresql/get_st_list.sql')
        return data['st'].tolist()

    def product_quote_price_is_updated(self):
        result = self.run_sql('../postgresql/check_product_quote_price_is_updated.sql', 'Updated List Price')
        if result.empty:
            return True
        else:
            return False

    def get_not_updated_quote_price_product_list(self, number):
        product_list = self.run_sql('../postgresql/get_not_updated_quote_price_product_list.sql', 'Updated List Price',
                                    number)
        return product_list['sku'].tolist()

    def get_discontinued_product_list(self):
        product_list = self.run_sql('../postgresql/get_discontinued_product_list.sql')
        return product_list['sku'].tolist()

    def remove_discontinued_data_in_table(self, product_list, table):
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM " + table + " WHERE sku IN (" + str(product_list)[1:-1] + ")")
        self.connection.commit()

    def move_to_product_discontinued_list(self, product_list):
        cursor = self.connection.cursor()
        for product in product_list:
            cursor.execute("INSERT INTO product_discontinued_list (sku) VALUES ('" + product + "')")
        self.connection.commit()
        self.remove_discontinued_data_in_table(product_list, 'product_list')

    def remove_expired_data_in_table(self, table):
        cursor = self.connection.cursor()
        cursor.execute(
            "DELETE FROM " + table + " WHERE expiration_date < TIMESTAMP '" + datetime.datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S') + "'")
        self.connection.commit()

    def move_to_product_action_list_backup(self):
        data = self.run_sql('../postgresql/get_expired_product_action_list.sql')
        cursor = self.connection.cursor()
        for index in range(0, data.shape[0]):
            sku_value = data.iloc[index, 0]
            action_value = data.iloc[index, 1]
            updated_date_value = data.iloc[index, 2]
            update_sql = self.generate_sql('../postgresql/insert_product_action_list_backup.sql', sku_value,
                                           action_value, updated_date_value)
            cursor.execute(update_sql)
        cursor.execute("DELETE FROM product_action_list WHERE updated_date < NOW() - INTERVAL '3 MONTHS'")
        self.connection.commit()
