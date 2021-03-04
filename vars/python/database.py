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
        if not data.empty:
            data['SKU'] = data['SKU'].str.strip()
            data['EFFECTIVE_DATE'] = data['EFFECTIVE_DATE'].apply(jde_julian_date_to_datetime)
            data['EXPIRATION_DATE'] = data['EXPIRATION_DATE'].apply(jde_julian_date_to_datetime, var='235959')
            data['UPDATED_DATE'] = data['UPDATED_DATE'].apply(jde_julian_date_to_datetime)
        return data

    def get_product_quote_price(self, product_list, st_list):
        current_date = datetime_to_jde_julian_date(datetime.datetime.now())
        sku_list = product_list
        product_line = self.get_product_line(product_list)
        sku_pl_mapping = product_line.groupby('PL', as_index=False).agg({'SKU': lambda x: list(x)})
        pl_list = sku_pl_mapping['PL'].to_list()
        f_quote_price = self.get_quote_price(sku_list, pl_list, sku_pl_mapping, st_list, 'f', current_date)
        e_quote_price = self.get_quote_price(sku_list, pl_list, sku_pl_mapping, st_list, 'e', current_date)
        d_quote_price = self.get_quote_price(sku_list, pl_list, sku_pl_mapping, st_list, 'd', current_date)
        p_quote_price = self.get_quote_price(sku_list, pl_list, sku_pl_mapping, st_list, 'p', current_date)
        common_d_quote_price = self.get_quote_price(sku_list, pl_list, sku_pl_mapping, [90714], 'd', current_date)
        common_p_quote_price = self.get_quote_price(sku_list, pl_list, sku_pl_mapping, [90714], 'p', current_date)
        common_quote_price = pd.concat([common_d_quote_price, common_p_quote_price])
        common_mapping = pd.DataFrame({'ST': [90714], 'SKU_LIST': [st_list]})
        common_quote_price['ST'] = common_quote_price['ST'].map(common_mapping.set_index('ST')['SKU_LIST'])
        common_quote_price = common_quote_price.explode('ST')
        result = pd.concat([f_quote_price, e_quote_price, d_quote_price, p_quote_price, common_quote_price],
                           ignore_index=True)

        if not result.empty:
            result['SKU'] = result['SKU'].str.strip()
            result['EFFECTIVE_DATE'] = result['EFFECTIVE_DATE'].apply(jde_julian_date_to_datetime)
            result['EXPIRATION_DATE'] = result['EXPIRATION_DATE'].apply(jde_julian_date_to_datetime, var='235959')
        return result

    def get_quote_price(self, sku_list, pl_list, sku_pl_mapping, st_list, flag, date):
        sku_quote_price = None
        pl_quote_price = None
        if sku_list:
            sku_quote_price = self.run_sql('../oracle/get_' + flag + '_quote_price.sql', str(sku_list)[1:-1],
                                           'Q5LITM', ','.join(map(str, st_list)), date)
        if pl_list:
            pl_quote_price = self.run_sql('../oracle/get_' + flag + '_quote_price.sql', str(pl_list)[1:-1],
                                          'Q5ITTP', ','.join(map(str, st_list)), date)
        if pl_quote_price is not None:
            pl_quote_price['SKU'] = pl_quote_price['SKU'].str.strip().map(sku_pl_mapping.set_index('PL')['SKU'])
            pl_quote_price = pl_quote_price.explode('SKU')
        return pd.concat([sku_quote_price, pl_quote_price])

    def get_product_line(self, product_list):
        data = self.run_sql('../oracle/get_product_line.sql', str(product_list)[1:-1])
        data['SKU'] = data['SKU'].str.strip()
        data['PL'] = data['PL'].str.strip()
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
        result = self.run_sql('../postgresql/check_product_discontinued_status_is_updated.sql', 'Updated Discontinued')
        if result.empty:
            return True
        else:
            return False

    def get_not_updated_discontinued_status_product_list(self, number):
        product_list = self.run_sql('../postgresql/get_not_updated_discontinued_status_product_list.sql',
                                    'Updated Discontinued', number)
        return product_list['sku'].tolist()

    def update_list_price(self, data):
        for index in range(0, data.shape[0]):
            sku_value = data.iloc[index, 0].strip()
            list_price_value = data.iloc[index, 1]
            effective_date_value = data.iloc[index, 2]
            expiration_date_value = data.iloc[index, 3]
            updated_date_value = data.iloc[index, 4]
            update_sql = None
            if self.list_price_is_exist(sku_value, effective_date_value, expiration_date_value):
                if not self.list_price_is_updated(sku_value, effective_date_value, expiration_date_value,
                                                  list_price_value, updated_date_value):
                    update_sql = self.generate_sql('../postgresql/update_list_price.sql', sku_value, list_price_value,
                                                   effective_date_value, expiration_date_value, updated_date_value,
                                                   'System', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            else:
                update_sql = self.generate_sql('../postgresql/insert_list_price.sql', sku_value, list_price_value,
                                               effective_date_value, expiration_date_value, updated_date_value,
                                               'System', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            if update_sql is not None:
                cursor = self.connection.cursor()
                cursor.execute(update_sql)
                self.connection.commit()

    def update_discontinued_status(self, data):
        cursor = self.connection.cursor()
        for index in range(0, data.shape[0]):
            sku_value = data.iloc[index, 0].strip()
            discontinued_status_value = data.iloc[index, 1]
            if not self.discontinued_status_is_updated(sku_value, discontinued_status_value):
                update_sql = self.generate_sql('../postgresql/update_discontinued_status.sql', sku_value,
                                               discontinued_status_value)
                cursor.execute(update_sql)
        self.connection.commit()

    def discontinued_status_is_updated(self, sku, discontinued_status):
        result = self.run_sql('../postgresql/check_discontinued_status_updated.sql', sku, discontinued_status)
        if result.empty:
            return False
        else:
            return True

    def update_product_list(self, data, operator='System', time='NOW()'):
        cursor = self.connection.cursor()
        update_sql = self.generate_sql('../postgresql/update_product_list.sql', str(data)[1:-1], operator, time)
        cursor.execute(update_sql)
        self.connection.commit()

    def list_price_is_exist(self, sku, effective_date, expiration_date):
        result = self.run_sql('../postgresql/check_list_price_exist.sql', sku, effective_date, expiration_date)
        if result.empty:
            return False
        else:
            return True

    def list_price_is_updated(self, sku, effective_date, expiration_date, list_price, updated_date):
        result = self.run_sql('../postgresql/check_list_price_updated.sql', sku, effective_date, expiration_date,
                              list_price, updated_date)
        if result.empty:
            return False
        else:
            return True

    def update_quote_price(self, data):
        empty = 'NULL'
        cursor = self.connection.cursor()
        for index in range(0, data.shape[0]):
            quote_type_value = data.iloc[index, 0]
            quote_number_value = data.iloc[index, 1]
            sku_value = data.iloc[index, 2]
            st_value = data.iloc[index, 3]
            min_order_quantity_value = data.iloc[index, 4]
            discount_value = data.iloc[index, 5]
            if discount_value == 0:
                discount_value = empty
            fixed_price_value = data.iloc[index, 6]
            if fixed_price_value == 0:
                fixed_price_value = empty
            effective_date_value = data.iloc[index, 7]
            expiration_date_value = data.iloc[index, 8]
            updated_time_value = jde_julian_date_to_datetime(data.iloc[index, 9], data.iloc[index, 10])
            update_sql = None
            if self.quote_price_is_exist(sku_value, st_value, quote_type_value, quote_number_value):
                if not self.quote_price_is_updated(sku_value, st_value, quote_type_value, quote_number_value,
                                                   min_order_quantity_value, discount_value, fixed_price_value,
                                                   effective_date_value, expiration_date_value, updated_time_value):
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
            if update_sql is not None:
                cursor.execute(update_sql)
        self.connection.commit()

    def quote_price_is_exist(self, sku, st, quote_type, quote_number):
        result = self.run_sql('../postgresql/check_quote_price_exist.sql', sku, st, quote_type, quote_number)
        if result.empty:
            return False
        else:
            return True

    def quote_price_is_updated(self, sku, st, quote_type, quote_number, min_order_quantity, discount, fixed_price,
                               effective_date, expiration_date, updated_time):
        result = self.run_sql('../postgresql/check_quote_price_updated.sql', sku, st, quote_type, quote_number,
                              min_order_quantity, discount, fixed_price, effective_date, expiration_date, updated_time)
        if result.empty:
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
        result = self.run_sql('../postgresql/check_product_list_price_is_updated.sql', 'Updated List Price')
        if result.empty:
            return True
        else:
            return False

    def get_not_updated_list_price_product_list(self, number):
        product_list = self.run_sql('../postgresql/get_not_updated_list_price_product_list.sql', 'Updated List Price',
                                    number)
        return product_list['sku'].tolist()

    def get_st_list(self):
        data = self.run_sql('../postgresql/get_st_list.sql')
        return data['st'].tolist()

    def product_quote_price_is_updated(self):
        result = self.run_sql('../postgresql/check_product_quote_price_is_updated.sql', 'Updated Quote Price')
        if result.empty:
            return True
        else:
            return False

    def get_not_updated_quote_price_product_list(self, number):
        product_list = self.run_sql('../postgresql/get_not_updated_quote_price_product_list.sql', 'Updated Quote Price',
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
            cursor.execute(
                "INSERT INTO product_discontinued_list (sku) VALUES ('" + product + "') ON CONFLICT DO NOTHING")
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
            updated_by_value = data.iloc[index, 2]
            updated_date_value = data.iloc[index, 3]
            update_sql = self.generate_sql('../postgresql/insert_product_action_list_backup.sql', sku_value,
                                           action_value, updated_by_value, updated_date_value)
            cursor.execute(update_sql)
        cursor.execute("DELETE FROM product_action_list WHERE updated_date < NOW() - INTERVAL '3 MONTHS'")
        self.connection.commit()

    def export_data_to_csv(self, name, is_night=False):
        hour = self.config['other']['only_get_the_updated_data_within_hours']
        data = self.run_sql('../postgresql/export_' + name + '_data.sql', hour, is_night)
        data.to_csv('../postgresql/' + name + '.csv', index=False)
