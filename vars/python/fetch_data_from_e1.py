import datetime

import cx_Oracle
import pandas as pd
import psycopg2

import json


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
    return julian_date


class Database(object):
    def __init__(self, login_file):
        self.login = self.load_config(login_file)
        self.connection = None

    @staticmethod
    def load_config(login_file):
        file = open(login_file, 'r', encoding='UTF-8')
        login = json.load(file)
        file.close()
        return login['db']

    @staticmethod
    def generate_sql(sql, *args):
        return open(sql, 'r').read().format(*args)

    def run_sql(self, sql, *args):
        sql = self.generate_sql(sql, *args)
        return pd.read_sql(sql, con=self.connection)

    def close_connection(self):
        self.connection.close()


class OracleDatabase(Database):
    def __init__(self, login_file):
        super().__init__(login_file)

    def open_connection(self):
        connection = cx_Oracle.connect(self.login['username'], self.login['password'],
                                       self.login['host'] + ':' + str(self.login['port']) + '/' + self.login['schema'])
        self.connection = connection

    def export_list_price_data(self, sku_list):
        data = self.run_sql('../oracle/get_list_price.sql', str(sku_list)[1:-1],
                            datetime_to_jde_julian_date(datetime.datetime.now()))
        data['EFFECTIVE_DATE'] = data['EFFECTIVE_DATE'].apply(jde_julian_date_to_datetime)
        data['EXPIRATION_DATE'] = data['EXPIRATION_DATE'].apply(jde_julian_date_to_datetime, var='235959')
        data['UPDATED_DATE'] = data['UPDATED_DATE'].apply(jde_julian_date_to_datetime)
        return data

    def sku_is_discontinued(self, sku):
        data = self.run_sql('../oracle/get_sku_stocking_type.sql', sku)
        if data.empty:
            return True
        else:
            return False

    def export_quote_price_data(self, sku_list, st_list, postgresql):
        for st in st_list:
            for sku in sku_list:
                f_quote_price_data = self.run_sql('../oracle/get_f_quote_price.sql', sku, st,
                                                  datetime_to_jde_julian_date(datetime.datetime.now()))
                if not f_quote_price_data.empty:
                    postgresql.update_quote_price(f_quote_price_data)
                e_quote_price_data = self.run_sql('../oracle/get_e_quote_price.sql', sku, st,
                                                  datetime_to_jde_julian_date(datetime.datetime.now()))
                if not e_quote_price_data.empty:
                    postgresql.update_quote_price(e_quote_price_data)
                d_quote_price_data = self.run_sql('../oracle/get_d_quote_price.sql', sku, st,
                                                  datetime_to_jde_julian_date(datetime.datetime.now()))
                if not d_quote_price_data.empty:
                    postgresql.update_quote_price(d_quote_price_data)
                p_quote_price_data = self.run_sql('../oracle/get_p_quote_price.sql', sku, st,
                                                  datetime_to_jde_julian_date(datetime.datetime.now()))
                if not p_quote_price_data.empty:
                    postgresql.update_quote_price(p_quote_price_data)


class PostgresqlDatabase(Database):
    def __init__(self, login_file):
        super().__init__(login_file)

    def open_connection(self):
        connection = psycopg2.connect(database=self.login['database'], user=self.login['username'],
                                      password=self.login['password'], host=self.login['host'], port=self.login['port'])
        self.connection = connection

    def product_is_updated(self, hour):
        result = self.run_sql('../postgresql/check_product_list_update.sql', hour)
        if result.empty:
            return True
        else:
            return False

    def update_list_price(self, data):
        for index in range(0, data.shape[0]):
            sku_value = data.iloc[index, 0].strip()
            list_price_value = data.iloc[index, 1]
            effective_date_value = data.iloc[index, 2]
            expiration_date_value = data.iloc[index, 3]
            updated_date_value = data.iloc[index, 4]
            if self.list_price_is_exist(sku_value, effective_date_value, expiration_date_value):
                if self.list_price_is_updated(sku_value, list_price_value, effective_date_value, expiration_date_value,
                                              updated_date_value):
                    continue
                else:
                    query = self.generate_sql('../postgresql/update_list_price.sql',
                                              sku_value, list_price_value, effective_date_value,
                                              expiration_date_value,
                                              'System',
                                              updated_date_value)
                    cursor = self.connection.cursor()
                    cursor.execute(query)
                    self.connection.commit()
            else:
                query = self.generate_sql('../postgresql/insert_list_price.sql',
                                          sku_value, list_price_value, effective_date_value,
                                          expiration_date_value,
                                          'System',
                                          datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                          'System',
                                          updated_date_value)
                cursor = self.connection.cursor()
                cursor.execute(query)
                self.connection.commit()

    def update_product_list_date(self, sku_list, operator='System', time='NOW()'):
        query = self.generate_sql('../postgresql/update_product_list_updated_date.sql', str(sku_list)[1:-1], operator,
                                  time)
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()

    def export_table_data(self, table):
        data = self.run_sql('../postgresql/export_' + table + '_data.sql')
        data.to_csv('../postgresql/' + table + '.csv', index=False)

    def update_sku_to_discontinued(self, sku):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE product_list SET discontinued = '1' WHERE sku = '" + sku + "'")
        self.connection.commit()

    def list_price_is_exist(self, sku, effective_date, expiration_date):
        data = self.run_sql('../postgresql/check_list_price_exist.sql', sku, effective_date, expiration_date)
        if data.empty:
            return False
        else:
            return True

    def list_price_is_updated(self, sku, list_price, effective_date, expiration_date, updated_date):
        if self.list_price_is_exist(sku, effective_date, expiration_date):
            data = self.run_sql('../postgresql/check_list_price_updated.sql', sku, list_price, effective_date,
                                expiration_date, updated_date)
            if data.empty:
                return False
            else:
                return True
        else:
            return False

    def remove_discontinued_sku(self, sku_list):
        result = []
        for sku in sku_list:
            if not self.sku_is_discontinued(sku):
                result.append(sku)
        return result

    def update_quote_price(self, data):
        data['EFFECTIVE_DATE'] = data['EFFECTIVE_DATE'].apply(jde_julian_date_to_datetime)
        data['EXPIRATION_DATE'] = data['EXPIRATION_DATE'].apply(jde_julian_date_to_datetime, var='235959')
        for index in range(0, data.shape[0]):
            quote_type_value = data.iloc[index, 0].strip()
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
                if self.quote_price_is_updated(sku_value, st_value, quote_type_value, quote_number_value,
                                               min_order_quantity_value, discount_value, fixed_price_value,
                                               effective_date_value, expiration_date_value, updated_time_value):
                    continue
                else:
                    query = self.generate_sql('../postgresql/update_quote_price.sql', sku_value, st_value,
                                              quote_type_value, quote_number_value, min_order_quantity_value,
                                              discount_value, fixed_price_value, effective_date_value,
                                              expiration_date_value, 'System', updated_time_value)
                    cursor = self.connection.cursor()
                    cursor.execute(query)
                    self.connection.commit()
            else:
                query = self.generate_sql('../postgresql/insert_quote_price.sql', discount_value, fixed_price_value,
                                          quote_type_value, quote_number_value, min_order_quantity_value, sku_value,
                                          st_value, effective_date_value, expiration_date_value,
                                          'System', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                          'System', updated_time_value)
                cursor = self.connection.cursor()
                cursor.execute(query)
                self.connection.commit()

    def sku_is_discontinued(self, sku):
        data = self.run_sql('../postgresql/check_sku_discontinued.sql', sku)
        if data.empty:
            return False
        else:
            return True

    def quote_price_is_exist(self, sku, st, quote_type, quote_number):
        data = self.run_sql('../postgresql/check_quote_price_exist.sql', sku, st, quote_type, quote_number)
        if data.empty:
            return False
        else:
            return True

    def quote_price_is_updated(self, sku, st, quote_type, quote_number, min_order_quantity, discount, fixed_price,
                               effective_date, expiration_date, updated_time):
        if self.quote_price_is_exist(sku, st, quote_type, quote_number):
            data = self.run_sql('../postgresql/check_quote_price_updated.sql', sku, st, quote_type, quote_number,
                                min_order_quantity, discount, fixed_price, effective_date, expiration_date,
                                updated_time)
            if data.empty:
                return False
            else:
                return True
        else:
            return False


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

        sku_list = local_db.remove_discontinued_sku(sku_list)
        e1_db.open_connection()
        e1_db.export_quote_price_data(sku_list, st_list, local_db)
        e1_db.close_connection()

        local_db.update_product_list_date(sku_list)

    local_db.export_table_data('product')
    local_db.export_table_data('quote')
    local_db.close_connection()

    time_ended = datetime.datetime.utcnow()
    total_time = (time_ended - time_started).total_seconds()
    print('Total time is %ss.' % round(total_time))


if __name__ == '__main__':
    main()
