import datetime

import cx_Oracle
import pandas as pd
import psycopg2
import psycopg2.extras

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


def flow_from_dataframe(dataframe: pd.DataFrame, chunk_size: int = 1000):
    for start_row in range(0, dataframe.shape[0], chunk_size):
        end_row = min(start_row + chunk_size, dataframe.shape[0])
        yield dataframe.iloc[start_row:end_row, :]


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
    def read_sql(sql):
        with open(sql, 'r', encoding='UTF-8') as file:
            return file.read()

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
        non_discontinued_df['DISCONTINUED'] = False
        discontinued_df = pd.DataFrame(discontinued_list, columns=['SKU'])
        discontinued_df['DISCONTINUED'] = True
        return non_discontinued_df.append(discontinued_df, ignore_index=True)

    def get_product_list_price(self, product_list):
        data = self.run_sql('../oracle/get_product_list_price.sql', str(product_list)[1:-1],
                            datetime_to_jde_julian_date(datetime.datetime.now()))
        if not data.empty:
            data['SKU'] = data['SKU'].str.strip()
            data['EFFECTIVE_DATE'] = data['EFFECTIVE_DATE'].apply(jde_julian_date_to_datetime)
            data['EXPIRATION_DATE'] = data['EXPIRATION_DATE'].apply(jde_julian_date_to_datetime, var='235959')
            data['E1_UPDATED_DATE'] = data['E1_UPDATED_DATE'].apply(jde_julian_date_to_datetime)
        return data

    def get_product_quote_price(self, product_list, st_list):
        current_date = datetime_to_jde_julian_date(datetime.datetime.now())
        sku_list = product_list
        product_line = self.get_product_line(product_list)
        sku_pl_mapping = product_line.groupby('PL', as_index=False).agg({'SKU': lambda x: list(x)})
        pl_list = sku_pl_mapping['PL'].to_list()
        f_quote_price = self.get_quote_price(sku_list, pl_list, sku_pl_mapping, st_list, 'f', current_date)
        e_quote_price = self.get_quote_price(sku_list, pl_list, sku_pl_mapping, st_list, 'e', current_date)
        d_p_quote_price = self.get_quote_price(sku_list, pl_list, sku_pl_mapping, st_list, 'd_p', current_date)
        common_d_p_quote_price = self.get_quote_price(sku_list, pl_list, sku_pl_mapping, [90714], 'd_p', current_date)
        common_mapping = pd.DataFrame({'ST': [90714], 'SKU_LIST': [st_list]})
        common_d_p_quote_price['ST'] = common_d_p_quote_price['ST'].map(common_mapping.set_index('ST')['SKU_LIST'])
        common_d_p_quote_price = common_d_p_quote_price.explode('ST')
        result = pd.concat([f_quote_price, e_quote_price, d_p_quote_price, common_d_p_quote_price], ignore_index=True)

        result["E1_UPDATED_DATE"] = ""
        if not result.empty:
            result['SKU'] = result['SKU'].str.strip()
            result['EFFECTIVE_DATE'] = result['EFFECTIVE_DATE'].apply(jde_julian_date_to_datetime)
            result['EXPIRATION_DATE'] = result['EXPIRATION_DATE'].apply(jde_julian_date_to_datetime, var='235959')
            result['DISCOUNT'] = result['DISCOUNT'].apply(lambda x: None if x == 0 else x)
            result['FIXED_PRICE'] = result['FIXED_PRICE'].apply(lambda x: None if x == 0 else x)
            result['E1_UPDATED_DATE'] = result.apply(
                lambda row: jde_julian_date_to_datetime(row.DATE_UPDATED, row.TIME_UPDATED), axis=1)
        result.drop(['DATE_UPDATED', 'TIME_UPDATED'], inplace=True, axis=1)
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

    def update_list_price(self, data):
        cursor = self.connection.cursor()
        get_chunk = flow_from_dataframe(data)
        for chunk in get_chunk:
            prices = chunk.itertuples(index=False, name=None)
            need_to_update = []
            for price in prices:
                if not self.list_price_is_updated(price):
                    need_to_update.append(price)
            if need_to_update:
                records_list_template = ','.join(['%s'] * len(need_to_update))
                query = self.read_sql('../postgresql/update_list_price.sql').format(records_list_template)
                cursor.execute(query, need_to_update)
                self.connection.commit()

    def update_discontinued_status(self, data):
        cursor = self.connection.cursor()
        statuses = data.itertuples(index=False, name=None)
        need_to_update = []
        for status in statuses:
            if not self.discontinued_status_is_updated(status):
                need_to_update.append(status)
        if need_to_update:
            query = self.read_sql('../postgresql/update_discontinued_status.sql')
            psycopg2.extras.execute_values(cursor, query, need_to_update)

    def discontinued_status_is_updated(self, status):
        cursor = self.connection.cursor()
        query = cursor.mogrify(self.read_sql('../postgresql/check_discontinued_status_updated.sql'), status).decode(
            'utf-8')
        cursor.execute(query)
        return cursor.fetchone() is not None

    def update_product_list(self, data, operator='System', time='NOW()'):
        cursor = self.connection.cursor()
        update_sql = self.generate_sql('../postgresql/update_product_list.sql', str(data)[1:-1], operator, time)
        cursor.execute(update_sql)
        self.connection.commit()

    def list_price_is_updated(self, price):
        cursor = self.connection.cursor()
        query = cursor.mogrify(self.read_sql('../postgresql/check_list_price_updated.sql'), price).decode('utf-8')
        cursor.execute(query)
        return cursor.fetchone() is not None

    def update_quote_price(self, data):
        cursor = self.connection.cursor()
        get_chunk = flow_from_dataframe(data)
        for chunk in get_chunk:
            quotes = chunk.itertuples(index=False, name=None)
            need_to_update = []
            for quote in quotes:
                if not self.quote_price_is_updated(quote):
                    need_to_update.append(quote)
            if need_to_update:
                records_list_template = ','.join(['%s'] * len(need_to_update))
                query = self.read_sql('../postgresql/update_quote_price.sql').format(records_list_template)
                cursor.execute(query, need_to_update)
                self.connection.commit()

    def quote_price_is_updated(self, quote):
        cursor = self.connection.cursor()
        query = cursor.mogrify(self.read_sql('../postgresql/check_quote_price_updated.sql'), quote).decode('utf-8')
        cursor.execute(query)
        return cursor.fetchone() is not None

    def move_non_discontinued_to_product_list(self, product_list):
        non_discontinued_df = product_list[~product_list['DISCONTINUED']]
        if non_discontinued_df.empty:
            return None
        non_discontinued_list = non_discontinued_df['SKU'].tolist()
        data = self.run_sql('../postgresql/get_product_discontinued_list.sql', str(non_discontinued_list)[1:-1])
        data["discontinued"] = False
        products = list(data.itertuples(index=False, name=None))
        records_list_template = ','.join(['%s'] * len(products))
        insert_query = self.read_sql('../postgresql/insert_product_list.sql').format(records_list_template)
        delete_sql = self.generate_sql('../postgresql/delete_product_discontinued_list.sql',
                                       str(non_discontinued_list)[1:-1])
        cursor = self.connection.cursor()
        cursor.execute(insert_query, products)
        cursor.execute(delete_sql)
        self.connection.commit()

    def get_st(self):
        data = self.run_sql('../postgresql/get_st.sql')
        get_chunk = flow_from_dataframe(data)
        return get_chunk

    def get_product(self, sql):
        data = self.run_sql(sql)
        get_chunk = flow_from_dataframe(data)
        return get_chunk

    def get_discontinued_product_list(self):
        return self.run_sql('../postgresql/get_discontinued_product_list.sql')

    def remove_discontinued_data_in_table(self, data, table):
        product_list = data['sku'].tolist()
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM " + table + " WHERE sku IN (" + str(product_list)[1:-1] + ")")
        self.connection.commit()

    def move_to_product_discontinued_list(self, data):
        products = list(data.itertuples(index=False, name=None))
        records_list_template = ','.join(['%s'] * len(products))
        query = self.read_sql('../postgresql/insert_product_discontinued_list.sql').format(records_list_template)
        cursor = self.connection.cursor()
        cursor.execute(query, products)
        self.connection.commit()
        self.remove_discontinued_data_in_table(data, 'product_list')

    def remove_expired_data_in_table(self, table):
        cursor = self.connection.cursor()
        cursor.execute(
            "DELETE FROM " + table + " WHERE expiration_date < TIMESTAMP '" + datetime.datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S') + "'")
        self.connection.commit()

    def move_to_product_action_list_backup(self):
        data = self.run_sql('../postgresql/get_expired_product_action_list.sql')
        if not data.empty:
            expired_products_action = list(data.itertuples(index=False, name=None))
            records_list_template = ','.join(['%s'] * len(expired_products_action))
            insert_query = self.read_sql('../postgresql/insert_product_action_list_backup.sql').format(
                records_list_template)
            cursor = self.connection.cursor()
            cursor.execute(insert_query, expired_products_action)
            cursor.execute("DELETE FROM product_action_list WHERE updated_date < NOW() - INTERVAL '3 MONTHS'")
            self.connection.commit()

    def export_data_to_csv(self, name, is_night=False):
        hour = self.config['other']['only_get_the_updated_data_within_hours']
        data = self.run_sql('../postgresql/export_' + name + '_data.sql', hour, is_night)
        data.to_csv('../postgresql/' + name + '.csv', index=False)
