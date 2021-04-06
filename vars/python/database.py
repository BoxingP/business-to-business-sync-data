import csv
import datetime
from io import StringIO
from sqlalchemy import create_engine

import cx_Oracle
import pandas as pd
import psycopg2
import psycopg2.extras

import yaml

from logger import Logger


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
        module = self.__class__.__module__
        if module is None or module == str.__class__.__module__:
            self.fullname = self.__class__.__name__
        else:
            self.fullname = module + '.' + self.__class__.__name__
        self.logger = Logger(self.fullname)

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
        return None

    def get_product_quote_price(self, product_list, st_list, quote_type_list):
        sku_list = product_list
        product_line = self.get_product_line(product_list)
        sku_ppl_mapping = product_line.groupby('PPL', as_index=False).agg({'SKU': lambda x: list(x)})
        ppl_list = sku_ppl_mapping['PPL'].to_list()

        sku_result = []
        ppl_result = []
        for quote_type in quote_type_list:
            sku_quote_price, ppl_quote_price = self.get_quote_price(sku_list, ppl_list, quote_type, st_list)
            sku_result.append(sku_quote_price)
            ppl_result.append(ppl_quote_price)
        sku_quote_price = self.format_quote_price_result(sku_result)
        ppl_quote_price = self.format_quote_price_result(ppl_result, sku_ppl_mapping)

        return sku_quote_price, ppl_quote_price

    def get_quote_price(self, sku_list, ppl_list, flag, st_list,
                        date=datetime_to_jde_julian_date(datetime.datetime.now())):
        sku_quote_price = None
        ppl_quote_price = None
        if sku_list:
            sku_quote_price = self.run_sql('../oracle/get_' + flag + '_quote_price.sql', str(sku_list)[1:-1],
                                           'Q5LITM', ','.join(map(str, st_list)), date)
        if ppl_list:
            ppl_quote_price = self.run_sql('../oracle/get_' + flag + '_quote_price.sql', str(ppl_list)[1:-1],
                                           'Q5ITTP', ','.join(map(str, st_list)), date)
        return sku_quote_price, ppl_quote_price

    @staticmethod
    def format_quote_price_result(df_list, sku_ppl_mapping=None):
        if all(df is None for df in df_list):
            return None
        result = pd.concat(df_list, ignore_index=True)
        if sku_ppl_mapping is not None:
            result['SKU'] = result['SKU'].str.strip().map(sku_ppl_mapping.set_index('PPL')['SKU'])
            result = result.explode('SKU')
        result["E1_UPDATED_DATE"] = ""
        if not result.empty:
            result = result.drop_duplicates(keep=False, ignore_index=True)
            result['SKU'] = result['SKU'].str.strip()
            result['EFFECTIVE_DATE'] = result['EFFECTIVE_DATE'].apply(jde_julian_date_to_datetime)
            result['EXPIRATION_DATE'] = result['EXPIRATION_DATE'].apply(jde_julian_date_to_datetime, var='235959')
            result['DISCOUNT'] = result['DISCOUNT'].apply(lambda x: None if x == 0 else x)
            result['FIXED_PRICE'] = result['FIXED_PRICE'].apply(lambda x: None if x == 0 else x)
            result['E1_UPDATED_DATE'] = result.apply(
                lambda row: jde_julian_date_to_datetime(row.DATE_UPDATED, row.TIME_UPDATED), axis=1)
        result.drop(['DATE_UPDATED', 'TIME_UPDATED'], inplace=True, axis=1)
        return result

    def get_product_line(self, product_list):
        data = self.run_sql('../oracle/get_product_line.sql', str(product_list)[1:-1])
        data['SKU'] = data['SKU'].str.strip()
        data['PPL'] = data['PPL'].str.strip()
        return data


class PostgresqlDatabase(Database):
    def __init__(self, config_file):
        super().__init__(config_file)

    def open_connection(self):
        login = self.config['postgresql']
        connection = psycopg2.connect(database=login['database'], user=login['username'], password=login['password'],
                                      host=login['host'], port=login['port'])
        self.connection = connection

    def create_engine(self):
        login = self.config['postgresql']
        connect = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(login['username'], login['password'], login['host'],
                                                                login['port'], login['database'])
        return create_engine(connect)

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

    def update_product(self, data, operator='System', time='NOW()'):
        cursor = self.connection.cursor()
        update_sql = self.generate_sql('../postgresql/update_product.sql', str(data)[1:-1], operator, time)
        cursor.execute(update_sql)
        self.connection.commit()

    def update_price(self, data, table):
        get_chunk = flow_from_dataframe(data, 50000)
        for chunk in get_chunk:
            chunk.to_sql(name=table, con=self.create_engine(), method=self.upsert_table, index=False,
                         if_exists='append')

    def upsert_table(self, table, conn, keys, data_iter):
        postgresql_conn = conn.connection

        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerows(data_iter)
        buffer.seek(0)

        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        tmp_table_name = table_name + '_staging'

        with postgresql_conn.cursor() as cursor:
            df_columns = list(map(str.lower, keys))

            cursor.execute(cursor.mogrify(self.read_sql('../postgresql/get_table_index_columns.sql'),
                                          (table_name, True)).decode('utf-8'))
            primary_column = cursor.fetchone()[0]

            cursor.execute(cursor.mogrify(self.read_sql('../postgresql/get_table_index_columns.sql'),
                                          (table_name, False)).decode('utf-8'))
            index_columns = cursor.fetchone()[0]

            data_columns = list(set(df_columns) - set(index_columns))
            set_ = ', '.join(['{} = EXCLUDED.{}'.format(k, k) for k in data_columns])

            cursor.execute(cursor.mogrify(self.read_sql('../postgresql/get_table_columns.sql'),
                                          (table_name,)).decode('utf-8'))
            full_columns = cursor.fetchone()[0]
            drop_ = ', '.join(['DROP COLUMN {}'.format(k) for k in list(set(full_columns) - set(df_columns))])

            flag_columns = tuple(set(full_columns) - set(df_columns) - set(primary_column))
            query = cursor.mogrify(self.read_sql('../postgresql/get_table_columns_default_value.sql'),
                                   (table_name, flag_columns)).decode('utf-8')
            flag_columns_value = (pd.read_sql(query, con=self.connection)).itertuples(index=False, name=None)
            flag_ = ", ".join([" = ".join(tup) for tup in flag_columns_value])

            cursor.execute('CREATE TEMPORARY TABLE {} ( LIKE {} ) ON COMMIT DROP'.format(tmp_table_name, table_name))
            cursor.execute('ALTER TABLE {} {}'.format(tmp_table_name, drop_))
            query = 'COPY {} ({}) FROM STDIN WITH CSV'.format(tmp_table_name, ', '.join(df_columns))
            cursor.copy_expert(sql=query, file=buffer)
            query = self.read_sql('../postgresql/update_price.sql').format(table_name, ', '.join(df_columns),
                                                                           ', '.join(df_columns), tmp_table_name,
                                                                           ', '.join(index_columns),
                                                                           set_ + ', ' + flag_)
            cursor.execute(query)

    def move_non_discontinued_to_product(self, product_list):
        non_discontinued_df = product_list[~product_list['DISCONTINUED']]
        if non_discontinued_df.empty:
            return None
        non_discontinued_list = non_discontinued_df['SKU'].tolist()
        data = self.run_sql('../postgresql/get_product_non_discontinued.sql', str(non_discontinued_list)[1:-1])
        data["discontinued"] = False
        products = list(data.itertuples(index=False, name=None))
        records_list_template = ','.join(['%s'] * len(products))
        insert_query = self.read_sql('../postgresql/insert_product.sql').format(records_list_template)
        delete_sql = self.generate_sql('../postgresql/delete_product_discontinued.sql',
                                       str(non_discontinued_list)[1:-1])
        cursor = self.connection.cursor()
        cursor.execute(insert_query, products)
        cursor.execute(delete_sql)
        self.connection.commit()

    def get_st(self):
        data = self.run_sql('../postgresql/get_st.sql')
        get_chunk = flow_from_dataframe(data)
        return data.shape[0], get_chunk

    def get_product(self, sql):
        data = self.run_sql(sql)
        get_chunk = flow_from_dataframe(data)
        return data.shape[0], get_chunk

    def get_discontinued_product(self):
        return self.run_sql('../postgresql/get_discontinued_product.sql')

    def remove_discontinued_data_in_table(self, data, table):
        product_list = data['sku'].tolist()
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM " + table + " WHERE sku IN (" + str(product_list)[1:-1] + ")")
        self.connection.commit()

    def move_to_product_discontinued(self, data):
        products = list(data.itertuples(index=False, name=None))
        records_list_template = ','.join(['%s'] * len(products))
        query = self.read_sql('../postgresql/insert_product_discontinued_list.sql').format(records_list_template)
        cursor = self.connection.cursor()
        cursor.execute(query, products)
        self.connection.commit()
        self.remove_discontinued_data_in_table(data, 'product')

    def remove_expired_data_in_table(self, table):
        cursor = self.connection.cursor()
        cursor.execute(
            "DELETE FROM " + table + " WHERE expiration_date < TIMESTAMP '" + datetime.datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S') + "'")
        self.connection.commit()

    def move_to_product_action_backup(self):
        data = self.run_sql('../postgresql/get_expired_product_action.sql')
        if not data.empty:
            expired_products_action = list(data.itertuples(index=False, name=None))
            records_list_template = ','.join(['%s'] * len(expired_products_action))
            insert_query = self.read_sql('../postgresql/insert_product_action_backup.sql').format(
                records_list_template)
            cursor = self.connection.cursor()
            cursor.execute(insert_query, expired_products_action)
            cursor.execute("DELETE FROM product_action WHERE updated_date < NOW() - INTERVAL '3 MONTHS'")
            self.connection.commit()

    def export_data_to_csv(self, name, *args):
        hour = int(self.config['other']['only_get_the_updated_data_within_hours'])
        if name == 'product':
            parameters = (hour, *args)
        else:
            parameters = (hour,)
        cursor = self.connection.cursor()
        query = cursor.mogrify(self.read_sql('../postgresql/export_' + name + '_data.sql'), parameters).decode('utf-8')
        with open('../postgresql/' + name + '.csv', 'w') as file:
            cursor.copy_expert(query, file)
