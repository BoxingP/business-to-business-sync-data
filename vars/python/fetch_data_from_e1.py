import cx_Oracle

import json


def load_config(path):
    file = open(path, 'r', encoding='UTF-8')
    data = json.load(file)
    file.close()
    return data


def read_sql(file):
    content = open(file, 'r').read()
    return content


def run_sql(login, sql):
    connection = cx_Oracle.connect(login['username'], login['password'],
                                   login['host'] + ':' + str(login['port']) + '/' + login['schema'])
    cursor = connection.cursor()

    cursor.execute(sql)
    connection.commit()
    row = cursor.fetchall()

    print(row)

    cursor.close()
    connection.close()


db_login = load_config('./db.json')['db']
test = read_sql('../oracle/test_connection.sql')
run_sql(db_login, test)
