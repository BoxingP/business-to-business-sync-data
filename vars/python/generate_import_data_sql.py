import os
import sys


def main():
    item_class = sys.argv[1]
    item_type = None
    item = sys.argv[1]
    if len(sys.argv) == 3:
        item = sys.argv[2]
        item_type = item[0].upper()

    with open('../postgresql/insert_' + item_class + '_data.sql.template', 'r') as file:
        sql_template = file.read()

    sql = '../postgresql/insert_' + item + '_data.sql'

    if os.path.exists(sql):
        os.remove(sql)

    with open('../postgresql/' + item + '.txt', 'r') as file:
        items = [line.rstrip().strip() for line in file]
    for index, value in enumerate(items):
        with open(sql, 'a+') as file:
            if index != len(items) - 1:
                file.write(
                    sql_template.format(name=value, type=item_type, business='BID', is_discontinued=False) + ';\n')
            else:
                file.write(sql_template.format(name=value, type=item_type, business='BID', is_discontinued=False))


if __name__ == '__main__':
    main()
