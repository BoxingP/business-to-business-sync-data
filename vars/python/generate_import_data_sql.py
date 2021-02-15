import os
import sys


def main():
    item = sys.argv[1]

    with open('../postgresql/' + item + '.txt', 'r') as file:
        items = [line.rstrip().strip() for line in file]

    with open('../postgresql/import_' + item + '_data.sql.template', 'r') as file:
        sql_template = file.read()

    sql = '../postgresql/import_' + item + '_data.sql'

    if os.path.exists(sql):
        os.remove(sql)

    for item in items:
        with open(sql, 'a+') as file:
            if item != items[-1]:
                file.write(sql_template.format(item) + ';\n')
            else:
                file.write(sql_template.format(item))


if __name__ == '__main__':
    main()
