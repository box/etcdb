import click
import readline

import sys

import etcdb
import time


def get_query(prompt):
    readline.parse_and_bind('set editing-mode vi')
    query = ""
    while True:
        line = raw_input(prompt)
        query += "%s " % line
        if ';' in line:
            break
    user_input = query.split(';', 1)[0]
    user_input = user_input.strip(' \n\t')
    return user_input


@click.command()
@click.option('--host', help='Host to connect to etcd node', default='127.0.0.1', show_default=True)
def main(host):
    prompt = 'etcd@%s> ' % host
    connection = etcdb.connect(host=host)
    cursor = connection.cursor()
    while True:
        query = get_query(prompt)
        if query == 'quit':
            exit(0)
        try:
            start_time = time.time()
            cursor.execute(query)
            _print_table(cursor, time.time() - start_time)
        except etcdb.Error as err:
            print(err)
        except KeyboardInterrupt:
            print('Query is interrupted')


def _print_table(cursor, execution_time=0):
    n_rows = cursor.n_rows

    def _print_dashes():
        printf('-' * (sum(c.width for c in cursor.col_infos) + 3 * cursor.n_cols + 1) + '\n')

    def _print_header():
        printf("|")
        for colinfo in cursor.col_infos:
            printf(" %-*s |" % (colinfo.width, colinfo.name))
        printf('\n')

    def _print_row(r):
        printf("|")
        i = 0
        for f in r:
            printf(" %-*s |" % (cursor.col_infos[i].width, f))
            i += 1
        printf('\n')

    if n_rows > 0:
        _print_dashes()
        _print_header()
        _print_dashes()

    while True:
        row = cursor.fetchone()
        if not row:
            break
        _print_row(row)

    if n_rows > 0:
        _print_dashes()

    def _rows(n):
        return 'row' if n == 1 else 'rows'

    printf("%d %s in set (%0.2f sec)\n\n" % (n_rows, _rows(n_rows), execution_time))


def printf(fmt, *args):
    sys.stdout.write(fmt % args)
