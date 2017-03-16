"""
Command line functions
"""
from __future__ import print_function

import readline
import sys
import time

import click
import pyetcd

import etcdb


def get_query(prompt):
    """
    Get input from a user terminated by a semicolon and return a string.

    :param prompt: A prompt string.
    :type prompt: str
    :return: User input without a trailing semicolon.
    :rtype: str
    """
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
@click.option('-h', '--host', help='Host to connect to etcd node', default='127.0.0.1', show_default=True)
def main(host):
    """
    Read and execute user input.
    """
    print('Etcdb version %s, pyetcd version %s' % (etcdb.__version__,
                                                   pyetcd.__version__))
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
        except (etcdb.Error, pyetcd.EtcdException) as err:
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

    def _print_row(line):
        printf("|")
        i = 0
        for char in line:
            printf(" %-*s |" % (cursor.col_infos[i].width, char))
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

    def _rows(num):
        return 'row' if num == 1 else 'rows'

    printf("%d %s in set (%0.2f sec)\n\n" % (n_rows, _rows(n_rows), execution_time))


def printf(fmt, *args):
    """
    Printf implemetnation in Python.

    :param fmt: Format string. See man 3 printf for syntax.
    :type fmt: str
    :param args: Arguments to print.
    """
    sys.stdout.write(fmt % args)
