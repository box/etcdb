from __future__ import print_function

states = (
    ('quoted', 'exclusive'),
)
reserved = [
    'SELECT', 'VERSION', 'AUTO_INCREMENT', 'CREATE', 'DEFAULT', 'FULL',
    'INTEGER', 'KEY', 'NULL',
    'PRIMARY', 'SHOW', 'TABLE', 'TABLES', 'VARCHAR', 'NOT', 'DATETIME',
    'DATABASE', 'DATABASES',
    'USE', 'INT', 'FROM', 'COMMIT', 'WHERE', 'OR', 'AND', 'IS', 'SET',
    'AUTOCOMMIT',
    'LONGTEXT', 'SMALLINT', 'UNSIGNED', 'BOOL', 'TINYINT', 'UNIQUE', 'NAMES',
    'INSERT', 'INTO', 'VALUES', 'DROP', 'LIMIT', 'AS', 'UPDATE', 'COUNT',
    'ORDER', 'BY', 'ASC', 'DESC',
    'WAIT', 'IF', 'EXISTS', 'DELETE', 'IN', 'AFTER', 'LOCK'
]

tokens = ['NUMBER', 'STRING', 'STRING_VALUE',
          'GREATER_OR_EQ', 'LESS_OR_EQ', 'N_EQ'] + reserved
literals = "(),`'.@=><*"

t_ignore = ' \t\n'
t_quoted_ignore = t_ignore
t_NUMBER = r'[0-9]+'
t_GREATER_OR_EQ = r'>='
t_LESS_OR_EQ = r'<='
t_N_EQ = r'<>|!='


def t_begin_quoted(t):
    r"""'"""
    t.lexer.begin('quoted')


def t_quoted_STRING_VALUE(t):
    r"""[- :.a-zA-Z0-9$/+=_@]+"""
    if t.value.upper() in reserved:
        t.type = t.value.upper()
        t.value = t.value.upper()
    else:
        t.type = 'STRING_VALUE'
    return t


def t_quoted_end(t):
    r"""'"""
    t.lexer.begin('INITIAL')


def t_STRING(t):
    r"""[_a-zA-Z0-9]*[_a-zA-Z]+[_a-zA-Z0-9]*"""
    if t.value.upper() in reserved:
        t.type = t.value.upper()
        t.value = t.value.upper()
    else:
        t.type = 'STRING'
    return t


def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)


def t_quoted_error(t):
    t_error(t)
