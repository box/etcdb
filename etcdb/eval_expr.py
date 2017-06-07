"""
Module provides functions to evaluate an expression given in
a WHERE statement.
Grammar of expression is described on MySQL website
(https://dev.mysql.com/doc/refman/5.7/en/expressions.html).
"""
from etcdb.sqlparser.parser import SQLParserError


def eval_identifier(row, identifier):
    """
    Get value of identifier for a given row

    :param row: row
    :type row: tuple
    :param identifier: Identifier
    :type identifier: str
    :return: value of identifier
    """
    fields = row[0]
    data = row[1]
    pos = fields.index(identifier)
    return data[pos]


def eval_string(value):
    """Evaluate string token"""
    return value


def eval_simple_expr(row, tree):
    """Evaluate simple_expr"""
    if tree[0] == 'IDENTIFIER':
        return eval_identifier(row, tree[1])
    elif tree[0] == 'STRING':
        return eval_string(tree[1])
    else:
        raise SQLParserError('%s is not implemented' % tree[0])


def eval_bit_expr(row, tree):
    """Evaluate bit_expr"""
    if tree[0] == 'simple_expr':
        return eval_simple_expr(row, tree[1])
    else:
        raise SQLParserError('%s is not implemented' % tree[0])


def eval_predicate(row, tree):
    """Evaluate predicate"""
    if tree[0] == 'bit_expr':
        return eval_bit_expr(row, tree[1])
    else:
        raise SQLParserError('%s is not implemented' % tree[0])


def eval_bool_primary(row, tree): # pylint: disable=too-many-return-statements
    """Evaluate bool_primary"""
    if tree[0] == 'IS NULL':
        return eval_bool_primary(row, tree[1]) is None
    elif tree[0] == 'IS NOT NULL':
        return eval_bool_primary(row, tree[1]) is not None
    elif tree[0] == '=':
        return eval_bool_primary(row, tree[1]) == \
               eval_bool_primary(row, tree[2])
    elif tree[0] == '>=':
        return eval_bool_primary(row, tree[1]) >= \
               eval_bool_primary(row, tree[2])
    elif tree[0] == '>':
        return eval_bool_primary(row, tree[1]) > \
               eval_bool_primary(row, tree[2])
    elif tree[0] == '<=':
        return eval_bool_primary(row, tree[1]) <= \
               eval_bool_primary(row, tree[2])
    elif tree[0] == '<':
        return eval_bool_primary(row, tree[1]) < \
               eval_bool_primary(row, tree[2])
    elif tree[0] in ['<>', '!=']:
        return eval_bool_primary(row, tree[1]) != \
               eval_bool_primary(row, tree[2])
    elif tree[0] == 'predicate':
        return eval_predicate(row, tree[1])
    elif tree[0] == 'bit_expr':
        return eval_bit_expr(row, tree[1])
    else:
        raise SQLParserError('%s is not implemented' % tree[0])


def eval_expr(row, tree):
    """Evaluate expression"""
    if tree[0] == 'OR':
        return eval_expr(row, tree[1]) or eval_expr(row, tree[2])
    elif tree[0] == 'AND':
        return eval_expr(row, tree[1]) or eval_expr(row, tree[2])
    elif tree[0] == 'bool_primary':
        return eval_bool_primary(row, tree[1])
    else:
        raise SQLParserError('%s is not implemented' % tree[0])
