import logging

import ply.yacc as yacc
import lexer
from etcdb.sqlparser.sql_tree import SQLTree

tokens = lexer.tokens
_parse_tree = SQLTree()


def p_statement(p):
    """statement : select_statement
        | show_tables_statement
        | create_table_statement
        | create_database_statement
        | show_databases_statement
        | use_database_statement
        | commit_statement
        | set_statement
        | insert_statement
        | drop_database_statement
        | desc_table_statement
        | update_table_statement
        | wait_statement"""
    _parse_tree.success = True


def p_wait_statement(p):
    """wait_statement : WAIT '(' arg ')' FROM identifier"""
    _parse_tree.query_type = "WAIT"
    _parse_tree.table = p[6]
    _parse_tree.expressions = [
        {
            'type': 'function',
            'name': 'WAIT',
            'args': [p[3]]
        }
    ]


def p_update_table_statement(p):
    """update_table_statement : UPDATE identifier SET identifier '=' STRING_VALUE opt_WHERE """
    _parse_tree.query_type = "UPDATE"
    _parse_tree.table = p[2]


def p_desc_table_statement(p):
    """desc_table_statement : DESC identifier"""
    _parse_tree.table = p[2]
    _parse_tree.query_type = "DESC_TABLE"


def p_drop_database_statement(p):
    """drop_database_statement : DROP DATABASE identifier"""
    _parse_tree.db = p[3]
    _parse_tree.query_type = "DROP_DATABASE"


def p_insert_statement(p):
    """insert_statement : INSERT INTO identifier opt_fieldlist VALUES '(' values_list ')'"""
    _parse_tree.query_type = "INSERT"
    _parse_tree.table = p[3]
    n_fields = len(p[4])
    n_values = len(p[7])
    if n_fields != n_values:
        msg = 'There are {n_fields} fields, but {n_values} values'.format(
            n_fields=n_fields,
            n_values=n_values
        )
        raise SQLParserError(msg)
    for i in xrange(n_fields):
        _parse_tree.fields[p[4][i]] = p[7][i]


def p_opt_fieldlist_empty(p):
    """opt_fieldlist : """
    p[0] = {}


def p_opt_fieldlist(p):
    """opt_fieldlist : '(' fieldlist ')'"""
    p[0] = p[2]


def p_fieldlist_one(p):
    """fieldlist : identifier"""
    p[0] = [p[1]]


def p_fieldlist_many(p):
    """fieldlist : fieldlist ',' identifier  """
    if p[1] is None:
        p[0] = [p[3]]
    else:
        p[1].append(p[3])
        p[0] = p[1]


def p_values_list_one(p):
    """values_list : value"""
    p[0] = [p[1]]


def p_values_list_many(p):
    """values_list : values_list ',' value"""
    if p[1] is None:
        p[0] = p[3]
    else:
        p[1].append(p[3])
        p[0] = p[1]


def p_set_statement(p):
    """set_statement : set_autocommit_statement
        | set_names_statement"""


def p_set_names_statement(p):
    """set_names_statement : SET NAMES STRING"""
    _parse_tree.query_type = "SET_NAMES"


def p_set_statement_autocommit(p):
    """set_autocommit_statement : SET AUTOCOMMIT '=' NUMBER"""
    _parse_tree.query_type = "SET_AUTOCOMMIT"
    _parse_tree.options['autocommit'] = int(p[4])


def p_commit_statement(p):
    """commit_statement : COMMIT"""
    _parse_tree.query_type = "COMMIT"


def p_create_table_statement(p):
    """create_table_statement : CREATE TABLE identifier '(' create_definition_list ')'"""
    _parse_tree.query_type = "CREATE_TABLE"
    _parse_tree.table = p[3]


def p_create_database_statement(p):
    """create_database_statement : CREATE DATABASE identifier"""
    _parse_tree.query_type = "CREATE_DATABASE"
    _parse_tree.db = p[3]


def p_show_databases_statement(p):
    """show_databases_statement : SHOW DATABASES"""
    _parse_tree.query_type = "SHOW_DATABASES"


def p_use_database_statement(p):
    """use_database_statement : USE identifier"""
    _parse_tree.query_type = "USE_DATABASE"
    _parse_tree.db = p[2]


def p_identifier(p):
    """identifier : STRING"""
    p[0] = p[1]


def p_identifier_escaped(p):
    """identifier : '`' STRING '`'"""
    p[0] = p[2]


def p_create_definition_list(p):
    """create_definition_list : create_definition
        | create_definition_list ',' create_definition"""


def p_create_definition(p):
    """create_definition : identifier column_definition"""
    _parse_tree.fields[p[1]] = p[2]


def p_column_definition(p):
    """column_definition : data_type opt_column_def_options_list"""
    p[0] = {
        'type': p[1]
    }
    p[0]['options'] = p[2]


def p_data_type(p):
    """data_type : INTEGER opt_UNSIGNED
        | VARCHAR '(' NUMBER ')'
        | DATETIME
        | DATETIME '(' NUMBER ')'
        | INT opt_UNSIGNED
        | LONGTEXT
        | SMALLINT opt_UNSIGNED
        | TINYINT
        | BOOL"""
    p[0] = p[1]


def p_opt_UNSIGNED(p):
    """opt_UNSIGNED :
        | UNSIGNED"""


def p_opt_column_def_options_list_empty(p):
    """opt_column_def_options_list : """
    p[0] = {
        'nullable': True
    }


def p_opt_column_def_options_list(p):
    """opt_column_def_options_list : opt_column_def_options opt_column_def_options_list"""
    if p[2] is None:
        p[0] = p[1]
    else:
        p[2].update(p[1])
        p[0] = p[2]


def p_DEFAULT_CLAUSE(p):
    """opt_column_def_options : DEFAULT value"""
    p[0] = {
        'default': p[2]
    }


def p_NULL(p):
    """opt_column_def_options : NULL"""
    p[0] = {
        'nullable': True
    }


def p_NOT_NULL(p):
    """opt_column_def_options : NOT NULL"""
    p[0] = {
        'nullable': False
    }


def p_AUTO_INCREMENT(p):
    """opt_column_def_options : AUTO_INCREMENT"""
    p[0] = {
        'auto_increment': True
    }


def p_PRIMARY_KEY(p):
    """opt_column_def_options : PRIMARY KEY"""
    p[0] = {
        'primary': True
    }


def p_UNIQUE(p):
    """opt_column_def_options : UNIQUE"""
    p[0] = {
        'unique': True
    }


def p_value(p):
    """value : q_STRING
        | NUMBER
        | STRING_VALUE """
    p[0] = p[1]


def p_q_STRING(p):
    """q_STRING : "'" STRING "'" """
    p[0] = p[2]


def p_select_statement(p):
    """select_statement : SELECT select_expr_list opt_FROM opt_WHERE opt_ORDER_BY opt_LIMIT"""
    _parse_tree.query_type = "SELECT"
    try:
        _parse_tree.limit = int(p[6])
    except TypeError:
        _parse_tree.limit = None


def p_opt_ORDER_BY_empty(p):
    """opt_ORDER_BY : """


def p_opt_ORDER_BY_simple(p):
    """opt_ORDER_BY : ORDER BY identifier opt_ORDER_DIRECTION"""
    _parse_tree.order['by'] = p[3]
    _parse_tree.order['direction'] = p[4]


def p_opt_ORDER_BY_extended(p):
    """opt_ORDER_BY : ORDER BY identifier '.' identifier opt_ORDER_DIRECTION"""
    _parse_tree.order['by'] = p[5]
    _parse_tree.order['direction'] = p[6]


def p_opt_ORDER_DIRECTION_empty(p):
    """opt_ORDER_DIRECTION : """
    p[0] = 'ASC'


def p_opt_ORDER_DIRECTION(p):
    """opt_ORDER_DIRECTION : ASC
        | DESC """
    p[0] = p[1]


def p_opt_LIMIT_empty(p):
    """opt_LIMIT : """
    p[0] = None


def p_opt_LIMIT(p):
    """opt_LIMIT : LIMIT NUMBER"""
    p[0] = p[2]


def p_show_tables_statement(p):
    """show_tables_statement : SHOW opt_FULL TABLES"""
    _parse_tree.query_type = "SHOW_TABLES"
    _parse_tree.options['full'] = p[2]


def p_opt_from_empty(p):
    """opt_FROM : """


def p_opt_from(p):
    """opt_FROM : FROM table_reference"""
    _parse_tree.table = p[2]


def p_table_reference(p):
    """table_reference : identifier"""
    p[0] = p[1]


def p_table_reference_w_database(p):
    """table_reference : identifier '.' identifier"""
    _parse_tree.db = p[1]
    p[0] = p[3]


def p_opt_FULL_empty(p):
    """opt_FULL : """
    p[0] = False


def p_opt_FULL(p):
    """opt_FULL : FULL"""
    p[0] = True


def p_select_expr_list(p):
    """select_expr_list : select_expr opt_AS
        | select_expr opt_AS ',' select_expr_list"""


def p_opt_AS(p):
    """opt_AS :
        | AS identifier"""


def p_select_one(p):
    """select_expr : '(' NUMBER ')'"""


def p_select_expr_func(p):
    """select_expr : function '(' arglist ')'"""
    func_def = {
        'type': 'function',
        'name': p[1].upper()
    }
    if p[3]:
        func_def['args'] = p[3]
    _parse_tree.expressions.append(func_def)


def p_select_expr_field(p):
    """select_expr : identifier"""
    _parse_tree.expressions.append({
        'type': 'field',
        'name': p[1]
    })


def p_select_expr_field_w_table(p):
    """select_expr : identifier '.' identifier"""
    _parse_tree.expressions.append({
        'type': 'field',
        'table_name': p[1],
        'name': p[3]
    })


def p_select_expr_variable(p):
    """select_expr : '@' '@' STRING"""
    _parse_tree.expressions.append({
        'type': 'variable',
        'name': p[3].upper()
    })


def p_arglist_empty(p):
    """arglist : """
    p[0] = []


def p_arglist_one(p):
    """arglist : arg"""
    p[0] = [p[1]]


def p_arglist_many(p):
    """arglist : arg ',' arglist"""
    p[0] = p[3].append[p[1]]


def p_function(p):
    """function : VERSION
        | COUNT"""
    p[0] = p[1]


def p_agr(p):
    """arg : STRING"""
    p[0] = p[1]


def p_agr_star(p):
    """arg : '*'"""
    p[0] = '*'


def p_opt_WHERE_empty(p):
    """opt_WHERE : """


def p_opt_WHERE(p):
    """opt_WHERE : WHERE expr"""
    _parse_tree.where = p[2]


def p_expr_OR(p):
    """expr : expr OR expr"""
    p[0] = ('OR', p[1], p[3])


def p_expr_AND(p):
    """expr : expr AND expr"""
    p[0] = ('AND', p[1], p[3])


def p_expr_bool_primary(p):
    """expr : boolean_primary"""
    p[0] = ('bool_primary', p[1])


def p_boolean_primary_is_null(p):
    """boolean_primary : boolean_primary IS NULL"""
    p[0] = ('IS NULL', p[1])


def p_boolean_primary_is_not_null(p):
    """boolean_primary : boolean_primary IS NOT NULL"""
    p[0] = ('IS NOT NULL', p[1])


def p_boolean_primary_comparison(p):
    """boolean_primary : boolean_primary comparison_operator predicate"""
    p[0] = (p[2], p[1], p[3])


def p_boolean_primary_predicate(p):
    """boolean_primary : predicate"""
    p[0] = p[1]


def p_comparison_operator(p):
    """comparison_operator : '='
        | GREATER_OR_EQ
        | '>'
        | LESS_OR_EQ
        | '<'
        | N_EQ"""
    p[0] = p[1]


def p_predicate(p):
    """predicate : bit_expr """
    p[0] = p[1]


def p_bit_expr(p):
    """bit_expr : simple_expr"""
    p[0] = p[1]


def p_simple_expr_identifier(p):
    """simple_expr : identifier"""
    p[0] = ('IDENTIFIER', p[1])


def p_simple_expr_identifier_full(p):
    """simple_expr : identifier '.' identifier"""
    p[0] = ('IDENTIFIER', p[1] + '.' + p[3])


def p_simple_expr_string(p):
    """simple_expr : STRING_VALUE"""
    p[0] = ('STRING', p[1])


def p_simple_expr_parent(p):
    """simple_expr : '(' expr ')'"""
    p[0] = p[2]


def p_error(t):
    if t:
        msg = "Query: {query}\n" \
              "Syntax error at lexeme '{value}' (type: '{type}'). " \
              "Line: {lineno}, position: {lexpos}".format(query=_parse_tree.query,
                                                          value=t.value,
                                                          type=t.type,
                                                          lineno=t.lineno,
                                                          lexpos=t.lexpos)
        raise SQLParserError(msg)
    else:
        raise SQLParserError("Query: {query}\nSyntax error".format(
            query=_parse_tree.query))


class SQLParser(object):
    def __init__(self):
        self._parser = yacc.yacc(debug=True)

    def parse(self, *args, **kwargs):
        # global _parse_tree
        # del _parse_tree
        _parse_tree.reset()
        _parse_tree.query = args[0]
        log = logging.getLogger()
        # kwargs['debug'] = log
        self._parser.parse(*args, **kwargs)
        return _parse_tree


class SQLParserError(Exception):
    """All SQL parsing errors"""
