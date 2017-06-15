from pprint import pprint

import pytest


def test_select_star(parser):
    tree = parser.parse('SELECT * from bar')
    assert tree.query_type == "SELECT"
    assert tree.table == 'bar'
    assert tree.expressions == [
        (
            ('*', None),
            None
        )
    ]


def test_select_table_wild(parser):
    tree = parser.parse('SELECT bar.* from bar')
    assert tree.query_type == "SELECT"
    assert tree.table == 'bar'
    assert tree.expressions == [
        (
            ('*', 'bar'),
            None
        )
    ]


@pytest.mark.parametrize('query', [
    'SELECT VERSION()',
    'select version()'
])
def test_select_version(query, parser):
    tree = parser.parse(query)
    assert tree.query_type == "SELECT"
    assert tree.table is None
    pprint(tree.expressions)
    assert tree.expressions == [
        (
            ('bool_primary',
                ('predicate',
                    ('bit_expr',
                        ('simple_expr',
                            ('function_call', 'VERSION')
                         )
                     )
                 )
             ),
            None
        )
    ]


def test_select_version_table_wild(parser):
    tree = parser.parse('SELECT VERSION(), bar.* FROM bar')
    assert tree.query_type == "SELECT"
    assert tree.table == 'bar'
    pprint(tree.expressions)
    assert tree.expressions == [
        (
            ('bool_primary',
             ('predicate',
              ('bit_expr',
               ('simple_expr',
                ('function_call', 'VERSION')
                )
               )
              )
             ),
            None
        ),
        (
            ('*', 'bar'),
            None
        )
    ]


@pytest.mark.parametrize('query,table,fields', [
    (
        """CREATE TABLE `django_migrations` (
        `id` INTEGER AUTO_INCREMENT NOT NULL PRIMARY KEY,
        `app` VARCHAR(255) NOT NULL,
        `name` VARCHAR(255) NOT NULL,
        `applied` DATETIME NOT NULL)""",
        'django_migrations',
        {
            'id': {
                'type': 'INTEGER',
                'options': {
                    'auto_increment': True,
                    'nullable': False,
                    'primary': True
                }
            },
            'app': {
                'type': 'VARCHAR',
                'options': {
                    'nullable': False
                }
            },
            'name': {
                'type': 'VARCHAR',
                'options': {
                    'nullable': False
                }
            },
            'applied': {
                'type': 'DATETIME',
                'options': {
                    'nullable': False
                }
            }
        }
    ),

    (
        'CREATE TABLE `auth_group_permissions` ('
        '`id` INTEGER AUTO_INCREMENT NOT NULL PRIMARY KEY, '
        '`group_id` INTEGER NOT NULL, '
        '`permission_id` INTEGER NOT NULL)',
        'auth_group_permissions',
        {
            'id': {
                'type': 'INTEGER',
                'options': {
                    'auto_increment': True,
                    'nullable': False,
                    'primary': True
                }

            },
            'group_id': {
                'type': 'INTEGER',
                'options': {
                    'nullable': False
                }
            },
            'permission_id': {
                'type': 'INTEGER',
                'options': {
                    'nullable': False
                }
            }
        }
    ),

    (
        'CREATE TABLE `t1` ('
        '`id` INT)',
        't1',
        {
            'id': {
                'type': 'INT',
                'options': {
                    'nullable': True
                }
            }
        }
    ),
    (
        'CREATE TABLE t2 ('
        '`id` INT)',
        't2',
        {
            'id': {
                'type': 'INT',
                'options': {
                    'nullable': True
                }
            }
        }
    ),
    (
        'CREATE TABLE 1t ('
        '`id` INT)',
        '1t',
        {
            'id': {
                'type': 'INT',
                'options': {
                    'nullable': True
                }
            }
        }
    ),
    (
        'CREATE TABLE 1t1 ('
        '`id` INT)',
        '1t1',
        {
            'id': {
                'type': 'INT',
                'options': {
                    'nullable': True
                }
            }
        }
    ),
    (
        """
        CREATE TABLE `django_admin_log` (
        `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
        `action_time` datetime NOT NULL,
        `object_id` longtext NULL,
        `object_repr` varchar(200) NOT NULL,
        `action_flag` smallint UNSIGNED NOT NULL,
        `change_message` longtext NOT NULL,
        `content_type_id` integer NULL,
        `user_id` integer NOT NULL)
        """,
        'django_admin_log',
        {
            'id': {
                'type': 'INTEGER',
                'options': {
                    'auto_increment': True,
                    'nullable': False,
                    'primary': True
                }
            },
            'action_time': {
                'type': 'DATETIME',
                'options': {
                    'nullable': False,
                }
            },
            'object_id': {
                'type': 'LONGTEXT',
                'options': {
                    'nullable': True,
                }
            },
            'object_repr': {
                'type': 'VARCHAR',
                'options': {
                    'nullable': False,
                }
            },
            'action_flag': {
                'type': 'SMALLINT',
                'options': {
                    'nullable': False
                }
            },
            'change_message': {
                'type': 'LONGTEXT',
                'options': {
                    'nullable': False
                }
            },
            'content_type_id': {
                'type': 'INTEGER',
                'options': {
                    'nullable': True
                }
            },
            'user_id': {
                'type': 'INTEGER',
                'options': {
                    'nullable': False
                }
            }
        }
    ),
    (
        """
        CREATE TABLE `auth_user` (
        `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
        `password` varchar(128) NOT NULL,
        `last_login` datetime(6) NOT NULL,
        `is_superuser` bool NOT NULL,
        `username` varchar(30) NOT NULL UNIQUE,
        `first_name` varchar(30) NOT NULL,
        `last_name` varchar(30) NOT NULL,
        `email` varchar(75) NOT NULL,
        `is_staff` bool NOT NULL,
        `is_active` bool NOT NULL,
        `date_joined` datetime(6) NOT NULL)
        """,
        'auth_user',
        {
            'id': {
                'type': 'INTEGER',
                'options': {
                    'auto_increment': True,
                    'nullable': False,
                    'primary': True
                }
            },
            'password': {
                'type': 'VARCHAR',
                'options': {
                    'nullable': False,
                }
            },
            'last_login': {
                'type': 'DATETIME',
                'options': {
                    'nullable': False,
                }
            },
            'is_superuser': {
                'type': 'BOOL',
                'options': {
                    'nullable': False,
                }
            },
            'username': {
                'type': 'VARCHAR',
                'options': {
                    'unique': True,
                    'nullable': False
                }
            },
            'first_name': {
                'type': 'VARCHAR',
                'options': {
                    'nullable': False,
                }
            },
            'last_name': {
                'type': 'VARCHAR',
                'options': {
                    'nullable': False,
                }
            },
            'email': {
                'type': 'VARCHAR',
                'options': {
                    'nullable': False,
                }
            },
            'is_staff': {
                'type': 'BOOL',
                'options': {
                    'nullable': False,
                }
            },
            'is_active': {
                'type': 'BOOL',
                'options': {
                    'nullable': False,
                }
            },
            'date_joined': {
                'type': 'DATETIME',
                'options': {
                    'nullable': False,
                }
            }
        }
    ),
    (
        "CREATE TABLE `auth_group` ("
        "`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, "
        "`name` varchar(80) NOT NULL UNIQUE)",
        'auth_group',
        {
            'id': {
                'type': 'INTEGER',
                'options': {
                    'auto_increment': True,
                    'nullable': False,
                    'primary': True
                }
            },
            'name': {
                'type': 'VARCHAR',
                'options': {
                    'unique': True,
                    'nullable': False
                }
            }
        }
    )
])
def test_create_table(query, table, fields, parser):
    tree = parser.parse(query)
    assert tree.query_type == "CREATE_TABLE"
    assert tree.table == table
    pprint(tree.fields)
    assert tree.fields == fields


def test_create_database(parser):
    query = "CREATE DATABASE `foo`"
    tree = parser.parse(query)
    assert tree.query_type == "CREATE_DATABASE"
    assert tree.db == 'foo'


def test_show_databases(parser):
    query = "SHOW DATABASES"
    tree = parser.parse(query)
    assert tree.query_type == "SHOW_DATABASES"


def test_show_tables(parser):
    query = "SHOW TABLES"
    tree = parser.parse(query)
    assert tree.query_type == "SHOW_TABLES"
    assert not tree.options['full']
    assert tree.success


def test_show_full_tables(parser):
    query = "SHOW FULL TABLES"
    tree = parser.parse(query)
    assert tree.query_type == "SHOW_TABLES"
    assert tree.options['full']
    assert tree.success


def test_use_database(parser):
    query = "USE `foo`"
    tree = parser.parse(query)
    assert tree.query_type == "USE_DATABASE"
    assert tree.db == 'foo'


def test_create_table_int(parser):
    tree = parser.parse('CREATE TABLE t(id INT)')
    assert tree.success


def test_select_fields_from(parser):
    query = """SELECT `django_migrations`.`app`, `django_migrations`.`name` FROM `django_migrations`"""
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "SELECT"
    assert tree.table == "django_migrations"
    pprint(tree.expressions)
    assert tree.expressions == [
        (
            ('bool_primary',
                ('predicate',
                    ('bit_expr',
                        ('simple_expr',
                            ('IDENTIFIER', 'django_migrations.app')
                         )
                     )
                 )
             ),
            None),
        (
            ('bool_primary',
                ('predicate',
                    ('bit_expr',
                        ('simple_expr',
                            ('IDENTIFIER', 'django_migrations.name')
                         )
                     )
                 )
             ),
            None
        )
    ]


@pytest.mark.parametrize('query', [
    "SELECT app, foo FROM `django_migrations`",
    "SELECT `app`, `foo` FROM `django_migrations`"
])
def test_select_short_fields_from(query, parser):
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "SELECT"
    assert tree.table == "django_migrations"
    print(tree.expressions)
    assert tree.expressions == [
        (
            ('bool_primary',
             ('predicate',
              ('bit_expr',
               ('simple_expr',
                ('IDENTIFIER', 'app')
                )
               )
              )
             ),
            None),
        (
            ('bool_primary',
             ('predicate',
              ('bit_expr',
               ('simple_expr',
                ('IDENTIFIER', 'foo')
                )
               )
              )
             ),
            None
        )
    ]


def test_select_two_func(parser):
    query = "SELECT VERSION(), VERSION()"
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "SELECT"
    print(tree.expressions)
    assert tree.expressions == [
        (
            ('bool_primary',
             ('predicate',
              ('bit_expr',
               ('simple_expr',
                ('function_call', 'VERSION')
                )
               )
              )
             ),
            None
        ),
        (
            ('bool_primary',
             ('predicate',
              ('bit_expr',
               ('simple_expr',
                ('function_call', 'VERSION')
                )
               )
              )
             ),
            None
        )
    ]


def test_select_var(parser):
    query = "SELECT @@sql_mode"
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "SELECT"
    assert tree.expressions == [
        (
            ('bool_primary',
             ('predicate',
              ('bit_expr',
               ('simple_expr',
                ('variable', 'sql_mode')
                )
               )
              )
             ),
            None
        )
    ]


def test_select_var_SQL_AUTO_IS_NULL(parser):
    query = "SELECT @@SQL_AUTO_IS_NULL"
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "SELECT"
    assert tree.expressions == [
        (
            ('bool_primary',
             ('predicate',
              ('bit_expr',
               ('simple_expr',
                ('variable', 'SQL_AUTO_IS_NULL')
                )
               )
              )
             ),
            None
        )
    ]


def test_commit(parser):
    query = "COMMIT"
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "COMMIT"


def test_select_from_tbl(parser):
    query = """SELECT f1 FROM t1"""
    tree = parser.parse(query)
    assert tree.success
    assert tree.table == 't1'
    assert tree.db is None
    assert tree.expressions == [
        (
            ('bool_primary',
             ('predicate',
              ('bit_expr',
               ('simple_expr',
                ('IDENTIFIER', 'f1')
                )
               )
              )
             ),
            None
        ),
    ]


def test_select_from_db_tbl(parser):
    query = """SELECT f1, f2 FROM d1.t1"""
    tree = parser.parse(query)
    assert tree.success
    assert tree.table == 't1'
    assert tree.db == 'd1'
    assert tree.expressions == [
        (
            ('bool_primary',
             ('predicate',
              ('bit_expr',
               ('simple_expr',
                ('IDENTIFIER', 'f1')
                )
               )
              )
             ),
            None
        ),
        (
            ('bool_primary',
             ('predicate',
              ('bit_expr',
               ('simple_expr',
                ('IDENTIFIER', 'f2')
                )
               )
              )
             ),
            None
        ),
    ]


@pytest.mark.parametrize('query,table,expressions,where', [
    (
        "SELECT f1, f2 FROM t1 WHERE f1 = 'foo'",
        't1',
        [
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'f1')
                    )
                   )
                  )
                 ),
                None
            ),
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'f2')
                    )
                   )
                  )
                 ),
                None
            ),
        ],
        ('bool_primary',
         ('=',
          ('predicate', ('bit_expr', ('simple_expr', ('IDENTIFIER', 'f1')))),
          ('bit_expr', ('simple_expr', ('literal', 'foo')))))
    ),
    (
        """
        SELECT `id`,
        `app_label`,
        `model`
        FROM `django_content_type`
        WHERE `model` = 'logentry'
            AND `app_label` = 'admin'
        """,
        'django_content_type',
        [
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'id')
                    )
                   )
                  )
                 ),
                None
            ),
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'app_label')
                    )
                   )
                  )
                 ),
                None
            ),
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'model')
                    )
                   )
                  )
                 ),
                None
            ),
        ],
        ('AND',
         ('bool_primary',
          ('=',
           ('predicate', ('bit_expr', ('simple_expr', ('IDENTIFIER', 'model')))),
           ('bit_expr', ('simple_expr', ('literal', 'logentry'))))),
         ('bool_primary',
          ('=',
           ('predicate', ('bit_expr', ('simple_expr', ('IDENTIFIER', 'app_label')))),
           ('bit_expr', ('simple_expr', ('literal', 'admin'))))))
    ),
    (
        """
        SELECT `django_content_type`.`id`,
        `django_content_type`.`app_label`,
        `django_content_type`.`model`
        FROM `django_content_type`
        WHERE `django_content_type`.`model` = 'logentry'
            AND `django_content_type`.`app_label` = 'admin'
        """,
        'django_content_type',
        [
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'django_content_type.id')
                    )
                   )
                  )
                 ),
                None
            ),
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'django_content_type.app_label')
                    )
                   )
                  )
                 ),
                None
            ),
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'django_content_type.model')
                    )
                   )
                  )
                 ),
                None
            ),
        ],
        ('AND',
         ('bool_primary',
          ('=',
           ('predicate',
            ('bit_expr',
             ('simple_expr', ('IDENTIFIER', 'django_content_type.model')))),
           ('bit_expr', ('simple_expr', ('literal', 'logentry'))))),
         ('bool_primary',
          ('=',
           ('predicate',
            ('bit_expr',
             ('simple_expr', ('IDENTIFIER', 'django_content_type.app_label')))),
           ('bit_expr', ('simple_expr', ('literal', 'admin'))))))
    )
])
def test_select_from_tbl_where(parser, query, table, expressions, where):
    print(query)
    tree = parser.parse(query)
    assert tree.success
    assert tree.table == table
    assert tree.db is None
    assert tree.expressions == expressions
    print(query)
    print('Expected:')
    pprint(where)
    print('Got:')
    pprint(tree.where)
    assert tree.where == where


@pytest.mark.parametrize('autocommit', [
    0, 1
])
def test_set_autocommit(parser, autocommit):
    query = "set autocommit=%d" % autocommit
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "SET_AUTOCOMMIT"
    assert tree.options['autocommit'] == autocommit


def test_select_cols_with_tbl(parser):
    query = "SELECT `django_migrations`.`app`, `django_migrations`.`name` FROM `django_migrations`"
    tree = parser.parse(query)
    assert tree.success
    assert tree.table == 'django_migrations'
    assert tree.db is None
    assert tree.expressions == [
        (
            ('bool_primary',
             ('predicate',
              ('bit_expr',
               ('simple_expr',
                ('IDENTIFIER', 'django_migrations.app')
                )
               )
              )
             ),
            None
        ),
        (
            ('bool_primary',
             ('predicate',
              ('bit_expr',
               ('simple_expr',
                ('IDENTIFIER', 'django_migrations.name')
                )
               )
              )
             ),
            None
        ),
    ]


def test_set_names(parser):
    query = "SET NAMES utf8"
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "SET_NAMES"


def test_insert(parser):
    query = "INSERT INTO `django_migrations` (`app`, `name`, `applied`) " \
            "VALUES ('auth', '0003_alter_user_email_max_length', '2016-09-30 22:01:02.851495')"
    tree = parser.parse(query)
    assert tree.success
    assert tree.table == 'django_migrations'
    assert tree.query_type == "INSERT"
    assert tree.fields == {
        'app': 'auth',
        'name': '0003_alter_user_email_max_length',
        'applied': '2016-09-30 22:01:02.851495'
    }


def test_drop_database(parser):
    query = "DROP DATABASE foo"
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "DROP_DATABASE"
    assert tree.db == 'foo'


def test_drop_table(parser):
    query = "DROP TABLE foo"
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "DROP_TABLE"
    assert tree.table == 'foo'
    assert tree.options['if_exists'] is False


def test_drop_table_if_exists(parser):
    query = "DROP TABLE foo IF EXISTS"
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "DROP_TABLE"
    assert tree.table == 'foo'
    assert tree.options['if_exists'] is True


def test_desc_table(parser):
    query = "desc foo"
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "DESC_TABLE"
    assert tree.table == 'foo'
    assert tree.db is None


@pytest.mark.parametrize('query', [
    """SELECT
        `django_session`.`session_key`,
        `django_session`.`session_data`,
        `django_session`.`expire_date`
    FROM `django_session`
    WHERE (
        `django_session`.`session_key` = '5ly6avjqb20gmxav35soqnscb13iltd6'
        AND `django_session`.`expire_date` > '2016-10-10 18:23:18'
        )
    """,
    """SELECT
        `django_session`.`session_key`,
        `django_session`.`session_data`,
        `django_session`.`expire_date`
    FROM `django_session`
    WHERE (
        `django_session`.`session_key` = '5ly6avjqb20gmxav35soqnscb13iltd6'
        AND `django_session`.`expire_date` < '2016-10-10 18:23:18'
        )
    """
])
def test_select_with_more_in_where(query, parser):
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "SELECT"
    assert tree.table == 'django_session'
    assert tree.db is None
    pprint(tree.where)
    # assert 0


def test_select_with_limit(parser):
    query = "SELECT `auth_user`.`id`, `auth_user`.`password`, `auth_user`.`last_login`, `auth_user`.`is_superuser`, `auth_user`.`username`, `auth_user`.`first_name`, `auth_user`.`last_name`, `auth_user`.`email`, `auth_user`.`is_staff`, `auth_user`.`is_active`, `auth_user`.`date_joined` " \
            "FROM `auth_user` WHERE `auth_user`.`username` = 'admin' LIMIT 21"
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "SELECT"
    assert tree.table == 'auth_user'
    assert tree.db is None
    assert tree.limit == 21


def test_insert_create_super_user(parser):
    query = """INSERT INTO `auth_user`
    (`password`, `last_login`, `is_superuser`, `username`, `first_name`, `last_name`, `email`, `is_staff`, `is_active`, `date_joined`)
    VALUES ('pbkdf2_sha256$30000$rpn3UE9RjKsE$MNBqf1DSWdT2qFFzZ/h++60/pga3xBFUk0QrokaBqtY=',
    'None', 'True', 'admin', 'John', 'Smith',
    'john.smith@aaa.com', 'True', 'True',
    '2016-10-10 19:03:31')
    """
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "INSERT"
    assert tree.table == 'auth_user'
    assert tree.db is None


def test_select_one_parenthesis(parser):
    query = """
    SELECT (1) AS `a` FROM `django_session` WHERE `django_session`.`session_key` = 'mydfqwi234umfggb32s6hsc2vb4ewbpv' LIMIT 1
    """
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "SELECT"
    assert tree.table == 'django_session'
    assert tree.db is None

    pprint(tree.expressions)
    # 1/0
    assert tree.expressions == [
        (
            ('bool_primary',
                ('predicate',
                    ('bit_expr',
                        ('simple_expr',
                            ('expr',
                                ('bool_primary',
                                    ('predicate',
                                        ('bit_expr',
                                            ('simple_expr',
                                                ('literal', '1')
                                             )
                                         )
                                     )
                                 )
                             )
                         )
                     )
                 )
             ),
            'a'
        )
    ]


def test_update(parser):
    query = "UPDATE `auth_user` SET `last_login` = '2016-10-10 19:19:56' WHERE `auth_user`.`id` = '2'"
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "UPDATE"
    assert tree.table == 'auth_user'
    assert tree.db is None


def test_select_count_star(parser):
    query = "SELECT COUNT(*) AS `__count` FROM `foo_config`"
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "SELECT"
    assert tree.table == 'foo_config'
    assert tree.db is None
    pprint(tree.expressions)
    # 1/0
    assert tree.expressions == [
        (
            ('bool_primary',
                ('predicate',
                    ('bit_expr',
                        ('simple_expr',
                            ('function_call', 'COUNT')
                         )
                     )
                 )
             ),
            '__count'
        )
    ]


@pytest.mark.parametrize('query,direction', [
    ("SELECT foo FROM bar ORDER BY `foo_config`.`foo` ASC LIMIT 1", 'ASC'),
    ("SELECT foo FROM bar ORDER BY foo ASC", 'ASC'),
    ("SELECT foo FROM bar ORDER BY foo ASC LIMIT 1", 'ASC'),
    ("SELECT foo FROM bar ORDER BY foo DESC", 'DESC'),
    ("SELECT foo FROM bar ORDER BY foo", 'ASC')
])
def test_select_order(query, direction, parser):
    tree = parser.parse(query)
    assert tree.success
    assert tree.query_type == "SELECT"
    assert tree.table == 'bar'
    assert tree.db is None
    assert tree.expressions == [
        (
            ('bool_primary',
             ('predicate',
              ('bit_expr',
               ('simple_expr',
                ('IDENTIFIER', 'foo')
                )
               )
              )
             ),
            None
        ),

    ]
    assert tree.order['by'] == 'foo'
    assert tree.order['direction'] == direction


def test_select_wait(parser):
    tree = parser.parse("wait(foo) from bar")
    assert tree.query_type == "WAIT"
    assert tree.table == 'bar'
    pprint(tree.expressions)
    # 1/0
    assert tree.expressions == 'foo'


def test_insert_with_empty_values(parser):
    tree = parser.parse("insert into foo(id, name) values(1, '')")
    assert tree.query_type == "INSERT"
    assert tree.table == 'foo'
    # pprint(tree)
    # 1/0
    assert tree.fields == {
        'id': '1',
        'name': ''
    }


def test_where_and_not(parser):
    query = "SELECT (1) AS `a` FROM `bar` " \
            "WHERE (`bar`.`name` = 'foo' " \
            "AND NOT (`bar`.`name` = 'foo')) LIMIT 1"
    tree = parser.parse(query)
    assert tree.query_type == 'SELECT'
    assert tree.table == 'bar'
    assert tree.where == ('bool_primary',
                          ('predicate',
                           ('bit_expr',
                            ('simple_expr',
                             ('expr',
                              ('AND',
                               ('bool_primary',
                                ('=',
                                 ('predicate',
                                  ('bit_expr',
                                   ('simple_expr', ('IDENTIFIER', 'bar.name')))),
                                 ('bit_expr', ('simple_expr', ('literal', 'foo'))))),
                               ('NOT',
                                ('bool_primary',
                                 ('predicate',
                                  ('bit_expr',
                                   ('simple_expr',
                                    ('expr',
                                     ('bool_primary',
                                      ('=',
                                       ('predicate',
                                        ('bit_expr',
                                         ('simple_expr',
                                          ('IDENTIFIER', 'bar.name')))),
                                       ('bit_expr',
                                        ('simple_expr', ('literal', 'foo')))))))))))))))))


def test_update_with_where(parser):
    query = "UPDATE `foo` SET `body` = 'aaa', name='ccc' WHERE `foo`.`name` = 'bbb'"
    tree = parser.parse(query)
    assert tree.query_type == "UPDATE"
    assert tree.table == 'foo'
    assert tree.expressions == [('body',
                                 ('bool_primary',
                                  ('predicate', ('bit_expr', ('simple_expr', ('literal', 'aaa')))))),
                                ('name',
                                 ('bool_primary',
                                  ('predicate', ('bit_expr', ('simple_expr', ('literal', 'ccc'))))))]
    assert tree.where == ('bool_primary',
                          ('=',
                           ('predicate', ('bit_expr', ('simple_expr', ('IDENTIFIER', 'foo.name')))),
                           ('bit_expr', ('simple_expr', ('literal', 'bbb')))))


def test_delete(parser):
    query = "DELETE FROM foo WHERE id = 'bar'"
    tree = parser.parse(query)
    assert tree.query_type == "DELETE"
    assert tree.table == 'foo'
    assert tree.where == ('bool_primary',
                          ('=',
                           ('predicate', ('bit_expr', ('simple_expr', ('IDENTIFIER', 'id')))),
                           ('bit_expr', ('simple_expr', ('literal', 'bar')))))


def test_delete_with_in(parser):
    query = "DELETE FROM `foo` WHERE `foo`.`bar` IN ('xyz', 'abc')"
    tree = parser.parse(query)
    assert tree.query_type == "DELETE"
    assert tree.table == 'foo'
    pprint(tree.where)
    assert tree.where == ('bool_primary',
                          ('predicate',
                           ('IN',

                            ('simple_expr', ('IDENTIFIER', 'foo.bar')),

                            [
                                 ('bool_primary',
                                  ('predicate', ('bit_expr', ('simple_expr', ('literal', 'xyz'))))),

                                 ('bool_primary',
                                  ('predicate', ('bit_expr', ('simple_expr', ('literal', 'abc')))))
                            ]

                            )

                           )
                          )
