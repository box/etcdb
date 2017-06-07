import json

import etcdb
import pytest
import requests


@pytest.fixture
def cursor():
    # Clean database
    http_response = requests.get('http://10.0.1.10:2379/v2/keys/')
    content = http_response.content
    node = json.loads(content)['node']
    try:
        for n in node['nodes']:
            key = n['key']
            requests.delete('http://10.0.1.10:2379/v2/keys{key}?recursive=true'.format(key=key))
    except KeyError:
        pass

    connection = etcdb.connect(host='10.0.1.10', db='foo', timeout=1)
    return connection.cursor()


def test_select_version(cursor):
    cursor.execute('SELECT VERSION()')
    assert cursor.fetchone()[0] == '2.3.7'


def test_get_meta_lock(cursor):
    #print(cursor._get_meta_lock('foo', 'bar'))
    #print(cursor._release_meta_lock('foo', 'bar'))
    pass


def test_select_limit(cursor):
    cursor.execute('CREATE TABLE t1(id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))')
    cursor.execute("INSERT INTO t1(name) VALUES ('aaa')")
    cursor.execute("INSERT INTO t1(name) VALUES ('bbb')")
    cursor.execute("INSERT INTO t1(name) VALUES ('ccc')")
    cursor.execute("SELECT id, name FROM t1 LIMIT 2")
    assert cursor.fetchall() == (
        ('1', 'aaa'),
        ('2', 'bbb'),
    )


def test_select_where(cursor):
    cursor.execute("""
    CREATE TABLE `auth_user` (
  `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `password` varchar(128) NOT NULL,
  `last_login` datetime(6),
  `is_superuser` tinyint NOT NULL,
  `username` varchar(150) NOT NULL,
  `first_name` varchar(30) NOT NULL,
  `last_name` varchar(30) NOT NULL,
  `email` varchar(254) NOT NULL,
  `is_staff` tinyint NOT NULL,
  `is_active` tinyint NOT NULL,
  `date_joined` datetime(6) NOT NULL
)""")
    cursor.execute('SHOW TABLES')
    assert cursor.fetchone() == ('auth_user',)
    cursor.execute("INSERT INTO `auth_user` (`password`, `last_login`, "
                   "`is_superuser`, `username`, `first_name`, `last_name`, "
                   "`email`, `is_staff`, `is_active`, `date_joined`) "
                   "VALUES ('=', 'None', "
                   "'True', 'root1', 'a', 'b', "
                   "'a@a.com', 'True', 'True', '2017-06-06 20:33:38')")
    cursor.execute("INSERT INTO `auth_user` (`password`, `last_login`, "
                   "`is_superuser`, `username`, `first_name`, `last_name`, "
                   "`email`, `is_staff`, `is_active`, `date_joined`) "
                   "VALUES ('=', 'None', "
                   "'True', 'root2', 'a', 'b', "
                   "'a@a.com', 'True', 'True', '2017-06-06 20:33:38')")
    cursor.execute('SELECT id, username FROM auth_user')
    assert cursor.fetchone() == ('1', 'root1')
    assert cursor.fetchone() == ('2', 'root2')

    cursor.execute("SELECT id, username FROM auth_user WHERE username = 'root1'")
    assert cursor.fetchone() == ('1', 'root1')
