from multiprocessing import Process, Queue

import psutil as psutil
import time

import etcdb


def test_select_version(cursor):
    cursor.execute('SELECT VERSION()')
    assert cursor.fetchone()[0] == etcdb.__version__


def test_get_meta_lock(cursor):
    #print(cursor._get_meta_lock('foo', 'bar'))
    #print(cursor._release_meta_lock('foo', 'bar'))
    pass


def test_select_limit(cursor):
    cursor.execute('CREATE TABLE t1(id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))')
    cursor.execute("INSERT INTO t1(id, name) VALUES (1, 'aaa')")
    cursor.execute("INSERT INTO t1(id, name) VALUES (2, 'bbb')")
    cursor.execute("INSERT INTO t1(id, name) VALUES (3, 'ccc')")
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
    cursor.execute("INSERT INTO `auth_user` (id, `password`, `last_login`, "
                   "`is_superuser`, `username`, `first_name`, `last_name`, "
                   "`email`, `is_staff`, `is_active`, `date_joined`) "
                   "VALUES (1, '=', 'None', "
                   "'True', 'root1', 'a', 'b', "
                   "'a@a.com', 'True', 'True', '2017-06-06 20:33:38')")
    cursor.execute("INSERT INTO `auth_user` (id, `password`, `last_login`, "
                   "`is_superuser`, `username`, `first_name`, `last_name`, "
                   "`email`, `is_staff`, `is_active`, `date_joined`) "
                   "VALUES (2, '=', 'None', "
                   "'True', 'root2', 'a', 'b', "
                   "'a@a.com', 'True', 'True', '2017-06-06 20:33:38')")
    cursor.execute('SELECT id, username FROM auth_user')
    assert cursor.fetchone() == ('1', 'root1')
    assert cursor.fetchone() == ('2', 'root2')

    cursor.execute("SELECT id, username FROM auth_user WHERE username = 'root1'")
    assert cursor.fetchone() == ('1', 'root1')


def test_wait_after_increases_modified(cursor):
    cursor.execute('CREATE TABLE t1(id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))')
    cursor.execute("INSERT INTO t1(id, name) VALUES (1, 'aaa')")
    cursor.execute("SELECT id, name FROM t1 WHERE id = 1")
    assert cursor.n_rows == 1
    etcd_index = int(cursor.result_set.rows[0].etcd_index)
    assert etcd_index > 1
    wait_index = etcd_index + 1

    def _wait(que):

        query = "WAIT id, name FROM t1 WHERE id = 1 AFTER %d" \
                % wait_index

        cursor.execute(query)
        que.put(int(cursor.result_set.rows[0].modified_index))

    q = Queue()
    p = Process(target=_wait, args=(q, ))
    p.start()
    wait_proc = psutil.Process(p.pid)

    # Wait until the child establishes a TCP connection to etcd
    while True:
        tc_count = 0
        for tc in wait_proc.connections('tcp'):
            if tc.status == 'ESTABLISHED' and tc.raddr[1] == 2379:
                tc_count += 1
        if tc_count > 0:
            break

    time.sleep(1)

    cursor.execute("update t1 set name = 'bb'")
    p.join(timeout=10)
    modified_index = q.get()

    assert modified_index > wait_index


def test_select_sets_modified_index(cursor):
    cursor.execute('CREATE TABLE t1(id INT NOT NULL '
                   'PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))')
    cursor.execute("INSERT INTO t1(id, name) VALUES (1, 'aaa')")
    cursor.execute("SELECT id, name FROM t1 WHERE id = 1")
    assert int(cursor.result_set.rows[0].modified_index) > 0
