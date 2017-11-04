"""Locking for etcdb

etcdb implements MyISAM style locking.
There is an exclusive writer lock and there are many shared reader locks.
A client can acquire a writer lock if there are no readers and no writers.
A client can acquire a reader lock if there are no writers.


"""
import time
import uuid

from pyetcd import EtcdNodeExist, EtcdKeyNotFound

from etcdb import LOCK_WAIT_TIMEOUT, OperationalError, InternalError, \
    META_LOCK_WAIT_TIMEOUT


class Lock(object):
    """
    Instantiate Lock instance for a table.

    :param etcd_client: Etcd client
    :type etcd_client: Client
    :param db: Database name.
    :param tbl: Table name.
    """
    def __init__(self, etcd_client, db, tbl, lock_prefix=None, lock_id=None):  # pylint: disable=too-many-arguments
        self._etcd_client = etcd_client
        self._db = db
        self._tbl = tbl
        self._lock_prefix = lock_prefix
        self._id = lock_id

    def acquire(self, timeout=LOCK_WAIT_TIMEOUT, ttl=LOCK_WAIT_TIMEOUT):
        """Get a lock

        :param timeout: Timeout to acquire a lock.
        :type timeout: int
        :param ttl: Place a lock on this time in seconds
        :raise InternalError: This class shouldn't be used directly and
            if user doesn't set lock_prefix the method should
            raise exception."""
        if not self._lock_prefix:
            raise InternalError('_lock_prefix must be set')

        key = "/{db}/{tbl}/{lock_prefix}".format(
            db=self._db,
            tbl=self._tbl,
            lock_prefix=self._lock_prefix
        )

        if self._id:
            key += "/%s" % self._id

        expires = time.time() + timeout
        while time.time() < expires:
            try:
                self._etcd_client.compare_and_swap(
                    key,
                    '',
                    ttl=ttl,
                    prev_exist=False)
                return self._id
            except EtcdNodeExist:
                time.sleep(timeout/2.0)

        raise OperationalError('Lock wait timeout')

    def release(self):
        """Release a lock"""
        key = "/{db}/{tbl}/{lock_prefix}".format(
            db=self._db,
            tbl=self._tbl,
            lock_prefix=self._lock_prefix
        )

        if self._id:
            key += "/%s" % self._id
        try:
            self._etcd_client.delete(key)
        except EtcdKeyNotFound as err:
            raise InternalError('Failed to release a lock: %s' % err)

    def readers(self):
        """Get list of reader locks.

        :return: List of ReadLock() instances
        :rtype: list(ReadLock)
        """
        return self._get_locks_by_type('_lock_read')

    def writers(self):
        """Get list of writer locks.

        :return: List of WriteLock() instances
        :rtype: list(WriteLock)
        """
        return self._get_locks_by_type('_lock_write')

    def _get_locks_by_type(self, prefix):
        if prefix == '_lock_read':
            lock_class = ReadLock
        elif prefix == '_lock_write':
            lock_class = WriteLock
        else:
            raise InternalError('Trying to list unsupported lock type %s'
                                % prefix)
        locks = []
        key = "/{db}/{tbl}/{lock_prefix}".format(
            db=self._db,
            tbl=self._tbl,
            lock_prefix=prefix
        )
        try:
            result = self._etcd_client.read(key)
            for node in result.node['nodes']:
                locks.append(lock_class(self._etcd_client,
                                        self._db, self._tbl,
                                        lock_id=node['key']))
        except (KeyError, EtcdKeyNotFound):
            pass
        return locks


class MetaLock(Lock):
    """Meta lock is needed to place a read or write lock."""
    def __init__(self, etcd_client, db, tbl):
        """
        Instantiate MetaLock instance for a table.

        :param db: Database name.
        :param tbl: Table name.
        """
        super(MetaLock, self).__init__(etcd_client, db, tbl,
                                       lock_prefix='_lock_meta')


class WriteLock(Lock):
    """Write lock."""
    def __init__(self, etcd_client, db, tbl, lock_id=None):

        if not lock_id:
            lock_id = uuid.uuid4()

        super(WriteLock, self).__init__(etcd_client, db, tbl,
                                        lock_prefix='_lock_write',
                                        lock_id=lock_id)

    def acquire(self, timeout=LOCK_WAIT_TIMEOUT, ttl=LOCK_WAIT_TIMEOUT):
        """Get a write lock"""
        meta_lock = MetaLock(self._etcd_client, self._db, self._tbl)
        meta_lock.acquire(timeout=META_LOCK_WAIT_TIMEOUT,
                          ttl=META_LOCK_WAIT_TIMEOUT)

        try:
            expires = time.time() + timeout
            sleep_time = 1
            while time.time() < expires:
                if not self.writers() and not self.readers():
                    super(WriteLock, self).acquire(timeout=timeout, ttl=ttl)
                    return self._id
                time.sleep(sleep_time)
                sleep_time *= 2

            raise OperationalError('Lock wait timeout')
        finally:
            meta_lock.release()


class ReadLock(Lock):
    """Read lock."""
    def __init__(self, etcd_client, db, tbl, lock_id=None):

        if not lock_id:
            lock_id = uuid.uuid4()

        super(ReadLock, self).__init__(etcd_client, db, tbl,
                                       lock_prefix='_lock_read',
                                       lock_id=lock_id)

    def acquire(self, timeout=LOCK_WAIT_TIMEOUT, ttl=LOCK_WAIT_TIMEOUT):
        """Get a read lock"""
        meta_lock = MetaLock(self._etcd_client, self._db, self._tbl)
        meta_lock.acquire(timeout=META_LOCK_WAIT_TIMEOUT,
                          ttl=META_LOCK_WAIT_TIMEOUT)

        try:
            expires = time.time() + timeout
            sleep_time = 1
            while time.time() < expires:
                if not self.writers():
                    super(ReadLock, self).acquire(timeout=timeout,
                                                  ttl=ttl)
                    return self._id
                time.sleep(sleep_time)
                sleep_time *= 2

            raise OperationalError('Lock wait timeout')
        finally:
            meta_lock.release()
