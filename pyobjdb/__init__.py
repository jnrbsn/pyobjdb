import pickle
import random
import time

import msgpack
import plyvel


def encode_key(key):
    if isinstance(key, bytes):
        return key
    return key.encode('utf-8')


def msgpack_encode(x):
    return msgpack.packb(x, use_bin_type=True)


def msgpack_decode(x):
    return msgpack.unpackb(x, raw=False)


class PyObjDB(object):

    _leveldb_options_defaults = {
        'create_if_missing': True,
        'lru_cache_size': 256 * 1024 * 1024,
        'bloom_filter_bits': 16,
    }

    def __init__(
            self, path, cleanup_interval=300, compaction_interval=86400,
            **leveldb_options):
        self._cleanup_interval = cleanup_interval
        self._compaction_interval = compaction_interval

        self._db = plyvel.DB(
            path, **dict(self._leveldb_options_defaults, **leveldb_options))
        self._init_db()

    def _init_db(self):
        for key, value in self._db.iterator():
            is_empty = False
            break
        else:
            is_empty = True

        now_encoded = msgpack_encode(time.time() + 0.001)
        if is_empty:
            with self._db.write_batch() as wb:
                wb.put(b'meta:last_cleanup', now_encoded)
                wb.put(b'meta:last_compaction', now_encoded)

    def close(self):
        self._db.close()

    def _expire_queue_key(self, key, expire_time):
        return b'meta:expire_queue:%020d:%s' % (int(expire_time * 1000), key)

    def put(self, key, value, ttl=None):
        key = encode_key(key)

        pickled = False
        if not isinstance(value, (bytes, str, int, float)):
            value = pickle.dumps(value)
            pickled = True

        data = {
            'pickled': pickled,
            'value': value,
        }
        write_batch = []

        if ttl:
            data['expire_time'] = time.time() + ttl
            write_batch.append((
                self._expire_queue_key(key, data['expire_time']),
                b'\x00',
            ))

        write_batch.append((
            b'data:%s' % key,
            msgpack.packb(data, use_bin_type=True),
        ))

        with self._db.write_batch() as wb:
            for k, v in write_batch:
                wb.put(k, v)

        self.cleanup()

    def _get_msgpack(self, key):
        data = self._db.get(key)
        if data is None:
            return None
        return msgpack_decode(data)

    def get(self, key):
        key = encode_key(key)

        data = self._get_msgpack(b'data:%s' % key)
        if data is None:
            return None

        if 'expire_time' in data and data['expire_time'] <= time.time():
            return None

        if data['pickled']:
            value = pickle.loads(data['value'])
        else:
            value = data['value']

        if random.randint(1, 100) <= 5:
            self.cleanup()

        return value

    def delete(self, key):
        key = encode_key(key)
        self._db.delete(b'data:%s' % key)
        self.cleanup()

    def cleanup(self, force=False, compact=False):
        now = time.time() + 0.001

        last_cleanup = self._get_msgpack(b'meta:last_cleanup')
        if (last_cleanup is not None and
                last_cleanup + self._cleanup_interval > now and
                not force):
            return 0

        start_key = self._expire_queue_key(b'', 0)
        stop_key = self._expire_queue_key(b'', now)
        key_prefix_len = len(start_key)

        count = 0
        for key, _ in self._db.iterator(start=start_key, stop=stop_key):
            self._db.delete(key)
            self._db.delete(b'data:%s' % key[key_prefix_len:])
            count += 1

        now_encoded = msgpack_encode(now)
        self._db.put(b'meta:last_cleanup', now_encoded)

        last_compaction = self._get_msgpack(b'meta:last_compaction')
        if (last_compaction is not None and
                last_compaction + self._compaction_interval > now and
                not compact):
            return count

        self._db.put(b'meta:last_compaction', now_encoded)
        self._db.compact_range()

        return count
