from datetime import datetime, timedelta

from freezegun import freeze_time

from pyobjdb import PyObjDB


def test_basic(tmp_path):
    db = PyObjDB(str(tmp_path / 'test.db'))

    db.put('key_str', 'foo')
    assert db.get('key_str') == 'foo'
    assert db.get(b'key_str') == 'foo'
    db.put('key_str', 'bar')
    assert db.get('key_str') == 'bar'

    db.put('key_int', 42)
    assert db.get('key_int') == 42

    db.put('key_float', 4.125)
    assert db.get('key_float') == 4.125

    db.put('key_list', ['foo', 42, 4.125])
    assert db.get('key_list') == ['foo', 42, 4.125]

    db.put('key_tuple', ('foo', 42, 4.125))

    db.put('key_dict', {'foo': 42, 'bar': 4.125})
    assert db.get('key_dict') == {'foo': 42, 'bar': 4.125}

    db.close()


def test_reopen(tmp_path):
    db1 = PyObjDB(str(tmp_path / 'test.db'))

    db1.put('foo', 'bar')
    assert db1.get('foo') == 'bar'

    db1.close()

    db2 = PyObjDB(str(tmp_path / 'test.db'))

    assert db2.get('foo') == 'bar'

    db2.close()


def test_ttl(tmp_path):
    db = PyObjDB(str(tmp_path / 'test.db'))

    with freeze_time(datetime.utcnow()) as frozen_time:
        db.put('foo', 'bar', ttl=5)
        assert db.get('foo') == 'bar'

        frozen_time.tick(timedelta(seconds=3))
        assert db.get('foo') == 'bar'

        frozen_time.tick(timedelta(seconds=5))
        assert db.get('foo') is None


def test_delete(tmp_path):
    db = PyObjDB(str(tmp_path / 'test.db'))

    db.put('foo', 'bar')
    assert db.get('foo') == 'bar'

    db.delete('foo')
    assert db.get('foo') is None


class Greeter(object):
    def __init__(self, name):
        self.name = name

    def get_greeting(self):
        return f'Hello, {self.name}!'


def test_custom_object(tmp_path):
    db = PyObjDB(str(tmp_path / 'test.db'))

    obj1 = Greeter('Kermit')
    db.put('hello_kermit', obj1)

    obj2 = db.get('hello_kermit')
    assert isinstance(obj2, Greeter)
    assert obj2.name == 'Kermit'


def test_cleanup(tmp_path):
    db = PyObjDB(
        str(tmp_path / 'test.db'),
        cleanup_interval=60,
        compaction_interval=3600,
    )

    with freeze_time(datetime.utcnow()) as frozen_time:
        db.put('foo', 'bar', ttl=5)
        db.put('baz', 'qux', ttl=7)
        db.put('wibble', 'wobble', ttl=3600)

        assert db.get('foo') == 'bar'

        frozen_time.tick(timedelta(seconds=3))
        assert db.get('foo') == 'bar'

        frozen_time.tick(timedelta(seconds=5))
        assert db.get('foo') is None

        assert db.cleanup() == 0
        assert db.get('wibble') == 'wobble'

        frozen_time.tick(timedelta(seconds=120))
        assert db.cleanup() == 2
        assert db.get('wibble') == 'wobble'

        frozen_time.tick(timedelta(seconds=7200))
        db.cleanup()
