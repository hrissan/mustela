import hashlib
import subprocess
import tempfile
import os.path
import binascii
import hypothesis.strategies as st
from hypothesis import note, settings
from hypothesis.stateful import RuleBasedStateMachine, Bundle, rule, precondition, invariant
from sortedcontainers import SortedDict


MUSTELA_BINARY = './bin/mustela'
MUSTELA_DB = 'db.mustela'


def gen_bucket():
    return st.binary(max_size=41)


def gen_key():
    return st.binary(max_size=42)


def clone_db(db):
    return SortedDict((b, SortedDict((k, v) for k, v in kvs.items())) for b, kvs in db.items())


def encode_nulls(tag, b):
    r = bytearray(tag)
    for c in b:
        r.append(c)
        if c == 0:
            r.append(0xff)
    r.append(0)
    return r


def db_hash(db):
    h = hashlib.blake2b(digest_size=32)
    for b, kvs in db.items():
        h.update(encode_nulls(b'b', b))
        for k, v in kvs.items():
            h.update(encode_nulls(b'k', k))
            h.update(encode_nulls(b'v', v))
    return h.digest()


class MustelaTestMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.dir = tempfile.TemporaryDirectory()
        self.mustela = subprocess.Popen([MUSTELA_BINARY, '--test', os.path.join(self.dir.name, MUSTELA_DB)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=0, encoding='utf-8')
        self.committed = SortedDict()
        self.db = SortedDict()

    def teardown(self):
        self.mustela.stdin.close()
        self.mustela.wait()
        self.dir.cleanup()

    def roundtrip(self, cmd: str, *args):
        input_ = cmd + ',' + ','.join(binascii.hexlify(arg).decode('ascii') for arg in args)
        self.mustela.stdin.write(input_ + '\n')
        h = self.mustela.stdout.readline().strip()
        assert binascii.hexlify(db_hash(self.db)) == h.encode('ascii')

    @rule(bucket=gen_bucket())
    def create_bucket(self, bucket):
        if bucket in self.db:
            return
        self.db[bucket] = SortedDict()
        self.roundtrip('create-bucket', bucket)

    @precondition(lambda self: self.db)
    @rule(data=st.data())
    def drop_bucket(self, data):
        bucket = data.draw(st.sampled_from(list(self.db)), 'bucket')
        del self.db[bucket]
        self.roundtrip('drop-bucket', bucket)

    @rule(reset=st.booleans())
    def commit(self, reset):
        self.committed = clone_db(self.db)
        self.roundtrip('commit-reset' if reset else 'commit')

    @rule(reset=st.booleans())
    def rollback(self, reset):
        self.db = clone_db(self.committed)
        self.roundtrip('rollback-reset' if reset else 'rollback')

    @precondition(lambda self: self.db)
    @rule(data=st.data(), k=gen_key(), v=st.binary())
    def put(self, data, k, v):
        bucket = data.draw(st.sampled_from(list(self.db)), 'bucket')
        self.db[bucket][k] = v
        self.roundtrip('put', bucket, k, v)

    @precondition(lambda self: any(self.db.values()))
    @rule(data=st.data(), v=st.binary())
    def change(self, data, v):
        bucket = data.draw(st.sampled_from(list(b for b, kvs in self.db.items() if kvs)), 'bucket')
        k = data.draw(st.sampled_from(list(self.db[bucket])), 'key')
        self.db[bucket][k] = v
        self.roundtrip('put', bucket, k, v)

    @precondition(lambda self: any(self.db.values()))
    @rule(data=st.data())
    def del_(self, data):
        bucket = data.draw(st.sampled_from(list(b for b, kvs in self.db.items() if kvs)), 'bucket')
        k = data.draw(st.sampled_from(list(self.db[bucket])), 'key')
        del self.db[bucket][k]
        self.roundtrip('del', bucket, k)


with settings(max_examples=100, stateful_step_count=100):
    TestMustela = MustelaTestMachine.TestCase
