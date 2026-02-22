import base64
import hashlib
import json
import re

import requests
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import LoggingConfig, PandaproxyConfig, SISettings
from rptest.tests.redpanda_test import RedpandaTest

# Transient RPC timeouts can occur during kvstore DB startup.
KV_LOG_ALLOW_LIST = [
    re.compile(r"unknown status code: cluster::errc::timeout"),
]

log_config = LoggingConfig(
    "info",
    logger_levels={
        "pandaproxy": "trace",
        "kafka/client": "trace",
        "kvstore": "trace",
        "kvdb": "trace",
    },
)

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}


class PandaproxyKVStoreTest(RedpandaTest):
    KV_TOPIC = "kv-test-topic"

    def __init__(self, context, **kwargs):
        si_settings = SISettings(context)
        super().__init__(
            context,
            num_brokers=3,
            log_config=log_config,
            si_settings=si_settings,
            pandaproxy_config=PandaproxyConfig(),
            extra_rp_conf={"enable_kvstore": True},
            **kwargs,
        )

    def setUp(self):
        super().setUp()
        self._create_kv_topic(self.KV_TOPIC)

    def _create_kv_topic(self, name, partitions=1):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(name,
                         partitions=partitions,
                         config={"redpanda.kvstore": "cloud"})

    def _base_uri(self, node=None):
        node = node or self.redpanda.nodes[0]
        return f"http://{node.account.hostname}:8082"

    @staticmethod
    def _b64(s):
        if isinstance(s, str):
            s = s.encode()
        return base64.b64encode(s).decode()

    @staticmethod
    def _b64d(s):
        return base64.b64decode(s)

    @staticmethod
    def _sha256_hex(data):
        if isinstance(data, str):
            data = data.encode()
        return hashlib.sha256(data).hexdigest()

    def _kv_write(self, topic, partition, puts=None, deletes=None):
        body = {}
        if puts is not None:
            body["puts"] = puts
        if deletes is not None:
            body["deletes"] = deletes
        return requests.post(
            f"{self._base_uri()}/kvstore/{topic}/partition/{partition}/write",
            data=json.dumps(body),
            headers=HEADERS,
        )

    def _kv_batch_get(self, topic, partition, keys):
        body = {"keys": keys}
        return requests.post(
            f"{self._base_uri()}/kvstore/{topic}/partition/{partition}/batch_get",
            data=json.dumps(body),
            headers=HEADERS,
        )

    def _kv_scan(self,
                 topic,
                 partition,
                 start_key=None,
                 end_key=None,
                 limit=None):
        body = {}
        if start_key is not None:
            body["startKey"] = start_key
        if end_key is not None:
            body["endKey"] = end_key
        if limit is not None:
            body["limit"] = limit
        return requests.post(
            f"{self._base_uri()}/kvstore/{topic}/partition/{partition}/scan",
            data=json.dumps(body),
            headers=HEADERS,
        )

    def _put(self, key, value, precondition=None):
        """Build a single put entry for a write request."""
        entry = {"key": self._b64(key), "value": self._b64(value)}
        if precondition is not None:
            entry["precondition"] = precondition
        return entry

    def _delete(self, key, precondition=None):
        """Build a single delete entry for a write request."""
        entry = {"key": self._b64(key)}
        if precondition is not None:
            entry["precondition"] = precondition
        return entry

    def _write_with_retry(self, topic, partition, puts=None, deletes=None):
        """Write with retry to handle initial leader election and RPC timeouts."""

        def attempt():
            try:
                res = self._kv_write(topic,
                                     partition,
                                     puts=puts,
                                     deletes=deletes)
            except requests.ConnectionError:
                return False
            if res.status_code in (500, 503):
                return False
            assert res.status_code == 200, \
                f"Expected 200, got {res.status_code}: {res.text}"
            return True

        wait_until(attempt,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="Timed out waiting for kvstore write to succeed")

    def _get_with_retry(self, topic, partition, keys):
        """Batch get with retry to handle initial leader election and RPC timeouts."""
        result = [None]

        def attempt():
            try:
                res = self._kv_batch_get(topic, partition, keys)
            except requests.ConnectionError:
                return False
            if res.status_code in (500, 503):
                return False
            assert res.status_code == 200, \
                f"Expected 200, got {res.status_code}: {res.text}"
            result[0] = res
            return True

        wait_until(attempt,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="Timed out waiting for kvstore get to succeed")
        return result[0]

    def _wait_for_key(self, topic, partition, key, expected_value):
        """Poll batch_get until key has the expected value."""
        b64key = self._b64(key)

        def check():
            try:
                res = self._kv_batch_get(topic, partition, [b64key])
            except requests.ConnectionError:
                return False
            if res.status_code == 503:
                return False
            assert res.status_code == 200, \
                f"Expected 200, got {res.status_code}: {res.text}"
            r = res.json()["results"][0]
            if "value" not in r:
                return False
            return self._b64d(r["value"]) == expected_value

        wait_until(check,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg=f"Key {key!r} did not reach expected value")

    @cluster(num_nodes=3, log_allow_list=KV_LOG_ALLOW_LIST)
    def test_basic_put_and_get(self):
        """Write a single key-value pair and read it back."""
        self._write_with_retry(self.KV_TOPIC,
                               0,
                               puts=[self._put("key1", "value1")])

        self._wait_for_key(self.KV_TOPIC, 0, "key1", b"value1")

        res = self._kv_batch_get(self.KV_TOPIC, 0,
                                 [self._b64("key1"), self._b64("missing")])
        assert res.status_code == 200, \
            f"Expected 200, got {res.status_code}: {res.text}"
        body = res.json()
        results = body["results"]
        assert len(results) == 2

        # key1 should have a value
        r0 = results[0]
        assert self._b64d(r0["key"]) == b"key1"
        assert self._b64d(r0["value"]) == b"value1"

        # missing key should have no value field
        r1 = results[1]
        assert self._b64d(r1["key"]) == b"missing"
        assert "value" not in r1

    @cluster(num_nodes=3, log_allow_list=KV_LOG_ALLOW_LIST)
    def test_put_overwrite_and_delete(self):
        """Overwrite a key and then delete it."""
        self._write_with_retry(self.KV_TOPIC,
                               0,
                               puts=[self._put("k", "v1")])

        # Overwrite
        res = self._kv_write(self.KV_TOPIC, 0, puts=[self._put("k", "v2")])
        assert res.status_code == 200

        res = self._kv_batch_get(self.KV_TOPIC, 0, [self._b64("k")])
        assert res.status_code == 200
        assert self._b64d(res.json()["results"][0]["value"]) == b"v2"

        # Delete
        res = self._kv_write(self.KV_TOPIC, 0, deletes=[self._delete("k")])
        assert res.status_code == 200

        res = self._kv_batch_get(self.KV_TOPIC, 0, [self._b64("k")])
        assert res.status_code == 200
        assert "value" not in res.json()["results"][0]

        # Idempotent delete
        res = self._kv_write(self.KV_TOPIC, 0, deletes=[self._delete("k")])
        assert res.status_code == 200

    @cluster(num_nodes=3, log_allow_list=KV_LOG_ALLOW_LIST)
    def test_atomic_batch_write(self):
        """Verify puts and deletes in one batch are applied atomically."""
        self._write_with_retry(
            self.KV_TOPIC,
            0,
            puts=[self._put("a", "1"), self._put("b", "2")])

        # Batch: put c, delete a
        res = self._kv_write(self.KV_TOPIC,
                             0,
                             puts=[self._put("c", "3")],
                             deletes=[self._delete("a")])
        assert res.status_code == 200

        res = self._kv_batch_get(
            self.KV_TOPIC, 0,
            [self._b64("a"), self._b64("b"), self._b64("c")])
        assert res.status_code == 200
        results = res.json()["results"]

        assert "value" not in results[0], "a should be deleted"
        assert self._b64d(results[1]["value"]) == b"2"
        assert self._b64d(results[2]["value"]) == b"3"

    @cluster(num_nodes=3, log_allow_list=KV_LOG_ALLOW_LIST)
    def test_precondition_if_exists(self):
        """Test ifExists preconditions for puts and deletes."""
        self._write_with_retry(self.KV_TOPIC,
                               0,
                               puts=[self._put("exist", "val")])

        # Put with ifExists: true on existing key -> 200
        res = self._kv_write(
            self.KV_TOPIC,
            0,
            puts=[
                self._put("exist", "val2",
                           precondition={"ifExists": {"exists": True}})
            ])
        assert res.status_code == 200

        # Put with ifExists: true on missing key -> 409
        res = self._kv_write(
            self.KV_TOPIC,
            0,
            puts=[
                self._put("nope", "val",
                           precondition={"ifExists": {"exists": True}})
            ])
        assert res.status_code == 409

        # Put with ifExists: false on missing key -> 200 (create-if-not-exists)
        res = self._kv_write(
            self.KV_TOPIC,
            0,
            puts=[
                self._put("new", "val",
                           precondition={"ifExists": {"exists": False}})
            ])
        assert res.status_code == 200

        # Put with ifExists: false on existing key -> 409
        res = self._kv_write(
            self.KV_TOPIC,
            0,
            puts=[
                self._put("exist", "val3",
                           precondition={"ifExists": {"exists": False}})
            ])
        assert res.status_code == 409

        # Delete with ifExists: true on missing key -> 409
        res = self._kv_write(
            self.KV_TOPIC,
            0,
            deletes=[
                self._delete("gone",
                              precondition={"ifExists": {"exists": True}})
            ])
        assert res.status_code == 409

        # Delete with ifExists: true on existing key -> 200
        res = self._kv_write(
            self.KV_TOPIC,
            0,
            deletes=[
                self._delete("exist",
                              precondition={"ifExists": {"exists": True}})
            ])
        assert res.status_code == 200

    @cluster(num_nodes=3, log_allow_list=KV_LOG_ALLOW_LIST)
    def test_precondition_if_matches(self):
        """Test ifMatches preconditions using SHA-256 hash."""
        val1 = "hello"
        self._write_with_retry(self.KV_TOPIC,
                               0,
                               puts=[self._put("hk", val1)])

        hash1 = self._sha256_hex(val1)

        # Update with matching hash -> 200
        val2 = "world"
        res = self._kv_write(
            self.KV_TOPIC,
            0,
            puts=[
                self._put(
                    "hk", val2,
                    precondition={"ifMatches": {"sha256Hash": hash1}})
            ])
        assert res.status_code == 200

        # Update with stale hash -> 409
        res = self._kv_write(
            self.KV_TOPIC,
            0,
            puts=[
                self._put(
                    "hk", "nope",
                    precondition={"ifMatches": {"sha256Hash": hash1}})
            ])
        assert res.status_code == 409

        # Update with correct new hash -> 200
        hash2 = self._sha256_hex(val2)
        res = self._kv_write(
            self.KV_TOPIC,
            0,
            puts=[
                self._put(
                    "hk", "final",
                    precondition={"ifMatches": {"sha256Hash": hash2}})
            ])
        assert res.status_code == 200

    @cluster(num_nodes=3, log_allow_list=KV_LOG_ALLOW_LIST)
    def test_scan_basic(self):
        """Scan with various start/end key bounds."""
        keys = ["a", "b", "c", "d", "e"]
        puts = [self._put(k, f"val_{k}") for k in keys]
        self._write_with_retry(self.KV_TOPIC, 0, puts=puts)

        # Full scan
        res = self._kv_scan(self.KV_TOPIC, 0)
        assert res.status_code == 200
        entries = res.json()["entries"]
        assert len(entries) == 5
        scanned_keys = [self._b64d(e["key"]).decode() for e in entries]
        assert scanned_keys == keys

        # Scan with startKey="c" -> [c, d, e]
        res = self._kv_scan(self.KV_TOPIC, 0, start_key=self._b64("c"))
        assert res.status_code == 200
        entries = res.json()["entries"]
        scanned_keys = [self._b64d(e["key"]).decode() for e in entries]
        assert scanned_keys == ["c", "d", "e"]

        # Scan with endKey="c" (exclusive) -> [a, b]
        res = self._kv_scan(self.KV_TOPIC, 0, end_key=self._b64("c"))
        assert res.status_code == 200
        entries = res.json()["entries"]
        scanned_keys = [self._b64d(e["key"]).decode() for e in entries]
        assert scanned_keys == ["a", "b"]

        # Scan with startKey="b", endKey="d" -> [b, c]
        res = self._kv_scan(self.KV_TOPIC,
                            0,
                            start_key=self._b64("b"),
                            end_key=self._b64("d"))
        assert res.status_code == 200
        entries = res.json()["entries"]
        scanned_keys = [self._b64d(e["key"]).decode() for e in entries]
        assert scanned_keys == ["b", "c"]

    @cluster(num_nodes=3, log_allow_list=KV_LOG_ALLOW_LIST)
    def test_scan_limit(self):
        """Scan with limit parameter."""
        puts = [self._put(f"k{i:02d}", f"v{i}") for i in range(10)]
        self._write_with_retry(self.KV_TOPIC, 0, puts=puts)

        # limit=3 -> 3 entries
        res = self._kv_scan(self.KV_TOPIC, 0, limit=3)
        assert res.status_code == 200
        assert len(res.json()["entries"]) == 3

        # limit=0 -> defaults to 500, returns all 10
        res = self._kv_scan(self.KV_TOPIC, 0, limit=0)
        assert res.status_code == 200
        assert len(res.json()["entries"]) == 10

        # No limit -> defaults to 500, returns all 10
        res = self._kv_scan(self.KV_TOPIC, 0)
        assert res.status_code == 200
        assert len(res.json()["entries"]) == 10

        # limit=1001 -> clamped to 1000, returns all 10
        res = self._kv_scan(self.KV_TOPIC, 0, limit=1001)
        assert res.status_code == 200
        assert len(res.json()["entries"]) == 10

    @cluster(num_nodes=3, log_allow_list=KV_LOG_ALLOW_LIST)
    def test_batch_get_multiple_keys(self):
        """Batch get returns found and missing keys correctly."""
        self._write_with_retry(
            self.KV_TOPIC,
            0,
            puts=[self._put("x", "1"), self._put("y", "2")])

        res = self._kv_batch_get(
            self.KV_TOPIC, 0,
            [self._b64("x"), self._b64("y"), self._b64("z")])
        assert res.status_code == 200
        results = res.json()["results"]
        assert len(results) == 3

        assert self._b64d(results[0]["value"]) == b"1"
        assert self._b64d(results[1]["value"]) == b"2"
        assert "value" not in results[2]

    @cluster(num_nodes=3, log_allow_list=KV_LOG_ALLOW_LIST)
    def test_error_topic_without_kvstore(self):
        """Operations on a topic without kvstore config return 400."""
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("normal-topic")

        res = self._kv_write("normal-topic",
                             0,
                             puts=[self._put("k", "v")])
        assert res.status_code == 400

        res = self._kv_batch_get("normal-topic", 0, [self._b64("k")])
        assert res.status_code == 400

        res = self._kv_scan("normal-topic", 0)
        assert res.status_code == 400

    @cluster(num_nodes=3, log_allow_list=KV_LOG_ALLOW_LIST)
    def test_error_nonexistent_topic(self):
        """Operations on a nonexistent topic return 503 (no leader found)."""
        res = self._kv_write("no-such-topic",
                             0,
                             puts=[self._put("k", "v")])
        assert res.status_code == 503

        res = self._kv_batch_get("no-such-topic", 0, [self._b64("k")])
        assert res.status_code == 503

        res = self._kv_scan("no-such-topic", 0)
        assert res.status_code == 503

    @cluster(num_nodes=3, log_allow_list=KV_LOG_ALLOW_LIST)
    def test_error_key_too_large(self):
        """Keys larger than 16KiB are rejected with 400."""
        max_key_size = 16 * 1024

        # Key > 16KiB -> 400
        big_key = "x" * (max_key_size + 1)
        res = self._kv_write(self.KV_TOPIC,
                             0,
                             puts=[self._put(big_key, "v")])
        assert res.status_code == 400, \
            f"Expected 400 for oversized key, got {res.status_code}"

        # Key exactly 16KiB -> 200
        exact_key = "x" * max_key_size
        self._write_with_retry(self.KV_TOPIC,
                               0,
                               puts=[self._put(exact_key, "v")])

    @cluster(num_nodes=3, log_allow_list=KV_LOG_ALLOW_LIST)
    def test_precondition_atomic_failure(self):
        """A failed precondition in a batch rolls back the entire batch."""
        self._write_with_retry(self.KV_TOPIC,
                               0,
                               puts=[self._put("key_a", "original")])

        # Batch: put key_b (no precondition) + put key_a with ifExists:false
        # (should fail because key_a exists)
        res = self._kv_write(
            self.KV_TOPIC,
            0,
            puts=[
                self._put("key_b", "new_val"),
                self._put("key_a", "replaced",
                           precondition={"ifExists": {"exists": False}}),
            ])
        assert res.status_code == 409

        # key_a should be unchanged
        res = self._kv_batch_get(self.KV_TOPIC, 0, [self._b64("key_a")])
        assert res.status_code == 200
        assert self._b64d(res.json()["results"][0]["value"]) == b"original"

        # key_b should NOT have been written (atomic rollback)
        res = self._kv_batch_get(self.KV_TOPIC, 0, [self._b64("key_b")])
        assert res.status_code == 200
        assert "value" not in res.json()["results"][0], \
            "key_b should not exist after atomic batch failure"
