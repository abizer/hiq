import random
from datetime import datetime
from typing import Dict, List
import tomllib

from hiq.lib.item import Item, Metadata, Payload

# topics are prefixed with the namespace they belong to
# for ease of debugging
TOPICS = {
    "null:null": 0,
    "dev:test": 1,
    "dev:test2": 2,
    "dev:test3": 3,
}

# similarly with shards
SHARDS = {
    "null:null": {
        "null:null:0": 0,
    },
    "dev:test": {
        "dev:test:0": 1,
        "dev:test:1": 2,
        "dev:test:2": 3,
    },
    "dev:test2": {
        "dev:test2:0": 4,
    },
    "dev:test3": {
        "dev:test3:0": 5,
    },
}

# connection strings for how partitions should
# set up storage for this shard
SHARD_CXN = {
    0: "sys://dev/null",
    1: "mem://dev/test",
}

PARTITIONS = {
    # ns, topic, shard
}

REGISTRATION_CONFIG_NAME: str = "registration.config.toml"
ALLOCATOR_CONFIG_NAME: str = "allocator.config.toml"
CLIENT_CONFIG_NAME: str = "client.config.toml"


def read_config(
    path: str = REGISTRATION_CONFIG_NAME,
) -> Dict[str, Dict[str, int]]:
    with open(path, "rb") as config_file:
        config = tomllib.load(config_file)
        return config


def get_topic_id_for_topic(topic: str) -> int:
    return TOPICS.get(topic, TOPICS["null:null"])


def get_shards_for_topic(topic: str) -> int:
    return SHARDS.get(topic, SHARDS["null:null"])


def select_shard_for_enqueue(shards: List[Dict[str, int]]) -> int:
    choice = random.choice(shards.keys())
    return shards[choice]


def select_shard_for_dequeue(shards: List[Dict[str, int]]) -> int:
    choice = random.choice(shards.keys())
    return shards[choice]


def pack_partition_id_old(ns: int, topic: int, shard: int) -> int:
    # a partition id is 64 bits, or 8 bytes, formatted as
    # an IPv6 ULA. ULA format is the first 48 bits are
    # the prefix 0xFD + a 40 bit identifier, followed
    # by a 16 bit subnet id, and then 64 bits of addresses
    # in the subnet. In our case, the 64 bits are spent on
    # instance ids, while the partition id acts as an actual ULA

    # last 16 bits are the shard_id
    packed = 0xFFFF & shard
    # middle 32 bits are the topic_id
    packed |= (0xFFFFFFF & topic) << 16
    # first 16 bits are the namespace_id,
    packed |= (0xFFFF & ns) << 48
    # except the first 8 bits, which we overwrite
    # to be the fixed prefix 0xFD for IPv6 ULAs
    packed |= 0xFD << 56


def unpack_partition_id_old(partition_id: int) -> (int, int, int):
    shard = 0xFFFF & partition_id
    topic = 0xFFFFFFF & (partition_id >> 16)
    ns = 0xFFFF & (partition_id >> 48)
    ns |= ns << 8  # strip off the 0xFD prefix
    return (ns, topic, shard)


def pack_instance_id_old(prefix: int, ts: int, suffix: int) -> int:
    # in instance id is 64 bits.
    # the suffix is a monotonically-increasing, rolling
    # 16 bit counter designed to provide item uniqueness
    # on a shard.
    packed = 0xFFFF & suffix
    # the middle 32 bits are a standard unix timestamp
    packed |= ts << 16
    # the first 16 bits are the last 16 bits of the partition id,
    # with the first 4 bits replaced with a priority
    packed |= (0xFFFF & prefix) << 48
    return packed


from itertools import count, cycle

instance_id_counter = (random.randrange(2**16) for _ in count())


def gen_id(partition_id: int, priority=9) -> int:
    return pack_instance_id(
        prefix=(0xFFFF & (partition_id << 4) | (0xF & priority)),
        ts=int(datetime.now().timestamp()),
        suffix=next(instance_id_counter),
    )


def pack_partition_id_new(topic_id: int, shard_id: int) -> int:
    """Pack a 64 bit partition id in the form of an IPv6 ULA.

    The first 8 bits are the fixed prefix 0xFD, the next 32 bits
    are the topic id, which is hashed from the topic str, and the
    remaining 24 bits are the shard_id, which grow from 0.
    """

    packed = 0xFD << 56
    packed |= (0xFFFFFFFF & topic_id) << 24
    packed |= 0xFFFFFF & shard_id
    return packed


def unpack_partition_id_new(partition_id: int) -> (int, int):
    """Unpack a 64 bit partition id into a topic_id and shard_id."""

    topic_id = 0xFFFFFFFF & (partition_id >> 24)
    shard_id = 0xFFFFFF & partition_id
    return (topic_id, shard_id)


def pack_instance_id_new(priority, partition_id, ts, counter):
    """Pack a 64 bit instance id.

    The first 4 bits are the priority, the next 4 bits are the
    last 4 bits of the partition_id, i.e. the lower 4 bits of
    the shard id, the next 32 bits are the timestamp, and the
    last 24 bits are a random 24 bit number, allowing 16,577,216
    unique ids per shard per second.
    """

    packed = 0xF & priority
    packed |= (0xFFFFFF & partition_id) << 4
    packed |= (0xFFFFFFFF & ts) << 28
    packed |= 0xF & counter
    return packed


def unpack_instance_id_new(instance_id: int) -> (int, int, int, int):
    """Unpack a 64 bit instance id into a priority, partition_id, ts, and counter."""

    counter = 0xFFFFFF & instance_id
    ts = 0xFFFFFFFF & (instance_id >> 24)
    shard_id_lsb = 0xF & (instance_id >> 56)
    priority = 0xF & (instance_id >> 60)
    return (priority, shard_id_lsb, ts, counter)


def make_payload(data: bytes, context: str) -> Payload:
    return Payload(data=data, context=context)


def make_metadata(
    topic_id: int,
    created_at: int,
    modified_at: int,
    expires_at: int,
    delivered_at: int,
    lives_left: int,
    lease_sec: int,
) -> Metadata:
    return Metadata(
        topic=topic_id,
        created_at=created_at,
        modified_at=modified_at,
        expires_at=expires_at,
        delivered_at=delivered_at,
        lives_left=lives_left,
        lease_sec=lease_sec,
    )


def make_item(
    topic: str,
    expires_at: int,
    lives_left: int,
    lease_sec: int,
    payload_data: bytes,
    payload_context: str,
) -> Item:
    topic_id = get_topic_id_for_topic(topic)

    payload = make_payload(data=payload_data, context=payload_context)

    created_at = int(datetime.now().timestamp())
    modified_at = created_at
    delivered_at = -1
    metadata = make_metadata(
        topic=topic_id,
        created_at=created_at,
        modified_at=modified_at,
        expires_at=expires_at,
        delivered_at=delivered_at,
        lives_left=lives_left,
        lease_sec=lease_sec,
    )

    return Item(id=-1, payload=payload, metadata=metadata)
