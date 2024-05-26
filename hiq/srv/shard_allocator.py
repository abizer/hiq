"""
A service that allocates shards to nodes and responds to client requests
to route connections to nodes that host partitions for a given topic
"""

import sys
from typing import Any, Dict

from fastapi import FastAPI

from hiq.lib.util import read_config, REGISTRATION_CONFIG_NAME, ALLOCATOR_CONFIG_NAME

app = FastAPI()

# allocator_config = read_config(ALLOCATOR_CONFIG_NAME)
registered_shards = read_config(REGISTRATION_CONFIG_NAME)


@app.get("/")
async def dump_config():
    return registered_shards


@app.get("{ns}/{topic}")
async def get_shards(ns, topic):
    shards = registered_shards[ns][topic]
    return {v: k for k, v in shards.items()}


def main(args: Dict[str, Any] = None):
    config = read_config(args.registration_path)
    print(config)

    return 0


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--registration-path",
        type=str,
        default="registration.toml",
        help="Path to topic/shard registration config",
    )

    args = parser.parse_args()

    sys.exit(main(args))
