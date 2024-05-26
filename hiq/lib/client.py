import datetime
from hiq.lib.item import Item
from hiq.lib import util


class Client:
    def __init__(self):
        self.shards = util.read_config(util.REGISTRATION_CONFIG_NAME)

    def make_item(
        self,
        topic: str,
        expires_at: int,
        lives_left: int,
        lease_sec: int,
        payload_data: bytes,
        payload_context: str,
    ) -> Item:
        ns, topic = topic.split(":")
        topic_id = self.shards[ns][topic]

        payload = util.make_payload(data=payload_data, context=payload_context)

        created_at = int(datetime.now().timestamp())
        modified_at = created_at
        delivered_at = -1
        metadata = util.make_metadata(
            topic=topic_id,
            created_at=created_at,
            modified_at=modified_at,
            expires_at=expires_at,
            delivered_at=delivered_at,
            lives_left=lives_left,
            lease_sec=lease_sec,
        )

        return Item(id=-1, payload=payload, metadata=metadata)

    def enqueue(self, item) -> int:
        topic_id = self.topics[item.topic]

        pass

    def dequeue(self, topic: str, lease_sec: int) -> Item:
        pass

    def ack(self, instance_id: int) -> bool:
        pass

    def nack(self, instance_id: int) -> bool:
        pass
