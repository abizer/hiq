import datetime

from hiq.lib.item import Item
from hiq.lib.manager import Manager
from hiq.lib.util import make_item


def make_test_item(topic: str = "dev:test", data: str = "") -> Item:
    # 2 minutes from now
    expires_at = int(
        (datetime.datetime.now() + datetime.timedelta(seconds=120)).timestamp()
    )

    item = make_item(
        topic=topic,
        expires_at=expires_at,
        lives_left=1,
        lease_sec=30,
        payload_data=data,
        payload_context="test",
    )

    return item


def main():
    manager = Manager()

    item1 = make_test_item("dev:test", "hello world")
    item2 = make_test_item("dev:test", "hello world 2")

    item1_id = manager.enqueue(item1)
    print(f"Enqueued item1 with instance id: {item1_id}")

    dequeued_item1 = manager.dequeue(topic="dev:test", lease_seconds=30)
    assert dequeued_item1.id == item1_id

    item2_id = manager.enqueue(item2)
    print(f"Enqueued item2 with instance id: {item2_id}")

    dequeued_item2 = manager.dequeue(topic="dev:test", lease_seconds=30)
    assert dequeued_item2.id == item2_id

    manager.ack(dequeued_item1)


if __name__ == "__main__":
    main()
