from dataclasses import dataclass


@dataclass(kw_only=True)
class Metadata:
    topic: int

    created_at: int
    modified_at: int
    expires_at: int
    delivered_at: int

    lives_left: int
    lease_sec: int


@dataclass(kw_only=True)
class Payload:
    data: str
    context: str


@dataclass(kw_only=True)
class Item:
    id: int
    payload: Payload
    metadata: Metadata
