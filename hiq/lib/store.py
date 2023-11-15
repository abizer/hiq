from typing import List

from hiq.lib.item import Item


class Store:
    store: List

    def __init__(self):
        self.store = []

    def write(self, item: Item):
        self.store.append(item)
        self.store.sort()

    def read(self) -> Item:
        return self.store.pop(0)
