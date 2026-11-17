"""Ordered Decimal price levels, updated incrementally rather than re-sorted per tick."""
from bisect import bisect_left, insort


class Levels(dict):
    def __init__(self):
        super().__init__()
        self.prices = []
    def __setitem__(self, key, value):
        if key not in self:
            insort(self.prices, key)
        super().__setitem__(key, value)
    def pop(self, key, default=None):
        if key in self:
            self.prices.pop(bisect_left(self.prices, key))
        return super().pop(key, default)
    def clear(self):
        super().clear()
        self.prices.clear()
    def best(self, high=False):
        return self.prices[-1 if high else 0]
    def ordered(self, descending=False):
        return reversed(self.prices) if descending else iter(self.prices)
