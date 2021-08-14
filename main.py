#!/bin/env python

import csv
import sys
import dataclasses
from dataclasses import dataclass

@dataclass
class Item:
    size: float
    cost: float
    fees: float
    kind: str

    def take_at_most(self, size):
        """return an effective item that represents taking up to size
        from this item. mutates this item to match the transaction"""
        if size >= self.size:
            result = Item(self.size, self.cost, self.fees)
            self.size = 0
            self.cost = 0
            self.fees = 0
        else:
            result = Item(
                size,
                self.cost * size / self.size,
                self.fees * size / self.size,
            )
            self.size -= result.size
            self.cost -= result.cost
            self.fees -= result.fees
        return result

    def rate(self):
        """items per base currency"""
        return self.size / (self.cost + self.fees)


class Account:
    def __init__(self, kind):
        self.items = []
        self.kind = kind

    def deposit(self, item):
        if item.kind != self.kind:
            raise Exception("cannot deposit {} into account of type {}".format(item.kind, self.kind))

        self.items.append(item)

    def size(self):
        """in items"""
        return sum([i.size for i in self.items])

    def cost(self):
        """in base currency"""
        return sum([i.cost for i in self.items])

    def fees(self):
        """in base currency"""
        return sum([i.fees for i in self.items])

    def effective_item(self):
        return Item(
            size = self.size(),
            cost = self.cost(),
            fees = self.fees(),
            kind = self.kind,
        )

    def withdraw(self, size):
        """compute a potentially composite item to represent the fifo
        deposits that back this withdraw"""
        result = Account(self.kind)
        while self.items:
            taken = result.size()
            if taken >= size:
                break
            remaining = size - taken
            result.deposit(self.items[0].take_at_most(remaining))
            if self.items[0].size == 0:
                self.items.pop(0)
        return result.effective_item()

def item_adapter(rows):
    inpro_match = []
    ts = None

    def to_item(inpro_match):
        usd = next(iter(filter(lambda x: x['type'] == 'match' and x['amount/balance unit'] == 'USD', inpro_match)))
        fee = next(iter(filter(lambda x: x['type'] == 'fee', inpro_match)))
        acct = next(iter(filter(lambda x: x['type'] == 'match' and x['amount/balance unit'] != 'USD', inpro_match)))

        item = Item(
            size = abs(float(acct['amount'])),
            cost = abs(float(usd['amount'])),
            fees = abs(float(fee['amount'])),
            kind = acct['amount/balance unit'],
        )
        if float(usd['amount']) > 0:
            # this is a purchase
            return item
        else:
            # this is a sale
            return dataclasses.replace(item, size = -item.size)

    for row in rows:
        if row['type'] in ('match', 'fee'):
            if ts is None or row['time'] == ts:
                ts = row['time']
                inpro_match.append(row)
            else:
                raise Exception("{} didn't match expected set {}".format(row, inpro_match))
        else:
            pass
            #print("ignoring {}".format(row['type']))

        if len(inpro_match) == 3:
            yield to_item(inpro_match)
            inpro_match = []
            ts = None

    if len(inpro_match) == 3:
        yield to_item(inpro_match)
    elif inpro_match:
        raise Exception("{} didn't have enough matches".format(inpro_match))



def process(fname):
    with open(fname) as f:
        r = csv.DictReader(f)
        accounts = {}
        for item in item_adapter(r):
            if item.kind not in accounts:
                accounts[item.kind] = Account(item.kind)
            if item.size > 0:
                accounts[item.kind].deposit(item)
            elif item.size < 0:
                effective = accounts[item.kind].withdraw(item.size)
                effective.fees += item.fees
                print('profit = {}, fees = {}'.format(
                    item.cost - effective.cost,
                    effective.fees + item.fees,
                ))
        print('final cost basis')
        for acct, account in accounts.items():
            print(account.effective_item())

def test_driver():
    account = Account('btc')
    account.deposit(Item(1, 10000, 300, 'btc'))
    account.deposit(Item(.3, 10000, 200, 'btc'))
    item = account.effective_item()
    print(item, item.rate())
    item = account.withdraw(1.1)
    print(item, item.rate())
    item = account.effective_item() 
    print(item, item.rate())

if __name__ == '__main__':
    process(sys.argv[1])
    #test_driver()
