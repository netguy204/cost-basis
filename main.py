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
    date: str

    def take_at_most(self, size):
        """return an effective item that represents taking up to size
        from this item. mutates this item to match the transaction"""
        if size >= self.size:
            result = Item(
                size = self.size,
                cost = self.cost,
                fees = self.fees,
                kind = self.kind,
                date = self.date
            )
            self.size = 0
            self.cost = 0
            self.fees = 0
        else:
            # size is less than self
            result = Item(
                size = size,
                cost = self.cost * size / self.size,
                fees = self.fees * size / self.size,
                kind = self.kind,
                date = self.date,
            )
            self.size -= result.size
            self.cost -= result.cost
            self.fees -= result.fees
        return result

    def rate(self):
        """base currency per item"""
        if self.size == 0:
            return None
        return self.cost / self.size


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
            date = '?',
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

def group_by(src, fn):
    match = []
    for item in src:
        if not match or fn(match[0]) == fn(item):
            match.append(item)
        elif match:
            yield match
            match = [item]
    if match:
        yield match

def item_adapter(rows):
    inpro_match = []
    tid = None

    def to_item(inpro_match):
        try:
            usd = next(iter(filter(lambda x: x['type'] == 'match' and x['amount/balance unit'] == 'USD', inpro_match)))
            acct = next(iter(filter(lambda x: x['type'] == 'match' and x['amount/balance unit'] != 'USD', inpro_match)))
        except StopIteration:
            # wtf?
            #print('badness = {}'.format(inpro_match))
            return None
        try:
            fee = next(iter(filter(lambda x: x['type'] == 'fee', inpro_match)))
        except StopIteration:
            # ah, the good old days with no fees
            fee = {'amount': 0}

        item = Item(
            size = abs(float(acct['amount'])),
            cost = abs(float(usd['amount'])),
            fees = abs(float(fee['amount'])),
            kind = acct['amount/balance unit'],
            date = usd['time'],
        )
        if float(usd['amount']) > 0:
            # this is a sale
            return dataclasses.replace(item, size = -item.size)
        else:
            # this is a purchase
            return item

    for group in group_by(filter(lambda x: x['trade id'], rows), lambda x: int(x['trade id'])):
        item = to_item(group)
        if item:
            yield item


def process(fname):
    with open(fname) as f:
        r = csv.DictReader(f)
        accounts = {}
        profit = 0
        fees = 0
        for item in item_adapter(r):
            if item.kind not in accounts:
                account = Account(item.kind)
                accounts[item.kind] = account
            else:
                account = accounts[item.kind]
            if item.size > 0:
                account.deposit(item)
            elif item.size < 0:
                #print(account.items)
                effective = account.withdraw(abs(item.size))
                #print(account.items)
                effective.fees += item.fees
                pprofit = item.cost - effective.cost
                pfee = effective.fees + item.fees
                profit += pprofit
                fees += pfee
                print('profit = {}, fees = {}, date = {}, purchase = {}, sale = {}'.format(
                    pprofit,
                    pfee,
                    item.date,
                    effective.rate(),
                    item.rate(),
                ))
        print('final cost basis')
        for acct, account in accounts.items():
            print(account.effective_item(), account.effective_item().rate())
            #print(account.items)
        print('total profit: {}, fees: {}'.format(profit, fees))

if __name__ == '__main__':
    process(sys.argv[1])
    #test_driver()
