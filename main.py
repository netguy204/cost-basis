#!/bin/env python

import csv
import sys
import dataclasses
from dataclasses import dataclass

class InfAccount:
    def __init__(self, kind, basis):
        self.kind = kind
        self.basis = basis

    def withdraw(self, size):
        return Item(
            size = size,
            cost = size * self.basis,
            fees = 0,
            kind = self.kind,
            date = '?',
        )

    def deposit(self, item):
        pass

    def effective_item(self):
        return Item(0, 0, 0, self.kind, '?')

@dataclass
class Transfer:
    src: str
    src_size: float
    dest: str
    dest_size: float

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
        item_grp = fn(item)
        if item_grp is None:
            if match:
                yield match
                match = []
            yield item
        elif not match or fn(match[0]) == item_grp:
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
        src = next(iter(filter(lambda x: x['type'] == 'match' and float(x['amount']) < 0, inpro_match)))
        dest = next(iter(filter(lambda x: x['type'] == 'match' and float(x['amount']) > 0, inpro_match)))
        try:
            fee = next(iter(filter(lambda x: x['type'] == 'fee', inpro_match)))
        except StopIteration:
            # ah, the good old days with no fees
            fee = {'amount': 0}

        if 'USD' not in (src['amount/balance unit'], dest['amount/balance unit']):
            return Transfer(
                src = src['amount/balance unit'].lower(),
                src_size = abs(float(src['amount'])),
                dest = dest['amount/balance unit'].lower(),
                dest_size = abs(float(dest['amount'])),
            )

        if src['amount/balance unit'] == 'USD':
            usd = src
            acct = dest
        else:
            acct = src
            usd = dest

        item = Item(
            size = abs(float(acct['amount'])),
            cost = abs(float(usd['amount'])),
            fees = abs(float(fee['amount'])),
            kind = acct['amount/balance unit'].lower(),
            date = usd['time'],
        )
        if float(usd['amount']) > 0:
            # this is a sale
            return dataclasses.replace(item, size = -item.size)
        else:
            # this is a purchase
            return item

    for group in group_by(rows, trade_id_or_none):
        if isinstance(group, list):
            item = to_item(group)
            if item:
                yield item
        elif group['type'] == 'deposit':
            yield Transfer(
                src = 'inf_' + group['amount/balance unit'].lower(),
                src_size = abs(float(group['amount'])),
                dest = group['amount/balance unit'].lower(),
                dest_size = abs(float(group['amount'])),
            )
        elif group['type'] == 'withdrawal':
            yield Transfer(
                src = group['amount/balance unit'].lower(),
                src_size = abs(float(group['amount'])),
                dest = 'inf_' + group['amount/balance unit'].lower(),
                dest_size = abs(float(group['amount'])),
            )


def trade_id_or_none(item):
    tid = item['trade id']
    if len(tid) > 0:
        return int(tid)
    else:
        return None

def process(fname):
    with open(fname) as f:
        r = csv.DictReader(f)
        accounts = {
            'usd': InfAccount('usd', 1.0),
            'inf_usd': InfAccount('usd', 1.0),
            'inf_btc': InfAccount('inf_btc', 0.0),
            'inf_ltc': InfAccount('inf_ltc', 0.0),
        }
        def ensure_account(accounts, kind):
            if kind not in accounts:
                account = Account(kind)
                accounts[kind] = account
            else:
                account = accounts[kind]
            return account

        profit = 0
        fees = 0
        for item in item_adapter(r):
            if isinstance(item, Transfer):
                src = ensure_account(accounts, item.src)
                dest = ensure_account(accounts, item.dest)
                effective_item = src.withdraw(item.src_size)
                dest.deposit(Item(
                    size = item.dest_size,
                    cost = effective_item.cost,
                    fees = effective_item.fees,
                    kind = item.dest,
                    date = '?', # fixme
                ))
                continue
            account = ensure_account(accounts, item.kind)
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
            if isinstance(account, Account):
                print(account.effective_item(), account.effective_item().rate())
        print('total profit: {}, fees: {}'.format(profit, fees))

if __name__ == '__main__':
    process(sys.argv[1])
    #test_driver()
