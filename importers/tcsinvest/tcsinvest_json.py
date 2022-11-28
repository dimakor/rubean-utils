''' Importer for TCS Invest - JSON files
    TODO: chcp 65001 & set PYTHONIOENCODING=utf-8
'''
import datetime
from locale import currency
import re
import os
import pickle
import json

from dateutil.parser import parse
from rich import print

from decimal import *
from beancount.core.amount import D
from beancount.core import data
from beancount.core import flags
from beancount.core import account
from beancount.core import amount
from beancount.core import position
from beancount.ingest import importer

from tinkoff.invest import Client, OperationState, OperationType, MoneyValue

NOCOST = position.CostSpec(None, None, None, None, None, None)


class Importer(importer.ImporterProtocol):
    '''An importer for Tinkoff Invest broker API'''

    def __init__(self, general_agreement_id,
                 account_root,
                 account_cash,
                 account_currencyexchange,
                 account_dividends,
                 account_interest,
                 account_fees,
                 account_gains,
                 account_external,
                 account_repo=None,
                 balance=True,
                 token=None,
                 start_date=None):
        self.general_agreement_id = general_agreement_id
        self.account_root = account_root
        self.account_cash = account_cash
        self.account_currencyexchange = account_currencyexchange
        self.account_dividends = account_dividends
        self.account_interest = account_interest
        self.account_fees = account_fees
        self.account_gains = account_gains
        self.account_external = account_external
        self.balance = balance
        self.token = token
        self.start_date = start_date
        self.account_repo = account_repo if account_repo else account_fees

        self.assets = None

    def identify(self, file):
        ''' Match if the file is broker report from TCS Invest
        '''
        # Match file extension - should be JSON
        if not file.name.endswith('.json'):
            return False
        # Load JSON file
        with open(file.name, 'r') as f:
            # load first key from file and compare to self.general_agreement_id
            acc_data = json.load(f)
        if acc_data.get(self.general_agreement_id):
            return True
        return False

    def file_account(self, _):
        ''' *
        '''
        return self.account_root

    def file_date(self, file):
        ''' * Extract the statement date from the file
        '''
        # No report creation date - use file creation date instead
        return None

    def extract(self, file):
        ''' Connect to Tinkoff API and download all data
        '''
        entries = []
        # load tickers from JSON file
        with open("tickers.json", 'r') as f:
            self.assets = json.load(f)

        # load account data from JSON file
        with open(file.name, 'r') as f:
            acc_data = json.load(f).get(self.general_agreement_id)

        if not acc_data:
            return []

        entries += self.get_oper(file, acc_data)
        entries += self.get_balances(file, acc_data)

        return entries

    def get_balances(self, file, acc_data : dict) -> list:
        result = []
        for currency, amt in acc_data['cash'].items():
            amt_d = amount.Amount(D(amt), currency)
            # TODO decide what to do with line number in meta
            meta = data.new_metadata(file.name, 1)
            result.append(
                data.Balance(
                    meta,
                    parse(acc_data['statement_date']).date() +
                    datetime.timedelta(days=1),
                    self.account_cash,
                    amt_d,
                    None, None
                )
            )
        for ticker, amt in acc_data['securities'].items():
            amt_d = amount.Amount(D(amt), ticker)
            # TODO decide what to do with line number in meta
            meta = data.new_metadata(file.name, 1)
            result.append(
                data.Balance(
                    meta,
                    parse(acc_data['statement_date']).date() +
                    datetime.timedelta(days=1),
                    account.join(self.account_root, ticker),
                    amt_d,
                    None, None
                )
            )
        return result

    def get_oper(self, file, acc_data):
        '''
        '''
        entries = []
        resp = None

        start_date = self.start_date if self.start_date else acc_data['opened_date']

        for trn in acc_data['operations']:
            #import IPython; IPython.embed()
            delivery_date = parse(trn['date']).date()  # date of execution of transaction
            deal_code = trn['id'] if trn['id'] else trn['parent_id']
            trn_tags = { deal_code } if deal_code else data.EMPTY_SET
            # TODO: check if it's convenient and adjust if necessary
            meta = data.new_metadata(file.name, 1)
            if trn['type'] in [OperationType.OPERATION_TYPE_BUY, OperationType.OPERATION_TYPE_SELL]:
                figi = trn['figi']
                asset_type = self.assets[figi]['type']
                readable_name = self.assets[figi]['name']
                sign = -1 if trn['type'] == OperationType.OPERATION_TYPE_SELL else 1
                if asset_type == 'currency':
                    ticker = self.assets[figi]['nominal_currency']
                    account_inst = self.account_cash
                else:
                    ticker = self.assets[figi]['ticker']
                    account_inst = account.join(self.account_root, ticker)
                txn = list()
                # cash leg of the transaction
                trn_pmt = trn.get('payment')
                if trn_pmt: 
                    payment = Decimal(-sign)*abs(Decimal(trn_pmt))
                    payment_amt = amount.Amount(payment, trn['payment_currency'])
                    p1 = data.Posting(account=self.account_cash,
                                    units=payment_amt,
                                    cost=None,
                                    price=None,
                                    flag=None, meta=None)
                    txn.append(p1)
                else:
                    amt = amount.Amount(sign*Decimal(trn['quantity']), ticker)
                    paym_d = Decimal(trn['price'])
                    payment = amount.Amount(paym_d, trn['currency'])

                for trade in trn['trades']:
                    paym_d_trade = Decimal(trade['price'])
                    amt_trade = amount.Amount(sign*Decimal(trade['quantity']), ticker)
                    if trn['type'] == OperationType.OPERATION_TYPE_SELL:
                        payment_trade = amount.Amount(paym_d_trade, trade['currency'])
                        if asset_type == 'currency':
                            p = data.Posting(account=account_inst,
                                                units=amt_trade,
                                                cost=None,
                                                price=payment_trade,
                                                flag=None, meta=None)
                        else:
                            # asset leg of the transaction
                            p = data.Posting(account=account_inst,
                                                units=amt_trade,
                                                cost=NOCOST,
                                                price=payment_trade,
                                                flag=None, meta=None)
                        txn.append(p)
                    else:
                        # TODO: currency buy?
                        # cost of the transaction
                        cost_bnc = position.CostSpec(
                                            number_per=paym_d_trade,
                                            number_total=None,
                                            currency=trade['currency'],
                                            date=None,
                                            label=None,
                                            merge=False)
                        # asset leg of the transaction
                        p = data.Posting(account=account_inst,
                                            units=amt_trade,
                                            cost=cost_bnc,
                                            price=None,
                                            flag=None, meta=None)
                        # TODO: we're buying currency here!
                        if account_inst == self.account_cash:
                            print(trn)
                        txn.append(p)
                if asset_type == 'bond' or (trn['type'] == OperationType.OPERATION_TYPE_SELL and asset_type != 'currency'):
                    # add leg for NKD or sell operation
                    p = data.Posting(account=self.account_gains.format(ticker),
                                     units=None,
                                     cost=None,
                                     price=None,
                                     flag=None, meta=None)
                    txn.append(p)
                # prepare transaction record
                t = data.Transaction(
                    meta=meta, date=delivery_date, flag=self.FLAG, payee=None,
                    narration=readable_name, tags=trn_tags, links=data.EMPTY_SET,
                    postings=txn)
                entries.append(t)
            elif trn['type'] in [OperationType.OPERATION_TYPE_OUTPUT,
                                        OperationType.OPERATION_TYPE_INPUT,
                                        OperationType.OPERATION_TYPE_OVERNIGHT,
                                        OperationType.OPERATION_TYPE_TAX]:
                payment = amount.Amount(Decimal(trn["payment"]), trn["payment_currency"])
                # TODO decide what to do with line number in meta
                meta = data.new_metadata(file.name, 1)
                txn = data.Transaction(
                    meta=meta, date=delivery_date, flag=self.FLAG, payee=None, 
                    narration=trn["label"], tags=trn_tags, links=data.EMPTY_SET,
                    postings=[
                        data.Posting(self.account_cash, payment, None, None, None,
                                     None),
                        data.Posting(self.account_external, -payment, None, None, None,
                                     None),
                    ])
                entries.append(txn)
            elif trn['type'] in [OperationType.OPERATION_TYPE_BROKER_FEE,
                                        OperationType.OPERATION_TYPE_MARGIN_FEE,
                                        OperationType.OPERATION_TYPE_SERVICE_FEE]:
                payment = amount.Amount(
                    round(Decimal(trn["payment"]), 2), trn["payment_currency"])
                # TODO decide what to do with line number in meta
                meta = data.new_metadata(file.name, 1)
                txn = data.Transaction(
                    meta=meta, date=delivery_date, flag=self.FLAG, payee=None, 
                    narration=trn["label"], tags=trn_tags, links=data.EMPTY_SET,
                    postings=[
                        data.Posting(self.account_cash, payment, None, None, None,
                                     None),
                        data.Posting(self.account_fees, -payment, None, None, None,
                                     None),
                    ])
                entries.append(txn)
            elif trn['type'] in [OperationType.OPERATION_TYPE_COUPON,
                                        OperationType.OPERATION_TYPE_DIVIDEND,
                                        OperationType.OPERATION_TYPE_DIVIDEND_TAX]:
                payment = amount.Amount(Decimal(trn["payment"]), trn["payment_currency"])
                # TODO decide what to do with line number in meta
                meta = data.new_metadata(file.name, 1)
                txn = data.Transaction(
                    meta=meta, date=delivery_date, flag=self.FLAG, payee=None, 
                    narration=trn["label"], tags=trn_tags, links=data.EMPTY_SET,
                    postings=[
                        data.Posting(self.account_cash, payment, None, None, None,
                                     None),
                        data.Posting(self.account_dividends, -payment, None, None, None,
                                     None),
                    ])
                entries.append(txn)
            else:
                print("UNKNOWN OPERATION:")
                print(trn)
        return entries

    def mv2d(self, mv: MoneyValue):
        return Decimal(str(mv.units+mv.nano/1e9).rstrip('0'))
