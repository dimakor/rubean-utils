''' Importer for VTB broker - broker reports from XML files
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
                 account_repo = None,
                 balance = True, 
                 token = None,
                 start_date = None):
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
        ''' Match if the file is broker report from VTB Broker
        '''
        # Match extension - should be XML
        #if not re.match(r"\.xml", os.path.splitext(file.name)[1]):
        #    return False
        # Check if we have broker name in header and check general agreement id
        return True

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
        acc = None
        # with open("assets.pickle", "rb") as f:
        #     self.assets = pickle.load(f)
        # with open("accounts.pickle", "rb") as f:
        #         acc = pickle.load(f)
        
        # client = None
        with Client(self.token) as client:
            self.assets = (self.get_list_structure(client.instruments.shares().instruments, 'share') |
                            self.get_list_structure(client.instruments.bonds().instruments, 'bond') |
                            self.get_list_structure(client.instruments.etfs().instruments, 'etf') |
                            self.get_list_structure(client.instruments.currencies().instruments, 'currency')
                        )
        #     with open("assets.pickle", "wb") as f:
        #         pickle.dump(self.assets, f)            
            acc = client.users.get_accounts()
            # with open("accounts.pickle", "wb") as f:
            #     pickle.dump(acc, f)

            for a in acc.accounts:
                entries += self.get_oper(file, client, a)
                entries += self.get_balances(file, a)
        
        return entries

    def get_balances(self, file, acc):
        result = []
        with open(file.name) as f:
            balances = json.load(f)[acc.id]
            for currency, amt in balances['cash'].items():
                amt_d = amount.Amount(D(amt), currency)
                meta = data.new_metadata(file.name, 1) # TODO decide what to do with line number in meta
                result.append(
                    data.Balance(
                                meta, 
                                parse(balances['date']).date() + datetime.timedelta(days=1),
                                self.account_cash,
                                amt_d,
                                None, None
                                )
                            )
            for ticker, amt in balances['securities'].items():
                amt_d = amount.Amount(D(amt), ticker)
                meta = data.new_metadata(file.name, 1) # TODO decide what to do with line number in meta
                result.append(
                    data.Balance(
                                meta, 
                                parse(balances['date']).date() + datetime.timedelta(days=1),
                                account.join(self.account_root, ticker),
                                amt_d,
                                None, None
                                )
                            )
        return result


    def get_oper(self, file, client, acc):
        '''
        '''
        entries = []
        resp = None
                
        start_date = self.start_date if self.start_date else acc.opened_date
        if client:
            resp = client.operations.get_operations(account_id=acc.id,
                                                from_=start_date,
                                                to=datetime.datetime.now(),
                                                state=OperationState.OPERATION_STATE_EXECUTED)
        else:
            with open("operations.pickle", "rb") as f:
                resp = pickle.load(f)
        # with open("operations.pickle", "wb") as f:
        #         pickle.dump(resp, f)
        for trn in resp.operations:
            #import IPython; IPython.embed()
            delivery_date = trn.date.date() #date of execution of transaction
            deal_code = trn.id
            meta = data.new_metadata(file.name, 1) # TODO: check if it's convenient and adjust if necessary
            if trn.operation_type in [OperationType.OPERATION_TYPE_BUY, OperationType.OPERATION_TYPE_SELL]:
                asset_type = self.assets[trn.figi]['type']
                sign = -1 if trn.operation_type == OperationType.OPERATION_TYPE_SELL else 1
                if asset_type == 'currency':
                    ticker = self.assets[trn.figi]['nominal'].currency.upper()
                    account_inst = self.account_cash
                else:
                    ticker = self.assets[trn.figi]['ticker']
                    account_inst = account.join(self.account_root, ticker)
                readable_name = self.assets[trn.figi]['name']
                # cash leg of the transaction
                payment = Decimal(-sign)*abs(self.mv2d(trn.payment))
                payment_amt = amount.Amount(payment, trn.payment.currency.upper())
                p1 = data.Posting(account = self.account_cash, 
                        units = payment_amt, 
                        cost = None, 
                        price = None, 
                        flag = None, meta = None)
                txn = [p1]
                if asset_type == 'bond' or (trn.operation_type == OperationType.OPERATION_TYPE_SELL and asset_type != 'currency'):
                    # add leg for NKD or sell operation
                    p = data.Posting(account = self.account_gains.format(ticker), 
                            units = None, 
                            cost = None, 
                            price = None, 
                            flag = None, meta = None)
                    txn.append(p)
                for trade in trn.trades:
                    amt = amount.Amount(sign*Decimal(trade.quantity), ticker)
                    paym_d = self.mv2d(trade.price)
                    if trn.operation_type == OperationType.OPERATION_TYPE_SELL:
                        payment = amount.Amount(paym_d, trade.price.currency.upper())
                        if asset_type == 'currency':
                            p = data.Posting(account = account_inst, 
                                    units = amt, 
                                    cost = None, 
                                    price = payment, 
                                    flag = None, meta = None)    
                        else:
                            # asset leg of the transaction
                            p = data.Posting(account = account_inst, 
                                    units = amt, 
                                    cost = NOCOST, 
                                    price = payment, 
                                    flag = None, meta = None)
                        txn.append(p)
                    else:
                        # TODO: currency buy?
                        # cost of the transaction
                        cost_bnc = position.CostSpec(
                            number_per=paym_d,
                            number_total=None,
                            currency=trade.price.currency.upper(),
                            date=None,
                            label=None,
                            merge=False)
                        # asset leg of the transaction
                        p = data.Posting(account = account_inst, 
                                    units = amt, 
                                    cost = cost_bnc, 
                                    price = None, 
                                    flag = None, meta = None)
                        # TODO: we're buying currency here!
                        if account_inst == self.account_cash:
                            print(trn)
                        txn.append(p)
                # prepare transaction record
                t = data.Transaction(
                        meta = meta, date = delivery_date, flag = self.FLAG, payee = None, 
                        narration = readable_name, tags = {deal_code}, links = data.EMPTY_SET, 
                        postings = txn)
                entries.append(t)
            elif trn.operation_type in [OperationType.OPERATION_TYPE_OUTPUT, 
                                        OperationType.OPERATION_TYPE_INPUT, 
                                        OperationType.OPERATION_TYPE_OVERNIGHT,
                                        OperationType.OPERATION_TYPE_TAX]:
                payment = amount.Amount(self.mv2d(trn.payment), trn.payment.currency.upper())
                meta = data.new_metadata(file.name, 1) # TODO decide what to do with line number in meta
                txn = data.Transaction(
                            meta, delivery_date, self.FLAG, None, trn.type, data.EMPTY_SET, {deal_code}, 
                            [
                                data.Posting(self.account_cash, payment, None, None, None,
                                                None),
                                data.Posting(self.account_external, -payment, None, None, None,
                                                None),
                            ])
                entries.append(txn)
            elif trn.operation_type in [OperationType.OPERATION_TYPE_BROKER_FEE, 
                                        OperationType.OPERATION_TYPE_MARGIN_FEE,
                                        OperationType.OPERATION_TYPE_SERVICE_FEE]:
                payment = amount.Amount(round(self.mv2d(trn.payment), 2), trn.payment.currency.upper())
                meta = data.new_metadata(file.name, 1) # TODO decide what to do with line number in meta
                txn = data.Transaction(
                            meta, delivery_date, self.FLAG, None, trn.type, data.EMPTY_SET, {deal_code}, 
                            [
                                data.Posting(self.account_cash, payment, None, None, None,
                                                None),
                                data.Posting(self.account_fees, -payment, None, None, None,
                                                None),
                            ])
                entries.append(txn)
            elif trn.operation_type in [OperationType.OPERATION_TYPE_COUPON, 
                                        OperationType.OPERATION_TYPE_DIVIDEND, 
                                        OperationType.OPERATION_TYPE_DIVIDEND_TAX]:
                payment = amount.Amount(self.mv2d(trn.payment), trn.payment.currency.upper())
                meta = data.new_metadata(file.name, 1) # TODO decide what to do with line number in meta
                txn = data.Transaction(
                            meta, delivery_date, self.FLAG, None, trn.type, data.EMPTY_SET, {deal_code}, 
                            [
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

    def mv2d(self, mv : MoneyValue):
        return Decimal(str(mv.units+mv.nano/1e9).rstrip('0'))

    def get_list_structure(self, instruments, asset):
        struct = {}

        for i in instruments:
            struct[i.figi] = {
                'name': i.name,
                'isin': i.isin,
                'ticker': i.ticker,
                'lot': i.lot,
                'currency' : i.currency.upper(),
                'nominal' : 0 if asset == 'etf' else i.nominal,
                'type' : asset
            }

        return struct