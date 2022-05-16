''' Importer for VTB broker - broker reports from XML files
    TODO: chcp 65001 & set PYTHONIOENCODING=utf-8
'''
import datetime
from locale import currency
import re
import os
import csv

import xml.etree.ElementTree as ET
from dateutil.parser import parse
import pprint

from decimal import *
from beancount.core.amount import D
from beancount.core.amount import abs
from beancount.core import data
from beancount.core import flags
from beancount.core import account
from beancount.core import amount
from beancount.core import position
from beancount.ingest import importer
from beancount.parser import printer

from ..rufinlib import rufinlib

NOCOST = position.CostSpec(None, None, None, None, None, None)
CASH_OPER = {'Списание денежных средств', 'Вознаграждение Брокера', 'Зачисление денежных средств', 
        'Сальдо расчётов по сделкам с ценными бумагами', 'Сальдо расчётов по сделкам с иностранной валютой', 'НДФЛ'}

class Importer(importer.ImporterProtocol):
    '''An importer for VTB broker report XML files'''

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
                 balance = True):
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
        self.account_repo = account_repo if account_repo else account_fees

        self.ns = ''

    def check_vtb(self, xmlfile, genid):
        ''' Verify if file from VTB broker
        '''
        tree = ET.parse(xmlfile)
        root = tree.getroot()
        if 'Отчет Банка ВТБ (ПАО)' not in root[0].attrib['Textbox290']:
            return False
        if root[1].attrib['agr_num1'] == genid:
            return True
        return False

    def identify(self, file):
        ''' Match if the file is broker report from VTB Broker
        '''
        # Match extension - should be XML
        if not re.match(r"\.xml", os.path.splitext(file.name)[1]):
            return False
        # Check if we have broker name in header and check general agreement id
        return self.check_vtb(file.name, self.general_agreement_id)

    def file_account(self, _):
        ''' *
        '''
        return self.account_root

    def file_date(self, file):
        ''' * Extract the statement date from the file
        '''
        # No report creation date - use file creation date instead
        return None

    def c(self, currency):
        return 'RUB' if 'RUR' in currency else currency

    def extract(self, file):
        ''' Open XLS file and create directives
        '''
        entries = []
        self.isindb = rufinlib.load_isin()
        
        tree = ET.parse(file.name)
        root = tree.getroot()
        self.ns = {"": root.attrib['Name']}
        # extract broker report dates
        t = root[0].attrib['Textbox290']
        self.stmt_begin = datetime.datetime.strptime(t[34:44], '%d.%m.%Y').date()
        self.stmt_end = datetime.datetime.strptime(t[48:58], '%d.%m.%Y').date()

        # Load assets transactions
        # check if there is necessary data in file
        el = root.find("Tablix_b11", self.ns)
        if el:
            entries += self.get_trn(el, file)
        # Load assets balances
        el = root.find("Tablix6", self.ns).find("bond_type_Collection", self.ns)
        if el:
            entries += self.get_assets_balances(el, file)
        # Load cash transaction
        el = root.find("Tablix_b4", self.ns)
        if el:
            entries += self.get_cashflow(el, file)
        # Load cash balances
        el = root.find("Tablix_b2", self.ns)
        if el:
            entries += self.get_cash_balances(el, file)
        # Load fx operations
        el = root.find("Tablix_b12", self.ns)
        if el:
            entries += self.get_fx(el, file)
        # Load repo operations
        el = root.find("Tablix_b16", self.ns)
        if el:
            entries += self.get_repo(el, file)

        # # 1. Load end of report balances: 'Динамика позиций' sheet
        # sheet = workbook.sheet_by_name('Динамика позиций') #TODO change sheet to 0 (first sheet in workbook)
        # del sheet

        # if self.balance:
        #     entries += self.get_balance(workbook, file)

        # entries += self.get_trn(workbook, file)
        # entries += self.get_cflow(workbook, file)

        return entries

    def get_ticker(self, text):
        nm = text.split(', ')
        isin = self.isindb[nm[2]] # get ISIN code
        ticker = isin['ticker'] # get stock ticker 
        readable_name = nm[0] # store human-readable name of the asset
        return ticker, readable_name, isin

    def get_repo_id(self, text):
        try:
            return text[:text.rindex('-')]
        except ValueError:
            return text[3:]


    def get_repo(self, element, file):
        ''' * Parse broker report for repo operations
            In: XML element "Tablix_b16", file
            Out: list of transactions
        '''
        result = []
        oper = dict()

        for r in element[0]:
            oper_id = self.get_repo_id(r.attrib['deal_number2'])
            sign = 1 if 'Покупка' in r.attrib['NameEnd4'] else -1
            try:
                repo_deal = oper[oper_id]
                repo_deal['code'] = repo_deal['code'] + ' ' + r.attrib['deal_number2']
                repo_deal['date'] = max(repo_deal['date'], parse(r.attrib['date_pay2']).date())
                repo_deal['amount'] += sign*Decimal(r.attrib['deal_cost2'])
                repo_deal['comis'] += Decimal(r.get('bank_сommition2', 0))
            except KeyError:
                oper[oper_id] = {
                    'code' : r.attrib['deal_number2'],
                    'date': parse(r.attrib['date_pay2']).date(),
                    'amount' : sign*Decimal(r.attrib['deal_cost2']),
                    'comis' : Decimal(r.attrib['bank_сommition2']),
                    'currency' : r.attrib['currency_paym2']
                }

        for o in oper.values():
            meta = data.new_metadata(file.name, 1)
            p1 = data.Posting(account = self.account_cash, 
                            units = None, 
                            cost = None, 
                            price = None, 
                            flag = None, meta = None)
            p2 = data.Posting(account = self.account_repo, 
                            units = amount.Amount(o['amount'], self.c(o['currency'])), 
                            cost = None, 
                            price = None, 
                            flag = None, meta = None)
            p3 = data.Posting(account = self.account_fees, 
                            units = amount.Amount(o['comis'], self.c(o['currency'])), 
                            cost = None, 
                            price = None, 
                            flag = None, meta = None)
            t = data.Transaction(
                        meta = meta, date = o['date'], flag = self.FLAG, payee = None, 
                        narration = o['code'], tags = {o['code'].replace(' ',' #')}, links = data.EMPTY_SET, 
                        postings = [p1, p2, p3])
            result.append(t)
        return result


    def get_fx(self, element, file):
        ''' * Parse broker report for foreign exchange operations
            In: XML element "Tablix_b12", file
            Out: list of transactions
        '''
        result = []

        for r in element[0]:
            try:
                currency_sell = r.attrib['NameBeg7'][0:3]
                currency_buy = r.attrib['deal_price5']
                trn_date = parse(r.attrib['bank_сommition5']).date()
                amt_sell = amount.Amount(-D(r.attrib['NameEnd7']), self.c(currency_sell))
                amt_buy = amount.Amount(D(r.attrib['currency_price5']), self.c(currency_buy))
                fx_rate = amount.Amount(D(r.attrib['deal_count5']), self.c(currency_buy))
                note = r.attrib['deliv_date4']
                meta = data.new_metadata(file.name, 1) # TODO decide what to do with line number in meta
                commision = amount.Amount(Decimal(r.attrib['currency_paym5']) + 
                                    Decimal(r.attrib['deal_cost5']), 'RUB') # broker comission
                txn = data.Transaction(
                        meta, trn_date, self.FLAG, None, note, data.EMPTY_SET, {trn_date}, [
                            data.Posting(self.account_cash, amt_sell, None, fx_rate, None, None),
                            data.Posting(self.account_cash, amt_buy, None, None, None, None),
                            data.Posting(self.account_cash, -commision, None, None, None, None),
                            data.Posting(self.account_fees, commision, None, None, None, None)
                        ])
                result.append(txn)
            except KeyError as e:
                print(e)
                print("Unknown key:", r)
        return result

    def get_cashflow(self, element, file):
        ''' * Parse broker report for cash operations
            In: XML element "Tablix_b4", file
            Out: list of transactions
        '''
        result = []

        for r in element[0][0][0]:
            try:
                operation = r.attrib['operation_type']
                if operation not in CASH_OPER:
                    print("Unknown operation: {}".format(operation))
                    continue
                note = r.attrib.get('notes1', operation)
                if (operation in ['Списание денежных средств', 'Зачисление денежных средств', 'НДФЛ'] or
                ('Вознаграждение Брокера' in operation) and 'Проведение расчетных операций с ценными бумагами' in note):
                    trn_date = parse(r.attrib['debt_type4']).date()
                    cur = r.attrib['decree_amount2'] # get currency of transaction
                    amt = amount.Amount(D(r.attrib['debt_date4']), self.c(cur))
                    meta = data.new_metadata(file.name, 1) # TODO decide what to do with line number in meta
                    txn = data.Transaction(
                            meta, trn_date, self.FLAG, None, note, data.EMPTY_SET, {trn_date}, [
                                data.Posting(self.account_cash, amt, None, None, None,
                                                None),
                                data.Posting(self.account_external, -amt, None, None, None,
                                                None),
                            ])
                    result.append(txn)
            except KeyError as e:
                print(e)
                print("No key:", r)
        return result

    def get_cash_balances(self, element, file):
        ''' * Parse broker report for end of period balances - cash
            In: XML element "Tablix_b2", file
            Out: list of transactions
        '''
        result = []

        for r in element[0]:
            cur = r.attrib['currency_ISO2']
            amt = amount.Amount(D(r.attrib['outpl_2']), self.c(cur))
            meta = data.new_metadata(file.name, 1) # TODO decide what to do with line number in meta
            result.append(
                data.Balance(
                            meta, 
                            self.stmt_end + datetime.timedelta(days=1),
                            self.account_cash,
                            amt,
                            None, None
                            )
                        )
        return result
    
    def get_assets_balances(self, element, file):
        ''' * Parse broker report for end of period balances - assets
            In: XML element "Tablix6", file
            Out: list of transactions
        '''
        result = []

        for collection in element:
            for r in collection[1]:
                ticker, readable_name, isin = self.get_ticker(r.attrib['FinInstr'])
                account_inst = account.join(self.account_root, ticker)
                amt = amount.Amount(D(r[0][0][0][0].attrib['remains_out'].split('.')[0]), self.c(ticker))
                meta = data.new_metadata(file.name, 1) # TODO decide what to do with line number in meta
                result.append(
                    data.Balance(
                                meta, 
                                self.stmt_end + datetime.timedelta(days=1),
                                account_inst,
                                amt,
                                None, None
                                )
                            )
        return result

    def get_trn(self, element, file):
        ''' Parse broker report for all assets transactions
        '''
        result = []

        for r in element[0]:
            p1 = p2 = p3 = p4 = None
            payment = amt = 0

            # extract asset name from record
            ticker, readable_name, isin = self.get_ticker(r.attrib['NameBeg10']) # store human-readable name of the asset
            
            cur = r.attrib['currency_price8'] # get currency of transaction
            delivery_date = parse(r.attrib['deliv_date7']).date() #date of execution of transaction
            deal_code = r.attrib['deal_code7'] # transaction code for narration/tag
            meta = data.new_metadata(file.name, 1) # TODO: check if it's convenient and adjust if necessary
            commision = amount.Amount(Decimal(r.attrib['bank_сommition8']) + 
                                    Decimal(r.attrib['deal_code6']), self.c(r.attrib['currency_price8'])) # broker comission

            # get cost information
            # if isin['isbond']:
            #     cost_d = Decimal(r.attrib['deal_price8'])*isin['facevalue']/Decimal(100)
            #     price = amount.Amount(cost_d, isin['currency'])
            # else:
            cost_d = Decimal(r.attrib['deal_price8'])
            price = amount.Amount(cost_d, self.c(cur)) 

            account_inst = account.join(self.account_root, ticker) # account for asset

            if 'Продажа' in r.attrib['currency_ISO10']:
                amt = amount.Amount(Decimal(-1*int(r.attrib['NameEnd10'].split('.')[0])), self.c(ticker))
                payment = amount.Amount(Decimal(r.attrib['currency_paym8']), self.c(cur))
                # asset leg of the transaction
                p2 = data.Posting(account = account_inst, 
                                units = amt, 
                                cost = NOCOST, 
                                price = price, 
                                flag = None, meta = None)
                # we're selling - gains leg of the transaction
                p4 = data.Posting(account = self.account_gains.format(ticker), 
                            units = None, 
                            cost = None, 
                            price = None, 
                            flag = None, meta = None)
            elif 'Покупка' in r.attrib['currency_ISO10']:
                amt = amount.Amount(Decimal(int(r.attrib['NameEnd10'].split('.')[0])), self.c(ticker))
                payment = amount.Amount(Decimal(-1)*Decimal(r.attrib['currency_paym8']), self.c(cur))
                if not isin['isbond']:
                    cost2 = position.CostSpec(
                        number_per=cost_d,
                        number_total=None,
                        currency=self.c(cur),
                        date=None,
                        label=None,
                        merge=False)
                    # asset leg of the transaction
                    p2 = data.Posting(account = account_inst, 
                                units = amt, 
                                cost = cost2, 
                                price = None, 
                                flag = None, meta = None)
                else:
                    # asset leg of the transaction for bond (no cost basis info - only purchase price) 
                    p2 = data.Posting(account = account_inst, 
                                units = amt, 
                                cost = NOCOST, 
                                price = abs(payment), 
                                flag = None, meta = None)
            else:
                print(r)
                assert(True, "Unknown operation!")
            # cash leg of the transaction
            p1 = data.Posting(account = self.account_cash, 
                            units = payment, 
                            cost = None, 
                            price = None, 
                            flag = None, meta = None)
            # comission
            p3 = data.Posting(account = self.account_fees, 
                            units = commision, 
                            cost = None, 
                            price = None, 
                            flag = None, meta = None) 
            # balance comission
            p3_1 = data.Posting(account = self.account_cash, 
                            units = -commision, 
                            cost = None, 
                            price = None, 
                            flag = None, meta = None) 
            pp = [p1, p2, p3, p3_1]
            if p4:
                pp.append(p4)
            t = data.Transaction(
                        meta = meta, date = delivery_date, flag = self.FLAG, payee = None, 
                        narration = readable_name, tags = {deal_code}, links = data.EMPTY_SET, 
                        postings = pp)
            result.append(t)

        return result
       