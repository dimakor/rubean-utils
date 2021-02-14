'''Importer for BCS Express broker - broker reports from XLS files
   Description of broker report format: https://broker.ru/f/support/daily-trading-report.pdf
'''
import xlrd
import datetime
import re
import logging
from os import path

from dateutil.parser import parse

from beancount.core.amount import D
from beancount.core import data
from beancount.core import flags
from beancount.core import account
from beancount.core import amount
from beancount.core import position
from beancount.ingest import importer
from xlrd.biffh import XLRDError

NOCOST = position.CostSpec(None, None, None, None, None, None)

def fix_ticker(ticker):
    if ticker == 'CHMF_02':
        return 'CHMF'
    if ticker == 'PAI_BCS4':
        return 'PAIBCS4'
    if ticker == 'PAI_BCS1':
        return 'PAIBCS1'
    if ticker == 'OGK2_2':
        return 'OGK2'
    if ticker == 'TGK1_01':
        return 'TGKA'
    if ticker == 'GAZP2':
        return 'GAZP'
    if ticker == 'MICEX_09':
        return 'MICEX'
    if ticker == 'PHOR_0':
        return 'PHOR'
    if ticker == 'ENPL_LI':
        return 'ENPLADR'
    if ticker == 'HK_486':
        return 'RUSALADR'
    return ticker

def fix_currency(ticker):
    return 'RUB' if ticker == 'Рубль' else ticker

def check_bcsexpress(xlsfile, genid):
    ''' Verify if file from BCS Express broker
    '''
    workbook = xlrd.open_workbook(xlsfile)
    try:
        sheet = workbook.sheet_by_name('TDSheet')
    except XLRDError:
        return False # No correct sheet in file
    broker_name = sheet.row_values(0, start_colx=1, end_colx=None)
    if not re.match(r'ООО "Компания БКС"', broker_name[4]):
        return False
    # Check general agreement id
    genagr = sheet.row_values(4, start_colx=1, end_colx=None)
    if not re.match(genid, genagr[4]):
        return False
    return True

class Importer(importer.ImporterProtocol):
    '''An importer for BCS Express XLS files'''

    def __init__(self, general_agreement_id,
                 account_root,
                 account_cash,
                 account_currencyexchange,
                 account_dividends,
                 account_interest,
                 account_fees, 
                 account_gains,
                 account_external,
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

        self.exchanges = {
                            'ММВБ':self.account_cash,
                            'МосБирж(Валютный рынок)':self.account_currencyexchange
                        }

    def identify(self, file):
        ''' Match if the filename is broker report from BCS Express
        '''
        if not re.match(r".*B[_ ]k-.*", path.basename(file.name)):
            return False
        if re.match(r"\.~lock", path.basename(file.name)):
            return False
        # Match extension - should be XLS
        if not re.match(r".xls", path.splitext(file.name)[1]):
            return False
        # Check if we have broker name in header and check general agreement id
        return check_bcsexpress(file.name, self.general_agreement_id)

    def file_account(self, _):
        return self.account_root

    def file_date(self, file):
        ''' Extract the statement date from the file
        '''
        workbook = xlrd.open_workbook(file.name)
        sheet = workbook.sheet_by_name('TDSheet')
        rows = sheet.get_rows()
        for row in rows:
            if re.match('Дата составления отчета:', row[1].value):
                return datetime.datetime.strptime(row[4].value, '%d.%m.%Y').date()
        # Couldn't extract date - use file creation date instead
        return None
 
    def get_balance(self, sheet, file):
        ''' Will parse broker report for end of period balances - cash and assets
            In: XLS sheet, file
            Out: list of transactions
        '''
        result =[]
        acc = ''
        for ii in range(sheet.nrows):
            meta = data.new_metadata(file.name, ii)
            
            if sheet.row(ii)[1].value[:6] == '1.1.1.':
                acc = self.account_cash
            if sheet.row(ii)[1].value[:6] == '1.1.2.':
                acc = self.account_currencyexchange
            if (self.stmt_begin < datetime.date(2018, 11, 1) and 
                    re.match(r'^Остаток денежных средств на конец периода \(', sheet.row(ii)[1].value)):
                balance_currency = fix_currency(re.search(r'\(.*\)', sheet.row(ii)[1].value)[0][1:-1])
                result.append( data.Balance(meta, self.stmt_end + datetime.timedelta(days=1),
                                            acc,
                                            amount.Amount(D(str(sheet.row(ii)[7].value)), balance_currency),
                                            None, None))
            if (re.match(r'^Портфель по ценным бумагам и денежным средствам \(', sheet.row(ii)[1].value) and 
                    sheet.row(ii)[6].value == 'на начало периода'):
                sec_currency = re.search(r'\(.*\)', sheet.row(ii)[1].value)[0][1:-1]
                ii += 2
                while sheet.row(ii)[1].value == sec_currency:
                    if self.stmt_begin < datetime.date(2018, 11, 1):
                        ii +=1
                        continue
                    meta = data.new_metadata(file.name, ii)
                    result.append( data.Balance(meta, self.stmt_end + datetime.timedelta(days=1),
                                            self.exchanges[sheet.row(ii)[14].value],
                                            amount.Amount(D(str(sheet.row(ii)[13].value)), fix_currency(sec_currency)),
                                            None, None))
                    ii += 1
                while sheet.row(ii)[1].value != 'Итого:':
                    ticker = fix_ticker(sheet.row(ii)[1].value)
                    account_inst = account.join(self.account_root, ticker)
                    result.append( data.Balance(meta, self.stmt_end + datetime.timedelta(days=1),
                                            account_inst,
                                            amount.Amount(D(str(sheet.row(ii)[10].value)), ticker),
                                            None, None))
                    ii += 1
            if (re.match(r'^Портфель по ценным бумагам', sheet.row(ii)[1].value) and 
                    sheet.row(ii)[6].value == 'на начало периода'):
                ii += 3
                while sheet.row(ii)[1].value != 'Итого:':
                    if re.match('.*\(в пути\)', sheet.row(ii)[1].value):
                        ii += 1
                        continue
                    ticker = fix_ticker(sheet.row(ii)[1].value)
                    account_inst = account.join(self.account_root, ticker)
                    result.append( data.Balance(meta, self.stmt_end + datetime.timedelta(days=1),
                                            account_inst,
                                            amount.Amount(D(str(sheet.row(ii)[10].value)), ticker),
                                            None, None))
                    ii += 1
        return result

    def get_cashflow(self, book, sheet, index, file):
        ''' Will parse broker report for all cash operations (deposits, drawback, fees, dividends)
            In: XLS sheet, index of section title(1.1.)
            Out: list of transactions -- empty if there is none
        '''
        result = []
        acc = ''
        dedup = []
        # find beggining of next block
        ii = index + 1
        while ii<sheet.nrows-1:
            # check if it's not the next section's head
            if (re.match('^1\.3', sheet.row(ii)[1].value) or sheet.row(ii)[1].value=='3. Активы:' 
                    or re.match('^2\.1', sheet.row(ii)[1].value) or re.match('^2\.3', sheet.row(ii)[1].value)):
                break

            if sheet.row(ii)[1].value[:6] == '1.1.1.':
                acc_choice = self.account_cash
                acc = self.account_cash
            if sheet.row(ii)[1].value[:6] == '1.1.2.':
                acc_choice = self.account_currencyexchange
                acc = self.account_currencyexchange

            xfx = sheet.cell_xf_index(ii, 1)
            xf = book.xf_list[xfx]
            bgx = xf.background.pattern_colour_index
            if bgx != 24:
                ii +=1
                continue
            
            tt = sheet.row(ii-1)[1].value
            if tt[:11] == 'Валюта цены':
                trn_currency = fix_currency(tt[tt.find('=')+2:tt.find(',')])
            else: 
                trn_currency = fix_currency(tt)
            ii += 1 # skip table head
            while ii<sheet.nrows-1:
                if sheet.row(ii)[2].value == 'Итого:':
                    ii += 1
                    continue
                if sheet.row(ii)[1].value[:15] == 'Итого по валюте':
                    ii += 4
                    break

                trn_date = datetime.datetime.strptime(sheet.row(ii)[1].value, '%d.%m.%y').date()
                try:
                    acc = self.exchanges[sheet.row(ii)[12].value]
                except KeyError:
                    acc = acc_choice
                meta = data.new_metadata(file.name, ii)
                if sheet.row(ii)[2].value == 'Приход ДС':
                    amt = amount.Amount(D(str(sheet.row(ii)[6].value)), trn_currency)
                    txn = data.Transaction(
                        meta, trn_date, self.FLAG, None, sheet.row(ii)[2].value, data.EMPTY_SET, {trn_date}, [
                            data.Posting(acc, amt, None, None, None,
                                            None),
                            data.Posting(self.account_external, -amt, None, None, None,
                                            None),
                        ])
                    result.append(txn)
                elif sheet.row(ii)[2].value == 'Вывод ДС':
                    amt = amount.Amount(D(str(sheet.row(ii)[7].value)), trn_currency)
                    txn = data.Transaction(
                        meta, trn_date, self.FLAG, None, sheet.row(ii)[2].value, data.EMPTY_SET, {trn_date}, [
                            data.Posting(acc, -amt, None, None, None,
                                            None),
                            data.Posting(self.account_external, amt, None, None, None,
                                            None),
                        ])
                    result.append(txn)
                elif sheet.row(ii)[2].value == 'Переводы между площадками':
                    try:
                        acc1 = self.exchanges[sheet.row(ii)[11].value]
                        acc2 = self.exchanges[sheet.row(ii)[13].value]
                    except KeyError:
                        try:
                            acc1 = self.exchanges[sheet.row(ii)[10].value]
                            acc2 = self.exchanges[sheet.row(ii)[12].value]
                        except KeyError:
                            acc1 = self.exchanges[sheet.row(ii)[12].value]
                            acc2 = self.exchanges[sheet.row(ii)[14].value]

                    if sheet.row(ii)[6].value:
                        x = sheet.row(ii)[6].value
                        amt = amount.Amount(D(str(sheet.row(ii)[6].value)), trn_currency)
                    else:
                        x = -sheet.row(ii)[7].value
                        amt = amount.Amount(-D(str(sheet.row(ii)[7].value)), trn_currency)
                    
                    dd = [acc1, acc2, x] if x>0 else [acc2, acc1, -x]
                    try:
                        dedup.index(dd)
                        dedup.remove(dd)
                        ii += 1
                        continue
                    except ValueError:
                        dedup.append(dd)

                    txn = data.Transaction(
                        meta, trn_date, self.FLAG, None, sheet.row(ii)[2].value, data.EMPTY_SET, {trn_date}, [
                            data.Posting(acc1, amt, None, None, None,
                                            None),
                            data.Posting(acc2, -amt, None, None, None,
                                            None),
                        ])
                    result.append(txn)
                elif sheet.row(ii)[2].value == 'Дивиденды':
                    # unfortunately we don't have ticker info for dividends
                    amt = amount.Amount(D(str(sheet.row(ii)[6].value)), trn_currency)
                    txn = data.Transaction(
                        meta, trn_date, self.FLAG, None, sheet.row(ii)[2].value, data.EMPTY_SET, {trn_date}, [
                            data.Posting(acc, amt, None, None, None,
                                            None),
                            data.Posting(self.account_dividends, -amt, None, None, None,
                                            None),
                        ])
                    result.append(txn)
                elif sheet.row(ii)[2].value in ['Урегулирование сделок','Вознаграждение за обслуживание счета депо', 'Хранение ЦБ',
                                                'Вознаграждение компании', 'Quik','Оплата за вывод денежных средств', 
                                                'Комиссия за займы "овернайт ЦБ"', 'Урегулирование сделок по Айсберг-заявкам']:
                    # unfortunately we don't have ticker info for fees
                    amt = amount.Amount(D(str(sheet.row(ii)[7].value)), trn_currency)
                    txn = data.Transaction(
                        meta, trn_date, self.FLAG, None, sheet.row(ii)[2].value, data.EMPTY_SET, {trn_date}, [
                            data.Posting(acc, -amt, None, None, None,
                                            None),
                            data.Posting(self.account_fees, amt, None, None, None,
                                            None),
                        ])
                    result.append(txn)
                elif sheet.row(ii)[2].value in ['Займы "овернайт"', 'Проценты по займам "овернайт"', 'Проценты по займам "овернайт ЦБ"',
                                                'НКД от операций', 'НДФЛ', 'Подоходный налог']:
                    if sheet.row(ii)[7].value:
                        amt = amount.Amount(D(str(sheet.row(ii)[7].value)), trn_currency) 
                        txn = data.Transaction(
                            meta, trn_date, self.FLAG, None, sheet.row(ii)[2].value, data.EMPTY_SET, {trn_date}, [
                            data.Posting(acc, -amt, None, None, None,
                                            None),
                            data.Posting(self.account_interest, amt, None, None, None,
                                            None),
                        ])
                        result.append(txn)
                    if sheet.row(ii)[6].value:
                        amt = amount.Amount(-D(str(sheet.row(ii)[6].value)), trn_currency)
                        txn = data.Transaction(
                            meta, trn_date, self.FLAG, None, sheet.row(ii)[2].value, data.EMPTY_SET, {trn_date}, [
                                data.Posting(acc, -amt, None, None, None,
                                                None),
                                data.Posting(self.account_interest, amt, None, None, None,
                                                None),
                            ])
                        result.append(txn)

                ii += 1
                if sheet.row(ii)[1].value[:5] == 'Итого':
                    ii += 4 # skip notification after table
                    break
            # it's next section - lets find if this is fees
            if sheet.row(ii)[1].value in ['1.2. Займы "Овернайт":', '1.3. Удержанные сборы/штрафы (итоговые суммы):', 
                                        '1.2. Займы "Овернайт"/"Овернайт ГО":', '1.2. Займы:']:
                return result
        return result

    def extract(self, file):
        ''' Open XLS file and create directives
        '''
        entries = []
        index = 0
        workbook = xlrd.open_workbook(file.name, formatting_info=True)
        sheet = workbook.sheet_by_name('TDSheet')
        # extract broker report dates - row 2 col 5
        per = sheet.row(2)[5].value
        self.stmt_begin = datetime.datetime.strptime(per[2:12], '%d.%m.%Y').date()
        self.stmt_end = datetime.datetime.strptime(per[16:], '%d.%m.%Y').date()

        for index in range(sheet.nrows):
            if sheet.row(index)[1].value == r'1. Движение денежных средств': #'1.1. Движение денежных средств по совершенным сделкам:':
                cashflow = self.get_cashflow(workbook, sheet, index, file)
                entries += cashflow
            # Find section 2.1 - transactions completed in report's period
            if sheet.row(index)[1].value == r'2.1. Сделки:': 
                entries += self.get_transactions(workbook, sheet, index, file)
        
        if self.balance:
            entries += self.get_balance(sheet, file)
        return entries

    def get_transactions(self, book, sheet, index, file):
        # Cash and papers are described in different subsections index += 1
        entries = []
        acc = ''
        ii = index
        while ii<sheet.nrows-1:
            # find heading of table - using background color
            xfx = sheet.cell_xf_index(ii, 1)
            xf = book.xf_list[xfx]
            bgx = xf.background.pattern_colour_index
            if bgx != 24:
                ii +=1
                continue
            # Find section with stocks
            if sheet.row(ii-2)[1].value in ['Акция', 'АДР', 'Пай']:
                acc = self.account_cash
                ii += 1
                # Transactions table begins in row+3 (first ticker) and continues until blank line
                while sheet.row(ii)[1].value != '':
                    # read ticker
                    ticker = fix_ticker(sheet.row(ii)[1].value)
                    account_inst = account.join(self.account_root, ticker)
                    isin = sheet.row(ii)[7].value
                    title = sheet.row(ii)[8].value
                    ii += 1
                    #start to read transactions - pass to first transaction
                    while sheet.row(ii)[1].value[:5] != r'Итого':
                        trn_date = datetime.datetime.strptime(sheet.row(ii)[1].value, '%d.%m.%y').date()
                        trn_num = sheet.row(ii)[2].value #transaction id by broker
                        trn_currency = 'RUB' if sheet.row(ii)[11].value == 'Рубль' else sheet.row(ii)[11].value
                        # are we selling our buying? cols 4,5 - buying, cols 7,8 - selling
                        if sheet.row(ii)[4].value != '':
                            # we bought the ticker
                            # instantiate amount bought, 'currency' - ticker itself
                            meta = data.new_metadata(file.name, ii)
                            units_inst = amount.Amount(D(sheet.row(ii)[4].value), ticker) # amount of ticker bought
                            price = amount.Amount(-1*D(str(sheet.row(ii)[6].value)), trn_currency) # payment for transaction
                            cost = position.Cost(D(str(sheet.row(ii)[5].value)), trn_currency, None, None) # cost of single unit
                            #cost = amount.Amount(D(str(sheet.row(ii)[5].value)), trn_currency) # cost of single unit
                            #pos = position.Position(units_inst, cost)
                            txn = data.Transaction(
                                    meta, trn_date, self.FLAG, None, title, data.EMPTY_SET, {trn_num}, [
                                        data.Posting(acc, price, None, None, None, None),
                                        #data.Posting(self.account_fees, fees, None, None, None,
                                        #            None), # no fees in transaction description
                                        data.Posting(account_inst, units_inst, cost, None, None, None),
                                    ])
                            entries.append(txn)
                        elif sheet.row(ii)[7].value != '':
                            # we sold ticker
                            meta = data.new_metadata(file.name, ii)
                            units_inst = amount.Amount(-1*D(sheet.row(ii)[7].value), ticker) # amount of ticker sold
                            price = amount.Amount(D(str(sheet.row(ii)[9].value)), trn_currency) # payment for transaction
                            cost = amount.Amount(D(str(sheet.row(ii)[8].value)), trn_currency) # cost of single unit
                            account_gains = self.account_gains.format(ticker)
                            txn = data.Transaction(
                                    meta, trn_date, self.FLAG, None, title, data.EMPTY_SET, {trn_num}, [
                                        data.Posting(acc, price, None, None, None,
                                                        None),
                                        #data.Posting(self.account_fees, fees, None, None, None,
                                        #                None), # no fees associated with transaction
                                        data.Posting(account_inst, units_inst, NOCOST, cost, None,
                                                        None),
                                        data.Posting(account_gains, None, None, None, None,
                                                        None),
                                ])
                            entries.append(txn)
                        ii +=1
                    ii +=1
            # pass line with totals for current ticker
            elif sheet.row(ii-2)[1].value == 'Иностранная валюта':
                # Currency conversion
                acc = self.account_currencyexchange
                # First ticker in 3d line from subsection head
                ii += 1
                while sheet.row(ii)[1].value != '':
                    sold_currency = fix_currency(sheet.row(ii)[5].value)
                    bought_currency = fix_currency(sheet.row(ii)[8].value)
                    ticker = sheet.row(ii)[1].value
                    ii += 1
                    while sheet.row(ii)[1].value[:5] != r'Итого':
                        meta = data.new_metadata(file.name, ii)
                        trn_date = datetime.datetime.strptime(sheet.row(ii)[1].value, '%d.%m.%y').date()
                        trn_num = sheet.row(ii)[2].value #transaction id by broker
                        # check if we buy or sell
                        if sheet.row(ii)[4].value:
                            # we buy
                            conv_rate = amount.Amount(D(str(sheet.row(ii)[4].value)), 
                                                      bought_currency)
                            amount_bought = amount.Amount(D(sheet.row(ii)[5].value), sold_currency) # amount of ticker sold
                            price = amount.Amount(-1*D(str(sheet.row(ii)[6].value)), bought_currency) # payment for transaction,                            
                        else:
                            #we sell
                            conv_rate = amount.Amount(D(str(sheet.row(ii)[7].value)), 
                                                    bought_currency)
                            amount_bought = amount.Amount(-1*D(sheet.row(ii)[8].value), sold_currency) # amount of ticker sold
                            price = amount.Amount(D(str(sheet.row(ii)[9].value)), bought_currency) # payment for transaction
                        txn = data.Transaction(
                                    meta, trn_date, self.FLAG, None, ticker, data.EMPTY_SET, {trn_num}, [
                                        #data.Posting(acc, amount_bought, conv_rate, None, None, None),
                                        data.Posting(acc, amount_bought, None, conv_rate, None, None),
                                        data.Posting(acc, price, None, None, None, None),
                                    ])
                        entries.append(txn)
                        ii +=1
                    ii +=1
            elif sheet.row(ii-2)[1].value == 'Облигация':
                acc = self.account_cash
                ii += 1
                # Transactions table begins in row+3 (first ticker) and continues until blank line
                while sheet.row(ii)[1].value != '':
                    # read ticker
                    ticker = fix_ticker(sheet.row(ii)[1].value)
                    account_inst = account.join(self.account_root, ticker)
                    isin = sheet.row(ii)[7].value
                    title = sheet.row(ii)[8].value
                    ii += 1
                    #start to read transactions - pass to first transaction
                    while sheet.row(ii)[1].value[:5] != r'Итого':
                        trn_date = datetime.datetime.strptime(sheet.row(ii)[1].value, '%d.%m.%y').date()
                        trn_num = sheet.row(ii)[2].value #transaction id by broker
                        trn_currency = 'RUB' if sheet.row(ii)[13].value == 'Рубль' else sheet.row(ii)[13].value
                        # are we selling our buying? cols 4,5 - buying, cols 7,8 - selling
                        if sheet.row(ii)[4].value != '':
                            # we bought the ticker
                            # instantiate amount bought, 'currency' - ticker itself
                            meta = data.new_metadata(file.name, ii)
                            units_inst = amount.Amount(D(sheet.row(ii)[4].value), ticker) # amount of ticker bought
                            price = amount.Amount(-1*D(str(sheet.row(ii)[6].value)), trn_currency) # payment for transaction
                            cost = position.Cost(D(str(sheet.row(ii)[5].value))*10, trn_currency, None, None) # cost of single unit
                            txn = data.Transaction(
                                    meta, trn_date, self.FLAG, None, title, data.EMPTY_SET, {trn_num}, [
                                        data.Posting(acc, price, None, None, None, None),
                                        #data.Posting(self.account_fees, fees, None, None, None,
                                        #            None), # no fees in transaction description
                                        data.Posting(account_inst, units_inst, cost, None, None, None),
                                    ])
                            entries.append(txn)
                        elif sheet.row(ii)[8].value != '':
                            # we sold ticker
                            meta = data.new_metadata(file.name, ii)
                            units_inst = amount.Amount(-1*D(sheet.row(ii)[8].value), ticker) # amount of ticker sold
                            price = amount.Amount(D(str(sheet.row(ii)[10].value)), trn_currency) # payment for transaction
                            cost = amount.Amount(D(str(sheet.row(ii)[9].value))*10, trn_currency) # cost of single unit
                            account_gains = self.account_gains.format(ticker)
                            txn = data.Transaction(
                                    meta, trn_date, self.FLAG, None, title, data.EMPTY_SET, {trn_num}, [
                                        data.Posting(acc, price, None, None, None,
                                                        None),
                                        #data.Posting(self.account_fees, fees, None, None, None,
                                        #                None), # no fees associated with transaction
                                        data.Posting(account_inst, units_inst, NOCOST, cost, None,
                                                        None),
                                        data.Posting(account_gains, None, None, None, None,
                                                        None),
                                ])
                            entries.append(txn)
                        ii +=1
                    ii +=1
            
            ii +=1
    
        return entries
