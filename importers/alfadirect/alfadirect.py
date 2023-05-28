''' Importer for Alfa Direct broker - broker reports from XLS files
    TODO: chcp 65001 & set PYTHONIOENCODING=utf-8
'''
import xlrd
import datetime
import re
import os
import csv
from importlib.resources import open_text

from beancount.core.amount import D
from beancount.core import data
from beancount.core import flags
from beancount.core import account
from beancount.core import amount
from beancount.core import position
from beancount.ingest import importer
from xlrd.biffh import XLRDError
from xlrd.xldate import xldate_as_datetime
from rich import print

NOCOST = position.CostSpec(None, None, None, None, None, None)

class Importer(importer.ImporterProtocol):
    '''An importer for Alfa Direct XLS files'''

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

        self.isindb = {}
        self.isincur = {} # dictionary of isin code with corresponding asset base currencies
        self.isinbond = {} # True if bond
        self.isinfv = {} # Face value
        self.exchanges = {
                            'РЦБ':self.account_cash,
                            'Вал. рынок':self.account_currencyexchange,
                            'Фонд. рынок':self.account_cash
                        }
        self.cur = ['c Доллар США', 'c Евро']

    def check_alfadirect(self, xlsfile, genid):
        ''' * Verify if file from Alfa Direct broker
        '''
        workbook = xlrd.open_workbook(xlsfile, logfile=open(os.devnull, 'w'))
        try:
            sheet = workbook.sheet_by_index(0)
        except XLRDError:
            return False # No correct sheet in file
        # No broker name as string - only logo TODO: check logo?
        # broker_name = sheet.row_values(0, start_colx=1, end_colx=None)
        # if not re.match(r'ООО "Компания БКС"', broker_name[4]):
        #     return False
        # Check general agreement id
        if sheet.row(5)[8].value.find(genid) == -1:
            return False
        return True

    def load_isin(self):
        ''' Load file with ISIN database used to find out tickers of assets.
            It is possible to download up-to-date ISIN database
            from: https://www.moex.com/msn/stock-instruments
            TODO: fix loader to process original file (now I delete blank lines)
        '''
        # get file from module's directory
        fn = os.path.join(os.path.dirname(__file__), 'moex_db.csv')
        with open(fn, newline='', encoding='cp1251') as f:
            r = csv.reader(f, delimiter=';')
            line_count = 0
            for row in r:
                if line_count == 0:
                    line_count += 1
                    continue
                try:
                    self.isindb[row[4]] = row[0] # ticker
                    self.isincur[row[4]] = row[8] # currency
                    self.isinfv[row[4]] = float(row[7].replace(',', '.')) # face value
                    if (row[3] == r'ОФЗ' or re.match(r'.*[Оо]блигации.*', row[3])):
                        self.isinbond[row[4]] = True
                    line_count += 1
                except IndexError:
                    break
                except ValueError:
                    continue
        self.isindb['US29355E2081'] = 'ENPLADR'
        self.isindb['JE00B5BCW814'] = 'RUAL'

    def identify(self, file):
        ''' * Match if the filename is broker report from Alfa Direct
        '''
        # Check if it isn't LibreOffice lock file
        if re.match(r"\.~lock", os.path.basename(file.name)):
            return False
        # Match extension - should be XLS
        if not re.match(r"\.xls", os.path.splitext(file.name)[1]):
            return False
        # Check file name format and correct general agreement id
        if not re.match(r"Брокерский\+"+self.general_agreement_id, os.path.basename(file.name)):
            return False
        # Check if we have broker name in header and check general agreement id
        return self.check_alfadirect(file.name, self.general_agreement_id)

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
        ''' Open XLS file and create directives
        '''
        entries = []
        workbook = xlrd.open_workbook(file.name, logfile=open(os.devnull, 'w'))
        # 1. Load end of report balances: 'Динамика позиций' sheet
        sheet = workbook.sheet_by_name('Динамика позиций') #TODO change sheet to 0 (first sheet in workbook)
        # extract broker report dates - row 3 col 8
        per = sheet.row(4)[8].value
        self.stmt_begin = datetime.datetime.strptime(per[:10], '%d.%m.%Y').date()
        self.stmt_end = datetime.datetime.strptime(per[13:], '%d.%m.%Y').date()
        del sheet

        self.load_isin()
        # for index in range(sheet.nrows):
        #     if sheet.row(index)[1].value == r'1. Движение денежных средств': #'1.1. Движение денежных средств по совершенным сделкам:':
        #         cashflow = self.get_cashflow(workbook, sheet, index, file)
        #         entries += cashflow
        #     # Find section 2.1 - transactions completed in report's period
        #     if sheet.row(index)[1].value == r'2.1. Сделки:': 
        #         entries += self.get_transactions(workbook, sheet, index, file)
        
        if self.balance:
            entries += self.get_balance(workbook, file)

        entries += self.get_trn(workbook, file)
        entries += self.get_cflow(workbook, file)

        return entries

    def get_balance(self, workbook, file):
        ''' * Parse broker report for end of period balances - cash and assets
            In: XLS sheet, file
            Out: list of transactions
        '''
        try:
            sheet = workbook.sheet_by_name('Динамика позиций')
        except XLRDError:
            return None # No balances sheet in file

        result = []
        ii = 0
        market = 0
        asset_type = 0
        #acc = ''
        while sheet.row(ii)[6].value != 'Стоимость всех позиций, руб.':
            if sheet.row(ii)[6].value[:5] == 'Актив':
                hh = sheet.row(ii) # header of the table
                jj = 6
                while True:
                    if hh[jj].value == 'хранения':
                        market = jj
                        break
                    jj += 1
            # check if it's not the next section's head
            if sheet.row(ii)[2].value == 'Валюта':
                asset_type = 1 # Currency
            elif sheet.row(ii)[2].value == 'Акции' or sheet.row(ii)[2].value == 'Прочее':
                asset_type = 2 # Stocks and ADRs
            if sheet.row(ii)[6].value:
                meta = data.new_metadata(file.name, ii)
                if asset_type == 1:
                    # line with currencies
                    ticker = sheet.row(ii)[6].value
                    acc = self.exchanges[sheet.row(ii)[market].value]
                    result.append(data.Balance(meta, 
                                    self.stmt_end + datetime.timedelta(days=1),
                                    acc,
                                    amount.Amount(D(str(sheet.row(ii)[15].value)), ticker),
                                    None, None))
                elif asset_type == 2:
                    # line with stocks
                    ticker = self.isindb[sheet.row(ii+1)[6].value]
                    account_inst = account.join(self.account_root, ticker)
                    amt = amount.Amount(D(str(sheet.row(ii)[15].value)), ticker)
                    result.append(data.Balance(meta, 
                                    self.stmt_end + datetime.timedelta(days=1),
                                    account_inst,
                                    amt,
                                    None, None))
                    # each stock adr ADR occupy 2 lines so skip additional line
                    ii += 1
            ii += 1

        return result

    def get_trn(self, workbook, file):
        ''' Parse broker report for all assets transactions
        '''
        try:
            sheet = workbook.sheet_by_name('Завершенные сделки')
        except XLRDError:
            return None # No balances sheet in file

        result = []
        ii = 0

        # Find beggining of table
        while sheet.row(ii)[4].value != 'Завершенные сделки':
            ii += 1
        if sheet.row(ii+2)[4].value =='За указанный период сделок нет':
            return []
        ii +=2
        jj = 0
        while sheet.row(ii)[jj].value != 'Дата\nрасчетов':
            jj += 1
        date_col = jj
        while '\nМесто\nзаключения\nсделки' not in sheet.row(ii)[jj].value:
            jj += 1
        market_col = jj
        while sheet.row(ii)[jj].value != 'ISIN/рег.код':
            jj += 1
        isin_col = jj
        while sheet.row(ii)[jj].value != 'Актив':
            jj += 1
        ticker_col = jj
        while sheet.row(ii)[jj].value != '\nКуплено\n(продано),\nшт.':
            jj += 1
        amt_col = jj
        while sheet.row(ii)[jj].value != 'Цена':
            jj += 1
        conv_col = jj
        while 'Сумма\nсделки' not in sheet.row(ii)[jj].value:
            jj += 1
        price_col = jj
        while 'Вал.' not in sheet.row(ii)[jj].value:
            jj += 1
        cur_col = jj


        ii +=1 # pass to first row of the table
        while sheet.row(ii)[4].value != '':
            meta = data.new_metadata(file.name, ii)
            trn_date = datetime.datetime.strptime(sheet.row(ii)[date_col].value[:10], '%d.%m.%Y').date()
            ticker_cur = sheet.row(ii)[cur_col].value

            # currency exchange
            # if sheet.row(ii)[market_col].value == 'МБ ВР':
            #     ticker = sheet.row(ii)[ticker_col].value
            #     acc = self.account_currencyexchange
            #     amt = amount.Amount(D(sheet.row(ii)[amt_col].value), ticker)
            #     sign = -1 if sheet.row(ii)[amt_col].value>=0 else 1
            #     conv_rate = amount.Amount(D(str(sheet.row(ii)[conv_col].value)), ticker_cur)
            #     price = amount.Amount(sign*D(str(sheet.row(ii)[price_col].value)), ticker_cur)
                
            #     txn = data.Transaction(
            #                         meta, trn_date, self.FLAG, None, ticker, data.EMPTY_SET, data.EMPTY_SET, 
            #                         [
            #                             data.Posting(acc, amt, None, conv_rate, None, None),
            #                             data.Posting(acc, price, None, None, None, None),
            #                         ])
            #     result.append(txn)

            # assets transactions
            if sheet.row(ii)[market_col].value == 'МБ ФР' or sheet.row(ii)[market_col].value == 'КЦ МФБ':
                try:
                    ticker = self.isindb[sheet.row(ii)[isin_col].value]
                except KeyError:
                    ticker = sheet.row(ii)[isin_col].value
                desc = sheet.row(ii)[12].value #TODO column?
                amt = amount.Amount(D(sheet.row(ii)[amt_col].value), ticker)
                sign = -1 if sheet.row(ii)[amt_col].value>=0 else 1
                price = amount.Amount(sign*D(str(sheet.row(ii)[price_col].value)), ticker_cur)
                account_inst = account.join(self.account_root, ticker)
                '''
                isbond = False
                if sheet.row(ii)[20].value:
                    # bonds - we need to calculate cost differently: take into account face value
                    #print(ticker, sheet.row(ii)[20].value)
                    isbond = True
                    try:
                        cost = position.Cost(D(str(sheet.row(ii)[17].value*self.isinfv[sheet.row(ii)[11].value]/100)), 
                                            ticker_cur, None, None)
                    except KeyError:
                        # no such ticker in DB - assume 1000 face value
                        cost = position.Cost(D(str(sheet.row(ii)[17].value*10)), ticker_cur, None, None)
                else:
                    cost = position.Cost(D(str(sheet.row(ii)[17].value)), ticker_cur, None, None)
                '''
                if sign == -1:
                    # we bought ticket
                    txn = data.Transaction(
                                        meta, trn_date, self.FLAG, None, desc, data.EMPTY_SET, data.EMPTY_SET, 
                                        [
                                            data.Posting(self.account_cash, price, None, None, None, None),
                                            #data.Posting(account_inst, amt, cost, None, None, None),
                                            data.Posting(account_inst, amt, NOCOST, None, None, None),
                                        ])
                else:
                    # we sold ticket
                    account_gains = self.account_gains.format(ticker)
                    '''
                    if isbond:
                        try:
                            cost = amount.Amount(D(str(sheet.row(ii)[17].value*self.isinfv[sheet.row(ii)[11].value]/100)), ticker_cur)
                        except KeyError:
                            cost = amount.Amount(D(str(sheet.row(ii)[17].value*10)), ticker_cur)
                    else:
                        cost = amount.Amount(D(str(sheet.row(ii)[17].value)), ticker_cur)
                    '''
                    txn = data.Transaction(
                                    meta, trn_date, self.FLAG, None, desc, data.EMPTY_SET, data.EMPTY_SET, 
                                    [
                                        data.Posting(self.account_cash, price, None, None, None, None),
                                        #data.Posting(account_inst, amt, NOCOST, cost, None, None),
                                        data.Posting(account_inst, amt, NOCOST, None, None, None),
                                        data.Posting(account_gains, None, None, None, None, None),
                                    ])
                result.append(txn)

            # move to next line    
            ii +=1

        return result

    def get_cflow(self, workbook, file):
        ''' Parse broker report for all cash transactions
        '''
        try:
            sheet = workbook.sheet_by_name(' Движение ДС') # Note space in sheet name
        except XLRDError:
            return None # No balances sheet in file

        result = []
        currconv = []
        currconvamt = {}
        ii = 0

        while ii < sheet.nrows-1:
        
            if (sheet.row(ii)[2].value == 'Фондовый рынок' and 
                    sheet.row(ii+1)[2].value !='За указанный период движений денежных средств нет'):
                ii +=6 # pass to first row of the table
                cur = self.proc_header(sheet.row(ii-1))
                ncur = len(cur)
                #print(ncur, cur)
                while sheet.row(ii)[10].value != 'Итого:':
                    meta = data.new_metadata(file.name, ii)
                    if sheet.row(ii)[2].value !='': # keep last date that was in 2d column
                        trn_date = xldate_as_datetime(sheet.row(ii)[2].value, 0).date()
                    #print(trn_date, sheet.row(ii)[9].value)
                    for c in cur:
                        if sheet.row(ii)[c[1]].value !='':
                            trn_cur = c[0]
                            trn_amt = sheet.row(ii)[c[1]].value
                            break
                    amt = amount.Amount(D(str(trn_amt)), trn_cur)
                    desc = sheet.row(ii)[10].value
                    if sheet.row(ii)[9].value == 'Комиссия':
                        if desc == 'по сделке РЕПО':
                            txn = data.Transaction(
                            meta, trn_date, self.FLAG, None, sheet.row(ii)[9].value+' '+sheet.row(ii)[10].value, data.EMPTY_SET, {trn_date}, [
                                data.Posting(self.account_cash, amt, None, None, None,
                                                None),
                                data.Posting(self.account_repo, -amt, None, None, None,
                                                None),
                            ])
                        else:
                            txn = data.Transaction(
                                meta, trn_date, self.FLAG, None, sheet.row(ii)[9].value+' '+sheet.row(ii)[10].value, data.EMPTY_SET, {trn_date}, [
                                    data.Posting(self.account_cash, amt, None, None, None,
                                                    None),
                                    data.Posting(self.account_fees, -amt, None, None, None,
                                                    None),
                                ])
                        result.append(txn)
                    if sheet.row(ii)[9].value[:17] == 'Расчеты по сделке' and desc.find('РЕПО ч.')!=-1:
                        txn = data.Transaction(
                                meta, trn_date, self.FLAG, None, sheet.row(ii)[9].value+' '+sheet.row(ii)[10].value, data.EMPTY_SET, {trn_date}, [
                                    data.Posting(self.account_cash, amt, None, None, None,
                                                    None),
                                    data.Posting(self.account_repo, -amt, None, None, None,
                                                    None),
                                ])
                        result.append(txn)
                    if sheet.row(ii)[9].value[:17] == 'Расчеты по сделке' and desc in self.cur:
                        opertime = xldate_as_datetime(sheet.row(ii)[6].value, 0)
                        try:
                            price = currconvamt[(opertime, desc)]
                            if price.currency == amt.currency:
                                raise ValueError
                            currconv.remove((opertime, desc))
                            rate = amount.Amount(abs(amt.number/price.number), amt.currency)
                            #print("R:", rate)
                            txn = data.Transaction(
                                    meta, trn_date, self.FLAG, None, 'Расчеты по сделке ' + desc, data.EMPTY_SET, data.EMPTY_SET, 
                                    [
                                        data.Posting(self.account_cash, amt, None, None, None, None),
                                        data.Posting(self.account_cash, price, None, rate, None, None),
                                    ])
                            result.append(txn)
                        except (ValueError, KeyError):
                            currconv.append((opertime, desc))
                            currconvamt[(opertime, desc)] = amt

                    
                    if sheet.row(ii)[9].value == 'Перевод':
                        if desc == 'Между рынками':
                            acc = self.account_currencyexchange
                        elif (desc[:9] == 'Дивиденды' or desc.find('погашение купона')!=-1 or 
                                desc.find('INTEREST PAYMENT')!=-1 or desc.find('Cash Dividend')!=-1):
                            acc = self.account_dividends
                        else:
                            acc = self.account_external
                        
                        txn = data.Transaction(
                            meta, trn_date, self.FLAG, None, sheet.row(ii)[9].value+' '+desc, data.EMPTY_SET, {trn_date}, [
                                data.Posting(self.account_cash, amt, None, None, None,
                                                None),
                                data.Posting(acc, -amt, None, None, None,
                                                None),
                            ])
                        result.append(txn)
                    elif sheet.row(ii)[9].value == 'НДФЛ':
                        txn = data.Transaction(
                            meta, trn_date, self.FLAG, None, sheet.row(ii)[9].value+' '+desc, data.EMPTY_SET, {trn_date}, [
                                data.Posting(self.account_cash, amt, None, None, None,
                                                None),
                                data.Posting(self.account_external, -amt, None, None, None,
                                                None),
                            ])
                        result.append(txn)

                    # next line for Фондовый рынок
                    ii += 1
        
            if (sheet.row(ii)[2].value == 'Валютный рынок'  and 
                    sheet.row(ii+1)[2].value !='За указанный период движений денежных средств нет'):
                ii +=6 # pass to first row of the table
                cur = self.proc_header(sheet.row(ii-1))
                ncur = len(cur)
                #print(ncur, cur)
                
                while sheet.row(ii)[10].value != 'Итого:':
                    meta = data.new_metadata(file.name, ii)
                    if sheet.row(ii)[2].value !='': # keep last date that was in 2d column
                        trn_date = xldate_as_datetime(sheet.row(ii)[2].value, 0).date()
                    #print(trn_date, sheet.row(ii)[9].value)
                    for c in cur:
                        if sheet.row(ii)[c[1]].value !='':
                            trn_cur = c[0]
                            trn_amt = sheet.row(ii)[c[1]].value
                            break
                    amt = amount.Amount(D(str(trn_amt)), trn_cur)
                    desc = sheet.row(ii)[10].value
                    if sheet.row(ii)[9].value == 'Комиссия':
                        # if desc == 'по сделке РЕПО':
                        #     txn = data.Transaction(
                        #     meta, trn_date, self.FLAG, None, sheet.row(ii)[9].value+' '+sheet.row(ii)[10].value, data.EMPTY_SET, {trn_date}, [
                        #         data.Posting(self.account_cash, amt, None, None, None,
                        #                         None),
                        #         data.Posting(self.account_repo, -amt, None, None, None,
                        #                         None),
                        #     ])
                        # else:
                        txn = data.Transaction(
                            meta, trn_date, self.FLAG, None, sheet.row(ii)[9].value+' '+sheet.row(ii)[10].value, data.EMPTY_SET, {trn_date}, [
                                data.Posting(self.account_currencyexchange, amt, None, None, None,
                                                None),
                                data.Posting(self.account_fees, -amt, None, None, None,
                                                None),
                            ])
                        result.append(txn)
                    if sheet.row(ii)[9].value == 'Расчеты по сделке' and desc in self.cur:
                        opertime = xldate_as_datetime(sheet.row(ii)[6].value, 0)
                        #print(sheet.row(ii)[9].value, ' ', desc, opertime)
                        try:
                            price = currconvamt[(opertime, desc)]
                            if price.currency == amt.currency:
                                raise ValueError
                            currconv.remove((opertime, desc))
                            rate = amount.Amount(abs(amt.number/price.number), amt.currency)
                            txn = data.Transaction(
                                    meta, trn_date, self.FLAG, None, 'Расчеты по сделке ' + desc, data.EMPTY_SET, data.EMPTY_SET, 
                                    [
                                        data.Posting(self.account_currencyexchange, amt, None, None, None, None),
                                        data.Posting(self.account_currencyexchange, price, None, rate, None, None),
                                    ])
                            #print(txn)
                            result.append(txn)
                        except (ValueError, KeyError):
                            #print(sheet.row(ii)[9].value, ' ', desc, opertime)
                            currconv.append((opertime, desc))
                            currconvamt[(opertime, desc)] = amt
                    if sheet.row(ii)[9].value == 'Перевод':
                        if desc == 'Между рынками':
                            ii += 1
                            continue # we processed it in previous section
                        else:
                            acc = self.account_external
                        
                        txn = data.Transaction(
                            meta, trn_date, self.FLAG, None, sheet.row(ii)[9].value+' '+desc, data.EMPTY_SET, {trn_date}, [
                                data.Posting(self.account_currencyexchange, amt, None, None, None,
                                                None),
                                data.Posting(acc, -amt, None, None, None,
                                                None),
                            ])
                        result.append(txn)

                    ii +=1

            # Next line
            ii += 1
        return result

    def proc_header(self, header):
        ''' process header of the table
        input: two lines of header
            return list of currencies
        '''
        ii = 14 # first column with currency
        result = []
        while header[ii].value != 'ден. позиций':
            if header[ii].value != '':
                result.append([header[ii].value, ii])
            ii += 1
        
        return result

    def get_cashflow(self, book, sheet, index, file):
        ''' Parse broker report for all cash operations (deposits, drawback, fees, dividends)
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
