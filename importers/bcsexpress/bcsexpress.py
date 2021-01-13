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
from beancount.core import account
from beancount.core import amount
from beancount.core import position
from beancount.ingest import importer
from xlrd.biffh import XLRDError

def fix_ticker(ticker):
    if ticker == 'TGK1_01':
        return 'TGKA'
    return ticker

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
                 account_dividends,
                 account_fees, 
                 account_gains):
        self.general_agreement_id = general_agreement_id
        self.account_root = account_root
        self.account_cash = account_cash
        self.account_dividends = account_dividends
        self.account_fees = account_fees
        self.account_gains = account_gains

    def identify(self, file):
        ''' Match if the filename is broker report from BCS Express
        '''
        if not re.match(r".*B[_ ]k-.*", path.basename(file.name)):
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

    def extract(self, file):
        ''' Open XLS file and create directives
        '''
        entries = []
        index = 0
        workbook = xlrd.open_workbook(file.name)
        sheet = workbook.sheet_by_name('TDSheet')
        # extract broker report dates - row 2 col 5
        per = sheet.row(2)[5].value
        stmt_begin = datetime.datetime.strptime(per[2:12], '%d.%m.%Y').date()
        stmt_end = datetime.datetime.strptime(per[16:], '%d.%m.%Y').date()

        for index in range(sheet.nrows):
            # TODO describe what meta is
            meta = data.new_metadata(file.name, index)
            # Find section 2.1 - transactions completed in report's period
            if sheet.row(index)[1].value[:11] != r'2.1. Сделки':
                continue
            # TODO: Cash and papers are described in different subsections
            # Transactions table begins in row+5 (first ticker) and continues until blank line
            index += 5
            while sheet.row(index)[1].value != '':
                # read ticker
                ticker = fix_ticker(sheet.row(index)[1].value)
                account_inst = account.join(self.account_root, ticker)
                isin = sheet.row(index)[7].value
                title = sheet.row(index)[8].value
                index += 1 #start to read transactions - pass to first transaction
                while sheet.row(index)[1].value[:5] != r'Итого':
                    trn_date = datetime.datetime.strptime(sheet.row(index)[1].value, '%d.%m.%y').date()
                    trn_num = sheet.row(index)[2].value #transaction id by broker
                    trn_currency = 'RUB' if sheet.row(index)[11].value == 'Рубль' else sheet.row(index)[11].value
                    # are we selling our buying? cols 4,5 - buying, cols 7,8 - selling
                    if sheet.row(index)[4].value != '':
                        # we bought the ticker
                        # instantiate amount bought, 'currency' - ticker itself
                        units_inst = amount.Amount(D(sheet.row(index)[4].value), ticker) # amount of ticker bought
                        price = amount.Amount(-1*D(str(sheet.row(index)[6].value)), trn_currency) # payment for transaction
                        cost = position.Cost(D(str(sheet.row(index)[5].value)), trn_currency, None, None) # cost of single unit
                        txn = data.Transaction(
                            meta, trn_date, self.FLAG, None, title, data.EMPTY_SET, {trn_num}, [
                                data.Posting(self.account_cash, price, None, None, None,
                                            None),
                                #data.Posting(self.account_fees, fees, None, None, None,
                                #            None), # no fees in transaction description
                                data.Posting(account_inst, units_inst, cost, None, None,
                                            None),
                            ])
                        entries.append(txn)
                    elif sheet.row(index)[7].value != '':
                        # we sold ticker
                        units_inst = amount.Amount(-1*D(sheet.row(index)[7].value), ticker) # amount of ticker sold
                        price = amount.Amount(D(str(sheet.row(index)[9].value)), trn_currency) # payment for transaction
                        cost = amount.Amount(D(str(sheet.row(index)[8].value)), trn_currency) # cost of single unit
                        account_gains = self.account_gains.format(ticker)
                        txn = data.Transaction(
                            meta, trn_date, self.FLAG, None, title, data.EMPTY_SET, {trn_num}, [
                                data.Posting(self.account_cash, price, None, None, None,
                                                None),
                                #data.Posting(self.account_fees, fees, None, None, None,
                                #                None), # no fees associated with transaction
                                data.Posting(account_inst, units_inst, None, cost, None,
                                                None),
                                data.Posting(account_gains, None, None, None, None,
                                                None),
                        ])
                        entries.append(txn)
                    index +=1

                index +=1 # pass line with totals for current ticker

            # date = parse(row['DATE']).date()
            # rtype = row['TYPE']
            # link = "ut{0[REF #]}".format(row)
            # desc = "({0[TYPE]}) {0[DESCRIPTION]}".format(row)
            # units = amount.Amount(D(row['AMOUNT']), self.currency)
            # fees = amount.Amount(D(row['FEES']), self.currency)
            # other = amount.add(units, fees)

            # if rtype == 'XFER':
            #     assert fees.number == ZERO
            #     txn = data.Transaction(
            #         meta, date, self.FLAG, None, desc, data.EMPTY_SET, {link}, [
            #             data.Posting(self.account_cash, units, None, None, None,
            #                             None),
            #             data.Posting(self.account_external, -other, None, None, None,
            #                             None),
            #         ])

            # elif rtype == 'DIV':
            #     assert fees.number == ZERO

            #     # Extract the instrument name from its description.
            #     match = re.search(r'~([A-Z]+)$', row['DESCRIPTION'])
            #     if not match:
            #         logging.error("Missing instrument name in '%s'", row['DESCRIPTION'])
            #         continue
            #     instrument = match.group(1)
            #     account_dividends = self.account_dividends.format(instrument)

            #     txn = data.Transaction(
            #         meta, date, self.FLAG, None, desc, data.EMPTY_SET, {link}, [
            #             data.Posting(self.account_cash, units, None, None, None, None),
            #             data.Posting(account_dividends, -other, None, None, None, None),
            #         ])

            # elif rtype in ('BUY', 'SELL'):

            #     # Extract the instrument name, number of units, and price from
            #     # the description. That's just what we're provided with (this is
            #     # actually realistic of some data from some institutions, you
            #     # have to figure out a way in your parser).
            #     match = re.search(r'\+([A-Z]+)\b +([0-9.]+)\b +@([0-9.]+)',
            #                         row['DESCRIPTION'])
            #     if not match:
            #         logging.error("Missing purchase infos in '%s'", row['DESCRIPTION'])
            #         continue
            #     instrument = match.group(1)
            #     account_inst = account.join(self.account_root, instrument)
            #     units_inst = amount.Amount(D(match.group(2)), instrument)
            #     rate = D(match.group(3))

            #     if rtype == 'BUY':
            #         cost = position.Cost(rate, self.currency, None, None)
            #         txn = data.Transaction(
            #             meta, date, self.FLAG, None, desc, data.EMPTY_SET, {link}, [
            #                 data.Posting(self.account_cash, units, None, None, None,
            #                                 None),
            #                 data.Posting(self.account_fees, fees, None, None, None,
            #                                 None),
            #                 data.Posting(account_inst, units_inst, cost, None, None,
            #                                 None),
            #             ])

            #     elif rtype == 'SELL':
            #         # Extract the lot. In practice this information not be there
            #         # and you will have to identify the lots manually by editing
            #         # the resulting output. You can leave the cost.number slot
            #         # set to None if you like.
            #         match = re.search(r'\(LOT ([0-9.]+)\)', row['DESCRIPTION'])
            #         if not match:
            #             logging.error("Missing cost basis in '%s'", row['DESCRIPTION'])
            #             continue
            #         cost_number = D(match.group(1))
            #         cost = position.Cost(cost_number, self.currency, None, None)
            #         price = amount.Amount(rate, self.currency)
            #         account_gains = self.account_gains.format(instrument)
            #         txn = data.Transaction(
            #             meta, date, self.FLAG, None, desc, data.EMPTY_SET, {link}, [
            #                 data.Posting(self.account_cash, units, None, None, None,
            #                                 None),
            #                 data.Posting(self.account_fees, fees, None, None, None,
            #                                 None),
            #                 data.Posting(account_inst, units_inst, cost, price, None,
            #                                 None),
            #                 data.Posting(account_gains, None, None, None, None,
            #                                 None),
            #             ])

            # else:
            #     logging.error("Unknown row type: %s; skipping", rtype)
            #     continue

        # Insert a final balance check.
        # if index:
        #     entries.append(
        #         data.Balance(meta, date + datetime.timedelta(days=1),
        #                      self.account_cash,
        #                      amount.Amount(D(row['BALANCE']), self.currency),
        #                      None, None))

        return entries
