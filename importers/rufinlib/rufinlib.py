from decimal import Decimal
import os
import csv
import re

def load_isin(filename=None):
    ''' Load file with ISIN database used to find out tickers of assets.
        It is possible to download up-to-date ISIN database
        from: https://www.moex.com/msn/stock-instruments
        TODO: fix loader to process original file (now I delete blank lines)
    '''
    isindb = {}

    if not filename:
        # get file from module's directory
        filename = os.path.join(os.path.dirname(__file__), 'moex_db.csv')
    
    with open(filename, newline='', encoding='cp1251') as f:
        r = csv.reader(f, delimiter=';')
        for line_count, row in enumerate(r):
            if line_count == 0:
                continue
            try:
                isindb[row[4]] = {'ticker' : row[0], 'currency' : row[8], 'facevalue' : Decimal(row[7].replace(',', '.')), 
                                    'isbond' : False}
                if (row[3] == r'ОФЗ' or re.match(r'.*[Оо]блигации.*', row[3])):
                    isindb[row[4]]['isbond'] = True
            except IndexError:
                break
    isindb['US29355E2081'] = 'ENPLADR'
    isindb['JE00B5BCW814'] = 'RUAL'

    return isindb