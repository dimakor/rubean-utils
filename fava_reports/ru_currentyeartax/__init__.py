"""RU Current Year To Date Tax Report extension for Fava.
"""
import datetime
import collections
from dateutil.parser import parse
from rich import print

from beancount.core.number import Decimal
from beancount.core.number import ZERO
from beancount.core import amount
from beancount.core import convert

from fava.ext import FavaExtensionBase
from fava.helpers import FavaAPIException
from fava.template_filters import cost_or_value
from fava.context import g



class ru_cy_taxreport(FavaExtensionBase):  # pragma: no cover
    """Tax Base for traded assets
    """

    report_title = "RU CY Tax"

    def get_year(self):
        self.config = {'report_year' : datetime.datetime.now().year}
        return self.config['report_year']

    def numfmt(self, num):
        return '{:,}'.format(round(num,2)).replace(',',' ')

    def find_posting_by_field(self, posting_list, field_name, field_value):
        """ Return first posting where specified field equals specified value
        """
        for row in posting_list:
            if getattr(row, field_name) == field_value:
                return row
        return None

    def build_tax_tables(self):
        """Build fava tables
        """
        sql = """ SELECT id, date, narration, account, position, cost_date, date_diff(date, cost_date) as dff, cost_number, weight, price
                WHERE year = {year} and account ~ "Assets" and NOT account ~ "Cash" 
                and NOT account ~ "Assets:RU:BCS:FX" and NUMBER(weight) <0
        """.format(year=self.config['report_year'])
        sql_cash = """SELECT id, date, narration, account, position, price, weight
                WHERE year = {year} and account ~ "Cash"
        """.format(year=self.config['report_year'])
        
        
        sql_fee = """SELECT last(balance) as fees WHERE year = {year} and account ~ "BrokerFees"
        """.format(year=self.config['report_year'])
        contents, rtypes, rrows = self.ledger.query_shell.execute_query(g.filtered.entries, sql_fee)
        if rrows:
            broker_fee = rrows[0].fees.get_only_position().units.number
        else:
            broker_fee = Decimal(0)
        del contents, rtypes, rrows

        contents, rtypes, rrows = self.ledger.query_shell.execute_query(g.filtered.entries, sql)
        contents_cash, rtypes_cash, rrows_cash = self.ledger.query_shell.execute_query(g.filtered.entries, sql_cash)
        # TODO: error check
        # rtypes, rrows = self.query_func(sql)
        # if not rtypes:
        #     return [], {}, [[]]
        res = []
        res_ldv = []
        sum = sum_ldv = sum_total = 0
        totalsale = totalcost = 0
        cur = None
        lots = {} # dict for lots accumulation
        for row in rrows:
            try:
                x = lots[row.id]
                lots[row.id] = x + row.position.units.number
            except KeyError:
                lots[row.id] = row.position.units.number
        for row in rrows:
            cash = self.find_posting_by_field(rrows_cash, 'id', row.id) #find corresponding cash leg of the transaction
            if cash is None:
                continue
            amt = cash.position.units.number # this is total sum of the sale (assume same currency as cost)
            cur = cash.position.units.currency
            rr = row._asdict()
            partprice =  amt*rr['position'].units.number/lots[row.id]
            totalsale += partprice
            rr['price'] = round(partprice, 2)
            base = partprice - rr['cost_number']*(-rr['position'].units.number)
            rr['base'] = round(base,2)
            rr['cost'] = round(rr['cost_number']*(-rr['position'].units.number),2)
            sum_total += base
            totalcost += rr['cost']
            if row.dff >= 365*3 and base > 0:
                res_ldv.append(rr)
                sum_ldv += base
                continue
            sum += base
            res.append(rr)

        if cur is None:
            return None, None, None, None, None, None, None, None

        return res, res_ldv, self.numfmt(sum), self.numfmt(sum_ldv), self.numfmt(sum_total), \
            self.numfmt(totalsale), self.numfmt(totalcost), self.numfmt(broker_fee)

    def build_ldv_table(self):
        """Build fava tables
        """
        sql = """ SELECT LEAF(account) as account, 
                        units(sum(position)) as units, 
                        cost_date as acquisition_date, 
                        value(sum(position)) as market_value, 
                        cost(sum(position)) as basis 
                    WHERE account ~ "Assets" 
                    GROUP BY LEAF(account), cost_date, currency, cost_currency, cost_number
        """
        contents, rtypes, rrows = self.ledger.query_shell.execute_query(g.filtered.entries, sql)
        # TODO: error check
        # rtypes, rrows = self.query_func(sql)
        # if not rtypes:
        #     return [], {}, [[]]
        ldv_table = []

        # our output table is slightly different from our query table:
        retrow_types = rtypes + [('ldv_date', datetime.date)]
        RetRow = collections.namedtuple('RetRow', [i[0] for i in retrow_types])
        
        for row in rrows:
            # skip cash and empty(?) lots
            if row.acquisition_date is None or row.units.is_empty():
                continue
            # find out date of LDV
            ldv_date = row.acquisition_date + datetime.timedelta(days=3*365)
            #print(f"{row.account}: buy {row.acquisition_date} - ldv {ldv_date}")
            if ldv_date < datetime.date(year=self.config['report_year']+1, month=1, day=1):
                ldv_table.append(RetRow(row.account, row.units, row.acquisition_date, 
                                        row.market_value, row.basis, ldv_date))
            if ldv_date < datetime.datetime.now().date():
                ldv_table.append(RetRow(row.account, row.units, row.acquisition_date, 
                                        row.market_value, row.basis, ldv_date))

        print(retrow_types)
        print(ldv_table)
        return retrow_types, ldv_table
        #return rtypes, rrows
        '''
        res = []
        res_ldv = []
        sum = sum_ldv = sum_total = 0
        totalsale = totalcost = 0
        cur = None
        lots = {} # dict for lots accumulation
        for row in rrows:
            try:
                x = lots[row.id]
                lots[row.id] = x + row.position.units.number
            except KeyError:
                lots[row.id] = row.position.units.number
        for row in rrows:
            cash = self.find_posting_by_field(rrows_cash, 'id', row.id) #find corresponding cash leg of the transaction
            if cash is None:
                continue
            amt = cash.position.units.number # this is total sum of the sale (assume same currency as cost)
            cur = cash.position.units.currency
            rr = row._asdict()
            partprice =  amt*rr['position'].units.number/lots[row.id]
            totalsale += partprice
            rr['price'] = round(partprice, 2)
            base = partprice - rr['cost_number']*(-rr['position'].units.number)
            rr['base'] = round(base,2)
            rr['cost'] = round(rr['cost_number']*(-rr['position'].units.number),2)
            sum_total += base
            totalcost += rr['cost']
            if row.dff >= 365*3 and base > 0:
                res_ldv.append(rr)
                sum_ldv += base
                continue
            sum += base
            res.append(rr)

        if cur is None:
            return None, None, None, None, None, None, None, None

        return res, res_ldv, self.numfmt(sum), self.numfmt(sum_ldv), self.numfmt(sum_total), \
            self.numfmt(totalsale), self.numfmt(totalcost), self.numfmt(broker_fee)
        '''
        return None
