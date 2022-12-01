"""RU Tax Report extension for Fava.
"""
import datetime

from fava.ext import FavaExtensionBase
from fava.context import g

class ru_taxreport(FavaExtensionBase):  # pragma: no cover
    """Tax Base for traded assets
    """

    report_title = "RU Tax Report"

    # def query_func(self, sql):
    #     contents, rtypes, rrows = self.ledger.query_shell.execute_query(sql)
    #     return rtypes, rrows
    def get_year(self):
        try:
            return self.config['report_year']
        except TypeError:
            self.config = {'report_year' : datetime.datetime.now().year - 1}
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
        """Build fava TLH tables using TLH library
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
        broker_fee = rrows[0].fees.get_only_position().units.number
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
