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



class ru_futureldv(FavaExtensionBase):  # pragma: no cover
    """Report Lots that will reach LDV in the near future
    """

    report_title = "RU Future LDV"

    def get_year(self):
        return datetime.datetime.now().year

    def get_future(self):
        try:
            return self.config['future']
        except TypeError:
            return 1

    def numfmt(self, num):
        return '{:,}'.format(round(num,2)).replace(',',' ')

    def find_posting_by_field(self, posting_list, field_name, field_value):
        """ Return first posting where specified field equals specified value
        """
        for row in posting_list:
            if getattr(row, field_name) == field_value:
                return row
        return None

    def build_ldv_table(self):
        """Build fava tables
        """
        # sql = """ SELECT LEAF(account) as account, 
        #                 units(sum(position)) as units, 
        #                 cost_date as acquisition_date, 
        #                 value(sum(position)) as market_value, 
        #                 cost(sum(position)) as basis 
        #             WHERE account ~ "Assets" 
        #             GROUP BY LEAF(account), cost_date, currency, cost_currency, cost_number
        # """
        sql = """ SELECT account, 
                        units(sum(position)) as units, 
                        cost_date as acquisition_date, 
                        value(sum(position)) as market_value, 
                        cost(sum(position)) as basis 
                    WHERE account ~ "Assets" 
                    GROUP BY account, cost_date, currency, cost_currency, cost_number
        """
        contents, rtypes, rrows = self.ledger.query_shell.execute_query(g.filtered.entries, sql)
        # TODO: error check
        # rtypes, rrows = self.query_func(sql)
        # if not rtypes:
        #     return [], {}, [[]]
        ldv_table = [] # lots reached LDV on the current date
        future_ldv_table = [] # lots that will reach LDV until the end of the year

        # our output table is slightly different from our query table:
        retrow_types = rtypes + [('ldv_date', datetime.date)]
        RetRow = collections.namedtuple('RetRow', [i[0] for i in retrow_types])
        
        for row in rrows:
            # skip cash and empty(?) lots
            if row.acquisition_date is None or row.units.is_empty():
                continue
            # find out date of LDV
            ldv_date = row.acquisition_date + datetime.timedelta(days=3*365)
            if ldv_date < datetime.datetime.now().date():
                ldv_table.append(RetRow(row.account, row.units, row.acquisition_date, 
                                        row.market_value, row.basis, ldv_date))
                continue # we don't want this lot to get to future LDV list
            until = datetime.date(year=datetime.datetime.now().year + self.get_future(), 
                                        month=1, 
                                        day=1)
            if ldv_date < until:
                future_ldv_table.append(RetRow(row.account, row.units, row.acquisition_date, 
                                        row.market_value, row.basis, ldv_date))

        return [retrow_types, ldv_table], [retrow_types, future_ldv_table], until