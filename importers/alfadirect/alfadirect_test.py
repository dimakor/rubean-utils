import unittest
from os import path

from beancount.ingest import regression_pytest as regtest
from . import bcsexpress

# Create an importer instance for running the regression tests.
importer = bcsexpress.Importer("153625/14",
                        "Assets:RU:BCS",
                        "Assets:RU:BCS:Cash",
                        "Assets:RU:BCS:Cash",
                        "Income:RU:BCS:Dividends",
                        "Income:RU:BCS:Interest",
                        "Expenses:RU:BCS:BrokerFees",
                        "Assets:RU:BCS:{}:Gains",
                        "Equity:BCS:External")

@regtest.with_importer(importer)
@regtest.with_testdir(path.dirname(__file__))
class TestImporter(regtest.ImporterTestBase):
    pass

if __name__ == '__main__':
    unittest.main()
