import os
import configparser
from urllib import response
from tinkoff.invest import Client, PositionsResponse, MoneyValue, OperationState, OperationsResponse, Operation
from rich import print
from decimal import *
import json
import datetime

def mv2str(mv : MoneyValue) -> str:
        return str(mv.units+mv.nano/1e9) #.rstrip('0')

def get_positions_securities(response : PositionsResponse, tickers : dict) -> dict:
    if len(response.securities) == 0:
        return None
    return {tickers[i.figi]['ticker'] : i.balance for i in response.securities}

def get_tickers_list(instruments : list, asset : str) -> dict:
    return { i.figi : {
                'name': i.name,
                'isin': i.isin,
                'ticker': i.ticker,
                'lot': i.lot,
                'currency' : i.currency.upper(),
                'nominal' : 0 if asset == 'etf' else i.nominal,
                'type' : asset
                }
            for i in instruments
            }

def get_positions_cash(response : PositionsResponse, tickers : dict) -> dict:
    if len(response.money) == 0:
        return {'RUB':0}
    return {i.currency.upper() : mv2str(i) for i in response.money}

def get_operations_list(resp : OperationsResponse, tickers : dict) -> list:
    result = list()
    for op in resp.operations:
        # 1. find out ticker of asset
        if op.figi == '':
            # Cash operations
            ticker = op.currency.upper() # use operation's currency as ticker
        elif tickers[op.figi]['type'] == 'currency':
            # Currency conversion
            ticker = tickers[op.figi]['nominal'].currency.upper()
        else:
            ticker = tickers[op.figi]['ticker']
        
        oper_dict = {'date' : str(op.date.date()),
                    'id' : op.id,
                    'type' : op.operation_type,
                    'figi' : op.figi,
                    'name' : op.currency.upper() if op.figi == '' else tickers[op.figi]['name'],
                    'ticker' : ticker,
                    'asset type' : 'cash' if op.figi == '' else tickers[op.figi]['type'],
                    
                    }
        if len(op.trades) != 0:
            for tr in op.trades:
                result.append([oper_dict | 
                                { 'price' : mv2str(tr.price),
                                  'quantity' : tr.quantity
                                }
                            ])
        else:
            # no trades in operations
            result.append([oper_dict | {'payment' : mv2str(op.payment)}])

    return result

configParser = configparser.RawConfigParser()   
configFilePath = r'tcsdownload.cfg'
configParser.read(configFilePath)

token = configParser.get('tcsinvest', 'token')
output = configParser.get('tcsinvest', 'output')

result = {}
client = Client(token)
with Client(token) as client:
    tickers =  ( get_tickers_list(client.instruments.shares().instruments, 'share') |
                get_tickers_list(client.instruments.bonds().instruments, 'bond') |
                get_tickers_list(client.instruments.etfs().instruments, 'etf') |
                get_tickers_list(client.instruments.currencies().instruments, 'currency') )
    accounts = client.users.get_accounts().accounts
    for acc in accounts:
        positions = client.operations.get_positions(account_id=acc.id)
        positions_dict = get_positions_securities(positions, tickers)
        cash_dict = get_positions_cash(positions, tickers)
        resp = client.operations.get_operations(account_id=acc.id,
                                                 from_=acc.opened_date,
                                                 to=datetime.datetime.now(),
                                                 state=OperationState.OPERATION_STATE_EXECUTED)
        result |= {acc.id:{'cash' : cash_dict, 'securities' : positions_dict, 'operations' : get_operations_list(resp, tickers)}}
        #result |= {acc.id:{'date' : str(datetime.datetime.now().date()), 'cash' : cash_dict, 'securities' : positions_dict}}

#print(result)
with open(output, "w") as f:
    json.dump(result, f, indent=4)
