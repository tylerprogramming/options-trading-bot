import datetime
import time
import pandas as pd
import constants
from ib_insync import Option

def display_trade_information(action, condition, price, result, right, symbol):
    print("\n*********** START Trade ***********\n")
    print("Time: {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    print("Company:      {}".format(symbol))
    print("Condition:    {}".format(condition))
    print("Entry Price:  {}".format(price))
    print("Action:       {}".format(action))
    print("Right:        {}".format(right))
    print("Result W/L/P: {}\n".format(result))

def log(info):
    print("{} | {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), info))

def get_correct_options_expiration(expirations):
    today_date = datetime.date.today().strftime("%Y%m%d")

    if expirations[0] == today_date:
        print("This is a zero day expiration date, so use the next expiration date.")
        expiration = expirations[1]
    else:
        expiration = expirations[0]

    print("The correct expiration chosen from list {} based on today's date: {} is {}."
          .format(expirations, today_date, expiration))

    return expiration

def create_options_contract(symbol, expiration, strike, right):
    """
    Create an Option Contract with following parameters:

    Parameters:
        symbol: Symbol name.
        expiration: The option's last trading day or contract month.
            YYYYMMDD format
        strike: The option's strike price.
        right: Put or call option.
            Valid values are 'P', 'PUT', 'C' or 'CALL'.
    """
    return Option(
        symbol,
        expiration,
        strike,
        right,
        constants.SMART
    )

def set_pandas_configuration():
    pd.options.display.width = None
    pd.options.display.max_columns = None
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_columns', 3000)

def is_amazon(symbol):
    if symbol == constants.AMAZON:
        return True
    return False