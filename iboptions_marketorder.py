import datetime
import time
import numpy
import asyncio
import nest_asyncio
import redis

import config
import constants
import helperclasses
import tables
import json
import mysql.connector
import helper
from ib_insync import IB, Stock, Option, LimitOrder, MarketOrder
from apscheduler.schedulers.asyncio import AsyncIOScheduler

async def run_periodically(interval, periodic_function):
    """
        This runs a function on a specific interval.
    """
    while True:
        await asyncio.gather(asyncio.sleep(interval), periodic_function())

async def put_prices(correct_expiration, strikes_after_entry_price_call, strikes_before_entry_price_call, symbol):
    put_above_entry_price = [
        Option(symbol, correct_expiration, strike, right, constants.SMART, tradingClass=symbol)
        for right in ['P']
        for strike in strikes_after_entry_price_call[:constants.NUMBER_OF_STRIKE_PRICES]]
    put_below_entry_price = [
        Option(symbol, correct_expiration, strike, right, constants.SMART, tradingClass=symbol)
        for right in ['P']
        for strike in strikes_before_entry_price_call[-constants.NUMBER_OF_STRIKE_PRICES:]]

    return put_above_entry_price, put_below_entry_price

async def call_prices(correct_expiration, strikes_after_entry_price_call, strikes_before_entry_price_call, symbol):
    call_above_entry_price = [
        Option(symbol, correct_expiration, strike, right, constants.SMART, tradingClass=symbol)
        for right in ['C']
        for strike in strikes_after_entry_price_call[:constants.NUMBER_OF_STRIKE_PRICES]]
    call_below_entry_price = [
        Option(symbol, correct_expiration, strike, right, constants.SMART, tradingClass=symbol)
        for right in ['C']
        for strike in strikes_before_entry_price_call[-constants.NUMBER_OF_STRIKE_PRICES:]]

    return call_above_entry_price, call_below_entry_price

class OptionsBot:
    def __init__(self):
        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        print("************************************")
        print("*       Starting Trading Bot       *")
        print("*      ", current_time, "       *")
        print("************************************")

        self.breakout_amazon_call_options_contract = None
        self.breakout_amazon_put_options_contract = None
        self.sma_amazon_call_options_contract = None
        self.sma_amazon_put_options_contract = None

        self.breakout_nvidia_call_options_contract = None
        self.breakout_nvidia_put_options_contract = None
        self.sma_nvidia_call_options_contract = None
        self.sma_nvidia_put_options_contract = None
        self.sma_yellow_nvidia_call_options_contract = None
        self.sma_yellow_nvidia_put_options_contract = None
        self.sma_green_nvidia_call_options_contract = None
        self.sma_green_nvidia_put_options_contract = None

        self.breakout_apple_call_options_contract = None
        self.breakout_apple_put_options_contract = None
        self.sma_apple_call_options_contract = None
        self.sma_apple_put_options_contract = None

        helper.set_pandas_configuration()

        nest_asyncio.apply()

        # Redis connection
        self.r = redis.Redis(host='localhost', port=config.redis_port, db=0)
        helper.log("Connected to Redis Server...")

        try:
            self.r.ping()
            helper.log("Successfully Connected to Redis!")
            helper.log(self.r.client())
        except redis.exceptions.ConnectionError as redis_conn_error:
            helper.log(str(redis_conn_error))

        self.p = self.r.pubsub()
        self.p.subscribe('tradingview')

        self.cnx = mysql.connector.connect(**config.database_config)
        self.cursor = self.cnx.cursor(buffered=True)

        try:
            self.cursor.execute(tables.CREATE_TRADE_TABLE)
            self.cursor.execute(tables.CREATE_OPTIONS_TABLE)
            self.cursor.execute(tables.CREATE_ACCOUNT_SUMMARY_TABLE)
            self.cnx.commit()
        except mysql.connector.Error as err:
            print("Failed creating table: {}".format(err))
            exit(1)

        print("Retrieving initial option chains...")

        try:
            self.ib = IB()
            self.ib.connect('127.0.0.1', config.interactive_brokers_port, clientId=1)
        except Exception as e:
            print(str(e))

        self.amazon_stock_contract = Stock(constants.AMAZON, constants.SMART, constants.USD)
        self.nvidia_stock_contract = Stock(constants.NVIDIA, constants.SMART, constants.USD)
        self.apple_stock_contract = Stock(constants.APPLE, constants.SMART, constants.USD)
        self.ib.qualifyContracts(self.amazon_stock_contract)
        self.ib.qualifyContracts(self.nvidia_stock_contract)
        self.ib.qualifyContracts(self.apple_stock_contract)

        # request a list of option chains
        self.amazon_option_chains = self.ib.reqSecDefOptParams(self.amazon_stock_contract.symbol, '',
                                                               self.amazon_stock_contract.secType,
                                                               self.amazon_stock_contract.conId)
        self.nvidia_option_chains = self.ib.reqSecDefOptParams(self.nvidia_stock_contract.symbol, '',
                                                               self.nvidia_stock_contract.secType,
                                                               self.nvidia_stock_contract.conId)
        self.apple_option_chains = self.ib.reqSecDefOptParams(self.apple_stock_contract.symbol, '',
                                                              self.apple_stock_contract.secType,
                                                              self.apple_stock_contract.conId)

        print("Running Live!")

        self.schedule = AsyncIOScheduler(daemon=True)
        self.schedule.add_job(self.update_options_chains, 'cron', day_of_week='mon-fri', hour='8')
        self.schedule.add_job(self.check_connection, 'cron', day_of_week='mon-fri', hour='9')
        self.schedule.add_job(self.sell_remaining_contracts_end_of_day, 'cron', day_of_week='mon-fri', hour='15',
                              minute='55')
        self.schedule.start()

        asyncio.run(run_periodically(1, self.check_messages))
        self.ib.run()

    async def check_messages(self):
        """
            On an interval set to 1 second, and constantly checks for new
            messages from redis.  Once the message is received, it will
            then parse it and then check what to do such as Buy or Sell
            an Options Contract.
        """

        message = self.p.get_message()

        if message is not None and message['type'] == 'message':
            await self.check_connection()
            await self.check_database_connection()

            message_data = json.loads(message['data'])

            symbol = message_data['symbol']
            condition = message_data['order']['condition']
            price = message_data['order']['price']
            right = message_data['order']['right']
            action = message_data['order']['action']
            result = message_data['order']['result']

            helper.display_trade_information(action, condition, price, result, right, symbol)

            if action == constants.BUY:
                options_chain = self.get_correct_options_chain(symbol)

                strikes_after_entry_price_call = [strike for strike in options_chain.strikes
                                                  if strike > price]
                strikes_before_entry_price_call = [strike for strike in options_chain.strikes
                                                   if strike < price]
                expirations = sorted(exp for exp in options_chain.expirations)[:2]

                correct_expiration = helper.get_correct_options_expiration(expirations)

                if symbol == constants.AMAZON:
                    if right == constants.CALL:
                        call_above_entry_price, call_below_entry_price = await call_prices(correct_expiration, strikes_after_entry_price_call, strikes_before_entry_price_call, symbol)
                        call_contracts = numpy.concatenate((call_below_entry_price, call_above_entry_price))
                        valid_contracts = self.ib.qualifyContracts(*call_contracts)

                        print("All valid contracts:", valid_contracts)

                        if condition == "breakout":
                            self.breakout_amazon_call_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            if self.breakout_amazon_call_options_contract is not None:
                                await self.place_options_order(
                                    message_data,
                                    action,
                                    condition,
                                    self.breakout_amazon_call_options_contract
                                )
                            else:
                                print(constants.NO_VALID_CONTRACTS)
                        elif condition == "sma":
                            self.sma_amazon_call_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            if self.sma_amazon_call_options_contract is not None:
                                await self.place_options_order(
                                    message_data,
                                    action,
                                    condition,
                                    self.sma_amazon_call_options_contract
                                )
                            else:
                                print(constants.NO_VALID_CONTRACTS)
                    else:
                        put_above_entry_price, put_below_entry_price = await put_prices(correct_expiration,
                                                                                             strikes_after_entry_price_call,
                                                                                             strikes_before_entry_price_call,
                                                                                             symbol)
                        put_contracts = numpy.concatenate((put_below_entry_price, put_above_entry_price))
                        valid_contracts = self.ib.qualifyContracts(*put_contracts)

                        print("All valid contracts:", valid_contracts)
                        print("Above:", put_above_entry_price)
                        print("Below:", put_below_entry_price)

                        if condition == "breakout":
                            self.breakout_amazon_put_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            if self.breakout_amazon_put_options_contract is not None:
                                await self.place_options_order(
                                    message_data,
                                    action,
                                    condition,
                                    self.breakout_amazon_put_options_contract
                                )
                            else:
                                print(constants.NO_VALID_CONTRACTS)
                        elif condition == "sma":
                            self.sma_amazon_put_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            if self.sma_amazon_put_options_contract is not None:
                                await self.place_options_order(
                                    message_data,
                                    action,
                                    condition,
                                    self.sma_amazon_put_options_contract
                                )
                            else:
                                print(constants.NO_VALID_CONTRACTS)
                elif symbol == constants.NVIDIA:
                    if right == constants.CALL:
                        call_above_entry_price, call_below_entry_price = await call_prices(correct_expiration,
                                                                                                strikes_after_entry_price_call,
                                                                                                strikes_before_entry_price_call,
                                                                                                symbol)
                        call_contracts = numpy.concatenate((call_below_entry_price, call_above_entry_price))
                        valid_contracts = self.ib.qualifyContracts(*call_contracts)

                        print("All valid contracts:", valid_contracts)
                        print("Above:", call_above_entry_price)
                        print("Below:", call_below_entry_price)

                        if condition == "breakout":
                            self.breakout_nvidia_call_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            await self.place_options_order(
                                message_data,
                                action,
                                condition,
                                self.breakout_nvidia_call_options_contract
                            )
                        elif condition == "sma":
                            self.sma_nvidia_call_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            await self.place_options_order(
                                message_data,
                                action,
                                condition,
                                self.sma_nvidia_call_options_contract
                            )
                        elif condition == constants.SMA_GREEN:
                            self.sma_green_nvidia_call_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            await self.place_options_order(
                                message_data,
                                action,
                                condition,
                                self.sma_green_nvidia_call_options_contract
                            )
                        elif condition == constants.SMA_YELLOW:
                            self.sma_yellow_nvidia_call_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            await self.place_options_order(
                                message_data,
                                action,
                                condition,
                                self.sma_yellow_nvidia_call_options_contract
                            )
                        else:
                            print("No condition with this name: {}".format(condition))
                    else:
                        put_above_entry_price, put_below_entry_price = await put_prices(correct_expiration,
                                                                                             strikes_after_entry_price_call,
                                                                                             strikes_before_entry_price_call,
                                                                                             symbol)
                        put_contracts = numpy.concatenate((put_below_entry_price, put_above_entry_price))
                        valid_contracts = self.ib.qualifyContracts(*put_contracts)

                        print("All valid contracts:", valid_contracts)
                        print("Above:", put_above_entry_price)
                        print("Below:", put_below_entry_price)

                        if condition == "breakout":
                            self.breakout_nvidia_put_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            await self.place_options_order(
                                message_data,
                                action,
                                condition,
                                self.breakout_nvidia_put_options_contract
                            )
                        elif condition == "sma":
                            self.sma_nvidia_put_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            await self.place_options_order(
                                message_data,
                                action,
                                condition,
                                self.sma_nvidia_put_options_contract
                            )
                        elif condition == constants.SMA_GREEN:
                            self.sma_green_nvidia_put_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            await self.place_options_order(
                                message_data,
                                action,
                                condition,
                                self.sma_green_nvidia_put_options_contract
                            )
                        elif condition == constants.SMA_YELLOW:
                            self.sma_yellow_nvidia_put_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            await self.place_options_order(
                                message_data,
                                action,
                                condition,
                                self.sma_yellow_nvidia_put_options_contract
                            )
                elif symbol == constants.APPLE:
                    if right == constants.CALL:
                        call_above_entry_price, call_below_entry_price = await call_prices(correct_expiration,
                                                                                                strikes_after_entry_price_call,
                                                                                                strikes_before_entry_price_call,
                                                                                                symbol)
                        call_contracts = numpy.concatenate((call_below_entry_price, call_above_entry_price))
                        valid_contracts = self.ib.qualifyContracts(*call_contracts)

                        if condition == "breakout":
                            self.breakout_apple_call_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            await self.place_options_order(
                                message_data,
                                action,
                                condition,
                                self.breakout_apple_call_options_contract
                            )
                        elif condition == "sma":
                            self.sma_apple_call_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            await self.place_options_order(
                                message_data,
                                action,
                                condition,
                                self.sma_apple_call_options_contract
                            )
                    else:
                        put_above_entry_price, put_below_entry_price = await put_prices(correct_expiration,
                                                                                             strikes_after_entry_price_call,
                                                                                             strikes_before_entry_price_call,
                                                                                             symbol)
                        put_contracts = numpy.concatenate((put_below_entry_price, put_above_entry_price))
                        valid_contracts = self.ib.qualifyContracts(*put_contracts)

                        if condition == "breakout":
                            self.breakout_apple_put_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            await self.place_options_order(
                                message_data,
                                action,
                                condition,
                                self.breakout_apple_put_options_contract
                            )
                        elif condition == "sma":
                            self.sma_apple_put_options_contract = await self.get_correct_contract_with_delta(
                                valid_contracts)

                            await self.place_options_order(
                                message_data,
                                action,
                                condition,
                                self.sma_apple_put_options_contract
                            )
            elif action == constants.SELL:
                if symbol == constants.AMAZON:
                    if condition == "breakout":
                        if right == "CALL":
                            await self.sell_contract(action, condition, symbol,
                                                     self.breakout_amazon_call_options_contract, result, price)
                            self.breakout_amazon_call_options_contract = None
                        if right == "PUT":
                            await self.sell_contract(action, condition, symbol,
                                                     self.breakout_amazon_put_options_contract, result, price)
                            self.breakout_amazon_put_options_contract = None
                    if condition == "sma":
                        if right == "CALL":
                            await self.sell_contract(action, condition, symbol,
                                                     self.sma_amazon_call_options_contract, result, price)
                            self.sma_amazon_call_options_contract = None
                        if right == "PUT":
                            await self.sell_contract(action, condition, symbol,
                                                     self.sma_amazon_put_options_contract, result, price)
                            self.sma_amazon_put_options_contract = None
                elif symbol == constants.NVIDIA:
                    if condition == constants.BREAKOUT:
                        if right == "CALL":
                            await self.sell_contract(action, condition, symbol,
                                                     self.breakout_nvidia_call_options_contract, result, price)
                            self.breakout_nvidia_call_options_contract = None
                        if right == "PUT":
                            await self.sell_contract(action, condition, symbol,
                                                     self.breakout_nvidia_put_options_contract, result, price)
                            self.breakout_nvidia_put_options_contract = None
                    elif condition == "sma":
                        if right == "CALL":
                            await self.sell_contract(action, condition, symbol,
                                                     self.sma_nvidia_call_options_contract, result, price)
                            self.sma_nvidia_call_options_contract = None
                        if right == "PUT":
                            await self.sell_contract(action, condition, symbol,
                                                     self.sma_nvidia_put_options_contract, result, price)
                            self.sma_nvidia_put_options_contract = None
                    elif condition == constants.SMA_GREEN:
                        if right == "CALL":
                            await self.sell_contract(action, condition, symbol,
                                                     self.sma_green_nvidia_call_options_contract, result, price)
                            self.sma_nvidia_call_options_contract = None
                        if right == "PUT":
                            await self.sell_contract(action, condition, symbol,
                                                     self.sma_green_nvidia_put_options_contract, result, price)
                            self.sma_nvidia_put_options_contract = None
                    elif condition == constants.SMA_YELLOW:
                        if right == "CALL":
                            await self.sell_contract(action, condition, symbol,
                                                     self.sma_yellow_nvidia_call_options_contract, result, price)
                            self.sma_nvidia_call_options_contract = None
                        if right == "PUT":
                            await self.sell_contract(action, condition, symbol,
                                                     self.sma_yellow_nvidia_put_options_contract, result, price)
                            self.sma_nvidia_put_options_contract = None
                elif symbol == constants.APPLE:
                    if condition == "breakout":
                        if right == "CALL":
                            await self.sell_contract(action, condition, symbol,
                                                     self.breakout_apple_call_options_contract, result, price)
                            self.breakout_apple_call_options_contract = None
                        if right == "PUT":
                            await self.sell_contract(action, condition, symbol,
                                                     self.breakout_apple_put_options_contract, result, price)
                            self.breakout_apple_put_options_contract = None
                    if condition == "sma":
                        if right == "CALL":
                            await self.sell_contract(action, condition, symbol,
                                                     self.sma_apple_call_options_contract, result, price)
                            self.sma_apple_call_options_contract = None
                        if right == "PUT":
                            await self.sell_contract(action, condition, symbol,
                                                     self.sma_apple_put_options_contract, result, price)
                            self.sma_apple_put_options_contract = None
            else:
                print("Only action known is BUY and SELL, we don't do anything with this:", action)

    def get_correct_options_chain(self, symbol):
        options_chain = None

        if symbol == constants.AMAZON:
            options_chain = next(c for c in self.amazon_option_chains if
                                 c.exchange == constants.SMART and
                                 c.tradingClass == constants.AMAZON)
        elif symbol == constants.NVIDIA:
            options_chain = next(c for c in self.nvidia_option_chains if
                                 c.exchange == constants.SMART and
                                 c.tradingClass == constants.NVIDIA)
        elif symbol == constants.APPLE:
            options_chain = next(c for c in self.apple_option_chains if
                                 c.exchange == constants.SMART and
                                 c.tradingClass == constants.APPLE)

        return options_chain

    async def check_database_connection(self):
        """ Connect to MySQL database """
        if not self.cnx.is_connected() or not self.ib.client.isConnected():
            try:
                print("Attempting Reconnection to MySQL Database...")
                self.cnx.disconnect()
                self.cnx = mysql.connector.connect(**config.database_config)
                print("Reconnected to MySQL Database @",
                      time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
            except mysql.connector.Error as err:
                print(err)
        else:
            print("Still connected to MySQL Database!")

    async def place_options_order(self, message_data, action, condition, contract):
        self.insert_option_contract(
            condition,
            contract,
            config.NUMBER_OF_CONTRACTS
        )

        ticker_data = self.ib.reqTickers(contract)

        # all greeks, then get ask and delta
        ask_greeks = ticker_data[0].modelGreeks
        bid = ticker_data[0].bid
        ask = ticker_data[0].ask
        mid = ticker_data[0].midpoint()
        theta = ask_greeks.theta
        delta = ask_greeks.delta
        gamma = ask_greeks.gamma
        implied_volatility = ask_greeks.impliedVol

        # if we continue to use, have check that ask/bid are both NOT nan
        limit_order = LimitOrder(action, config.NUMBER_OF_CONTRACTS, bid)

        trade = self.ib.placeOrder(
            contract,
            limit_order
        )

        self.save_data(message_data, config.NUMBER_OF_CONTRACTS, contract.strike, ask, bid, mid, gamma, delta, theta,
                       implied_volatility)

        helper.log("Successfully Placed Order!")
        helper.log(trade)
        print("*********** END Trade ***********")

    async def sell_contract(self, action, condition, symbol, contract, result, price):
        found_in_database = False
        contracts_from_buy_trade = 0

        if contract is None:
            print("{} | Attempt 1: Didn't have contract stored in session to Sell".format(
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
            retrieved_contract, number_of_contracts = self.check_for_options_contract(symbol, condition)
            print("Number of contracts is none:", number_of_contracts)

            if retrieved_contract is not None:
                print("{} | Attempt 2: Found in database".format(
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
                print("{} | Contract Found: {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                                                       retrieved_contract))
                found_in_database = True
                contracts_from_buy_trade = number_of_contracts
                contract = retrieved_contract
                print("contracts from buy trade:", contracts_from_buy_trade)

        if contract:
            if not found_in_database:
                contracts_from_buy_trade = self.get_trade_contracts(symbol, condition)

                if contracts_from_buy_trade == 0:
                    print("Couldn't find number of contracts in options database, can't sell contract:", contract)
                    return
                print("contract found and not in data_base:", contracts_from_buy_trade)

            sell_market_order = MarketOrder(action, contracts_from_buy_trade)
            sell_trade = self.ib.placeOrder(contract, sell_market_order)

            helper.log("Trade")
            helper.log(sell_trade)
            helper.log("Successfully Sold Trade!")

            ticker_data = self.ib.reqTickers(contract)
            model_greeks = ticker_data[0].modelGreeks
            ticker_dataclass = helperclasses.TickerData(ticker_data[0].ask, ticker_data[0].bid, ticker_data[0].midpoint(),
                model_greeks.delta, model_greeks.gamma, model_greeks.theta, model_greeks.impliedVol)

            ask = ticker_data[0].ask
            bid = ticker_data[0].bid
            mid = ticker_data[0].midpoint()
            delta = model_greeks.delta
            gamma = model_greeks.gamma
            theta = model_greeks.theta
            implied_vol = model_greeks.impliedVol

            helper.log(ticker_dataclass.print())

            self.delete_options_contract(symbol, condition)
            self.update_data(result, condition, symbol, price, ask, bid, mid, delta, gamma, theta, implied_vol)

            print("\n*********** END Trade ***********\n")
        else:
            print("Attempt 2: Couldn't find in database.")

    async def ticker_info(self, contracts):
        ticker_full_data = self.ib.reqTickers(*contracts)
        list(ticker_full_data)

        print("ticker data", ticker_full_data)

        if ticker_full_data is not None:
            valid_deltas = []
            invalid_deltas = []
            all_deltas = [ticker.modelGreeks.delta for ticker in ticker_full_data]

            if ticker_full_data[0].modelGreeks.delta > 0:
                for i in range(len(all_deltas)):
                    if all_deltas[i] is not None:
                        if constants.CALL_UPPER_DELTA_BOUNDARY > all_deltas[i] > constants.CALL_LOWER_DELTA_BOUNDARY:
                            valid_deltas.append(all_deltas[i])
                        else:
                            invalid_deltas.append(all_deltas[i])

                closest_ticker_index = max(range(len(ticker_full_data)),
                                           key=lambda i: ticker_full_data[
                                                             i].modelGreeks.delta < constants.CALL_UPPER_DELTA_BOUNDARY)
            else:
                for i in range(len(all_deltas)):
                    if all_deltas[i] is not None:
                        if constants.PUT_UPPER_DELTA_BOUNDARY < all_deltas[i] < constants.PUT_LOWER_DELTA_BOUNDARY:
                            valid_deltas.append(all_deltas[i])
                        else:
                            invalid_deltas.append(all_deltas[i])

                closest_ticker_index = min(range(len(ticker_full_data)),
                                           key=lambda i: ticker_full_data[
                                                             i].modelGreeks.delta > constants.PUT_UPPER_DELTA_BOUNDARY)

                if closest_ticker_index > 0:
                    closest_ticker_index = closest_ticker_index - 1

            print("{} | All Deltas:     {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                                                   all_deltas))
            print("{} | Valid Deltas:   {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                                                   valid_deltas))
            print("{} | Invalid Deltas: {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                                                   invalid_deltas))
            print("{} | Chosen Index:   {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                                                   closest_ticker_index))
            print("The delta:", ticker_full_data[closest_ticker_index].ask)
            print("The mid:", ticker_full_data[closest_ticker_index].midpoint())
            return ticker_full_data[closest_ticker_index].contract
        else:
            print("There is no ticker data to retrieve...")
            return None

    async def get_correct_contract_with_delta(self, contracts):
        if len(contracts) == 0:
            print("{} | No valid contracts to get the correct delta".format(
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
            return None
        else:
            chosen_options_contract = await self.ticker_info(contracts)

            if chosen_options_contract is not None:
                print("{} | The chosen contract with correct delta is: {}".format(
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), chosen_options_contract))
                return chosen_options_contract
            else:
                return None

    def delete_options_contract(self, symbol, condition):
        sql_query = tables.DELETE_OPTION_DATA
        sql_input = (symbol, condition)

        try:
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(sql_query, sql_input)
            self.cnx.commit()
            cursor.close()
            print("Successfully DELETED Option from table!")
        except mysql.connector.Error as err:
            print("Failed deleting option from table: {}".format(err))

    def save_data(self, message_data, number_of_contracts, strike_price, ask, bid, mid, gamma, delta, theta, implied_vol):
        sql_query = tables.INSERT_TRADE_DATA
        sql_input = (
            message_data['symbol'],
            message_data['order']['condition'],
            message_data['order']['action'],
            message_data['order']['right'],
            number_of_contracts,
            message_data['order']['price'],
            strike_price,
            message_data['order']['stoploss'],
            message_data['order']['takeProfit'],
            delta,
            gamma,
            theta,
            ask,
            bid,
            mid,
            implied_vol,
            message_data['order']['result']
        )

        try:
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(sql_query, sql_input)
            self.cnx.commit()
            cursor.close()
            print("{} | Successfully INSERTED Trade data into Database!".format(
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
        except mysql.connector.Error as err:
            print("Failed saving data to signals table: {}".format(err))

    def get_trade_contracts(self, symbol, condition):
        sqlite_insert_with_param = tables.GET_MATCHING_TRADE
        sqlite_data = (
            symbol,
            condition
        )

        try:
            self.cnx.row_factory = lambda curs, row: row[0]
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(sqlite_insert_with_param, sqlite_data)
            number_of_contracts = cursor.fetchone()
        except mysql.connector.Error as err:
            print("Failed getting number of trade contracts for {} - {}: {}".format(symbol, condition, err))

        if number_of_contracts is not None:
            print("Number of contracts found in Database: {}".format(number_of_contracts))
            return number_of_contracts[0]
        else:
            return 0

    def update_data(self, result, condition, symbol, sell_price, sell_ask, sell_bid, sell_mid, sell_delta, sell_gamma, sell_theta,
                    sell_implied_vol):
        sql_update_query = tables.UPDATE_TRADE_DATA
        sql_input_data = (
            result, sell_price, sell_delta, sell_gamma, sell_theta, sell_ask, sell_bid, sell_mid, sell_implied_vol, condition, symbol)

        try:
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(sql_update_query, sql_input_data)
            self.cnx.commit()
            rows_affected = cursor.rowcount
            print("Successfully UPDATED {} row(s) data into Database!".format(rows_affected))
            cursor.close()
        except mysql.connector.Error as err:
            print("Failed updating data to database: {}".format(err))

    def insert_option_contract(self, condition, contract, number_of_contracts):
        sqlite_insert_with_param = tables.INSERT_OPTION_DATA
        sqlite_data = (
            condition,
            contract.symbol,
            contract.lastTradeDateOrContractMonth,
            contract.strike,
            contract.right,
            contract.exchange,
            contract.tradingClass,
            number_of_contracts
        )

        try:
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(sqlite_insert_with_param, sqlite_data)
            self.cnx.commit()
            cursor.close()
            print("{} | Successfully INSERTED Options data into Database!".format(
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
        except mysql.connector.Error as err:
            print("{} | Failed INSERTING options data into database: {}".format(
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), err))

    def sell_remaining_contracts_end_of_day(self):
        sql_query = tables.RETRIEVE_OPTION_ALL_REMAINING_CONTRACTS

        try:
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(sql_query)
            rows = cursor.fetchall()
        except mysql.connector.Error as err:
            print("{} | Failed RETRIEVING remaining Option Contract(s) from Database: {}".format(
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), err))

        print(rows)

        if rows:
            for row in rows:
                options_symbol = row[0]
                options_condition = row[1]
                options_expiration = row[2]
                options_strike = row[3]
                options_right = row[4]
                number_of_contracts = row[5]
                contract = helper.create_options_contract(options_symbol, options_expiration, options_strike, options_right)
                print("Contract:", contract)
                print("Condition:", options_condition)
                # self.ib.qualifyContracts(contract)

                print("Contract:", contract)
                print("Condition:", options_condition)

                ticker_data = self.ib.reqTickers(contract)
                ask_greeks = ticker_data[0].modelGreeks
                ask = ticker_data[0].ask
                bid = ticker_data[0].bid
                mid = ticker_data[0].midpoint()
                delta = ask_greeks.delta
                gamma = ask_greeks.gamma
                theta = ask_greeks.theta
                implied_vol = ask_greeks.impliedVol

                sell_market_order = MarketOrder(constants.SELL, number_of_contracts)
                sell_trade = self.ib.placeOrder(contract, sell_market_order)

                print("{} | Trade: {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                                              sell_trade))
                print("{} | Successfully Sold Trade".format(
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))

                try:
                    sql_query_ask = tables.RETRIEVE_TRADE_ASK_PRICE
                    cursor = self.cnx.cursor(buffered=True)
                    cursor.execute(sql_query_ask)
                    row = cursor.fetchone()

                    trade_right = row[0]
                    trade_ask_price = row[1]
                except mysql.connector.Error as err:
                    print("Failed RETRIEVING reminaining Option Contract(s) from Database: {}".format(err))

                if trade_right == constants.CALL:
                    if trade_ask_price > ask:
                        result = "L"
                    else:
                        result = "W"
                else:
                    if trade_ask_price > ask:
                        result = "W"
                    else:
                        result = "L"

                self.delete_options_contract(options_symbol, options_condition)
                self.update_data(result, options_condition, options_symbol, 100.0, ask, bid, mid, delta, gamma, theta, implied_vol)
        else:
            print("{} | No Contracts to Sell at the end of the day".format(
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))

    def check_for_options_contract(self, symbol, condition):
        sql_query = tables.RETRIEVE_OPTION_CONTRACT
        sql_input = (symbol, condition)

        try:
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(sql_query, sql_input)
            row = cursor.fetchone()
        except mysql.connector.Error as err:
            print("{} | Failed RETRIEVING Options Contract from Database: {}".format(
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), err))

        if row:
            options_symbol = row[0]
            options_expiration = row[1]
            options_strike = row[2]
            options_right = row[3]
            number_of_contracts = row[4]

            found_contract = helper.create_options_contract(options_symbol, options_expiration, options_strike, options_right)
            self.ib.qualifyContracts(found_contract)
        else:
            print("{} | No contract found in database".format(
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
            return None, None

        return found_contract, number_of_contracts

    async def check_connection(self):
        """
        Check IB Connection
        """
        if not self.ib.isConnected() or not self.ib.client.isConnected():
            print("Attempting Reconnection to Interactive Brokers...")
            self.ib.disconnect()
            self.ib = IB()
            self.ib.connect('127.0.0.1', config.interactive_brokers_port, clientId=1)
            print("{} | Reconnected to Interactive Brokers".format(
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))

    async def update_options_chains(self):
        """
        Update Option Chains
        """
        await self.check_connection()

        try:
            self.schedule.print_jobs()
            print("{} | Updating Option Chains".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
            self.amazon_option_chains = self.ib.reqSecDefOptParams(
                self.amazon_stock_contract.symbol, '',
                self.amazon_stock_contract.secType,
                self.amazon_stock_contract.conId)
            self.nvidia_option_chains = self.ib.reqSecDefOptParams(
                self.nvidia_stock_contract.symbol, '',
                self.nvidia_stock_contract.secType,
                self.nvidia_stock_contract.conId)
            self.apple_option_chains = self.ib.reqSecDefOptParams(
                self.apple_stock_contract.symbol, '',
                self.apple_stock_contract.secType,
                self.apple_stock_contract.conId)
        except Exception as e:
            print(str(e))


# start the options bot
OptionsBot()
