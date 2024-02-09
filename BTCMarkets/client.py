#!/usr/bin/env python3

import hmac
import time
import json
import base64
import hashlib
import urllib.parse
import pandas as pd
from urllib.error import URLError
from urllib.request import urlopen, Request
from datetime import datetime, timedelta

base_url = "https://api.btcmarkets.net"

def buildHeaders(method, apiKey, privateKey, path, data):
    now = str(int(time.time() * 1000))
    message = method + path + now
    if data is not None:
        message += data
    signature = signMessage(privateKey, message)
    headers = {
        "Accept": "application/json",
        "Accept-Charset": "UTF-8",
        "Content-Type": "application/json",
        "BM-AUTH-APIKEY": apiKey,
        "BM-AUTH-TIMESTAMP": now,
        "BM-AUTH-SIGNATURE": signature,
    }
    return headers

def signMessage(privateKey, message):
    presignature = base64.b64encode(
        hmac.new(privateKey, message.encode("utf-8"), digestmod=hashlib.sha512).digest()
    )
    return presignature.decode("utf8")

def process_panda(panda, numeric=None, time=None):
    """Processes numerical and time fields for either a DataFrame or Series."""
    if numeric is not None:
        panda[numeric] = panda[numeric].apply(pd.to_numeric)
    if time is not None:
        panda[time] = panda[time].apply(pd.to_datetime)
    return panda

# 'make_df' and 'make_series' are kept as 2 separate functions so that it's clear
# what type of data is expected in the context of the calling function.
def make_df(data, numeric=None, time=None):
    """
    Create a pandas DataFrame from a list of dictionaries and optionally
    convert specified columns to numeric and datetime types.
    Returns 'data' unmodified if it's an error message.

    Args:
        data (list of dict): Data to be converted into a DataFrame.
            Each dictionary in the list represents a row.
        numeric (list of str, optional): List of column names to be converted to numeric types.
            If None, no conversion is applied. Defaults to None.
        time (list of str, optional): List of column names to be converted to datetime types.
            If None, no conversion is applied. Defaults to None.

    Returns: pandas.DataFrame created from the input data with specified columns
            converted to numeric or datetime types.

    Example:
        >>> data = [{'col1': '1', 'col2': '2021-01-01'}, {'col1': '2', 'col2': '2021-01-02'}]
        >>> df = make_df(data, numeric=['col1'], time=['col2'])
    """
    # Return error messages without trying to convert them. Let the callsite handle errors.
    if type(data) == type({}) and "statusCode" in data:
        return data
    return process_panda(pd.DataFrame(data), numeric, time)

def make_series(data, numeric=None, time=None):
    """
    Create a pandas Series from a dictionaries and optionally
    convert specified columns to numeric and datetime types.
    Returns 'data' unmodified if it's an error message.

    Args:
        data (dict): Data to be converted into a DataFrame.
            Each dictionary in the list represents a row.
        numeric (list of str, optional): List of column names to be converted to numeric types.
            If None, no conversion is applied. Defaults to None.
        time (list of str, optional): List of column names to be converted to datetime types.
            If None, no conversion is applied. Defaults to None.

    Returns: pandas.DataFrame created from the input data with specified columns
            converted to numeric or datetime types.

    Example:
        >>> data = {'col1': '1', 'col2': '2021-01-01'}
        >>> s = make_series(data, numeric=['col1'], time=['col2'])
    """
    # Return error messages without trying to convert them. Let the callsite handle errors.
    if type(data) == type({}) and "statusCode" in data:
        return data
    return process_panda(pd.Series(data), numeric, time)

def process_orderbook(orderbook):
    """
    Custom processing for orderbook data to convert price and amount strings to float.

    Args:
        orderbook (dict): A dictionary containing the orderbook information with keys for 'marketId', 'snapshotId',
            'asks', and 'bids'. Each 'asks' and 'bids' list contains lists of price and amount strings.

    Returns (dict): The processed orderbook with price and amount data converted to floats.

    Example:
        Input orderbook example:
        {
            "marketId": "BAT-AUD",
            "snapshotId": 1234123412341234,
            "asks": [["0.2677", "5665.85"], ...],
            "bids": [["0.2612", "17847.84"], ...]
        }

        Output will be the same structure with price and amount values as floats.
    """
    # Iterate over both 'bids' and 'asks' in the orderbook.
    for side in ["bids", "asks"]:
        if side in orderbook:
            orderbook[side] = [
                [float(price), float(amount)] for price, amount in orderbook[side]
            ]
    return orderbook

class BTCMarkets:
    """API client for BTCMarkets returning pandas DataFrames/Series.

    * This code was written with the help of an AI LLM.
    * It is designed to closely match the api spec oulined at docs.btcmarkets.net,
        this author strongly encourages reading the docs before trusting this code, or your own, with financial transacitons.
    * Optional features that are not native to the API are annotated with '[Non API Feature]' in the docstring.
    """

    def __init__(self, apiKey, privateKey, exception_on_error=True):
        """Creates a BTCMarkets api client.

        Args:
            apiKey (str): Your API key from BTC Markets.
            privateKey (str): Your private API key from BTC Markets.
            exception_on_error (bool, optional): If True (default), when the api returns an error
                an Exception is raised. This gives users basic control over error handling given the
                high stakes of financial trading.

        Example:
            >>> apiClient = BTCMarkets("your_api_key", "your_private_key")
        """
        self.apiKey = apiKey
        self.privateKey = base64.b64decode(privateKey)
        self.exception_on_error = exception_on_error

    def handle_error(self, msg):
        """
        Handles errors by either raising an exception or printing the error message based on the client's configuration.

        If `exception_on_error` is True, it raises a ValueError with the provided message.
        Otherwise, it prints the message and returns None.

        Args:
            msg (str): The error message to be handled.

        Returns None.

        Raises:
            ValueError: If `exception_on_error` is True, with the provided error message.

        Example:
            >>> if error_condition:
                    return self.handle_error("all the cryptopotamuses have escaped!")
        """
        msg = "No market_ids provided"
        if self.exception_on_error:
            raise ValueError(msg)
        print(msg)
        return None

    def makeHttpCall(self, method, path, query_params=None, data=None):
        """
        Makes an HTTP request to the BTC Markets API with the specified method, path, query parameters, and body data.

        This method constructs the appropriate request headers, including authentication headers,
        and handles both query parameters and request body data. It supports GET, POST, PUT, and DELETE HTTP methods.
        If the request is successful, it returns the JSON-decoded response. In case of an HTTP error,
        it returns an error object with the status code and error message.
        Optionally, it can raise an exception on error responses if `exception_on_error` is set to True.

        Args:
            method (str): The HTTP method to use for the request (e.g., 'GET', 'POST', 'PUT', 'DELETE').
            path (str): The API endpoint path to make the request to.
            query_params (dict, optional): A dictionary of query parameters to include in the request. Defaults to None.
            data (dict or str, optional): The request body data to send with the request. If a dictionary is provided, it will be JSON-encoded. Defaults to None.

        Returns (dict): The JSON-decoded response from the server if the request was successful.
            or
            dict: An error object containing 'statusCode' and error message if the request failed.

        Raises:
            Exception: If `exception_on_error` is True and the request fails, an exception is raised with the error message.

        Example:
            >>> apiClient = BTCMarkets("your_api_key", "your_private_key")
            >>> response = apiClient.makeHttpCall("GET", "/v3/markets")
            >>> print(response)
        """
        if data is not None:
            data = json.dumps(data)
        headers = buildHeaders(method, self.apiKey, self.privateKey, path, data)
        # Convert query parameters dictionary to a query string
        if query_params is not None and isinstance(query_params, dict):
            path += "?" + urllib.parse.urlencode(query_params)
        try:
            http_request = Request(base_url + path, data, headers, method=method)
            if method == "POST" or method == "PUT":
                response = urlopen(http_request, data=bytes(data, encoding="utf-8"))
            else:
                response = urlopen(http_request)
            return json.loads(str(response.read(), "utf-8"))
        except URLError as e:
            errObject = json.loads(e.read())
            if hasattr(e, "code"):
                errObject["statusCode"] = e.code
            if self.exception_on_error:
                raise Exception(errObject)
            return errObject

    # Market Data APIs
    # https://docs.btcmarkets.net/#tag/Market-Data-APIs

    def markets(self):
        """Retrieves list of active markets including configuration for each market.

        Returns: pandas.DataFrame with columns:
            - 'market_id' (str): The market identifier (e.g., 'BTC-AUD').
            - 'baseAssetName' (str): the asset being purchased or sold. In the case of ETH-AUD, the base asset is ETH.
            - 'quoteAssetName' (str): the asset that is used to price the base asset. In the case of ETH-AUD, the quote asset is AUD.
            - 'minOrderAmount' (float): minimum amount for an order
            - 'maxOrderAmount' (float): maximum amount for an order
            - 'amountDecimals' (float): maximum number of decimal places can be used for amounts
            - 'priceDecimals' (float): represents number of decimal places can be used for price when placing orders. For instance, for BTC-AUD market, priceDecimals is 2 meaning that a price of 100.12 is valid but 100.123 is not.
            - 'status' (str): current status of market, can be Online, Offline, Post Only, Limit Only, or Cancel Only
        """
        return make_df(
            self.makeHttpCall("GET", "/v3/markets"),
            numeric=[
                "minOrderAmount",
                "maxOrderAmount",
                "amountDecimals",
                "priceDecimals",
            ],
        )

    def ticker(self, market_id):
        """Retrieves ticker for the given marketId.

        Args:
            market_id (str): Unique Identifier for the requested market, e.g "ETH-AUD".

        Returns: pandas.Series with columns:
            - 'market_id' (str): The market identifier (e.g., 'BTC-AUD').
            - 'bestBid' (float): best buy order price
            - 'bestAsk' (float): best sell order price
            - 'lastPrice' (float): price of the last trade
            - 'volume24h' (float): represents total trading volume over the past 24 hours for the given market
            - 'volumeQte24h' (float): total volume over the past 24 hours in quote asset
            - 'price24h' (float): price change (difference between the first and last price over 24 hours)
            - 'pricePct24h' (float): percentage of price change
            - 'low24' (float): lowest price over the past 24 hours
            - 'high24' (float): highest price over the past 24 hours
            - 'timestamp' (datetime): timestamp
        """
        return make_series(
            self.makeHttpCall("GET", f"/v3/markets/{market_id}/ticker"),
            numeric=[
                "bestBid",
                "bestAsk",
                "lastPrice",
                "volume24h",
                "volumeQte24h",
                "price24h",
                "pricePct24h",
                "low24h",
                "high24h",
            ],
            time=["timestamp"],
        )

    def tickers(self, market_ids):
        """Retrieves tickers for the given market_ids.

        Args:
            market_ids (list of str): Unique Identifiers for the requested markets, e.g ["ETH-AUD"].

        Returns: pandas.DataFrame with columns:
            - market_id (str): The market identifier (e.g., 'BTC-AUD') for which trades are to be retrieved.
            - bestBid (float): best buy order price
            - bestAsk (float): best sell order price
            - lastPrice (float): price of the last trade
            - volume24h (float): represents total trading volume over the past 24 hours for the given market
            - volumeQte24h (float): total volume over the past 24 hours in quote asset
            - price24h (float): price change (difference between the first and last price over 24 hours)
            - pricePct24h (float): percentage of price change
            - low24 (float): lowest price over the past 24 hours
            - high24 (float): highest price over the past 24 hours
            - timestamp (datetime): timestamp
        """
        if len(market_ids) == 0:
            return self.handle_error("tickers called with no market_ids provided")
        return make_df(
            self.makeHttpCall(
                # Can't use dict method since it's the same key 'marketId' is repeated.
                "GET",
                f"/v3/markets/tickers?" + "marketId=" + "&marketId=".join(market_ids),
            ),
            numeric=[
                "bestBid",
                "bestAsk",
                "lastPrice",
                "volume24h",
                "volumeQte24h",
                "price24h",
                "pricePct24h",
                "low24h",
                "high24h",
            ],
            time=["timestamp"],
        )

    def market_trades(
        self, market_id, before=None, after=None, limit=None, add_cost=True
    ):
        """Retrieves list of most recent trades for the given market. This API supports pagination.

        Args:
            market_id (str): The market identifier (e.g., 'BTC-AUD') for which trades are to be retrieved.
            Pagination parameters:
                before (str, optional): Filter trades to fetch those before this trade ID. Defaults to None.
                after (str, optional): Filter trades to fetch those after this trade ID. Defaults to None.
                limit (int, optional): The maximum number of trades to retrieve. Defaults to None.
            add_cost (bool, optional): [Non API Feature] If True (default) adds a 'cost' column = price * amount (measured in quote asset).

        Returns: pandas.DataFrame where each row represents a trade with the following columns:
            - 'id' (str): The unique identifier of the trade.
            - 'price' (float): The price at which the trade was executed.
            - 'amount' (float): The amount of cryptocurrency traded.
            - 'timestamp' (datetime): The timestamp when the trade occurred.
            - 'side' (str): Indicates whether the trade was a bid (buy) or ask (sell). Authors note: Obviously both happened (someone bought and somone sold), I belive this is whichever was registered first.
            - 'cost' (float, optional): provided if 'add_cost' is True, this is price * amount (measured in quote asset).
        """
        query_params = {"before": before, "after": after, "limit": limit}
        # Filter out None values from query parameters
        query_params = {k: v for k, v in query_params.items() if v is not None}

        trades_df = make_df(
            self.makeHttpCall(
                "GET", f"/v3/markets/{market_id}/trades", query_params=query_params
            ),
            numeric=["price", "amount"],
            time=["timestamp"],
        )
        if trades_df is None:
            return trades_df
        if add_cost:
            trades_df["cost"] = trades_df["price"] * trades_df["amount"]
        return trades_df

    def orderbook(self, market_id, level=1):
        """Retrieves list of bids and asks for a given market.

        Args:
            market_id (str): Unique Identifier for the requested market, e.g "ETH-AUD".
            level (int):
                = 0 returns top bids and ask orders only.
                = 1 returns top 50 for bids and asks (default).
                = 2 returns full orderbook (full orderbook data is cached and usually updated every 10 seconds).

        Returns (dict):
            Each market order is represented as an array of 2 floats [price, volume].
            The attribute snapshotId is a unique number associated to orderbook and it changes every time orderbook changes.
            E.g:
            {
                "marketId": "BAT-AUD",
                "snapshotId": 1567334110144000,
                "asks": [[0.2677,5665.85], ... ],
                "bids": [[0.2612,17847.84], ...]
            }
        """
        return process_orderbook(
            self.makeHttpCall(
                "GET", f"/v3/markets/{market_id}/orderbook", {"level": level}
            )
        )

    def orderbooks(self, market_ids):
        """Retrieves list of bids and asks for a given market.

        Args:
            market_id (list of str): Unique Identifiers for the requested markets, e.g ["ETH-AUD"].

        Returns (list of dict):
            Each dict is an orderbook for a requested market.
            Each market order is represented as an array of 2 floats [price, volume].
            There are lists of market orders for 'asks' and 'bids', which make up an orderbook.
            The attribute snapshotId is a unique number associated to orderbook and it changes every time orderbook changes.
            E.g:
            [{
                "marketId": "BAT-AUD",
                "snapshotId": 1567334110144000,
                "asks": [[0.2677,5665.85], ... ],
                "bids": [[0.2612,17847.84], ...]
            }]
        """
        orderbooks = self.makeHttpCall(
            "GET",
            f"/v3/markets/orderbooks?" + "marketId=" + "&marketId=".join(market_ids),
        )
        # Convert the price and amount data to floats.
        for i in range(len(orderbooks)):
            orderbooks[i] = process_orderbook(orderbooks[i])
        return orderbooks

    def top_bid(self, market_id):
        """[Non API Feature] Convenience wrapper around orderbook to get just the top bid for a market."""
        return self.orderbook(market_id, level=0)["bids"][0]

    def top_ask(self, market_id):
        """[Non API Feature] Convenience wrapper around orderbook to get just the top ask for a market."""
        return self.orderbook(market_id, level=0)["asks"][0]

    def candles(self, market_id, timeWindow, from_time, to_time):
        """Fetches historical market candlestick data for a specified market and time range from the BTC Markets API and returns it as a pandas DataFrame.

        This method queries the BTC Markets API to retrieve candlestick (OHLC, Open High Low Close) data for a given market. The data includes open, high, low, and close prices, along with the trading volume for specified time intervals within the given time range. This information is useful for various forms of market analysis, particularly technical analysis.

        Args:
            market_id (str): The market identifier (e.g., 'BTC-AUD') for which candlestick data is to be retrieved.
            timeWindow (str): The granularity of the candlesticks (e.g., '1h' for one hour, '1d' for one day).
            from_time (str): The start time for the data in ISO 8601 format (e.g., '2021-01-01T00:00:00Z').
            to_time (str): The end time for the data in ISO 8601 format (e.g., '2021-01-07T00:00:00Z').

        Returns: pandas.DataFrame containing the candlestick data with the following columns:
            - 'timestamp' (datetime): The timestamp for each candlestick.
            - 'open' (float): The opening price for the time interval.
            - 'high' (float): The highest price during the time interval.
            - 'low' (float): The lowest price during the time interval.
            - 'close' (float): The closing price at the end of the time interval.
            - 'volume' (float): The trading volume during the time interval.
        """
        return make_df(
            pd.DataFrame(
                self.makeHttpCall(
                    "GET",
                    f"/v3/markets/{market_id}/candles",
                    {"timeWindow": timeWindow, "from": from_time, "to": to_time},
                ),
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            ),
            numeric=["open", "high", "low", "close", "volume"],
            time=["timestamp"],
        )

    def recent_candles(self, market_id, daysago=10, window_fmt="1h"):
        """[Non API Feature] Fetches candlestick (OHLC, Open High Low Close) data based on the current time"""
        now = datetime.now()
        prev = now - timedelta(days=daysago)
        now_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        prev_str = prev.strftime("%Y-%m-%dT%H:%M:%SZ")
        return self.candles(market_id, window_fmt, prev_str, now_str)

    # Order Placement APIs
    # https://docs.btcmarkets.net/#tag/Order-Placement-APIs

    def place_order(
        self,
        market_id,
        price,
        amount,
        side,
        order_type,
        triggerPrice=None,
        targetAmount=None,
        timeInForce="GTC",
        postOnly=False,
        selfTrade="P",  # Default to not trading with yourself.
        client_order_id=None,
    ):
        """
        Places a new order on the BTC Markets exchange with various customizable parameters.

        This method allows the user to place a new order, which can be a limit, market, stop limit, stop, or take profit order.
        Additional parameters like trigger price, target amount, time in force, post-only, self-trade prevention, and client-specified order ID can be specified.

        Args:
            market_id (str): The market identifier (e.g., 'BTC-AUD').
            price (str): The price at which to place the order.
            amount (str): The amount of the asset to buy or sell.
            side (str): The side of the order, either:
                - 'Bid' (buy).
                - 'Ask' (sell).
            order_type (str): The type of the order. Options are:
                - 'Limit'.
                - 'Market'.
                - 'Stop Limit'.
                - 'Stop'.
                - 'Take Profit'.
            triggerPrice (str, optional): Required if order type is 'Stop', 'Stop Limit', or 'Take Profit'.
            targetAmount (str, optional): Specify a target amount for the order.
            timeInForce (str, optional): Order execution strategy. Options are:
                - 'GTC' (Good Till Cancelled) (default).
                - 'FOK' (Fill or Kill).
                - 'IOC' (Immediate or Cancel).
            postOnly (bool, optional): Whether the order is post-only. Default is False.
            selfTrade (str, optional): Self-trade prevention setting, either:
                - 'A' allows self-trading.
                - 'P' prevents self-trading (Default).
            client_order_id (str, optional): A unique identifier for the order set by the client.

        Returns (dict): A response from the BTC Markets API indicating the status of the order placement.

        Example:
            >>> # Places a limit buy order for 0.1 BTC at a price of 1000 AUD per BTC.
            >>> btc_market_client = BTCMarkets("api_key", "private_key")
            >>> order_response = btc_market_client.place_order(
                market_id='BTC-AUD',
                price='1000',
                amount='0.1',
                side='Bid',
                order_type='Limit'
              )
        """
        if order_type in ["Stop", "Stop Limit", "Take Profit"] and triggerPrice is None:
            return self.handle_error(f"{order_type} requires triggerPrice")
        selfTrade = selfTrade.upper()
        if selfTrade not in ("P", "A"):
            return self.handle_error(
                f"{selfTrade} must be either 'P' (prevent) or 'A' (allow)"
            )

        data = {
            "marketId": market_id,
            "price": price,
            "amount": amount,
            "type": order_type,
            "side": side,
            "selfTrade": selfTrade,
            "timeInForce": timeInForce,
            "postOnly": postOnly,
            "triggerPrice": triggerPrice,
            "targetAmount": targetAmount,
            "clientOrderId": client_order_id,
        }
        # Filter out None values from query parameters
        data = {k: v for k, v in data.items() if v is not None}
        return self.makeHttpCall("POST", "/v3/orders", data=json.dumps(data))

    def orders(self, market_id=None, status=None, before=None, after=None, limit=None):
        """Returns an array of historical orders or open orders only.

        All query string parameters are optional, so by default and when no query parameter is provided, this API retrieves open orders only for all markets.
        This API supports pagination only when retrieving all orders status=all. When sending using status=open, all open orders are returned and with no pagination.

        Args:
            market_id (str, optional): The market identifier (e.g., 'BTC-AUD').
            status (str, optional): Enum:
                - "open" only returns orders with 'open' status.
                - "all"  return all orders.
            Pagination parameters:
                before (int64, optional): See orders before this order number.
                after (int64, optional):  See orders after this order number.
                limit (int32, optional):  Limit the number of orders to show.

        Returns: pandas.DataFrame with the following columns:
            - 'orderId' (str): Unique identifier for the order.
            - 'marketId' (str): The market identifier where the order was placed.
            - 'side' (str): Indicates whether the order is a bid (buy) or ask (sell).
            - 'type' (str): The type of the order (e.g., 'Limit').
            - 'creationTime' (datetime): The time when the order was created.
            - 'price' (float): The price at which the order was placed.
            - 'amount' (float): The total amount of the order.
            - 'openAmount' (float): The remaining open amount of the order.
            - 'status' (str): Current status of the order (e.g., 'Placed', 'Fully Matched').
            - 'postOnly' (bool): Flag indicating if the order is post only.
            - 'clientOrderId' (str): Client-provided identifier for the order.
        """
        query_params = {
            "marketId": market_id,
            "status": status,
            "before": before,
            "after": after,
            "limit": limit,
        }

        # Filter out None values from query parameters
        query_params = {k: v for k, v in query_params.items() if v is not None}
        data = self.makeHttpCall("GET", "/v3/orders", query_params=query_params)
        # If there are no orders return an empty array.
        if len(data) == 0:
            return data
        return make_df(
            data, numeric=["price", "amount", "openAmount"], time=["creationTime"]
        )

    def cancel_open_orders(self, market_id=None):
        """
        Cancels all open orders for all markets or optionally for a specific market.

        Args:
            market_id (str, optional): The market identifier (e.g., 'BTC-AUD') for which orders should be cancelled.
                If None, orders for all markets will be cancelled. Defaults to None.

        Returns (dict): A response indicating the success or failure of the order cancellation.
              The structure of the response will contain information about each order attempted to be cancelled.
              For example: {'success': True, 'details': [{'orderId': '12345', 'status': 'cancelled'}, ...]}
              In case of failure, an error message will be included.
        """
        query_params = {}
        if market_id is not None:
            query_params["marketId"] = market_id

        return self.makeHttpCall("DELETE", "/v3/orders", query_params)

    def order_by_id(self, order_id):
        """Returns an order by using either the exchange orderId or clientOrderId.

        Args:
            order_id (str):orderId or clientOrderId for an order.

        Returns: pandas.Series with the following columns:
            - 'orderId' (str): Unique identifier for the order.
            - 'marketId' (str): The market identifier where the order was placed.
            - 'side' (str): Indicates whether the order is a bid (buy) or ask (sell).
            - 'type' (str): The type of the order (e.g., 'Limit').
            - 'creationTime' (datetime): The time when the order was created.
            - 'price' (float): The price at which the order was placed.
            - 'amount' (float): The total amount of the order.
            - 'openAmount' (float): The remaining open amount of the order.
            - 'status' (str): Current status of the order (e.g., 'Placed', 'Fully Matched').
            - 'postOnly' (bool): Flag indicating if the order is post only.
            - 'clientOrderId' (str): Client-provided identifier for the order.
        """
        return make_series(
            self.makeHttpCall("GET", f"/v3/orders/{order_id}"),
            numeric=["price", "amount", "openAmount"],
            time=["creationTime"],
        )

    def cancel_order(self, order_id):
        """Cancels a single order specified by its ID.

        Args:
            order_id (str): The unique identifier of the order to be cancelled.

        Returns (dict): A response indicating the success or failure of the order cancellation.
            The structure of the response will contain information about the order that was attempted to be cancelled.
            For example: {'success': True, 'details': {'orderId': '12345', 'status': 'cancelled'}}
            In case of failure, an error message will be included.
        """
        return self.makeHttpCall("DELETE", f"/v3/orders/{order_id}")

    def replace_order(self, order_id, new_price, new_amount, client_order_id=None):
        """
        Replaces an existing order with a new order by first attempting to cancel the existing order and then placing a new order with updated price and amount.

        Args:
          order_id (str): The unique identifier of the order to be replaced.
          new_price (str): The price for the new order.
          new_amount (str): The amount for the new order.
          client_order_id (str, optional): A unique identifier for the new order set by the client.

        Returns (dict): A response from the BTC Markets API indicating the status of the order replacement.
            This includes the details of the new order if the replacement was successful.
            In case of failure, an error message will be included.
        """
        data = {"price": new_price, "amount": new_amount}
        if client_order_id:
            data["clientOrderId"] = client_order_id

        return self.makeHttpCall("PUT", f"/v3/orders/{order_id}", data=json.dumps(data))

    # Batch Order APIs
    # https://docs.btcmarkets.net/#tag/Batch-Order-APIs

    def place_and_cancel_orders(self, place_orders, cancel_orders):
        """
        Executes batch operations for placing and canceling orders. This allows multiple new orders to be placed and existing ones to be canceled in a single request.

        Args:
            place_orders (list of dict): A list of dictionaries, each representing an order to be placed. Each dictionary should contain the following keys: 'marketId', 'price', 'amount', 'type', 'side', and 'clientOrderId'.
            cancel_orders (list of dict): A list of dictionaries, each representing an order to be canceled. Each dictionary should contain either 'orderId' or 'clientOrderId'.

        Returns (dict): A response from the BTC Markets API indicating the status of the batch operations, including details of orders placed and canceled, and any unprocessed requests.
        """
        batch_orders = []

        for order in place_orders:
            batch_orders.append({"placeOrder": order})

        for order in cancel_orders:
            batch_orders.append({"cancelOrder": order})

        response = self.makeHttpCall(
            "POST", "/v3/batchorders", data=json.dumps(batch_orders)
        )
        return response

    def orders_by_ids(self, ids):
        """
        Retrieves a batch of orders by using either the exchange `orderId` or `clientOrderId`.

        Args:
          ids (list of str): A list of order IDs (either `orderId` or `clientOrderId`).

        Returns (dict):
            orders: pandas.DataFrame where each row represents an order with the following columns:
                - 'orderId' (str): Unique identifier for the order.
                - 'marketId' (str): The market identifier where the order was placed.
                - 'side' (str): Indicates whether the order is a bid (buy) or ask (sell).
                - 'type' (str): The type of the order (e.g., 'Limit').
                - 'creationTime' (datetime): The time when the order was created.
                - 'price' (float): The price at which the order was placed.
                - 'amount' (float): The total amount of the order.
                - 'openAmount' (float): The remaining open amount of the order.
                - 'status' (str): Current status of the order (e.g., 'Placed', 'Fully Matched').
                - 'postOnly' (bool): Flag indicating if the order is post only.
                - 'clientOrderId' (str, optional): Client-provided identifier for the order.
            unprocessedRequests (dict):
                - 'code' (str): API error code. For a complete list see: https://docs.btcmarkets.net/#tag/ErrorCodes.
                - 'messsage' (str): Error message.
                - 'requestId" (str): The requested order_id that was not able to be processed.

        Example output:
            {
                'orders': [{
                    'orderId': '123412341234',
                    'marketId': 'IMX-AUD',
                    'side': 'Ask',
                    'type': 'Limit',
                    'creationTime': '2024-03-01T01:00:00.000000Z',
                    'price': '3.7',
                    'amount': '45',
                    'openAmount': '45',
                    'status': 'Placed',
                    'postOnly': False,
                    'clientOrderId': '12345678-1234-4321-1111-abcdefghijkl'},
                }],
                'unprocessedRequests': [{
                    'code': 'OrderNotFound',
                    'message': 'order was not found',
                    'requestId': '123412341234',
                }]
        """
        # Joining the list of ids with commas to create a comma-separated string
        ids_str = ",".join([str(id) for id in ids])
        response = self.makeHttpCall("GET", f"/v3/batchorders/{ids_str}")

        # Check if the response is empty
        if not response:
            return None
        response["orders"] = make_df(
            response["orders"],
            numeric=["price", "amount", "openAmount"],
            time=["creationTime"],
        )
        return response

    def cancel_orders_by_ids(self, ids):
        """
        Cancels a list of orders specified by their IDs in a single request.

        Args:
            ids (list of str): A list of order IDs to be cancelled.

        Returns (dict): A response from the BTC Markets API indicating the status of the cancellation.
        E.g:
        {
          "cancelOrders": [
          {
            "orderId": "414186",
            "clientOrderId": "6"
          },
          {
            "orderId": "414192",
            "clientOrderId": "7"
          }
          ],
          "unprocessedRequests": [
          {
            "code": "OrderAlreadyCancelled",
            "message": "order is already cancelled.",
            "requestId": "1"
          }
          ]
        }
        """
        ids_str = ",".join([str(id) for id in ids])
        # Joining the list of ids with commas to create a comma-separated string
        return self.makeHttpCall("DELETE", f"/v3/batchorders/{ids_str}")

    def list_trades(
        self, market_id=None, order_id=None, before=None, after=None, limit=None
    ):
        """
        Retrieves trades and optionally filters by marketId or orderId/clientOrderId. The default behavior
        when no query parameter is specified is to return your most recent trades for all orders and markets.

        Args:
            market_id (str, optional): Optionally filter trades by marketId (e.g. 'XRP-AUD').
            order_id (str, optional): Optionally list all trades for a single order.
            Pagination parameters:
                before (int, optional): See trades before this trade number for pagination.
                after (int, optional): See trades after this trade number for pagination.
                limit (int, optional): Limit the number of trades to show.

        Returns: pandas.DataFrame where each row represents a trade with the following columns:
            - 'id' (str): The unique identifier of the trade.
            - 'marketId' (str): The market identifier where the trade occurred.
            - 'price' (float): The price at which the trade was executed.
            - 'amount' (float): The amount of cryptocurrency traded.
            - 'timestamp' (datetime): The timestamp when the trade occurred.
            - 'side' (str): Indicates whether the trade was a bid (buy) or ask (sell).
            - 'fee' (float): The fee associated with the trade.
            - 'orderId' (str): identifier for the order that led to the trade.
            - 'valueInQuoteAsset' (float): The value of the trade in the quote asset.
            - 'liquidityType' (str): Indicates the liquidity type of the trade (e.g., Maker or Taker).
            - 'clientOrderId' (str): Client-provided identifier for the trade, if available.
        """
        query_params = {
            "marketId": market_id,
            "orderId": order_id,
            "before": before,
            "after": after,
            "limit": limit,
        }

        # Filter out None values from query parameters
        query_params = {k: v for k, v in query_params.items() if v is not None}
        response = self.makeHttpCall("GET", "/v3/trades", query_params)

        # Process response into DataFrame
        return make_df(
            response,
            numeric=["price", "amount", "fee", "valueInQuoteAsset"],
            time=["timestamp"],
        )

    def trade_by_id(self, trade_id):
        """Retrieves a single trade by its unique identifier.

        Args:
            trade_id (str): The unique identifier of the trade.

        Returns: pandas.Series where each row represents a trade with the following columns:
            - 'id' (str): The unique identifier of the trade.
            - 'marketId' (str): The market identifier where the trade occurred.
            - 'price' (float): The price at which the trade was executed.
            - 'amount' (float): The amount of cryptocurrency traded.
            - 'timestamp' (datetime): The timestamp when the trade occurred.
            - 'side' (str): Indicates whether the trade was a bid (buy) or ask (sell).
            - 'fee' (float): The fee associated with the trade.
            - 'orderId' (str): identifier for the order that led to the trade.
            - 'valueInQuoteAsset' (float): The value of the trade in the quote asset.
            - 'liquidityType' (str): Indicates the liquidity type of the trade (e.g., Maker or Taker).
            - 'clientOrderId' (str): Client-provided identifier for the trade, if available.
        """
        response = self.makeHttpCall("GET", f"/v3/trades/{trade_id}")

        # Process response into DataFrame
        return make_series(
            response,
            numeric=["price", "amount", "fee", "valueInQuoteAsset"],
            time=["timestamp"],
        )

        # Fund Management APIs

    def request_withdrawal(
        self,
        asset_name,
        amount,
        to_address=None,
        account_name=None,
        account_number=None,
        bsb_number=None,
        bank_name=None,
        payment_description=None,
        client_transfer_id=None,
    ):
        """
        Requests a withdrawal of crypto assets or AUD.

        Args:
            asset_name (str): The name of the asset to withdraw (e.g., 'AUD', 'BTC').
            amount (str): The amount to withdraw.
            to_address (str, optional): The destination address for crypto withdrawal. Mandatory for crypto assets.
            account_name (str, optional): Optional for AUD withdrawal. When not specified, default bank information is used.
            account_number (str, optional): Optional for AUD withdrawal. When not specified, default bank information is used.
            bsb_number (str, optional): Optional for AUD withdrawal. When not specified, default bank information is used.
            bank_name (str, optional): Optional for AUD withdrawal. When not specified, default bank information is used.
            payment_description (str, optional): Optional for AUD withdrawal. Maximum character length of 18 and only alphanumeric.
            client_transfer_id (str, optional): Optional for withdrawal requests. Used for tracking.

        Returns (dict): A response from the BTC Markets API indicating the status of the withdrawal request.

        Usage example:
            >>> btc_markets_client = BTCMarkets("your_api_key", "your_private_key")
            >>> withdrawal_response = btc_markets_client.request_withdrawal(
            >>>     asset_name='BTC',
            >>>     amount='0.5',
            >>>     to_address='crypto_wallet_address'
            >>> )
        """

        if not asset_name or not isinstance(asset_name, str):
            return self.handle_error("Asset name must be a non-empty string.")
        if not amount or not isinstance(amount, str):
            return self.handle_error("Amount must be a non-empty string.")
        if asset_name == "AUD":
            if any([account_name, account_number, bsb_number, bank_name]) and not all(
                [account_name, account_number, bsb_number, bank_name]
            ):
                return self.handle_error(
                    "For AUD withdrawals, all bank details must be provided if any are provided."
                )
            if payment_description and len(payment_description) > 18:
                return self.handle_error(
                    "Payment description must be less than or equal to 18 characters."
                )
        else:
            if not to_address:
                return self.handle_error(
                    "Destination address is mandatory for crypto asset withdrawals."
                )

        data = {
            "assetName": asset_name,
            "amount": amount,
            "toAddress": to_address,
            "accountName": account_name,
            "accountNumber": account_number,
            "bsbNumber": bsb_number,
            "bankName": bank_name,
            "paymentDescription": payment_description,
            "clientTransferId": client_transfer_id,
        }
        # Remove 'None' values from dict.
        data = {key: value for key, value in data.items() if value is not None}
        return self.makeHttpCall("POST", "/v3/withdrawals", data=data)

    def list_withdrawals(self, before=None, after=None, limit=None):
        """
        Retrieves a list of withdrawals with optional pagination.

        Args (Pagination):
            before (int, optional): See withdrawals before this withdrawal number.
            after (int, optional): See withdrawals after this withdrawal number.
            limit (int, optional): Limit the number of withdrawals to show.

        Returns: pandas.DataFrame containing the withdrawal details with columns:
            - 'id' (str): Unique identifier for the withdrawal.
            - 'assetName' (str): Name of the asset withdrawn.
            - 'amount' (str): The amount withdrawn.
            - 'type' (str): The type of transaction (e.g., 'Withdraw').
            - 'creationTime' (datetime): The time when the withdrawal was requested.
            - 'status' (str): Current status of the withdrawal (e.g., 'Complete', 'Pending').
            - 'description' (str): Description of the withdrawal.
            - 'fee' (str): The fee associated with the withdrawal.
            - 'lastUpdate' (datetime): The last update time for the withdrawal.
            - 'clientTransferId' (str, optional): Client-specified identifier for the withdrawal.
            - 'paymentDetail' (dict, optional): Additional payment details, varies based on asset.

        Example:
            >>> btc_markets_client = BTCMarkets("your_api_key", "your_private_key")
            >>> withdrawals_df = btc_markets_client.list_withdrawals(limit=10)
        """
        query_params = {"before": before, "after": after, "limit": limit}

        # Filter out None values from query parameters
        query_params = {k: v for k, v in query_params.items() if v is not None}

        return make_df(
            self.makeHttpCall("GET", "/v3/withdrawals", query_params),
            numeric=["amount", "fee"],
            time=["creationTime", "lastUpdate"],
        )

    def withdrawal_by_id(self, withdrawal_id):
        """
        Retrieves details of a specific withdrawal using its ID.

        Args:
            withdrawal_id (str): The unique identifier of the withdrawal to retrieve.

        Returns: pandas.Series containing the details of the withdrawal, with columns:
            - 'id' (str): Unique identifier for the withdrawal.
            - 'assetName' (str): Name of the asset withdrawn.
            - 'amount' (str): The amount withdrawn.
            - 'type' (str): The type of transaction (e.g., 'Withdraw').
            - 'creationTime' (datetime): The time when the withdrawal was requested.
            - 'status' (str): Current status of the withdrawal (e.g., 'Complete', 'Pending').
            - 'description' (str): Description of the withdrawal.
            - 'fee' (str): The fee associated with the withdrawal.
            - 'lastUpdate' (datetime): The last update time for the withdrawal.
            - 'clientTransferId' (str, optional): Client-specified identifier for the withdrawal.
            - 'paymentDetail' (dict, optional): Additional payment details, varies based on asset.

        Example:
            >>> btc_markets_client = BTCMarkets("your_api_key", "your_private_key")
            >>> withdrawal_details = btc_markets_client.get_withdrawal_by_id("123456")
        """
        response = self.makeHttpCall("GET", f"/v3/withdrawals/{withdrawal_id}")
        return make_series(
            response, numeric=["amount", "fee"], time=["creationTime", "lastUpdate"]
        )

    def list_deposits(self, before=None, after=None, limit=None):
        """
        Retrieves a list of deposit transactions. This method supports pagination.

        Args (Pagination):
            before (int, optional): Get deposits before this deposit number. Defaults to None.
            after (int, optional): Get deposits after this deposit number. Defaults to None.
            limit (int, optional): Limit the number of deposits to show. Defaults to None.

        Returns: pandas.DataFrame containing deposit transactions with columns:
            - 'id' (str): Unique identifier for the deposit.
            - 'assetName' (str): Name of the asset deposited.
            - 'amount' (str): The amount deposited.
            - 'type' (str): The type of transaction (e.g., 'Deposit').
            - 'creationTime' (datetime): The time when the deposit was recorded.
            - 'status' (str): Current status of the deposit (e.g., 'Complete', 'Pending').
            - 'description' (str): Description of the deposit.
            - 'fee' (str): The fee associated with the deposit.
            - 'lastUpdate' (datetime): The last update time for the deposit.
            - 'clientTransferId' (str, optional): Client-specific identifier for the deposit.
            - 'paymentDetail' (dict, optional): Additional payment details, varies based on asset.

        Example:
            >>> btc_markets_client = BTCMarkets("your_api_key", "your_private_key")
            >>> deposits = btc_markets_client.list_deposits(before=78234976, limit=10)
        """
        query_params = {"before": before, "after": after, "limit": limit}

        # Filter out None values from query parameters
        query_params = {k: v for k, v in query_params.items() if v is not None}

        response = self.makeHttpCall("GET", "/v3/deposits", query_params=query_params)
        return make_df(
            response, numeric=["amount", "fee"], time=["creationTime", "lastUpdate"]
        )

    def deposit_by_id(self, deposit_id):
        """
        Retrieves details of a specific deposit transaction using its unique identifier.

        Args:
            deposit_id (str): The unique identifier of the deposit transaction.

        Returns: pandas.Series with each row containing the details of the deposit, with columns:
            - 'id' (str): Unique identifier for the deposit.
            - 'assetName' (str): Name of the asset deposited.
            - 'amount' (str): The amount deposited.
            - 'type' (str): The type of transaction (e.g., 'Deposit').
            - 'creationTime' (datetime): The time when the deposit was recorded.
            - 'status' (str): Current status of the deposit (e.g., 'Complete', 'Pending').
            - 'description' (str): Description of the deposit.
            - 'fee' (str): The fee associated with the deposit.
            - 'lastUpdate' (datetime): The last update time for the deposit.
            - 'clientTransferId' (str, optional): Client-specific identifier for the deposit.
            - 'paymentDetail' (dict, optional): Additional payment details, varies based on asset.

        Example:
            >>> btc_markets_client = BTCMarkets("your_api_key", "your_private_key")
            >>> deposit_details = btc_markets_client.deposit_by_id("12345678")
        """
        return make_series(
            self.makeHttpCall("GET", f"/v3/deposits/{deposit_id}"),
            numeric=["amount", "fee"],
            time=["creationTime", "lastUpdate"],
        )

    def list_deposits_withdrawals(self, before=None, after=None, limit=None):
        """
        Retrieves a list of deposit and withdrawal transactions. This method supports pagination.

        Args (Pagination):
            before (int, optional): Get records before this transaction ID for pagination. Defaults to None.
            after (int, optional): Get records after this transaction ID for pagination. Defaults to None.
            limit (int, optional): The number of records to retrieve. Defaults to None.

        Returns: pandas.DataFrame with each row containing the details of the transactions, with columns:
            - 'id' (str): Unique identifier for the transaction.
            - 'assetName' (str): Name of the asset involved in the transaction.
            - 'amount' (str): The amount of the asset in the transaction.
            - 'type' (str): The type of transaction (e.g., 'Deposit', 'Withdraw').
            - 'creationTime' (datetime): The time when the transaction was recorded.
            - 'status' (str): Current status of the transaction (e.g., 'Complete', 'Pending').
            - 'description' (str): Description of the transaction.
            - 'fee' (str): The fee associated with the transaction.
            - 'lastUpdate' (datetime): The last update time for the transaction.
            - 'clientTransferId' (str, optional): Client-specific identifier for the transaction.
            - 'paymentDetail' (dict, optional): Additional payment details, varies based on asset.

        Example:
            >>> btc_markets_client = BTCMarkets("your_api_key", "your_private_key")
            >>> transactions = btc_markets_client.list_deposits_withdrawals(limit=10)
        """
        query_params = {"before": before, "after": after, "limit": limit}

        # Filter out None values from query parameters
        query_params = {k: v for k, v in query_params.items() if v is not None}

        response = self.makeHttpCall("GET", "/v3/transfers", query_params=query_params)
        return make_df(
            response, numeric=["amount", "fee"], time=["creationTime", "lastUpdate"]
        )

    def deposits_withdrawals_by_id(self, transaction_id):
        """
        Retrieves the details of a specific deposit or withdrawal transaction by its ID.

        Args:
            transaction_id (str): The unique identifier of the transaction.

        Returns: pandas.Series containing the details of the transaction, with fields:
            - 'id' (str): Unique identifier for the transaction.
            - 'assetName' (str): Name of the asset involved in the transaction.
            - 'amount' (str): The amount of the asset in the transaction.
            - 'type' (str): The type of transaction (e.g., 'Deposit', 'Withdraw').
            - 'creationTime' (datetime): The time when the transaction was recorded.
            - 'status' (str): Current status of the transaction (e.g., 'Complete', 'Pending').
            - 'description' (str): Description of the transaction.
            - 'fee' (str): The fee associated with the transaction.
            - 'lastUpdate' (datetime): The last update time for the transaction.
            - 'clientTransferId' (str, optional): Client-specific identifier for the transaction.
            - 'paymentDetail' (dict, optional): Additional payment details, varies based on asset.

        Example:
            >>> btc_markets_client = BTCMarkets("your_api_key", "your_private_key")
            >>> transaction_details = btc_markets_client.deposits_withdrawals_by_id("transaction_id")
        """
        return make_series(
            self.makeHttpCall("GET", f"/v3/transfers/{transaction_id}"),
            numeric=["amount", "fee"],
            time=["creationTime", "lastUpdate"],
        )

    def deposit_address(self, asset_name):
        """
        Retrieves the deposit address for a given asset.

        Args:
            asset_name (str): The name of the asset for which the deposit address is requested.

        Returns: pandas.Series containing the deposit address details, with fields:
            - 'assetName' (str): The name of the asset.
            - 'address' (str): The deposit address for the specified asset.

        Example:
            >>> btc_markets_client = BTCMarkets("your_api_key", "your_private_key")
            >>> address_details = btc_markets_client.deposit_address("BTC")
        """
        return make_series(
            self.makeHttpCall("GET", "/v3/addresses", {"assetName": asset_name}),
        )

    def withdrawal_fees(self):
        """
        Retrieves the withdrawal fees for various assets.

        Returns: pandas.DataFrame containing the withdrawal fees for each asset, with columns:
            - 'assetName' (str): The name of the asset.
            - 'fee' (float): The withdrawal fee for the specified asset.

        Example:
            >>> btc_markets_client = BTCMarkets("your_api_key", "your_private_key")
            >>> fees = btc_markets_client.withdrawal_fees()
        """
        return make_df(
            self.makeHttpCall("GET", "/v3/withdrawal-fees"),
            numeric=["fee"],
        )

    def list_assets(self):
        """
        Retrieves a list of assets including their configuration details.

        Returns: pandas.DataFrame containing the details of each asset, with columns:
            - 'assetName' (str): The name of the asset.
            - 'assetFullName' (str): The full name of the asset.
            - 'minDepositAmount' (float): Minimum amount to deposit.
            - 'maxDepositAmount' (float): Maximum amount to deposit.
            - 'depositDecimals' (int): Number of decimal places allowed for deposits.
            - 'depositFee' (float): Deposit fee.
            - 'minWithdrawalAmount' (float): Minimum amount to withdraw.
            - 'maxWithdrawalAmount' (float): Maximum amount to withdraw.
            - 'withdrawalDecimals' (int): Number of decimal places allowed for withdrawals.
            - 'withdrawalFee' (float): Withdrawal fee.
            - 'status' (str): Current status of the asset.

        Example:
            >>> btc_markets_client = BTCMarkets("your_api_key", "your_private_key")
            >>> assets = btc_markets_client.list_assets()
        """
        return make_df(
            self.makeHttpCall("GET", "/v3/assets"),
            numeric=[
                "minDepositAmount",
                "maxDepositAmount",
                "depositFee",
                "minWithdrawalAmount",
                "maxWithdrawalAmount",
                "withdrawalFee",
                "depositDecimals",
                "withdrawalDecimals",  # Include these as integers
            ],
        )

        # Account APIs

    # The trading fee api is split into 2 functions because of the structure of the response.
    # 'trading_fees' has the full response, and 'fee_by_market' has a pandas.DataFrame made from feeByMarkets.
    def trading_fees(self):
        """
        Retrieves the trading fee information, including the 30-day trading volume and fee rates for various markets.

        Returns (dict):
            - 'volume30Day' (float): The trading volume over the past 30 days.
            - 'feeByMarkets' (list of dicts):
            - 'marketId' (str): Market identifier.
            - 'makerFeeRate' (float): The fee rate for the maker side of trades.
            - 'takerFeeRate' (float): The fee rate for the taker side of trades.
        """
        return self.makeHttpCall("GET", "/v3/accounts/me/trading-fees")

    def fee_by_market(self):
        """
        Retrieves the trading fee information, including the 30-day trading volume and fee rates for various markets.

        Returns: pandas.DataFrame containing the trading fee information, with columns:
            - 'marketId' (str): Market identifier.
            - 'makerFeeRate' (float): The fee rate for the maker side of trades.
            - 'takerFeeRate' (float): The fee rate for the taker side of trades.
        """
        response = self.trading_fees()
        fees_df = make_df(
            response.get("feeByMarkets", []),
            numeric=["makerFeeRate", "takerFeeRate"],
            time=[],
        )
        return fees_df

    def withdrawal_limits(self):
        """
        Retrieves the daily withdrawal limits per asset for the user's account.

        Returns (dict): A dictionary containing two lists:
            - 'dailyLimits': A list of dictionaries for each asset showing the remaining withdrawal limit.
            - 'totalDailyLimits': A list of dictionaries showing the total daily limits for different types (e.g., AUD, Crypto).

        Example:
            >>> btc_markets_client = BTCMarkets("your_api_key", "your_private_key")
            >>> withdrawal_limits = btc_markets_client.get_withdrawal_limits()
            >>> print(withdrawal_limits)
          {
            'dailyLimits': [
              {'assetName': 'BTC', 'remaining': '1.518'},
              # ... more assets ...
            ],
            'totalDailyLimits': [
              {'type': 'AUD', 'limit': '20000.0', 'used': '0.0', 'remaining': '20000.0'},
              {'type': 'Crypto', 'limit': '100000.0', 'used': '0.0', 'remaining': '100000.0'}
              # ... more types ...
            ]
          }
        """
        return self.makeHttpCall("GET", "/v3/accounts/me/withdrawal-limits")

    def account_balance(
        self, include_empty=False, add_locked_ratio=True, sort_balance=True
    ):
        """Fetch the account balances for crypto assets with options for improved readability.

        Args:
            include_empty (bool): [Non API Feature] Include assets with 0 balance (default False)
            add_locked_ratio (bool): [Non API Feature] Add the ratio between locked/available (default True)
            sort_balance (bool): [Non API Feature] Sort by decending balances (default True).

        Returns: pandas.DataFrame with the following columns:
            - assetName (str): The standard name of the asset e.g "ETH".
            - balance (float): The total amount of a currency owned.
            - available (float): The amount available of each currency.
            - locked (float): The amount in each currency locked in orders.
            - locked (locked_ratio): (If add_locked_ratio==True) The ratio between locked/available.
        """
        res = self.makeHttpCall("GET", "/v3/accounts/me/balances")
        df = make_df(res, numeric=["balance", "locked", "available"])
        if add_locked_ratio:
            df["locked_ratio"] = df["locked"] / df["balance"]
        if sort_balance:
            df.sort_values(by="balance", inplace=True, ascending=False)
        if not include_empty:
            df = df[df["balance"] != 0]
        return df.reset_index(drop=True)

    def transactions(self, asset_name=None, before=None, after=None, limit=None):
        """
        Retrieves detailed ledger records for underlying wallets. Supports optional pagination and filtering by specific asset.

        Args:
            asset_name (str, optional): Filter transactions for a specific asset.
            Pagination parameters:
                before (int, optional): See transactions before this transaction number for pagination.
                after (int, optional): See transactions after this transaction number for pagination.
                limit (int, optional): Limit the number of transactions to show.

        Returns: pandas.DataFrame with the following columns:
            - 'id' (str): Unique identifier for the transaction.
            - 'creationTime' (datetime): The time when the transaction was created.
            - 'description' (str): Description of the transaction.
            - 'assetName' (str): Name of the asset involved in the transaction.
            - 'amount' (float): The amount of the asset in the transaction.
            - 'balance' (float): The balance of the asset after the transaction.
            - 'type' (str): Type of the transaction (e.g., 'Deposit', 'Withdrawal').
            - 'recordType' (str): Record type of the transaction (e.g., 'Trade', 'Fund Transfer').
            - 'referenceId' (str): Reference ID associated with the transaction.
        """
        query_params = {
            "assetName": asset_name,
            "before": before,
            "after": after,
            "limit": limit,
        }

        # Filter out None values from query parameters
        query_params = {k: v for k, v in query_params.items() if v is not None}

        transactions = self.makeHttpCall(
            "GET", "/v3/accounts/me/transactions", query_params
        )
        return make_df(
            transactions, numeric=["amount", "balance"], time=["creationTime"]
        )

    # Report APIs
    # https://docs.btcmarkets.net/#tag/Report-APIs
    # This code has had issues with the BTC Markets API. The Permissions required are not clear.

    def create_new_report(self, report_type="TransactionReport", report_format="json"):
        """
        Requests the generation of a new report on the BTC Markets platform.

        Args:
            report_type (str): Type of the report. Currently, only 'TransactionReport' is accepted.
            report_format (str): Format of the report. Can be either 'csv' or 'json'.

        Returns (dict): A response from the BTC Markets API indicating the status of the report request.
              Includes 'id', 'contentUrl', 'creationTime', 'type', 'status', and 'format'.
        """
        data = {"type": report_type, "format": report_format}
        return self.makeHttpCall("POST", "/v3/reports", data=json.dumps(data))

    def report_by_id(self, report_id):
        """
        Retrieves the details of a previously requested report by its ID from the BTC Markets platform.

        Args:
            report_id (str): The unique identifier of the report.

        Returns (dict): A response from the BTC Markets API containing details of the report.
                Includes 'id', 'contentUrl', 'creationTime', 'type', 'status', 'format', and possibly other fields.
        """
        return self.makeHttpCall("GET", f"/v3/reports/{report_id}")

    # Misc APIs
    # https://docs.btcmarkets.net/#tag/Misc-APIs

    def get_server_time(self):
        """
        Retrieves the current server time from the BTC Markets API.

        Returns (datetime): The current server time in ISO 8601 format.
        """
        return pd.to_datetime(self.makeHttpCall("GET", "/v3/time")["timestamp"])