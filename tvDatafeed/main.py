
import datetime
import enum
import json
import logging
import random
import re
import string
import pandas as pd
from websocket import create_connection
import requests
import json

logger = logging.getLogger(__name__)


class Interval(enum.Enum):
    in_1_minute = "1"
    in_3_minute = "3"
    in_5_minute = "5"
    in_15_minute = "15"
    in_30_minute = "30"
    in_45_minute = "45"
    in_1_hour = "1H"
    in_2_hour = "2H"
    in_3_hour = "3H"
    in_4_hour = "4H"
    in_daily = "1D"
    in_weekly = "1W"
    in_monthly = "1M"


class TvDatafeed:
    __sign_in_url = 'https://www.tradingview.com/accounts/signin/'
    __search_url = 'https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production'
    __ws_headers = json.dumps({"Origin": "https://data.tradingview.com"})
    __signin_headers = {'Referer': 'https://www.tradingview.com'}
    __ws_timeout = 5

    def __init__(
        self,
        username: str = None,
        password: str = None,
        auth_token: str = 'eyJhbGciOiJSUzUxMiIsImtpZCI6IkdaeFUiLCJ0eXAiOiJKV1QifQ.eyJ1c2VyX2lkIjo1NDgwNTUyMSwiZXhwIjoxNzI3ODIxNzk0LCJpYXQiOjE3Mjc4MDczOTQsInBsYW4iOiJwcm9fcHJlbWl1bSIsImV4dF9ob3VycyI6MSwicGVybSI6IiIsInN0dWR5X3Blcm0iOiJQVUI7NmEyZWI4MDUxMjc5NDkxOTk0MDk2MWQ4ZjUxMTYwMzUsUFVCOzQ2NjMzNTU3ZDE5YjQwYjE5MjQzMmVmNmYyOTA4ZDE0LFBVQjtjMDYxOTIzODdlMmU0ZDJkYTdjZDEzNmVlMjgxOTVlNixQVUI7ZGJmZGMzZDNmNDA3NDIyZjkzYjE2NDAyYTFmOGMwMGQsUFVCO2YzM2IxNTAzZjIyMDQxYzViZTVmZTkxMzMxY2NjNDE4LFBVQjtjZTNhZDY5MzI5MjQ0NTQ5OTgwMjE3NmM2YzRiNzkxMSxQVUI7NmM5MWEyOTRiM2ViNGQ5MTg3MWZlNzFjMzYyY2VkNTEsUFVCOzdjNGIwOWIyYjRhNjQ5YTNhNjU0ZjY2ZWZmOTE1NTcwLHR2LWNoYXJ0cGF0dGVybnMsUFVCO2YwOTdhNzhmODM0MjRiNmY4MWMwODQ3ZTVjOTg4Y2M4LFBVQjtlYTA4M2Q5MDUzNjQ0NzZkOWRmN2FjODBhMjVkMjg1YixQVUI7NmZiZWE5YmRmMGNmNGQ2YWJiNmFjOWE4NmI4OGM5MjQsUFVCO2UxZjAwZjZlNWY2NjRkNGJiMzM2ZWE5NGVhYzlmY2FlLFBVQjszMGNiYzY5MWJiOTM0YTI1OTkyOGU3NzYxYzg5YjBmNCxQVUI7YTQ4MWI5YzMzMmEwNDZmNThiODhmYmVhYWY4YWFhOTgsUFVCOzNlODU4YWU2MTNiMjQwMGU4MzAyOTE5ZGQ0MWQzOTU4LFBVQjtjNzNlOWMzZDA0YTU0MWQwOTVkMGQ3MzVhYTdiZjU5MSxQVUI7NWEyMGMzZTY2MTdjNDJjNWFjMmRmYzZlZmYxMzlkZjIsUFVCOzAzNzA2NmMyY2VjZTRjYTk4MzI3ODA0MTIzZWFiMjcyLHR2LXByb3N0dWRpZXMsdHYtY2hhcnRfcGF0dGVybnMsUFVCOzJkZDYyMDBkNmJmNjQ1OTI5MGFiNzJiZDZiODQyMzkxLFBVQjtiM2Y3YzBiY2I0NWQ0MzJjYThkNDc4NDczMGE5YzIyMyx0di12b2x1bWVieXByaWNlLFBVQjszOTVkNjAyYzVlZTI0ODMwYTc1MjZjYWFmN2MyNjI3YyIsIm1heF9zdHVkaWVzIjoyNSwibWF4X2Z1bmRhbWVudGFscyI6MTAsIm1heF9jaGFydHMiOjgsIm1heF9hY3RpdmVfYWxlcnRzIjo0MDAsIm1heF9zdHVkeV9vbl9zdHVkeSI6MjQsImZpZWxkc19wZXJtaXNzaW9ucyI6WyJyZWZib25kcyJdLCJtYXhfb3ZlcmFsbF9hbGVydHMiOjIwMDAsIm1heF9hY3RpdmVfcHJpbWl0aXZlX2FsZXJ0cyI6NDAwLCJtYXhfYWN0aXZlX2NvbXBsZXhfYWxlcnRzIjo0MDAsIm1heF9jb25uZWN0aW9ucyI6NTB9.cAWGPm30ohtUEBNo1ryPxzxSgkzEHR_zjk8jS3cDI-ERQl2iv-Cs6x-ozIA4HQm9pwF607wk-XMl7AGWdBpRxCaTstq8COYg1pzCbPYYQDW3MIXLG_sL96PZDATfb-EXN3O4ZIPSffAv38QKeBHvnRuU7KbqS_GKjRp3pSXrtAc',
    ) -> None:
        """Create TvDatafeed object

        Args:
            username (str, optional): tradingview username. Defaults to None.
            password (str, optional): tradingview password. Defaults to None.
        """

        self.ws_debug = False

        if auth_token:
            self.token = auth_token
        else:
            self.token = self.__auth(username, password)

        if self.token is None:
            self.token = "unauthorized_user_token"
            logger.warning(
                "no login"
            )

        self.ws = None
        self.session = self.__generate_session()
        self.chart_session = self.__generate_chart_session()

    def __auth(self, username, password):
        if username is None or password is None:
            logger.warning("Username or password is None.")
            return None

        data = {"username": username, "password": password, "remember": "on"}

        try:
            response = requests.post(url=self.__sign_in_url, data=data, headers=self.__signin_headers)
            response_data = response.json()

            # Check if the response contains the auth_token
            if 'user' in response_data and 'auth_token' in response_data['user']:
                token = response_data['user']['auth_token']
                logger.info("Authorization successful.")
                return token
            else:
                logger.error("Authorization failed. Response: %s", response_data)
                return None

        except Exception as e:
            logger.error('Error during sign-in: %s', e)
            return None

    def __create_connection(self):
        logging.debug("creating websocket connection")
        self.ws = create_connection(
            "wss://data.tradingview.com/socket.io/websocket", headers=self.__ws_headers, timeout=self.__ws_timeout
        )

    @staticmethod
    def __filter_raw_message(text):
        try:
            found = re.search('"m":"(.+?)",', text).group(1)
            found2 = re.search('"p":(.+?"}"])}', text).group(1)

            return found, found2
        except AttributeError:
            logger.error("error in filter_raw_message")

    @staticmethod
    def __generate_session():
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters)
                                for i in range(stringLength))
        return "qs_" + random_string

    @staticmethod
    def __generate_chart_session():
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters)
                                for i in range(stringLength))
        return "cs_" + random_string

    @staticmethod
    def __prepend_header(st):
        return "~m~" + str(len(st)) + "~m~" + st

    @staticmethod
    def __construct_message(func, param_list):
        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    def __create_message(self, func, paramList):
        return self.__prepend_header(self.__construct_message(func, paramList))

    def __send_message(self, func, args):
        m = self.__create_message(func, args)
        if self.ws_debug:
            print(m)
        self.ws.send(m)

    @staticmethod
    def __create_df(raw_data, symbol):
        try:
            out = re.search(r'"s":\[(.+?)\}\]', raw_data).group(1)  # Use raw string
            x = out.split(',{"')
            data = list()
            volume_data = True

            for xi in x:
                xi = re.split(r"\[|:|,|\]", xi)  # Use raw string
                ts = datetime.datetime.fromtimestamp(float(xi[4]))

                row = [ts]

                for i in range(5, 10):
                    # skip converting volume data if does not exist
                    if not volume_data and i == 9:
                        row.append(0.0)
                        continue
                    try:
                        row.append(float(xi[i]))
                    except ValueError:
                        volume_data = False
                        row.append(0.0)
                        logger.debug('no volume data')

                data.append(row)

            data = pd.DataFrame(
                data, columns=["Datetime", "Open", "High", "Low", "Close", "Volume"]
            ).set_index("Datetime")
            data.insert(0, "Symbol", value=symbol)
            return data
        except AttributeError:
            logger.error("no data, please check the exchange and symbol")
    @staticmethod
    def __format_symbol(symbol, exchange, contract: int = None):

        if ":" in symbol:
            pass
        elif contract is None:
            symbol = f"{exchange}:{symbol}"

        elif isinstance(contract, int):
            symbol = f"{exchange}:{symbol}{contract}!"

        else:
            raise ValueError("not a valid contract")

        return symbol

    def get_hist(
        self,
        symbol: str,
        exchange: str = "NSE",
        interval: Interval = Interval.in_daily,
        n_bars: int = 10,
        fut_contract: int = None,
        extended_session: bool = False,
    ) -> pd.DataFrame:
        """get historical data

        Args:
            symbol (str): symbol name
            exchange (str, optional): exchange, not required if symbol is in format EXCHANGE:SYMBOL. Defaults to None.
            interval (str, optional): chart interval. Defaults to 'D'.
            n_bars (int, optional): no of bars to download, max 5000. Defaults to 10.
            fut_contract (int, optional): None for cash, 1 for continuous current contract in front, 2 for continuous next contract in front . Defaults to None.
            extended_session (bool, optional): regular session if False, extended session if True, Defaults to False.

        Returns:
            pd.Dataframe: dataframe with sohlcv as columns
        """
        symbol = self.__format_symbol(
            symbol=symbol, exchange=exchange, contract=fut_contract
        )

        interval = interval.value

        self.__create_connection()

        self.__send_message("set_auth_token", [self.token])
        self.__send_message("chart_create_session", [self.chart_session, ""])
        self.__send_message("quote_create_session", [self.session])
        self.__send_message(
            "quote_set_fields",
            [
                self.session,
                "ch",
                "chp",
                "current_session",
                "description",
                "local_description",
                "language",
                "exchange",
                "fractional",
                "is_tradable",
                "lp",
                "lp_time",
                "minmov",
                "minmove2",
                "original_name",
                "pricescale",
                "pro_name",
                "short_name",
                "type",
                "update_mode",
                "volume",
                "currency_code",
                "rchp",
                "rtc",
            ],
        )

        self.__send_message(
            "quote_add_symbols", [self.session, symbol,
                                  {"flags": ["force_permission"]}]
        )
        self.__send_message("quote_fast_symbols", [self.session, symbol])

        self.__send_message(
            "resolve_symbol",
            [
                self.chart_session,
                "symbol_1",
                '={"symbol":"'
                + symbol
                + '","adjustment":"splits","session":'
                + ('"regular"' if not extended_session else '"extended"')
                + "}",
            ],
        )
        self.__send_message(
            "create_series",
            [self.chart_session, "s1", "s1", "symbol_1", interval, n_bars],
        )
        self.__send_message("switch_timezone", [
                            self.chart_session, "exchange"])

        raw_data = ""

        logger.debug(f"getting data for {symbol}...")
        while True:
            try:
                result = self.ws.recv()
                raw_data = raw_data + result + "\n"
            except Exception as e:
                logger.error(e)
                break

            if "series_completed" in result:
                break

        return self.__create_df(raw_data, symbol)

    def search_symbol(self, text: str, exchange: str = ''):
        url = self.__search_url.format(text, exchange)

        symbols_list = []
        try:
            resp = requests.get(url)

            symbols_list = json.loads(resp.text.replace(
                '</em>', '').replace('<em>', ''))
        except Exception as e:
            logger.error(e)

        return symbols_list


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    tv = TvDatafeed()
    #print(tv.get_hist("CRUDEOIL", "MCX", fut_contract=1))
    #print(tv.get_hist("NIFTY", "NSE", fut_contract=1))
    print(
        tv.get_hist(
            "EICHERMOT",
            "NSE",
            interval=Interval.in_1_hour,
            n_bars=5,
            extended_session=False,
        )
    )
