import mysql.connector
import pandas as pd
import logging
import pytz
import json
import time
from collections import OrderedDict
import math
from datetime import timedelta
import time
from datetime import datetime, timedelta
from tvDatafeed import TvDatafeed, Interval


from datetime import datetime, timedelta # Configure logging
logger = logging.getLogger()

# Set up logging
logging.basicConfig(level=logging.INFO)

def create_db_config(suffix):
    try:
        return {
            'user': f'u417995338_stocksZoneData',
            'password': 'Host@0904',
            'host': 'srv1668.hstgr.io',
            'database': f'u417995338_stocksZoneData',
            'connection_timeout': 100000  # Set connection timeout
        }
    except Exception as e:
        logger.error(f"Error creating DB config: {e}")
        return None

def fetch_stock_data_and_resample(symbol, exchange, n_bars, htf_interval, interval, key):
    try:
        # Fetch historical data using tvDatafeed
        stock_data = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars)  # Use parameters correctly

        # Check if stock_data is None
        if stock_data is not None and not stock_data.empty:  # Added check for empty DataFrame
            stock_data.index = stock_data.index.tz_localize('UTC').tz_convert('Asia/Kolkata')

            df = stock_data.round(2)
            resample_rules = {
                '10 Minutes': '10T',
                '75 Minutes': '75T',
                '125 Minutes': '125T'
            }

            rule = resample_rules.get(key)

            df = df.resample(rule=rule, closed='left', label='left', origin=df.index.min()).agg(
                OrderedDict([
                    ('open', 'first'),
                    ('high', 'max'),
                    ('low', 'min'),
                    ('close', 'last'),
                    ('volume', 'sum')
                ])
            ).dropna()

            stock_data = df.round(2)

        # Fetch historical data using tvDatafeed
        stock_data_htf = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars)  # Use parameters correctly

        # Check if stock_data_htf is None
        if stock_data_htf is not None and not stock_data_htf.empty:  # Added check for empty DataFrame
            stock_data_htf.index = stock_data_htf.index.tz_localize('UTC').tz_convert('Asia/Kolkata')

            stock_data_htf = stock_data_htf.round(2)

            return stock_data, stock_data_htf
        else:
            print(f"No data found for {symbol} on {exchange}.")
            return None  # Return None if no data is found
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None  # Return None in case of an error


def fetch_stock_data(symbol, exchange, n_bars, htf_interval, interval):
    try:
        # print(f"Fetching data for {symbol} on {exchange} with n_bars={n_bars}, htf_interval={htf_interval}, interval={interval}")

        # Fetch historical data using tvDatafeed
        stock_data = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval , n_bars=n_bars)  # Use parameters correctly

        # Check if stock_data is None
        if stock_data is not None and not stock_data.empty:  # Added check for empty DataFrame
             stock_data.index = stock_data.index.tz_localize('UTC').tz_convert('Asia/Kolkata')

            stock_data = stock_data.round(2)
        # Fetch historical data using tvDatafeed
        stock_data_htf = tv.get_hist(symbol=symbol, exchange=exchange, interval=htf_interval, n_bars=n_bars)  # Use parameters correctly

        # Check if stock_data is None
        if stock_data_htf is not None and not stock_data_htf.empty:  # Added check for empty DataFrame
             stock_data_htf.index = stock_data_htf.index.tz_localize('UTC').tz_convert('Asia/Kolkata')

            stock_data_htf = stock_data_htf.round(2)


            return stock_data,stock_data_htf
        else:
            print(f"No data found for {symbol} on {exchange}.")
            return None  # Return None if no data is found
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None  # Return None in case of an error


def calculate_atr(stock_data, length=14):
    stock_data['previous_close'] = stock_data['close'].shift(1)
    stock_data['tr1'] = abs(stock_data['high'] - stock_data['low'])
    stock_data['tr2'] = abs(stock_data['high'] - stock_data['previous_close'])
    stock_data['tr3'] = abs(stock_data['low'] - stock_data['previous_close'])
    stock_data['TR'] = stock_data[['tr1', 'tr2', 'tr3']].max(axis=1)

    def rma(series, length):
        alpha = 1 / length
        return series.ewm(alpha=alpha, adjust=False).mean()

    stock_data['ATR'] = rma(stock_data['TR'], length)
    stock_data['Candle_Range'] = stock_data['high'] - stock_data['low']
    stock_data['Candle_Body'] = abs(stock_data['close'] - stock_data['open'])
    return stock_data.round(2)

def check_golden_crossover(stock_data_htf, pulse_check_start_date):
    is_pulse_positive = ""  # Initialize an empty string to store the is_pulse_positive
    is_candle_green = ""
    is_trend_up = ""  # Initialize is_trend_up
    try:
        # Calculate EMA20 and EMA50
        stock_data_htf['EMA20'] = stock_data_htf['close'].ewm(span=20, adjust=False).mean().round(2)
        stock_data_htf['EMA50'] = stock_data_htf['close'].ewm(span=50, adjust=False).mean().round(2)

        # Drop rows with NaN values in EMA columns
        stock_data_htf.dropna(subset=['EMA20', 'EMA50'], inplace=True)

        # Identify crossover points
        crossover_up = stock_data_htf['EMA20'] > stock_data_htf['EMA50']
        crossover_down = stock_data_htf['EMA20'] < stock_data_htf['EMA50']
        # Find the last index before the target date
        last_index_before_staring_check = stock_data_htf.index[stock_data_htf.index < pulse_check_start_date]

        if not last_index_before_staring_check.empty:
            last_index_before_staring_check = last_index_before_staring_check[-1]

            # Check crossover conditions just before the target date
            if crossover_up.loc[last_index_before_staring_check]:
                # Check if the crossover candle is bullish or bearish
                if stock_data_htf['close'].loc[last_index_before_staring_check] > stock_data_htf['open'].loc[last_index_before_staring_check]:
                    is_pulse_positive = "True"
                    is_candle_green = "True"

                else:
                    is_pulse_positive = "True"
                    is_candle_green = "False"
            elif crossover_down.loc[last_index_before_staring_check]:
                # Check if the crossover candle is bullish or bearish
                if stock_data_htf['close'].loc[last_index_before_staring_check] > stock_data_htf['open'].loc[last_index_before_staring_check]:
                    is_pulse_positive = "False"
                    is_candle_green = "True"

                else:
                    is_pulse_positive = "False "
                    is_candle_green = "False"

            else:
                is_pulse_positive = "invalid pulse"
                is_candle_green = "invalid closing"

            # New logic for trend label
            latest_candle_close = stock_data_htf['close'].iloc[-1]
            latest_candle_low = stock_data_htf['low'].iloc[-1]
            latest_candle_high = stock_data_htf['high'].iloc[-1]
            latest_closing_price = round(stock_data_htf['close'].iloc[-1], 2)
            ema20 = stock_data_htf['EMA20']

            if (latest_candle_close == ema20.iloc[-1] or
                (latest_candle_low <= ema20.iloc[-1] and latest_candle_high >= ema20.iloc[-1])):
                is_trend_up = "None"
            elif (latest_candle_close > ema20.iloc[-8] and latest_candle_close > ema20.iloc[-1]):
                is_trend_up = "True"
            elif (latest_candle_close < ema20.iloc[-8] and latest_candle_close < ema20.iloc[-1]):
                is_trend_up = "False"

        else:
            is_pulse_positive = "No data"

    except Exception as e:
        is_pulse_positive = f"({e})"

    return is_pulse_positive, is_candle_green, is_trend_up  # Return the is_pulse_positive string and trend label

def is_overlap_less_than_50(stock_data, legin_candle_index):

    legin_candle_body = stock_data['Candle_Body'].iloc[legin_candle_index]
    previous_candle_body = stock_data['Candle_Body'].iloc[legin_candle_index-1]
    return (previous_candle_body < legin_candle_body * 0.50)

def check_legout_covered(it_is_demand_zone, stock_data, i, entry_index, total_risk, reward_value, first_legout_candle_range, entry_price):
    first_legout_half = first_legout_candle_range * 0.50
    legout_covered_limit = (total_risk * reward_value if reward_value == 3 else 5) + entry_price if it_is_demand_zone else (total_risk * reward_value if reward_value == 3 else 5) - entry_price

    if entry_index is not None:
        if it_is_demand_zone:
            highest_high = stock_data['high'].iloc[i:entry_index + 1].max()
            return highest_high > legout_covered_limit
        else:
            lowest_low = stock_data['low'].iloc[i:entry_index + 1].min()
            return lowest_low < legout_covered_limit
    else:
        crossed = False
        for n in range(i, len(stock_data)):
            if not crossed:
                if (it_is_demand_zone and stock_data['low'].iloc[n] <= first_legout_half) or \
                   (not it_is_demand_zone and stock_data['high'].iloc[n] >= first_legout_half):
                    crossed = True
            else:
                if (it_is_demand_zone and stock_data['high'].iloc[n] > legout_covered_limit) or \
                   (not it_is_demand_zone and stock_data['low'].iloc[n] < legout_covered_limit):
                    return True
        return False


def find_patterns(ticker, stock_data, stock_data_htf, interval_key, max_base_candles,reward_value, scan_demand_zone_allowed, scan_supply_zone_allowed, fresh_zone_allowed, target_zone_allowed, stoploss_zone_allowed, htf_interval):
    try:
        patterns = []
        last_legout_high = []  # Initialize here to avoid error
        last_legout_low = [ ] # Intiialize here to avoid error

        if len(stock_data) < 3:
            print(f"Not enough stock_data for {ticker}")
            return []
        for i in range(len(stock_data) - 1, 2, -1):

            if scan_demand_zone_allowed and (stock_data['close'].iloc[i] > stock_data['open'].iloc[i] and
                stock_data['TR'].iloc[i] > stock_data['ATR'].iloc[i] and
                stock_data['open'].iloc[i] >= stock_data['close'].iloc[i - 1]): # opening of first legout should greater than 0.15% of boring closing

                first_legout_open = stock_data['open'].iloc[i]

                first_legout_candle_body = abs(stock_data['close'].iloc[i] - stock_data['open'].iloc[i])
                first_legout_candle_range = (stock_data['high'].iloc[i] - stock_data['low'].iloc[i])

                if first_legout_candle_body >= 0.5 * first_legout_candle_range:
                    high_prices = []
                    low_prices = []
                    for base_candles_count in range(1, max_base_candles + 1):
                        base_candles_found = 0

                        legin_candle_index = i - (base_candles_count + 1)
                        legin_candle_body = stock_data['Candle_Body'].iloc[legin_candle_index]
                        legin_candle_range = stock_data['Candle_Range'].iloc[legin_candle_index]

                        for k in range(1, base_candles_count + 1):
                            if (stock_data['ATR'].iloc[i - k] > stock_data['TR'].iloc[i - k] and
                               (legin_candle_body >= 0.50 * legin_candle_range) and (stock_data['TR'].iloc[legin_candle_index] > 0.8 * stock_data['ATR'].iloc[legin_candle_index])  ):

                                base_candles_found += 1
                                high_prices.append(stock_data['high'].iloc[i - k])
                                low_prices.append(stock_data['low'].iloc[i - k])

                            max_high_price = max(high_prices) if high_prices else None
                            min_low_price = min(low_prices) if low_prices else None

                            if  max_high_price is not None and min_low_price is not None:
                                actual_base_candle_range = max_high_price - min_low_price
                            actual_legout_candle_range = None
                            first_legout_candle_range_for_one_two_ka_four = (stock_data['high'].iloc[i] - stock_data['close'].iloc[i-1])
                            condition_met = False  # Flag to check if any condition was met


                            if base_candles_found == base_candles_count:
                                if (
                                    legin_candle_range >= 1.5 * actual_base_candle_range and
                                    first_legout_candle_range_for_one_two_ka_four >= 2 * legin_candle_range and

                                    stock_data['low'].iloc[i] >= stock_data['low'].iloc[legin_candle_index]):
                                    legout_count = '1'
                                    condition_met = True  # Set flag if this condition is met
                                    # Add your logic here if needed

                                else:  # This is the else part for the if statement above
                                    last_legout_high = []
                                    j = i + 1
                                    while j in range(i + 1, min(i + 3, len(stock_data))) and stock_data['close'].iloc[j] > stock_data['open'].iloc[j]:
                                        # Check if j == i + 1
                                        if j == i + 1:
                                            if (stock_data['open'].iloc[j] >= 0.10* stock_data['close'].iloc[i] and
                                                stock_data['low'].iloc[j] >= 0.50 * stock_data['Candle_Range'].iloc[i]):
                                                last_legout_high.append(stock_data['high'].iloc[j])

                                        # Check if j == i + 2
                                        elif j == i + 2:
                                            if stock_data['low'].iloc[j] >= stock_data['low'].iloc[i + 1]:
                                                last_legout_high.append(stock_data['high'].iloc[j])

                                        j += 1

                                    last_legout_high_value = max(last_legout_high) if last_legout_high else None

                                    if last_legout_high_value is not None:
                                        actual_legout_candle_range = last_legout_high_value - stock_data['close'].iloc[i - 1]

                                        if (legin_candle_range >= 1.5 * actual_base_candle_range and
                                            actual_legout_candle_range >= 2 * legin_candle_range and
                                            stock_data['low'].iloc[i] >= stock_data['low'].iloc[legin_candle_index]):
                                            legout_count = (j-i)+1
                                            condition_met = True  # Set flag if this condition is met


                            # Code block to execute if any condition was met
                            if condition_met:

                                if interval_key in ('1D','1Wk','1Mo') :
                                    legin_date = stock_data.index[legin_candle_index].strftime('%Y-%m-%d')
                                    legout_date = stock_data.index[i].strftime('%Y-%m-%d')
                                else:
                                    legin_date = stock_data.index[legin_candle_index].strftime('%Y-%m-%d %H:%M:%S')
                                    legout_date = stock_data.index[i].strftime('%Y-%m-%d %H:%M:%S')


                                if actual_legout_candle_range is not None:
                                    legout_candle_range = actual_legout_candle_range
                                else:
                                    legout_candle_range = first_legout_candle_range_for_one_two_ka_four


                                entry_occurred = False
                                target_hit = False
                                stop_loss_hit = False
                                entry_date = None
                                entry_index = None
                                exit_date = None
                                exit_index = None
                                Zone_status = None
                                total_risk = max_high_price - min_low_price
                                minimum_target = (total_risk * reward_value) + max_high_price
                                start_index = j+1 if last_legout_high else i + 1

                                for m in range(start_index, len(stock_data)):
                                    if not entry_occurred:
                                        # Check if the entry condition is met
                                        if stock_data['low'].iloc[m] <= max_high_price:
                                            entry_occurred = True
                                            entry_index = m
                                            entry_date = stock_data.index[m].strftime('%Y-%m-%d %H:%M:%S')

                                            # Check if the low and high of the current candle exceed the limits
                                            if stock_data['low'].iloc[m] < min_low_price:
                                                stop_loss_hit = True
                                                exit_index = m
                                                exit_date = stock_data.index[m].strftime('%Y-%m-%d %H:%M:%S')
                                                Zone_status = 'Stop loss'
                                                break  # Exit the loop after stop-loss is hit
                                            elif stock_data['high'].iloc[m] >= minimum_target:
                                                target_hit = True
                                                exit_index = m
                                                exit_date = stock_data.index[m].strftime('%Y-%m-%d %H:%M:%S')
                                                Zone_status = 'Target'
                                                break  # Exit the loop after target is hit
                                        elif min(stock_data['low'].iloc[start_index:]) > max_high_price:
                                             Zone_status = 'Fresh'
                                    else:
                                        # After entry, check if price hits stop-loss or minimum target
                                        if stock_data['low'].iloc[m] < min_low_price:
                                            stop_loss_hit = True
                                            exit_index = m
                                            exit_date = stock_data.index[m].strftime('%Y-%m-%d %H:%M:%S')
                                            Zone_status = 'Stop loss'
                                            break  # Exit the loop after stop-loss is hit
                                        elif stock_data['high'].iloc[m] >= minimum_target:
                                            target_hit = True
                                            exit_index = m
                                            exit_date = stock_data.index[m].strftime('%Y-%m-%d %H:%M:%S')
                                            Zone_status = 'Target'
                                            break  # Exit the loop after target is hit

                                # time_in_exit = exit_index - entry_index
                                Pattern_name_is = 'DZ(DBR)' if stock_data['open'].iloc[legin_candle_index] > stock_data['close'].iloc[legin_candle_index] else 'DZ(RBR)'
                                legin_base_legout_ranges = f"{round(legin_candle_range)}:{round(actual_base_candle_range)}:{round(legout_candle_range)}"

                                #ohlc_data = capture_ohlc_data(stock_data, exit_index, i)
                                #print(f"entry date is :{stock_data.index[m]}")
                                pulse_check_start_date = pd.to_datetime(entry_date).strftime('%Y-%m-%d %H:%M:%S') if entry_date is not None else pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                                #print(f"Pulse Check Start Date: {(pulse_check_start_date)}")

                                is_pulse_positive,is_candle_green,is_trend_up = check_golden_crossover(stock_data_htf, pulse_check_start_date)

                                if ((fresh_zone_allowed and Zone_status == 'Fresh') or \
                                   (target_zone_allowed and Zone_status == 'Target') or \
                                   (stoploss_zone_allowed and Zone_status == 'Stop loss')):

                                      white_area_condition = (stock_data['open'].iloc[i] >= stock_data['close'].iloc[i - 1]
                                            if stock_data['close'].iloc[i - 1] > stock_data['open'].iloc[i - 1]
                                            else stock_data['open'].iloc[i] >= stock_data['open'].iloc[i - 1])
                                      opposite_color_exist = ((stock_data['close'].iloc[legin_candle_index] > stock_data['open'].iloc[legin_candle_index] and
                                                stock_data['close'].iloc[legin_candle_index - 1] < stock_data['open'].iloc[legin_candle_index - 1]) or
                                                (stock_data['close'].iloc[legin_candle_index] < stock_data['open'].iloc[legin_candle_index] and
                                                stock_data['close'].iloc[legin_candle_index - 1] > stock_data['open'].iloc[legin_candle_index - 1]))


                                      if white_area_condition:
                                          white_area = 'True'
                                      else:
                                          white_area = 'False'

                                      if opposite_color_exist :
                                          if is_overlap_less_than_50(stock_data, legin_candle_index):
                                             legin_not_covered = 'True'
                                          else:
                                              legin_not_covered = 'False'
                                      else:
                                          legin_not_covered = 'True'

                                      if (first_legout_open <= stock_data['close'].iloc[legin_candle_index] + legin_candle_body):
                                          legout_formation = 'True'
                                      else:
                                          legout_formation = 'False'
                                      if ((stock_data['close'].iloc[legin_candle_index] > stock_data['open'].iloc[legin_candle_index]) and (stock_data['high'].iloc[legin_candle_index] > stock_data['close'].iloc[legin_candle_index])) or ((stock_data['open'].iloc[legin_candle_index] > stock_data['close'].iloc[legin_candle_index]) and (stock_data['low'].iloc[legin_candle_index] < stock_data['close'].iloc[legin_candle_index])):
                                          wick_in_legin = 'True'
                                      else:
                                          wick_in_legin = 'False'

                                      if (stock_data['TR'].iloc[legin_candle_index] > stock_data['ATR'].iloc[legin_candle_index]):
                                         legin_tr_check = 'True'
                                      else:
                                          legin_tr_check = 'False'

                                      if check_legout_covered(True, stock_data, i, entry_index, total_risk, reward_value, first_legout_candle_range, max_high_price):
                                         legout_covered = 'True'
                                      else:
                                          legout_covered = 'False'
                                      if (legin_candle_range >= 2 * actual_base_candle_range) :
                                          one_two_ka_four = 'True'
                                      else:
                                          one_two_ka_four = 'False'
                                      patterns.append({
                                            'symbol': ticker,
                                            'timeframe': interval_key,
                                            'zone_status': Zone_status,
                                            'zone_type': Pattern_name_is,  # corrected
                                            'entry_price': max_high_price,  # corrected
                                            'stop_loss': min_low_price,
                                            'target': minimum_target,
                                            'legin_date': legin_date,
                                            'base_count': base_candles_found,
                                            'legout_count': legout_count,
                                            'legout_date': legout_date,
                                            'entry_date': entry_date,
                                            'exit_date': exit_date,

                                            'is_pulse_positive': is_pulse_positive,
                                            'is_candle_green': is_candle_green,
                                            'is_trend_up': is_trend_up,

                                            'is_white_area': white_area,
                                            'legin_not_covered': legin_not_covered,
                                            'is_legout_formation': legout_formation,
                                            'is_wick_in_legin': wick_in_legin,
                                            'is_legin_tr_pass': legin_tr_check,
                                            'is_legout_covered': legout_covered,
                                            'is_one_two_ka_four': one_two_ka_four,

                                            #'ohlc_data': ohlc_data,
                                       })

            if scan_supply_zone_allowed and (stock_data['open'].iloc[i] > stock_data['close'].iloc[i] and
                stock_data['TR'].iloc[i] > stock_data['ATR'].iloc[i] and
                stock_data['open'].iloc[i] <=  stock_data['close'].iloc[i - 1]):

                first_legout_open = stock_data['open'].iloc[i]
                first_legout_candle_body = abs(stock_data['close'].iloc[i] - stock_data['open'].iloc[i])
                first_legout_candle_range = (stock_data['high'].iloc[i] - stock_data['low'].iloc[i])

                if first_legout_candle_body >= 0.5 * first_legout_candle_range:
                    high_prices = []
                    low_prices = []
                    for base_candles_count in range(1, max_base_candles + 1):
                        base_candles_found = 0

                        legin_candle_index = i - (base_candles_count + 1)
                        legin_candle_body = stock_data['Candle_Body'].iloc[legin_candle_index]
                        legin_candle_range = stock_data['Candle_Range'].iloc[legin_candle_index]

                        for k in range(1, base_candles_count + 1):
                            if (stock_data['ATR'].iloc[i - k] > stock_data['TR'].iloc[i - k] and
                                (legin_candle_body >= 0.50 * legin_candle_range)  and (stock_data['TR'].iloc[legin_candle_index] > 0.8 * stock_data['ATR'].iloc[legin_candle_index])):

                                base_candles_found += 1
                                high_prices.append(stock_data['high'].iloc[i - k])
                                low_prices.append(stock_data['low'].iloc[i - k])

                            max_high_price = max(high_prices) if high_prices else None
                            min_low_price = min(low_prices) if low_prices else None

                            if max_high_price is not None and min_low_price is not None:
                                actual_base_candle_range = max_high_price - min_low_price
                            actual_legout_candle_range = None
                            first_legout_candle_range_for_one_two_ka_four = (stock_data['close'].iloc[i-1] - stock_data['low'].iloc[i])
                            condition_met = False  # Flag to check if any condition was met



                            if base_candles_found == base_candles_count:
                                if (legin_candle_range >= 1.5 * actual_base_candle_range and
                                    first_legout_candle_range_for_one_two_ka_four >= 2 * legin_candle_range and

                                    stock_data['high'].iloc[i] <= stock_data['high'].iloc[legin_candle_index]):
                                    condition_met = True  # Set flag if this condition is met
                                    legout_count = 1
                                    # Add your logic here if needed

                                else:  # This is the else part for the if statement above
                                    last_legout_low = []
                                    j = i + 1
                                    while j in range(i + 1, min(i + 3, len(stock_data))) and stock_data['open'].iloc[j] > stock_data['close'].iloc[j]:
                                        # Check if j == i + 1
                                        if j == i + 1:
                                            if (stock_data['open'].iloc[j] <= 0.10* stock_data['close'].iloc[i] and
                                                stock_data['high'].iloc[j] <= 0.50 * stock_data['Candle_Range'].iloc[i]):
                                                last_legout_low.append(stock_data['low'].iloc[j])

                                        # Check if j == i + 2
                                        elif j == i + 2:
                                            if stock_data['high'].iloc[j] <= stock_data['high'].iloc[i + 1]:
                                                last_legout_low.append(stock_data['low'].iloc[j])

                                        j += 1

                                    last_legout_low_value = min(last_legout_low) if last_legout_low else None

                                    if last_legout_low_value is not None:
                                        actual_legout_candle_range = abs(last_legout_low_value - stock_data['close'].iloc[i - 1])

                                        if (legin_candle_range >= 1.5 * actual_base_candle_range and
                                            actual_legout_candle_range >= 2 * legin_candle_range and
                                            stock_data['high'].iloc[i] <= stock_data['high'].iloc[legin_candle_index]):

                                            condition_met = True  # Set flag if this condition is met
                                            legout_count = (j-i)+1

                            # Code block to execute if any condition was met
                            if condition_met:
                                if interval_key in ('1D','1Wk','1Mo') :
                                    legin_date = stock_data.index[legin_candle_index].strftime('%Y-%m-%d')
                                    legout_date = stock_data.index[i].strftime('%Y-%m-%d')
                                else:
                                    legin_date = stock_data.index[legin_candle_index].strftime('%Y-%m-%d %H:%M:%S')
                                    legout_date = stock_data.index[i].strftime('%Y-%m-%d %H:%M:%S')

                                if actual_legout_candle_range is not None:
                                    legout_candle_range = actual_legout_candle_range
                                else:
                                    legout_candle_range = first_legout_candle_range_for_one_two_ka_four


                                entry_occurred = False
                                target_hit = False
                                stop_loss_hit = False
                                entry_date = None
                                entry_index = None
                                exit_date = None
                                exit_index = None
                                Zone_status = None
                                total_risk = max_high_price - min_low_price
                                minimum_target = min_low_price - (total_risk * reward_value)
                                start_index = j+1 if last_legout_low else i + 1

                                for m in range(start_index, len(stock_data)):
                                    if not entry_occurred:
                                        # Check if the entry condition is met
                                        if stock_data['high'].iloc[m] >= min_low_price:
                                            entry_occurred = True
                                            entry_index = m
                                            entry_date = stock_data.index[m].strftime('%Y-%m-%d %H:%M:%S')

                                            # Check if the low and high of the current candle exceed the limits
                                            if stock_data['high'].iloc[m] > max_high_price:
                                                stop_loss_hit = True
                                                exit_index = m
                                                exit_date = stock_data.index[m].strftime('%Y-%m-%d %H:%M:%S')
                                                Zone_status = 'Stop loss'
                                                break  # Exit the loop after stop-loss is hit
                                            elif stock_data['low'].iloc[m] <= minimum_target:
                                                target_hit = True
                                                exit_index = m
                                                exit_date = stock_data.index[m].strftime('%Y-%m-%d %H:%M:%S')
                                                Zone_status = 'Target'
                                                break  # Exit the loop after target is hit
                                        elif max(stock_data['high'].iloc[start_index:]) < min_low_price:
                                             Zone_status = 'Fresh'
                                    else:
                                        # After entry, check if price hits stop-loss or minimum target
                                        if stock_data['high'].iloc[m] > max_high_price:
                                            stop_loss_hit = True
                                            exit_index = m
                                            exit_date = stock_data.index[m].strftime('%Y-%m-%d %H:%M:%S')
                                            Zone_status = 'Stop loss'
                                            break  # Exit the loop after stop-loss is hit
                                        elif stock_data['low'].iloc[m] <= minimum_target:
                                            target_hit = True
                                            exit_index = m
                                            exit_date = stock_data.index[m].strftime('%Y-%m-%d %H:%M:%S')
                                            Zone_status = 'Target'
                                            break  # Exit the loop after target is hit


                                Pattern_name_is = 'SZ(RBD)' if stock_data['close'].iloc[legin_candle_index] > stock_data['open'].iloc[legin_candle_index] else 'SZ(DBD)'

                                #ohlc_data = capture_ohlc_data(stock_data, exit_index, i)

                                pulse_check_start_date = pd.to_datetime(entry_date).strftime('%Y-%m-%d %H:%M:%S') if entry_date is not None else pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                                #print(f"Pulse Check Start Date: {(pulse_check_start_date)}")

                                is_pulse_positive,is_candle_green,is_trend_up = check_golden_crossover(stock_data_htf, pulse_check_start_date)

                                if ((fresh_zone_allowed and Zone_status == 'Fresh') or \
                                   (target_zone_allowed and Zone_status == 'Target') or \
                                   (stoploss_zone_allowed and Zone_status == 'Stop loss')):


                                       white_area_condition = (stock_data['open'].iloc[i] <= stock_data['close'].iloc[i - 1]
                                                    if stock_data['close'].iloc[i - 1] < stock_data['open'].iloc[i - 1]
                                                    else stock_data['open'].iloc[i] <= stock_data['open'].iloc[i - 1])
                                       opposite_color_exist = ((stock_data['close'].iloc[legin_candle_index] > stock_data['open'].iloc[legin_candle_index] and
                                                        stock_data['close'].iloc[legin_candle_index - 1] < stock_data['open'].iloc[legin_candle_index - 1]) or
                                                        (stock_data['close'].iloc[legin_candle_index] < stock_data['open'].iloc[legin_candle_index] and
                                                        stock_data['close'].iloc[legin_candle_index - 1] > stock_data['open'].iloc[legin_candle_index - 1]))



                                       if white_area_condition:
                                           white_area = 'True'
                                       else:
                                           white_area = 'False'
                                       if opposite_color_exist :
                                           if is_overlap_less_than_50(stock_data, legin_candle_index):
                                              legin_not_covered = 'True'
                                           else:
                                               legin_not_covered = 'False'
                                       else:
                                          legin_not_covered = 'True'


                                       if (first_legout_open >= stock_data['close'].iloc[legin_candle_index] - legin_candle_body):
                                           legout_formation = 'True'
                                       else:
                                           legout_formation = 'False'
                                       if ((stock_data['close'].iloc[legin_candle_index] > stock_data['open'].iloc[legin_candle_index]) and (stock_data['high'].iloc[legin_candle_index] > stock_data['close'].iloc[legin_candle_index])) or ((stock_data['open'].iloc[legin_candle_index] > stock_data['close'].iloc[legin_candle_index]) and (stock_data['low'].iloc[legin_candle_index] < stock_data['close'].iloc[legin_candle_index])):
                                           wick_in_legin = 'True'
                                       else:
                                           wick_in_legin = 'False'

                                       if (stock_data['TR'].iloc[legin_candle_index] > stock_data['ATR'].iloc[legin_candle_index]):
                                          legin_tr_check = 'True'
                                       else:
                                           legin_tr_check = 'False'

                                       if check_legout_covered(False, stock_data, i, entry_index, total_risk, reward_value, first_legout_candle_range, min_low_price):
                                           legout_covered = 'True'
                                       else:
                                           legout_covered = 'False'
                                       if (legin_candle_range >= 2 * actual_base_candle_range) :
                                          one_two_ka_four = 'True'
                                       else:
                                           one_two_ka_four = 'False'

                                       patterns.append({
                                            'symbol': ticker,
                                            'timeframe': interval_key,
                                            'zone_status': Zone_status,
                                            'zone_type': Pattern_name_is,  # corrected
                                            'entry_price': min_low_price,  # corrected
                                            'stop_loss': max_high_price,
                                            'target': minimum_target,
                                            'legin_date': legin_date,
                                            'base_count': base_candles_found,
                                            'legout_count': legout_count,
                                            'legout_date': legout_date,
                                            'entry_date': entry_date,
                                            'exit_date': exit_date,

                                            'is_pulse_positive': is_pulse_positive,
                                            'is_candle_green': is_candle_green,
                                            'is_trend_up': is_trend_up,

                                            'is_white_area': white_area,
                                            'legin_not_covered': legin_not_covered,
                                            'is_legout_formation': legout_formation,
                                            'is_wick_in_legin': wick_in_legin,
                                            'is_legin_tr_pass': legin_tr_check,
                                            'is_legout_covered': legout_covered,
                                            'is_one_two_ka_four': one_two_ka_four,

                                            #'ohlc_data': ohlc_data,
                                        })

        return patterns
    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        return []
def get_unique_symbols(cursor):
    try:
        query = "SELECT DISTINCT symbol FROM ohlc_data ORDER BY symbol ASC"
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error retrieving unique symbols: {e}")
        return []


def batch_insert_candles(cursor, data_to_insert):
    batch_size = 3000  # Adjust this size as necessary
    for i in range(0, len(data_to_insert), batch_size):
        batch = data_to_insert[i:i + batch_size]
        insert_query = """
            INSERT INTO zone_data (
                symbol, timeframe,
                zone_status, zone_type, entry_price, stop_loss, target,
                legin_date, base_count, legout_count, legout_date,
                entry_date, exit_date,
                is_pulse_positive, is_candle_green, is_trend_up,
                is_white_area, legin_not_covered, is_legout_formation,
                is_wick_in_legin, is_legin_tr_pass, is_legout_covered,
                is_one_two_ka_four
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                entry_price = VALUES(entry_price),
                stop_loss = VALUES(stop_loss),
                target = VALUES(target),
                legin_date = VALUES(legin_date),
                base_count = VALUES(base_count),
                legout_date = VALUES(legout_date),
                exit_date = VALUES(exit_date),
                zone_status = VALUES(zone_status),
                zone_type = VALUES(zone_type),
                is_pulse_positive = VALUES(is_pulse_positive),
                is_candle_green = VALUES(is_candle_green),
                is_trend_up = VALUES(is_trend_up),
                is_white_area = VALUES(is_white_area),
                legin_not_covered = VALUES(legin_not_covered),
                is_legout_formation = VALUES(is_legout_formation),
                is_wick_in_legin = VALUES(is_wick_in_legin),
                is_legin_tr_pass = VALUES(is_legin_tr_pass),
                is_legout_covered = VALUES(is_legout_covered),
                legout_count = VALUES(legout_count),
                is_one_two_ka_four = VALUES(is_one_two_ka_four)
        """
        try:
            cursor.executemany(insert_query, batch)
            logger.info(f"Inserted {len(batch)} rows of pattern data.")
        except mysql.connector.Error as err:
            logger.error(f"Database error during batch insert: {err}")
            raise
tickers = ['BORORENEW', 'RAJESHEXPO', 'SUZLON', 'ASAHIINDIA']

interval_options = {
    '1 Minutes': Interval.in_1_minute,
    #'3 Minutes': Interval.in_3_minute,
    #'5 Minutes': Interval.in_5_minute,
    #'10 Minutes': Interval.in_5_minute,  # Corrected
    #'15 Minutes': Interval.in_15_minute,
    #'30 Minutes': Interval.in_30_minute,
    #'1 Hour': Interval.in_1_hour,
    #'75 Minutes': Interval.in_15_minute,  # Consider adjusting this
    #'2 Hours': Interval.in_2_hour,
    #'125 Minutes': Interval.in_5_minute,  # Consider adjusting this
    #'1 Day': Interval.in_daily,
    #'1 Week': Interval.in_weekly,
    #'1 Month': Interval.in_monthly
}

htf_interval_option = {
    '1 Minutes': Interval.in_15_minute,
    '3 Minutes': Interval.in_1_hour,
    '5 Minutes': Interval.in_1_hour,
    '10 Minutes': Interval.in_daily,
    '15 Minutes': Interval.in_daily,
    '30 Minutes': Interval.in_daily,
    '1 Hour': Interval.in_weekly,
    '75 Minutes': Interval.in_weekly,
    '2 Hours': Interval.in_weekly,
    '125 Minutes': Interval.in_weekly,
    '1 Day': Interval.in_monthly,
    '1 Week': Interval.in_monthly,
    '1 Month': Interval.in_monthly
}

reward_mapping = {
    '1 Minutes': 3,
    '3 Minutes': 3,
    '5 Minutes': 3,
    '1 Day': 10,
    '1 Week': 10,
    '1 Month': 10
}

max_base_candles = 3
time_frame_patterns = {interval: [] for interval in interval_options}

# Set fixed values for zone status and type
fresh_zone_allowed = True
target_zone_allowed = True
stoploss_zone_allowed = True
scan_demand_zone_allowed = True
scan_supply_zone_allowed = True

tv = TvDatafeed()
exchange = 'NSE'
n_bars = 50000

# Iterate over each interval
for key, interval in interval_options.items():
    htf_interval = htf_interval_option.get(key, None)
    reward_value = reward_mapping.get(key, 5)

    # Iterate over each ticker
    for i, ticker in enumerate(tickers):
        try:
            if key in ['10 Minutes', '75 Minutes', '125 Minutes']:
                stock_data, stock_data_htf = fetch_stock_data_and_resample(ticker, exchange, n_bars, htf_interval, interval, key)
            else:
                stock_data, stock_data_htf = fetch_stock_data(ticker, exchange, n_bars, htf_interval, interval)


            stock_data = calculate_atr(stock_data)
            columns_to_remove = ['symbol', 'tr1', 'tr2', 'tr3', 'previous_close']
            stock_data = stock_data.drop(columns=columns_to_remove, errors='ignore')

            print(f" for {ticker} , {len(stock_data)} row(s) downloaded in keytimeframe: {key} =====>>> which is {interval} >>>>>>> and {len(stock_data_htf)} row(s) downloaded in HTF: {htf_interval}, used reward_value: {reward_value} ")
            patterns = find_patterns(ticker, stock_data, stock_data_htf, interval, max_base_candles, scan_demand_zone_allowed, scan_supply_zone_allowed, reward_value, fresh_zone_allowed, target_zone_allowed, stoploss_zone_allowed, htf_interval)

            if patterns:
                 time_frame_patterns[key].extend(patterns)
                 print(f"Processed {i + 1} out of {len(tickers)}: {ticker}")
                 print("\n")

        except Exception as e:
            print(f"Error processing ticker {ticker} with interval {key}: {e}")

                # Process collected patterns
                if patterns:
                    try:
                        df = pd.DataFrame(patterns)
                        df.fillna(0, inplace=True)
                        data_to_insert = [tuple(row) for row in df.values]
                        # Create zonedata connection
                        config3 = create_db_config(timeframe)
                        connections['zone'] = mysql.connector.connect(**config3)
                        cursors['zone'] = connections['zone'].cursor()
                        # Create table if not exists
                        create_table_query = """
                        CREATE TABLE IF NOT EXISTS zone_data (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            symbol VARCHAR(20),
                            timeframe VARCHAR(5),
                            zone_status VARCHAR(10),
                            zone_type VARCHAR(8),
                            entry_price DECIMAL(10, 2),
                            stop_loss DECIMAL(10, 2),
                            target DECIMAL(10, 2),
                            legin_date DATETIME,
                            base_count INT,
                            legout_count INT,
                            legout_date DATETIME,
                            entry_date DATETIME,
                            exit_date DATETIME,
                            is_pulse_positive VARCHAR(6),
                            is_candle_green VARCHAR(6),
                            is_trend_up VARCHAR(6),
                            is_white_area VARCHAR(6),
                            legin_not_covered VARCHAR(6),
                            is_legout_formation VARCHAR(6),
                            is_wick_in_legin VARCHAR(6),
                            is_legin_tr_pass VARCHAR(6),
                            is_legout_covered VARCHAR(6),
                            is_one_two_ka_four VARCHAR(6),
                            UNIQUE KEY unique_pattern (symbol, timeframe, entry_date, zone_type)
                        );
                        """
                        cursors['zone'].execute(create_table_query)

                        # Insert data
                        batch_insert_candles(cursors['zone'], data_to_insert)
                        connections['zone'].commit()
                        response_body["success"] = True
                        print(f" for {ticker} total {len(df)} zone data sucessfully uploaded to database")
                        #time.sleep(2)
                        #clear_output(wait=True)  # Clear previous output

                    except Exception as db_error:
                        logger.error(f"Database operation error: {db_error}")
                        response_body["errors"].append(f"Database operation error: {str(db_error)}")
                        if 'zone' in connections:
                            connections['zone'].rollback()

                    finally:
                        if 'zone' in cursors and cursors['zone']:
                            cursors['zone'].close()
                        if 'zone' in connections and connections['zone']:
                            connections['zone'].close()

    except Exception as e:
        logger.error(f"Main process error: {e}")
        response_body["errors"].append(str(e))

    return response_body


result = main()
if result["errors"]:
    logger.error(f"Process completed with errors: {result['errors']}")
if result["success"]:
    print("Process completed successfully.")
