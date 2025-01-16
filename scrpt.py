from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from tvDatafeed import TvDatafeed, Interval
import pandas as pd

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for testing; restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
INTERVAL_MAP = {
    "in_1_minute": Interval.in_1_minute,
    "in_3_minute": Interval.in_3_minute,
    "in_5_minute": Interval.in_5_minute,
    "in_10_minute": Interval.in_5_minute,
    "in_15_minute": Interval.in_15_minute,
    "in_30_minute": Interval.in_30_minute,
    "in_45_minute": Interval.in_45_minute,
    "in_75_minute": Interval.in_15_minute,
    "in_125_minute": Interval.in_5_minute,
    "in_1_hour": Interval.in_1_hour,
    "in_2_hour": Interval.in_2_hour,
    "in_3_hour": Interval.in_3_hour,
    "in_4_hour": Interval.in_4_hour,
    "in_5_hour": Interval.in_1_hour,
    "in_6_hour": Interval.in_3_hour,
    "in_8_hour": Interval.in_4_hour,
    "in_10_hour": Interval.in_1_hour,
    "in_12_hour": Interval.in_1_hour,
    "in_daily": Interval.in_daily,
    "in_weekly": Interval.in_weekly,
    "in_monthly": Interval.in_monthly,
}
HTF_INTERVAL_MAP = {
    "in_1_minute": Interval.in_15_minute,
    "in_3_minute": Interval.in_1_hour,
    "in_5_minute": Interval.in_1_hour,
    "in_10_minute": Interval.in_daily,
    "in_15_minute": Interval.in_daily,
    "in_30_minute": Interval.in_daily,
    "in_1_hour": Interval.in_weekly,
    "in_75_minute": Interval.in_weekly,
    "in_2_hour": Interval.in_weekly,
    "in_125_minute": Interval.in_weekly,
    "in_daily": Interval.in_monthly,
    "in_weekly": Interval.in_monthly,
    "in_monthly": Interval.in_monthly,
}

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

def fetch_data(tv_datafeed, symbol, exchange, interval, n_bars, fut_contract=None):
    """Fetches historical data for the given symbol and interval."""
    try:
        if fut_contract:
            data = tv_datafeed.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars, fut_contract=fut_contract)
        else:
            data = tv_datafeed.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars)
        
        if data is not None and not data.empty:
            data.index = data.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
            data = data.round(2)
        return data
    except Exception as e:
        print(f"Error fetching data for symbol {symbol}: {e}")
        return None

def fetch_stock_data_and_resample(symbol, exchange, interval_str, interval, htf_interval, n_bars, fut_contract):
    """
    Fetches and resamples stock data for a given symbol and interval.
    """
    tv_datafeed = TvDatafeed()

    # Mapping resampling rules
    RULE_MAP = {
            'in_10_minute': '10min',
            'in_75_minute': '75min',
            'in_125_minute': '125min',
            'in_5_hour': '5h',
            'in_6_hour': '6h',
            'in_8_hour': '8h',
            'in_10_hour': '10h',
            'in_12_hour': '12h',
    }

    rule = RULE_MAP.get(interval_str)
    if not rule:
        print(f"Invalid interval_str: {interval_str}. No resampling rule found.")
        return None, None

    # Fetch initial data
    symbol_data = fetch_data(tv_datafeed, symbol, exchange, interval, n_bars, fut_contract)
    if symbol_data is None or symbol_data.empty:
        print(f"No data found for symbol {symbol} on exchange {exchange} with interval {interval}")
        return None, None

    # Resample the data
    symbol_data_resampled = symbol_data.resample(rule=rule, closed='left', label='left', origin=symbol_data.index.min()).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()

    # Fetch higher time frame (HTF) data
    symbol_data_htf = fetch_data(tv_datafeed, symbol, exchange, htf_interval, n_bars, fut_contract)
    if symbol_data_htf is None or symbol_data_htf.empty:
        print(f"No HTF data found for symbol {symbol} on exchange {exchange} with interval {htf_interval}")
        return None, None

    return symbol_data_resampled, symbol_data_htf

@app.get("/")
def home():
    return {"message": "Welcome to the Stock Data API"}

@app.get("/fetch_data")
def fetch_data_endpoint(
    symbol: str = Query(..., description="Comma-separated stock symbols"),
    exchange: str = Query(..., description="Exchange name"),
    interval: str = Query("in_daily", description="Interval string"),
    n_bars: int = Query(5000, description="Number of bars to fetch"),
    fut_contract: int = Query(None, description="Futures contract ID, if applicable")
):
    """
    Endpoint to fetch stock data using tvDatafeed.
    """
    symbols = [s.strip() for s in symbol.split(",")]
    interval_enum = INTERVAL_MAP.get(interval)
    htf_interval_enum = HTF_INTERVAL_MAP.get(interval)

    if not interval_enum:
        raise HTTPException(status_code=400, detail=f"Invalid 'interval' value: {interval}")

    result = {}
    tv_datafeed = TvDatafeed()  # Initialize TvDatafeed once

    for sym in symbols:
        try:
            if interval in ['in_10_minute', 'in_75_minute', 'in_125_minute']:
                stock_data, stock_data_htf = fetch_stock_data_and_resample(sym, exchange, interval, interval_enum, htf_interval_enum, n_bars, fut_contract)
            else:
                stock_data = fetch_data(tv_datafeed, sym, exchange, interval_enum, n_bars, fut_contract)
                stock_data_htf = fetch_data(tv_datafeed, sym, exchange, htf_interval_enum, n_bars, fut_contract)

            if stock_data is not None and not stock_data.empty:
                stock_data = calculate_atr(stock_data)
                stock_data = stock_data.drop(columns=['tr1', 'tr2', 'tr3', 'previous_close'], errors='ignore')
                result[sym] = stock_data.to_dict(orient="records")
            else:
                result[sym] = {"error": f"No data found for symbol {sym}"}
        except Exception as e:
            result[sym] = {"error": f"Error fetching data for {sym}: {str(e)}"}

    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
