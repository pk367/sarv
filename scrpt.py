from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from tvDatafeed import TvDatafeed, Interval
from collections import OrderedDict
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
    "in_10_minute": Interval.in_10_minute,
    "in_15_minute": Interval.in_15_minute,
    "in_30_minute": Interval.in_30_minute,
    "in_45_minute": Interval.in_45_minute,
    "in_75_minute": Interval.in_75_minute,
    "in_125_minute": Interval.in_125_minute,
    "in_1_hour": Interval.in_1_hour,
    "in_2_hour": Interval.in_2_hour,
    "in_3_hour": Interval.in_3_hour,
    "in_4_hour": Interval.in_4_hour,
    "in_5_hour": Interval.in_5_hour,
    "in_6_hour": Interval.in_6_hour,
    "in_8_hour": Interval.in_8_hour,
    "in_10_hour": Interval.in_10_hour,
    "in_12_hour": Interval.in_12_hour,
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

    Example: /fetch_data?symbol=gold,silver&exchange=mcx&interval=in_5_minute&n_bars=100&fut_contract=1
    """
    symbols = [s.strip() for s in symbol.split(",")]
    interval_enum = INTERVAL_MAP.get(interval)
    htf_interval_enum = HTF_INTERVAL_MAP.get(interval)

    if not interval_enum:
        raise HTTPException(status_code=400, detail=f"Invalid 'interval' value: {interval}")

    result = {}
    tv_datafeed = TvDatafeed()

    for sym in symbols:
        try:
            if interval in ['in_10_minute', 'in_75_minute', 'in_125_minute']:
                stock_data, stock_data_htf = fetch_stock_data_and_resample(sym, exchange, interval, interval_enum, htf_interval_enum, n_bars, fut_contract)
            else:
                stock_data, stock_data_htf = fetch_stock_data(sym, exchange, interval, interval_enum, htf_interval_enum, n_bars, fut_contract)

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
