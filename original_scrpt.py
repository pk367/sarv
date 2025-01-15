import json
from tvDatafeed import TvDatafeed, Interval
from collections import OrderedDict
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

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

def fetch_stock_data_and_resample(symbol, exchange, interval_str, interval, n_bars, fut_contract):
    try:
        tv_datafeed = TvDatafeed()
        if fut_contract:
            data = tv_datafeed.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars, fut_contract=fut_contract)
        else:
            data = tv_datafeed.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars)

        if data is None or data.empty:
            return None

        data = data.round(2)

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

        df = data.resample(rule=rule, closed='left', label='left', origin=data.index.min()).agg(
            OrderedDict([
                ('Open', 'first'),
                ('High', 'max'),
                ('Low', 'min'),
                ('Close', 'last'),
                ('Volume', 'sum')
            ])
        ).dropna()

        return df

    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

@app.get("/")
def home():
    return {"message": "Welcome to the Stock Data API"}

@app.get("/fetch_data")
def fetch_data(
    symbol: str = Query(..., description="Stock symbol"),
    exchange: str = Query(..., description="Exchange name"),
    interval: str = Query("in_daily", description="Interval string"),
    n_bars: int = Query(5000, description="Number of bars to fetch"),
    fut_contract: int = Query(None, description="Futures contract ID, if applicable")
):
    """
    Endpoint to fetch stock data using tvDatafeed.

    Example: /fetch_data?symbol=gold&exchange=mcx&interval=in_5_minute&n_bars=100&fut_contract=1
    """
    # Validate interval
    interval_enum = INTERVAL_MAP.get(interval)
    if not interval_enum:
        raise HTTPException(status_code=400, detail=f"Invalid 'interval' value: {interval}")

    try:
        # Fetch the data
        if interval in ['in_10_minute', 'in_75_minute', 'in_125_minute']:
            data = fetch_stock_data_and_resample(symbol, exchange, interval, interval_enum, n_bars, fut_contract)
        else:
            tv_datafeed = TvDatafeed()
            if fut_contract:
                data = tv_datafeed.get_hist(symbol=symbol, exchange=exchange, interval=interval_enum, n_bars=n_bars, fut_contract=fut_contract)
            else:
                data = tv_datafeed.get_hist(symbol=symbol, exchange=exchange, interval=interval_enum, n_bars=n_bars)

        if data is None or data.empty:
            raise HTTPException(status_code=404, detail=f"No data found for symbol {symbol} on exchange {exchange}")

        # Convert DataFrame to JSON
        data_json = data.round(2).reset_index().to_dict(orient="records")
        return {"symbol": symbol, "data": data_json}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
