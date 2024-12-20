import json
from tvDatafeed import TvDatafeed, Interval
from collections import OrderedDict
from fastapi import FastAPI, Query, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from typing import Dict, Set
import logging
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.client_ids: Set[str] = set()

    async def connect(self, websocket: WebSocket, client_id: str) -> bool:
        try:
            await websocket.accept()
            if client_id in self.client_ids:
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
                return False
            self.active_connections[client_id] = websocket
            self.client_ids.add(client_id)
            logger.info(f"Client {client_id} connected successfully")
            return True
        except Exception as e:
            logger.error(f"Error connecting client {client_id}: {e}")
            return False

    def disconnect(self, client_id: str):
        if client_id in self.client_ids:
            self.client_ids.remove(client_id)
        if client_id in self.active_connections:
            del self.active_connections[client_id]
        logger.info(f"Client {client_id} disconnected")

manager = ConnectionManager()

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

def serialize_datetime(obj):
    """Custom JSON serializer for datetime objects"""
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def prepare_data_for_json(df):
    """Prepare DataFrame for JSON serialization"""
    if df is None or df.empty:
        return None
    
    # Reset index to make datetime a column
    df = df.reset_index()
    
    # Convert datetime column to ISO format string
    datetime_col = df.columns[0]  # First column is usually datetime
    df[datetime_col] = df[datetime_col].apply(lambda x: x.isoformat())
    
    # Round numeric columns to 2 decimal places
    numeric_columns = df.select_dtypes(include=['float64', 'float32']).columns
    df[numeric_columns] = df[numeric_columns].round(2)
    
    # Convert to dictionary
    return df.to_dict(orient="records")

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

@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    connection_successful = await manager.connect(websocket, client_id)
    
    if not connection_successful:
        return
    
    try:
        while True:
            message = await websocket.receive_text()
            
            try:
                symbol, exchange, interval_str, n_bars, fut_contract = message.split(",")
                n_bars = int(n_bars)
                fut_contract = int(fut_contract) if fut_contract != "None" else None
            except ValueError:
                await websocket.send_text(json.dumps({
                    "error": "Invalid message format. Expected: symbol,exchange,interval,n_bars,fut_contract"
                }))
                continue

            interval_enum = INTERVAL_MAP.get(interval_str)
            if not interval_enum:
                await websocket.send_text(json.dumps({
                    "error": f"Invalid interval: {interval_str}"
                }))
                continue

            try:
                # Fetch the data
                if interval_str in ['in_10_minute', 'in_75_minute', 'in_125_minute']:
                    data = fetch_stock_data_and_resample(symbol, exchange, interval_str, interval_enum, n_bars, fut_contract)
                else:
                    tv_datafeed = TvDatafeed()
                    if fut_contract:
                        data = tv_datafeed.get_hist(symbol=symbol, exchange=exchange, interval=interval_enum, n_bars=n_bars, fut_contract=fut_contract)
                    else:
                        data = tv_datafeed.get_hist(symbol=symbol, exchange=exchange, interval=interval_enum, n_bars=n_bars)

                if data is None or data.empty:
                    await websocket.send_text(json.dumps({
                        "error": f"No data found for symbol {symbol} on exchange {exchange}"
                    }))
                    continue

                # Prepare data for JSON serialization
                json_data = prepare_data_for_json(data)
                
                # Send the data
                await websocket.send_json({
                    "symbol": symbol,
                    "exchange": exchange,
                    "interval": interval_str,
                    "data": json_data
                })
                
                # Wait before next update
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {e}")
                await websocket.send_text(json.dumps({
                    "error": f"Error fetching data for {symbol}: {e}"
                }))
                continue

    except WebSocketDisconnect:
        manager.disconnect(client_id)
    except Exception as e:
        logger.error(f"Error in websocket connection for client {client_id}: {e}")
        manager.disconnect(client_id)
        try:
            await websocket.send_text(json.dumps({
                "error": str(e)
            }))
        except:
            pass
        await websocket.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
