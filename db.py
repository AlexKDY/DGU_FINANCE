import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, insert, select
from sqlalchemy.exc import IntegrityError
import yfinance as yf
from datetime import datetime, timezone

# 데이터베이스 연결 설정
DB_URL = "mysql+pymysql://root:rlawjdgus1!@localhost:3306/finance_db"
engine = create_engine(DB_URL)

# 메타데이터 객체 생성
metadata = MetaData()
metadata.reflect(bind=engine)

# 테이블 객체 가져오기
item_table = Table("Item", metadata, autoload_with=engine)
ohlcv_table = Table("OHLCV", metadata, autoload_with=engine)
fundamental_table = Table("Fundamental", metadata, autoload_with=engine)

# UNIX 타임스탬프를 DATE 또는 DATETIME 형식으로 변환하는 함수
def convert_unix_to_date(unix_timestamp):
    if unix_timestamp:
        return datetime.fromtimestamp(unix_timestamp, timezone.utc).strftime('%Y-%m-%d')
    return None

def convert_unix_to_datetime(unix_timestamp):
    if unix_timestamp:
        return datetime.fromtimestamp(unix_timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    return None

# 나스닥 종목 리스트 가져오기
def get_nasdaq_tickers():
    nasdaq_url = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"
    try:
        # 나스닥 데이터 크롤링
        nasdaq_data = pd.read_csv(nasdaq_url, sep='|')
        tickers = nasdaq_data['Symbol'].dropna().tolist()
        tickers = [ticker for ticker in tickers if ticker != 'Symbol']  # 헤더 제외
        return tickers
    except Exception as e:
        print(f"Error fetching NASDAQ tickers: {e}")
        return []

# 티커 데이터를 데이터베이스에 저장
def insert_ticker_data(ticker_symbol):
    try:
        # yfinance에서 데이터 가져오기
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info
        ohlcv_data = ticker.history(period="1mo", interval="1d")  # 지난 한 달간의 일봉 데이터

        # Step 1: Item 테이블에 데이터 삽입 (이미 있는 경우 무시)
        item_data = {
            "code": info.get("symbol"),
            "name": info.get("shortName"),
            "country": info.get("country"),
            "market": info.get("exchange"),
            "sector_name": info.get("sector"),
            "sector_code": info.get("sectorKey"),
            "type": "EQUITY" if info.get("quoteType") == "EQUITY" else None
        }
        item_data = {k: v for k, v in item_data.items() if v is not None}

        with engine.begin() as connection:
            existing_item = connection.execute(
                select(item_table).where(item_table.c.code == ticker_symbol)
            ).fetchone()
            if not existing_item:
                connection.execute(insert(item_table).values(item_data))
                print(f"Inserted Item data for {ticker_symbol}")
            else:
                print(f"Item data for {ticker_symbol} already exists, skipping.")

        # Step 2: OHLCV 테이블 데이터 삽입
        with engine.begin() as connection:
            for timestamp, row in ohlcv_data.iterrows():
                ohlcv_data_row = {
                    "code": ticker_symbol,
                    "window_size": 1440,  # 하루를 분 단위로
                    "timestamp": int(timestamp.timestamp()),  # UNIX timestamp
                    "open": row["Open"],
                    "high": row["High"],
                    "low": row["Low"],
                    "close": row["Close"],
                    "volume": row["Volume"],
                    "trading_val": row["Volume"] * row["Close"] if row["Volume"] and row["Close"] else None
                }
                ohlcv_data_row = {k: v for k, v in ohlcv_data_row.items() if v is not None}
                try:
                    connection.execute(insert(ohlcv_table).values(ohlcv_data_row))
                except IntegrityError:
                    continue

        # Step 3: Fundamental 테이블 데이터 삽입
        fundamental_data = {
            "code": info.get("symbol"),
            "timestamp": convert_unix_to_datetime(info.get("mostRecentQuarter")),
            "close": info.get("currentPrice"),
            "volume": info.get("regularMarketVolume"),
            "issued_share": info.get("sharesOutstanding"),
            "cap": info.get("marketCap"),
            "sector_per": info.get("trailingPE"),
            "dividend": info.get("dividendRate"),
            "div_release_date": convert_unix_to_date(info.get("exDividendDate")),
            "total_revenue": info.get("totalRevenue"),
            "operating_income": info.get("operatingCashflow"),
            "net_income": info.get("netIncomeToCommon"),
            "total_assets": info.get("totalAssets"),
            "total_liabilities": info.get("totalDebt"),
            "total_equity": info.get("bookValue")
        }
        fundamental_data = {k: v for k, v in fundamental_data.items() if v is not None}
        with engine.begin() as connection:
            try:
                connection.execute(insert(fundamental_table).values(fundamental_data))
                print(f"Inserted Fundamental data for {ticker_symbol}")
            except IntegrityError:
                print(f"Fundamental data for {ticker_symbol} already exists, skipping.")

    except Exception as e:
        print(f"Error processing {ticker_symbol}: {e}")

# 모든 나스닥 티커의 데이터를 가져오고 저장
def fetch_and_store_all_tickers():
    tickers = get_nasdaq_tickers()
    print(f"Total NASDAQ tickers: {len(tickers)}")
    
    for ticker_symbol in tickers:
        try:
            print(f"Fetching data for {ticker_symbol}...")
            insert_ticker_data(ticker_symbol)
        except Exception as e:
            print(f"Error processing {ticker_symbol}: {e}")

# 실행 예제
fetch_and_store_all_tickers()
