import yfinance as yf
import pandas as pd

# 데이터 필터링 및 파일에 저장 함수
def save_ticker_info_to_file(data, filename="ticker_data.txt"):
    try:
        # Item 테이블 데이터
        item_data = {
            "code": data.get("symbol"),
            "name": data.get("shortName"),
            "country": data.get("country"),
            "market": data.get("exchange"),
            "sector_name": data.get("sector"),
            "sector_code": data.get("sectorKey"),
            "type": "EQUITY" if data.get("quoteType") == "EQUITY" else None
        }
        item_data = {key: value for key, value in item_data.items() if value is not None}
        
        # Fundamental 테이블 데이터
        fundamental_data = {
            "code": data.get("symbol"),
            "timestamp": data.get("mostRecentQuarter"),
            "close": data.get("currentPrice"),
            "volume": data.get("regularMarketVolume"),
            "issued_share": data.get("sharesOutstanding"),
            "cap": data.get("marketCap"),
            "sector_per": data.get("trailingPE"),
            "dividend": data.get("dividendRate"),
            "div_release_date": data.get("exDividendDate"),
            "total_revenue": data.get("totalRevenue"),
            "operating_income": data.get("operatingCashflow"),
            "net_income": data.get("netIncomeToCommon"),
            "total_assets": data.get("totalAssets"),
            "total_liabilities": data.get("totalDebt"),
            "total_equity": data.get("bookValue")
        }
        fundamental_data = {key: value for key, value in fundamental_data.items() if value is not None}

        # 파일에 저장
        with open(filename, "a", encoding="utf-8") as file:
            file.write(f"Item Data for {item_data.get('code', 'Unknown')}:\n")
            file.write(f"{item_data}\n\n")
            file.write(f"Fundamental Data for {fundamental_data.get('code', 'Unknown')}:\n")
            file.write(f"{fundamental_data}\n")
            file.write("="*80 + "\n")
        
        print(f"Saved data for {data.get('symbol')} to {filename}")

    except Exception as e:
        print(f"Error saving data: {e}")

# NASDAQ 티커 가져오기
def get_nasdaq_tickers():
    nasdaq_url = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"
    try:
        nasdaq_data = pd.read_csv(nasdaq_url, sep='|')
        tickers = nasdaq_data['Symbol'].tolist()
        return tickers
    except Exception as e:
        print(f"Error fetching NASDAQ tickers: {e}")
        return []

# NASDAQ 티커의 데이터를 가져오고 저장
def fetch_and_save_all_tickers():
    tickers = get_nasdaq_tickers()
    print(f"Total NASDAQ tickers: {len(tickers)}")
    
    for ticker_symbol in tickers[:4816]: 
        try:
            print(f"Fetching data for {ticker_symbol}...")
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            save_ticker_info_to_file(info)
        except Exception as e:
            print(f"Error processing {ticker_symbol}: {e}")

# 실행
fetch_and_save_all_tickers()
