import yfinance as yf

# Apple Ticker 객체 생성
ticker_symbol = "AAPL"
apple = yf.Ticker(ticker_symbol)

# 모든 정보 출력
print(apple.info)  # 기본 정보
print(apple.history(period="max"))  # 최대 기간의 주가 데이터
print(apple.dividends)  # 배당금 데이터
print(apple.splits)  # 주식 분할 데이터
print(apple.recommendations)  # 추천 데이터
print(apple.calendar)  # 캘린더 데이터
