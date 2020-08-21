import sys, io, time, requests, json, re
from datetime import datetime, timedelta
from contextlib import contextmanager

import pandas as pd
import yfinance as yf

# pd.set_option('display.max_columns', None)

@contextmanager
def hide_print():
    sys.stdout = io.StringIO()
    try: yield
    finally: sys.stdout = sys.__stdout__

try:
    data = pd.read_pickle('stock_data_downloading.pickle')
except:
    print('Starting from scratch')
    data = None

tickers_json = requests.get('https://www.macrotrends.net/assets/php/ticker_search_list.php').json()
ticker_links = [t['s'].split(' - ', 1)[0] for t in tickers_json]

for i, link in enumerate(ticker_links):
    ticker = link.split('/', 1)[0]

    if ticker in "SPY QQQ": continue

    if data is not None and ticker in data['ticker'].values:
        print(f'{ticker} already done')
        continue

    print(f"Downloading {ticker} {i}/{len(ticker_links)}...", end='')

    try:

        def scrape_data(url):
            for i in range(5):
                try:
                    html = requests.get(url).text
                    break
                except:
                    time.sleep(10)
            start = html.index('var originalData = ')
            stop = html.index("]", start)
            return json.loads(html[start+len('var originalData = '):stop+1])

        key_financial_ratios = scrape_data(f"https://www.macrotrends.net/stocks/charts/{link}/financial-ratios?freq=Q")
        income_statement = scrape_data(f"https://www.macrotrends.net/stocks/charts/{link}/income-statement?freq=Q")

        def flt(val):
            try: return float(val)
            except: return None

        def get_values(col):
            name = col['field_name']
            if '<' in name:
                name = re.findall(">(.+?)<", col['field_name'])[0]
            name = name.split(' - ', 1)[0].replace(' ', '_').replace('-', '_').replace('/', '2').lower()
            values = [flt(val) if val != '-' else None for key, val in col.items() if key[0].isdigit()]
            return name, values

        dates = [datetime.strptime(key, "%Y-%m-%d") for key, val in key_financial_ratios[0].items() if key[0].isdigit()]
        key_financial_ratios = dict(get_values(col) for col in key_financial_ratios)
        income_statement = dict(get_values(col) for col in income_statement)

        # def get_price(date):
        #     for i in range(5):
        #         stop = date + timedelta(days=3 + i*20)
        #         sys.stdout = io.StringIO()
        #         prices = yf.download(ticker, date.strftime("%Y-%m-%d"), stop.strftime("%Y-%m-%d"))
        #         sys.stdout = sys.__stdout__
        #         if (len(prices)): return prices['Open'][0]
        #         print('retry...', end='')
        #     else:
        #         print('fail...', end='')

        with hide_print():
            all_prices = yf.download(ticker, start=dates[-1])
            try:
                info = yf.Ticker(ticker).get_info()
            except:
                print('no info...', end='')
                info = {}

        price_map = {_date.strftime("%Y-%m-%d"): row['Open'] for _date, row in all_prices.iterrows()}

        def get_price(date):
            for i in range(7):
                d = date + timedelta(days=i)
                try: return price_map[d.strftime("%Y-%m-%d")]
                except: pass

            # try:
            #     _date, row = next((_date, row) for _date, row in all_prices.iterrows() if _date > date)
            #     assert abs(_date - date) < timedelta(days=7), (_date - date, _date, date)
            #     return row['Open']
            # except:
            #     return None

        prices    = [get_price(date) for date in dates]
        prices_1m = [get_price(date + timedelta(days=30)) for date in dates]
        prices_6m = [get_price(date + timedelta(days=30*6)) for date in dates]
        prices_1y = [get_price(date + timedelta(days=365))  for date in dates]

        df = pd.DataFrame(dict(
            date = dates,
            price = prices,
            price_1m = prices_1m,
            price_6m = prices_6m,
            price_1y = prices_1y,
            ticker = [ticker]*len(dates),
            market = [info.get('market')]*len(dates),
            sector = [info.get('sector')]*len(dates),
            industry = [info.get('industry')]*len(dates),
            **income_statement,
            **key_financial_ratios,
        ))
    except Exception as error:
        print(f"error {error}")
        continue

    if data is None:
        data = df
    else:
        data = data.append(df)

    data.to_pickle('stock_data_downloading.pickle')
    print(f"Done!")

print(', '.join(data.columns))

data = data.dropna(subset='price_1y'.split())
data['earnings2price'] = data['eps'] / data['price']
data['book2price'] = data['book_value_per_share'] / data['price']

data = data['ticker date price price_1m price_6m price_1y market sector industry net_income revenue research_and_development_expenses earnings2price book2price current_ratio long_term_debt_2_capital debt2equity_ratio total_non_operating_income2expense'.split()]
data = data.convert_dtypes()

data.to_pickle('stock_data.pickle')