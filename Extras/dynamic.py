from http.cookiejar import DefaultCookiePolicy
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'Guilherme Noronha',
    'start_date': days_ago(1)
}

@dag(default_args=DEFAULT_ARGS, schedule_interval='@once')
def dynamic():

    @task
    def create_folder():
        import os
        os.makedirs('/opt/airflow/dags/data/tickers', exist_ok=True)
    
    @task
    def get_tickers():
        return ['BBAS3.SA', 'BBSE3.SA', 'PETR3.SA']

    @task
    def get_ticker_info(ticker_symbol):
        import yfinance as yf
        ticker = yf.Ticker(ticker_symbol)
        return ticker.info

    @task
    def save_all_tickers(ticker_list):
        import pandas as pd
        df_list = []
        for ticker in ticker_list:
            df = pd.DataFrame([ticker])
            df_list.append(df)
        final_df = pd.concat(df_list)
        final_df.to_csv('/opt/airflow/dags/data/tickers/tickers.csv')

    ticker_list = get_ticker_info.expand(ticker_symbol=get_tickers())
    create_folder() >> save_all_tickers(ticker_list)

dag = dynamic()