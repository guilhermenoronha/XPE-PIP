from airflow.decorators import dag, task, branch_task
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner' : 'Guilherme Noronha',
    'start_date': days_ago(1)
}

@dag(default_args=default_args, schedule_interval='@once')
def branching_dag():

    @branch_task
    def decision_make():
        import pandas as pd

        df = pd.read_csv('/opt/airflow/dags/data/tickers/tickers.csv')
        buy_count = 0
        sell_count = 0
        for idx, row in df.iterrows():
            if (row.recommendationKey == 'buy'):
                buy_count += 1
            elif (row.recommendationKey == 'sell'):
                sell_count += 1
        if buy_count >= sell_count:
            return 'buy_order'
        else:
            return 'sell_order'

    @task
    def buy_order():
        print('Estou comprando tudo!')
    
    @task 
    def sell_order():
        print('Estou vendendo tudo!')
    
    end = EmptyOperator(
        task_id = 'end',
        trigger_rule= TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    decision_make() >> [buy_order(), sell_order()] >> end

dag = branching_dag()