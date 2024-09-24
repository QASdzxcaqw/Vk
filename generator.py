import pandas as pd
import os
import sys
from datetime import datetime, timedelta

def aggregate_user_actions(date_str):
    target_date = datetime.strptime(date_str, '%Y-%m-%d')
    start_date = target_date - timedelta(days=7)

    all_logs = []
    os.makedirs('input', exist_ok=True)
    os.system('python input_data.py input 2024-09-10 30 10 2000') #Входные данные
    for single_date in (start_date + timedelta(n) for n in range(7)):
        filename = f'input/{single_date.strftime("%Y-%m-%d")}.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = pd.concat([pd.DataFrame([df.columns.values], columns=df.columns), df], ignore_index=True)
            df.columns = ['email', 'action', 'dt']
            all_logs.append(df)

    all_logs_df = pd.concat(all_logs, ignore_index=True)

    aggregated_data = all_logs_df.groupby('email').action.value_counts().unstack(fill_value=0)
    aggregated_data.columns.name = None
    aggregated_data.reset_index(inplace=True)

    aggregated_data.columns = ['email', 'create_count', 'read_count', 'update_count', 'delete_count']


    os.makedirs('output', exist_ok=True)

    output_filename = f'output/{date_str}.csv'
    aggregated_data.to_csv(output_filename, index=False)

    print(f"Aggregated data saved to {output_filename}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <YYYY-mm-dd>")
    else:
        aggregate_user_actions(sys.argv[1])
