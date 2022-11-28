import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import os
import re


if __name__ == '__main__':
    DIRECTORY = r'C:\Users\KW38770\Documents\WangK\Formation\Production Throughput Optimization\Transport Times'
    end_time = pd.DataFrame()
    start_time = pd.DataFrame()
    # Compile X500 data
    for file in os.listdir(DIRECTORY):
        if file[0:2] == 'VI' and file.endswith('.csv') and len(file) < 14:
            vi_df = pd.read_csv(rf'{DIRECTORY}\{file}', index_col=False, header=None)
            vi_df.columns = ['Date', 'Storage Unit', 'Transfer Status', 'Destination', 'From', 'To',
                             'Transfer Number', 'From MEM', 'To MEM', 'Barcode Data']
            end_time = pd.concat([end_time, vi_df])
        elif file.endswith('.csv') and len(file) < 14:
            crane_df = pd.read_csv(rf'{DIRECTORY}\{file}', index_col=False, header=None)
            crane_df.columns = ['Date', 'Storage Unit', 'Transfer Status', 'Destination', 'From', 'To',
                                'Transfer Number', 'From MEM', 'To MEM', 'Barcode Data']
            start_time = pd.concat([start_time, crane_df])
    # Filter out start time rows where 'To' is VI line
    regex = re.compile('VI.. Input CV')
    start_time = start_time[start_time['To'].apply(lambda x: True if re.match(regex, x) is not None else False)]
    # Convert data types
    start_time = start_time.astype({'Barcode Data': 'string', 'Date': 'datetime64[ns]'})
    end_time = end_time.astype({'Barcode Data': 'string', 'Date': 'datetime64[ns]'})
    # Remove duplicate entries
    start_time.sort_values('Date', ascending=False, inplace=True)
    start_time = start_time.drop_duplicates(['Barcode Data'], keep='first')
    # Join start & end dataframes to calculate transport time
    transport_time = start_time.merge(end_time, on='Barcode Data', how='inner', suffixes=('_Start', '_End'))
    transport_time['End - Start'] = transport_time['Date_End'] - transport_time['Date_Start']
    transport_time['Transport Time'] = transport_time['End - Start'].dt.total_seconds()
    # Clean up outliers from data
    print(len(transport_time))
    transport_time = transport_time[np.abs(stats.zscore(transport_time['Transport Time'])) < 3]
    transport_time.to_csv(r'C:\Users\KW38770\Documents\WangK\Formation\Production Throughput Optimization\Transport Times\P1_Transport_Times.csv')

    # Statistics
    print(f"Transport Time Min: {transport_time['Transport Time'].min():.2f}")
    print(f"Transport Time Max: {transport_time['Transport Time'].max():.2f}")
    print(f"Transport Time Average: {transport_time['Transport Time'].mean():.2f}")
    print(f"Transport Time Median: {transport_time['Transport Time'].median():.2f}")
    print(f"Median Transport Time by Line: {transport_time.groupby('To_Start').median()['Transport Time']}")

    # Plot Visualizations
    sns.set()
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(10, 10))
    axes = axes.flatten()
    sns.histplot(ax=axes[0], kde=True, data=transport_time, x='Transport Time', fill=True, hue='To_Start')
    sns.histplot(ax=axes[1], kde=True, data=transport_time, x='Transport Time', fill=True)

    plt.show()
