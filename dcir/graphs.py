import pandas as pd
from datetime import datetime
import multiprocessing as mp
import cx_Oracle
import os
from scipy.stats import norm
from scipy.interpolate import UnivariateSpline, splrep, splev
import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns
import numpy as np


if __name__ == '__main__':
    pd.set_option('display.width', 200, 'display.max_columns', 50)
    font = {'family': 'normal',
            'size': 22}
    plt.rcParams.update({'font.size': 18})
    MES_USER, MES_PW, MES_DSN = ['PENAENG', 'p6bru@aXE=am', '10.133.200.175/GEISDB.WORLD']
    COLD_USER, COLD_PW, COLD_DSN = ['kwang', 'XV7A6GneRC2a', '10.133.200.174:1521/colddb.america.gds.panasonic.com']
    separator_dict = {'G': 'S4', 'M': 'S5', 'Q': 'S6', 'P': 'S7'}
    cell_id_dict = {'1': '01', '2': '02', '3': '03', '4': '04', '5': '05', '6': '06', '7': '07', '8': '08', '9': '09',
                    'A': '10', 'B': '11', 'C': '12', 'D': '13', 'E': '14', 'F': '15', 'G': '16', 'H': '17', 'J': '18',
                    'K': '19', 'L': '20', 'M': '21', 'N': '22', 'P': '23', 'Q': '24', 'R': '25', 'S': '26', 'T': '27',
                    'U': '28', 'V': '29', 'W': '30', 'X': '31', 'Y': '32', 'Z': '33'}
    month_list = ['L8', 'L9', 'LA', 'LB', 'LC', 'M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9', 'MA', 'MB', 'MC', 'N1']
    DIRECTORY = r'C:\Users\KW38770\Documents\WangK\Formation\Retest\Characterization\Low Voltage Qualification'
    DVDIR = r'C:\Users\KW38770\Documents\WangK\Formation\Retest\dvdq'

    sample_data = pd.read_csv(rf'{DIRECTORY}\low_voltage_data.csv')
    pop_data = pd.read_csv(rf'{DIRECTORY}\retest_voltage_data.csv')
    low_volt_dcir_data = pd.read_csv(rf'{DIRECTORY}\low_voltage_dcir_data.csv')
    retest_lots = pd.read_csv(rf'{DIRECTORY}\retest_lots.csv', header=None)
    retest_lots.columns = ['LOTS']




    # Retest Age Composition
    retest_lots['WEEK'] = retest_lots.apply(lambda x: datetime(int('20' + x.LOTS[0:2]), int(x.LOTS[2:4]), int(x.LOTS[4:6])), axis=1)
    retest_lots['LOT_COUNT'] = 1
    retest_lots = retest_lots[:87]
    with cx_Oracle.connect(user=MES_USER, password=MES_PW, dsn=MES_DSN) as con:
        df = pd.read_sql(f"""
        SELECT TRUNC(MEASURE_DATE, 'DAY') AS WEEK, COUNT(CELL_ID) AS CELL_COUNT
        FROM GEIS.V_CELL_ENG_GC13_EN
        WHERE DEFECT_REASON_CD = '900W'
        GROUP BY TRUNC(MEASURE_DATE, 'DAY')""", con)
    df['LOT_COUNT'] = df.CELL_COUNT/128000
    master_df = pd.concat([retest_lots, df], ignore_index=True)
    master_df.to_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\dvdq\age_comp_data.csv')
    print(master_df)
    exit()

    # Low Voltage Data Cleaning
    sample_data['CELL_TYPE'] = 'Samples(250d+)'
    pop_data['CELL_TYPE'] = pop_data.apply(lambda x: 'Standard Process(45d)' if x.SITTING_TIME < 45
                                           else 'Break In Period(45-90d)' if x.SITTING_TIME < 90
                                           else 'Voltage NGs(90d+)' if x.RETEST_V2 > 3.07
                                           else 'Other', axis=1)
    pop_data = pd.concat([pop_data[pop_data.CELL_TYPE == 'Standard Process(45d)'].sample(448),
                          pop_data[pop_data.CELL_TYPE == 'Break In Period(45-90d)'].sample(448),
                          pop_data[pop_data.CELL_TYPE == 'Voltage NGs(90d+)'].sample(448)])
    plot_data = pd.concat([pop_data[['CELL_ID', 'CELL_TYPE', 'SITTING_TIME', 'RETEST_V2']],
                           sample_data[['CELL_ID', 'CELL_TYPE', 'SITTING_TIME', 'RETEST_V2']]], ignore_index=True)
    plot_data.columns = ['CELL_ID', 'CELL_TYPE', 'CELL_AGE', 'V2']  # rename parameters

    # Low Voltage - DCIR Distribution
    fig1, ax1 = plt.subplots(1, 3)  # dcir plots
    fig2, ax2 = plt.subplots(1, 2)  # cap plots
    sns.kdeplot(ax=ax1[0], data=low_volt_dcir_data, x='50% DCIR', hue='Type', bw_adjust=1.2, fill=True)
    sns.kdeplot(ax=ax1[1], data=low_volt_dcir_data, x='4.1V DCIR', hue='Type', bw_adjust=1.2, fill=True)
    sns.kdeplot(ax=ax1[2], data=low_volt_dcir_data, x='4.2V DCIR', hue='Type', bw_adjust=1.2, fill=True)
    sns.kdeplot(ax=ax2[0], data=low_volt_dcir_data, x='0.2C Capacity', hue='Type', bw_adjust=1.2, fill=True)
    sns.kdeplot(ax=ax2[1], data=low_volt_dcir_data, x='1C Capacity', hue='Type', bw_adjust=1.2, fill=True)
    fig1.suptitle('DCIR Testing Results')
    fig2.suptitle('Capacity Testing Results')
    plt.show()
    exit()

    # Low Voltage - Voltage Distribution
    # print('Data Summary Statistics')
    # palette = ['darkred', 'tomato', 'darkorange', 'skyblue']
    # for cell_type in plot_data.CELL_TYPE.unique():
    #     subset = plot_data[plot_data.CELL_TYPE == cell_type]
    #     print(f"{cell_type}:")
    #     print(f"N={len(subset)}, x={subset.V2.mean():.3f}, sigma={subset.V2.std():.3f}, min={subset.V2.min():.3f}, max={subset.V2.max():.3f}")
    # fig = sns.kdeplot(data=plot_data,
    #                   x='V2',
    #                   hue='CELL_TYPE',
    #                   fill=True,
    #                   common_norm=False,
    #                   palette=palette,
    #                   bw_adjust=1)
    # fig.set(xlim=(3.1, 3.5), xlabel='Voltage (V)')
    # fig.set_title('Voltage Distribution')
    # sns.move_legend(fig, 'lower left', bbox_to_anchor=(0.1, 0.1))
    # plt.show()



