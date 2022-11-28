import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import cx_Oracle
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import math


if __name__ == '__main__':
    font = {'family': 'normal',
            'size': 22}
    plt.rcParams.update({'font.size': 18})
    plt.style.use("dark_background")
    pd.set_option('display.width', 200, 'display.max_columns', 50)
    Z301_DIRECTORY = r'H:\Production\QC\Retest\Z301_Retest_Defects'
    DB_DIRECTORY = r'C:\Users\KW38770\Documents\WangK\Formation\Retest\Local_DB'
    MP_DIRECTORY = r'H:\Production\QC\Retest\Ready_To_Ship'
    DQ_DIRECTORY = r'C:\Users\KW38770\Documents\WangK\Formation\Retest\dvdq'
    LOT_DIRECTORY = r'C:\Users\KW38770\Documents\WangK\Formation\Retest\Processing'
    MES_USER, MES_PW, MES_DSN = ['PENAENG', 'p6bru@aXE=am', '10.133.200.175/GEISDB.WORLD']
    COLD_USER, COLD_PW, COLD_DSN = ['kwang', 'XV7A6GneRC2a', '10.133.200.174:1521/colddb.america.gds.panasonic.com']
    cell_id_dict = {'1': '01', '2': '02', '3': '03', '4': '04', '5': '05', '6': '06', '7': '07', '8': '08', '9': '09',
                    'A': '10', 'B': '11', 'C': '12', 'D': '13', 'E': '14', 'F': '15', 'G': '16', 'H': '17', 'J': '18',
                    'K': '19', 'L': '20', 'M': '21', 'N': '22', 'P': '23', 'Q': '24', 'R': '25', 'S': '26', 'T': '27',
                    'U': '28', 'V': '29', 'W': '30', 'X': '31', 'Y': '32', 'Z': '33'}

    # 11/22/22 dVdt Transient Behavior Analysis
    lot_list = ['220615HZ1', '220522HZ1', '220519HZ1', '220515HZ1', '220508HZ1', '220505HZ1', '211201HZ1', '211107HZ1',
                '211025HZ1', '211024HZ1', '211022HZ1', '210728HZ1', '210708HZ1', '210704HZ1', '210302HZ2', '210226HZ1',
                '210221HZ1', '210207HZ1', '210206HZ1']
    dv_data = pd.DataFrame()
    for lot in lot_list:
        print(lot)
        lot_data = pd.read_csv(rf'{LOT_DIRECTORY}\{lot}\{lot}_retest_data.csv', usecols=['SITTING_TIME', 'DVDT'])
        lot_data['LOT_NO'] = lot
        dv_data = pd.concat([lot_data, dv_data], ignore_index=True)
    bins = list(np.arange(int(dv_data.SITTING_TIME.max())))
    data = dv_data.groupby(pd.cut(dv_data.SITTING_TIME, bins)).median()[['DVDT']]
    data['COUNT'] = dv_data.groupby(pd.cut(dv_data.SITTING_TIME, bins)).count().DVDT.values
    data['NOISE'] = data.COUNT > 10000
    data['CELL_AGE'] = list(np.arange(int(dv_data.SITTING_TIME.max())))[1:]
    data['DV_OK_MODEL'] = data.apply(lambda x: (-0.00263 - (3.03104 / x.CELL_AGE)) / 1000
                                     if x.CELL_AGE > 30 else None, axis=1)
    data['DV_CUTOFF'] = data.apply(lambda x: (-0.09763 - (3.03104 / x.CELL_AGE)) / 1000
                                   if x.CELL_AGE > 30 else None, axis=1)
    # shade extrapolation region
    plt.fill_between(x=data[(data.CELL_AGE < 150) & (data.CELL_AGE > 30)].CELL_AGE,
                     y1=0.0001,
                     y2=-0.0005,
                     facecolor='slategrey')
    sns.lineplot(data=data,
                 x='CELL_AGE',
                 y='DV_OK_MODEL',
                 color='skyblue',
                 label='dV Model Behavior')
    sns.lineplot(data=data,
                 x='CELL_AGE',
                 y='DV_CUTOFF',
                 color='limegreen',
                 label='dV Cutoff')
    sns.regplot(data=data[(data.CELL_AGE > 150) & data.NOISE],
                x='CELL_AGE',
                y='DVDT',
                # label='Model Extrapolation',
                color='salmon',
                # scatter=False,
                ci=None,
                truncate=True)
    data = data.dropna()

    sns.scatterplot(data=data[~data.NOISE],
                    x='CELL_AGE',
                    y='DVDT',
                    marker='.',
                    color='black',
                    label='Raw Data (Median)',
                    s=50)
    sns.scatterplot(data=data[data.NOISE],
                    x='CELL_AGE',
                    y='DVDT',
                    color='salmon',
                    label='Filtered Data (10k+ pts)',
                    s=50)
    plt.ylim([-0.0005, 0.0001])
    plt.title('dVdt Decay Behavior (19 lots | 2.4M cells)')
    plt.xlabel('Cell Age (days)')
    plt.ylabel('dVdt (V/day)')
    plt.show()
    exit()


