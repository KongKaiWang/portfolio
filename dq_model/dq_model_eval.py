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
    pd.set_option('display.width', 200, 'display.max_columns', 50)
    Z301_DIRECTORY = r'H:\Production\QC\Retest\Z301_Retest_Defects'
    DB_DIRECTORY = r'C:\Users\KW38770\Documents\WangK\Formation\Retest\Local_DB'
    MP_DIRECTORY = r'H:\Production\QC\Retest\Ready_To_Ship'
    MES_USER, MES_PW, MES_DSN = ['PENAENG', 'p6bru@aXE=am', '10.133.200.175/GEISDB.WORLD']
    COLD_USER, COLD_PW, COLD_DSN = ['kwang', 'XV7A6GneRC2a', '10.133.200.174:1521/colddb.america.gds.panasonic.com']
    cell_id_dict = {'1': '01', '2': '02', '3': '03', '4': '04', '5': '05', '6': '06', '7': '07', '8': '08', '9': '09',
                    'A': '10', 'B': '11', 'C': '12', 'D': '13', 'E': '14', 'F': '15', 'G': '16', 'H': '17', 'J': '18',
                    'K': '19', 'L': '20', 'M': '21', 'N': '22', 'P': '23', 'Q': '24', 'R': '25', 'S': '26', 'T': '27',
                    'U': '28', 'V': '29', 'W': '30', 'X': '31', 'Y': '32', 'Z': '33'}

    # 11/22/22 Resistance Sensitivity Analysis
    dq_model = pd.read_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\dvdq\Cell Validation\resistance_sensitivity_analysis.csv', index_col=0)
    # lot_list = ['220505HZ1', '220508HZ1', '220515HZ1', '220519HZ1', '220522HZ1', '220615HZ1']
    # dq_model = pd.DataFrame()
    # for lot in lot_list:
    #     data = pd.read_csv(
    #         rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\Processing\{lot}\{lot}_dvdq_discharge.csv')
    #     sensitivity = [-4000 + delta for delta in range(0, 8050, 50)]
    #     r_short = []
    #     for delta in sensitivity:
    #         print(lot, delta)
    #         data['dQdV'] = (1 / data.dVdQ) + delta
    #         data['i_short'] = data.apply(lambda x: -x.DVDT * x.dQdV / 24000, axis=1)
    #         data['r_short'] = data.apply(lambda x: x.RETEST_V2 / x.i_short, axis=1)
    #         r_short.append(data.r_short.median())
    #     dq_model = pd.concat([dq_model, pd.DataFrame(data={'dQdV_Sensitivity': sensitivity,
    #                                                        'Short_Resistance': r_short,
    #                                                        'Lot': lot})], ignore_index=True)
    # dq_model.to_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\dvdq\Cell Validation\resistance_sensitivity_analysis.csv')
    sns.scatterplot(data=dq_model,
                    x='dQdV_Sensitivity',
                    y='Short_Resistance',
                    hue='Lot',
                    s=25,
                    palette=['lightcoral', 'cornflowerblue', 'limegreen', 'dodgerblue', 'coral', 'goldenrod'])
    plt.title('Resistance Sensitivity Analysis')
    plt.xlabel('dQdV (mAh/V)')
    plt.ylabel('Short Resistance (Ohms)')
    plt.show()
    exit()

    # 11/22/22 Yield Sensitivity Analysis Plot
    dq_model = pd.read_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\dvdq\Cell Validation\yield_sensitivity_analysis.csv', index_col=0)
    dq_line = dq_model[(dq_model.Sensitivity > -1000) & (dq_model.Sensitivity < 1000)]
    sns.lineplot(data=dq_model,
                 x='Sensitivity',
                 y='Yield',
                 hue='Lot',
                 palette=['lightcoral', 'cornflowerblue', 'limegreen', 'dodgerblue', 'coral', 'goldenrod'])
    print(np.polyfit(dq_line.Sensitivity, dq_line.Yield, 1))
    plt.title('Yield Sensitivity Analysis')
    plt.xlabel('dQdV (mAh/V)')
    plt.ylabel('Yield (%)')
    plt.show()
    exit()

    # 11/21/22 Yield Sensitivity Analysis
    lot_list = ['220505HZ1', '220508HZ1', '220515HZ1', '220519HZ1', '220522HZ1', '220615HZ1']
    dq_model = pd.DataFrame()
    for lot in lot_list:
        data = pd.read_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\Processing\{lot}\{lot}_dvdq_discharge.csv')
        sensitivity = [-4000+delta for delta in range(0, 8050, 50)]
        lot_yield = []
        for delta in sensitivity:
            print(lot, delta)
            data['dQdV'] = (1 / data.dVdQ) + delta
            data['i_short'] = data.apply(lambda x: -x.DVDT * x.dQdV / 24000, axis=1)
            data['r_short'] = data.apply(lambda x: x.RETEST_V2 / x.i_short, axis=1)
            data['dq_judge'] = data.apply(lambda x: 'PASS' if x.r_short > 85e3 or (0.0005 > x.DVDT > 0) else 'FAIL', axis=1)
            lot_yield.append(len(data[data.dq_judge == 'PASS'])/len(data))
        dq_model = pd.concat([dq_model, pd.DataFrame(data={'dQdV Sensitivity (mAh/V)': sensitivity,
                                                           'Yield': lot_yield,
                                                           'Lot': lot})], ignore_index=True)
    dq_model.to_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\dvdq\Cell Validation\yield_sensitivity_analysis.csv')
    sns.scatterplot(data=dq_model,
                    x='Sensitivity',
                    y='Yield',
                    hue='Lot',
                    s=25,
                    palette=['lightcoral', 'cornflowerblue', 'limegreen', 'dodgerblue', 'coral', 'goldenrod'])
    plt.show()
    exit()

    # 11/6/22 Testing dQdV approach
    DQ_DIRECTORY = r'C:\Users\KW38770\Documents\WangK\Formation\Retest\dvdq'
    LOT_DIRECTORY = r'C:\Users\KW38770\Documents\WangK\Formation\Retest\Processing'
    lot = '220505HZ1'
    rt_lot = pd.read_csv(rf'{LOT_DIRECTORY}\{lot}\{lot}_retest_data.csv')
    rt_lot = rt_lot[rt_lot.SITTING_TIME > 0]
    dq_model = pd.read_csv(rf'{DQ_DIRECTORY}\21L_discharge_dvdq.csv')
    dq_model['Voltage_Index'] = dq_model.apply(lambda x: np.abs(dq_model.Capacity - x.dQ_Capacity).argmin()
                                               if not math.isnan(x.Capacity) else None, axis=1)
    dq_model['dQ_Voltage'] = dq_model.apply(lambda x: dq_model.Voltage[x.Voltage_Index]
                                            if x.Voltage_Index > 0 else None, axis=1)
    rt_lot['dVdQ_Index'] = rt_lot.apply(lambda x: np.abs(dq_model.dQ_Voltage - x.RETEST_V2).argmin(), axis=1)
    rt_lot['dVdQ'] = rt_lot.apply(lambda x: dq_model.dVdQ[x.dVdQ_Index], axis=1)
    rt_lot['i_short'] = rt_lot.apply(lambda x: -(x.DVDT / x.dVdQ) / 24000, axis=1)
    rt_lot['r_short'] = rt_lot.apply(lambda x: x.RETEST_V2 / x.i_short, axis=1)
    rt_lot['dq_judge'] = rt_lot.apply(lambda x: 'PASS' if x.r_short > 85e3 or (0.00005 > x.DVDT > 0) else 'FAIL', axis=1)
    # t1_lot.to_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\Processing\{lot}\{lot}_dvdq_discharge.csv')
    print('Old Yield', len(rt_lot[~rt_lot.PROCESS_NG & ~rt_lot.IMPINGEMENT_NG]), '/', len(rt_lot), f"({len(rt_lot[~rt_lot.PROCESS_NG & ~rt_lot.IMPINGEMENT_NG]) / len(rt_lot):.2f})")
    print('New Yield', len(rt_lot[(rt_lot.dq_judge == 'PASS') & (~rt_lot.IMPINGEMENT_NG)]), '/', len(rt_lot), f"({len(rt_lot[(rt_lot.dq_judge == 'PASS') & (~rt_lot.IMPINGEMENT_NG)]) / len(rt_lot):.2f})")
    print('Extrapolated Data Points', len(rt_lot[rt_lot.RETEST_V2 < 2.5523]))
    data_summary = pd.DataFrame(data={'Cell Age Min': rt_lot.SITTING_TIME.min(),
                                      'Cell Age Max': rt_lot.SITTING_TIME.max(),
                                      'dV Model OK': len(rt_lot[~rt_lot.PROCESS_NG]),
                                      'dV Model Voltage NGs': len(rt_lot[(rt_lot.RETEST_V1 < 3.46) | (rt_lot.RETEST_V2 < 3.46)]),
                                      'dV Model dV NGs': len(rt_lot[rt_lot.DVDT < rt_lot.DV_CUTOFF]),
                                      'dV Model Total NGs': len(rt_lot[rt_lot.PROCESS_NG]),
                                      'dQ Model OK': len(rt_lot[rt_lot.dq_judge == 'PASS']),
                                      'dQ Model NG': len(rt_lot[rt_lot.dq_judge == 'FAIL']),
                                      'Total Cells': len(rt_lot)}, index=[lot])
    print(data_summary)

