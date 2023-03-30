"""
Flagging v2.0

This program is designed to flag retest cells for dV and impingement NGs

Authored by: Kong Wang
Date: 09/26/22

Notes:

Version Updates:
v2.0 - Initial release - Built off flagging_script_official.py (9/26)
v2.1 - dQ model and impingement module and config file implemented. (12/8)
"""
import os
import pandas as pd
import numpy as np
import time
import tqdm
import cx_Oracle
import configparser
import multiprocessing as mp

from datetime import datetime
from dateutil.relativedelta import relativedelta
from offset import getOffsets
from impingement import get_impingement_cells


def get_retest_date(lot: str, db: dict) -> list[str]:
    """
    Input: lot number and database credentials
    Function: determines retest formation date
    Output: formation table dates
    """
    print('get retest date')
    retest_month_list = []
    # look for retest formation month in MES tables
    with cx_Oracle.connect(user=db['mes_user'], password=db['mes_pw'], dsn=db['mes_dsn']) as con:
        for month in [(datetime.now() - relativedelta(months=x)).strftime('%y%m') for x in range(0, 12)]:
            v1_cell_count = pd.read_sql(f"""
            SELECT COUNT(CELL_ID)
            FROM GEIS.T_CELL_ENG_GC12_{month}
            WHERE LOT_NO = '{lot}'
            """, con)
            v2_cell_count = pd.read_sql(f"""
            SELECT COUNT(CELL_ID)
            FROM GEIS.T_CELL_ENG_GC13_{month}
            WHERE LOT_NO = '{lot}'
            """, con)
            if v1_cell_count.iloc[0, 0] != 0:
                retest_month_list.append(month)
            if v2_cell_count.iloc[0, 0] != 0:
                retest_month_list.append(month)
    if len(retest_month_list) == 0:
        with cx_Oracle.connect(user=db['cold_user'], password=db['cold_pw'], dsn=db['cold_dsn']) as con:
            for month in [(datetime.now() - relativedelta(months=x)).strftime('%y%m') for x in range(12, 24)]:
                v1_cell_count = pd.read_sql(f"""
                SELECT COUNT(CELL_ID)
                FROM GEIS.T_CELL_ENG_GC12_{month}
                WHERE LOT_NO = '{lot}'
                """, con)
                v2_cell_count = pd.read_sql(f"""
                SELECT COUNT(CELL_ID)
                FROM GEIS.T_CELL_ENG_GC13_{month}
                WHERE LOT_NO = '{lot}'
                """, con)
                if v1_cell_count.iloc[0, 0] != 0:
                    retest_month_list.append(month)
                if v2_cell_count.iloc[0, 0] != 0:
                    retest_month_list.append(month)
    retest_month_list = list(set(retest_month_list))
    print('retest month list', retest_month_list)
    return retest_month_list


def check_retest_complete(lot: str, db: dict, cold_db: bool, yymm_list: list[str]) -> bool:
    """
    Input: lot number, database credentials, cold boolean indicator and table month list
    Function: checks tray counts between v1 and v2
    Output: boolean indicator if retest formation is complete
    """
    print('check retest complete')
    user = db['mes_user']
    pw = db['mes_pw']
    dsn = db['mes_dsn']
    if cold_db:  # can delete?
        user = db['cold_user']
        pw = db['cold_pw']
        dsn = db['cold_dsn']
    v1_tables = ' UNION ALL '.join([f'(SELECT CELL_ID, LOT_NO, TRAY_NO FROM GEIS.T_CELL_ENG_GC12_{yymm})'
                                    for yymm in yymm_list])
    v2_tables = ' UNION ALL '.join([f'(SELECT CELL_ID, LOT_NO, TRAY_NO FROM GEIS.T_CELL_ENG_GC13_{yymm})'
                                    for yymm in yymm_list])
    with cx_Oracle.connect(user=user, password=pw, dsn=dsn) as con:
        v1_tray_count = pd.read_sql(f"""
        SELECT COUNT(DISTINCT(TRAY_NO))
        FROM ({v1_tables})
        WHERE LOT_NO = '{lot}'
        AND CELL_ID LIKE 'G%'
        """, con)
        v2_tray_count = pd.read_sql(f"""
        SELECT COUNT(DISTINCT(TRAY_NO))
        FROM ({v2_tables})
        WHERE LOT_NO = '{lot}'
        AND CELL_ID LIKE 'G%'
        """, con)
        print(v1_tray_count, v2_tray_count)
    return abs(v1_tray_count.iloc[0, 0] - v2_tray_count.iloc[0, 0]) <= 1 and v1_tray_count.iloc[0, 0] > 0


def get_retest_formation_data(lot: str, db: dict, cold_db: bool, yymm_list: list[str]) -> pd.DataFrame():
    """
    Input: lot number, database credentials, cold database boolean, and table month list
    Function: queries database for retest formation data
    Output: retest formation data
            index: [Default]
            columns: [TRAY_NO, TRAY_POSITION, RETEST_V1, METER, V1_MEASURE_DATE, V1_MACHINE, CELL_MODEL, CELL_ID,
                      RETEST_V2, V2_MEASURE_DATE, V2_MACHINE, LINE_NO, DEFECT_REASON_CD]
    """
    print('get retest formation data')
    formation_data = pd.DataFrame()
    user = db['mes_user']
    pw = db['mes_pw']
    dsn = db['mes_dsn']
    if cold_db:
        user = db['cold_user']
        pw = db['cold_pw']
        dsn = db['cold_dsn']
    v1_data = pd.DataFrame()
    v2_data = pd.DataFrame()
    with cx_Oracle.connect(user=user, password=pw, dsn=dsn) as con:
        for yymm in yymm_list:
            # may not need tray no, tray position
            v1_data = pd.concat([v1_data,pd.read_sql(f"""
            SELECT CELL_ID, TRAY_NO, TRAY_POSITION, DATA_01 AS RETEST_V1, MOD(SUBSTR(TRAY_POSITION, 2, 2), 4) AS METER,
            MEASURE_DATE AS V1_MEASURE_DATE, EQUIP_NO AS V1_MACHINE,(CASE
            WHEN SUBSTR(CELL_ID, 16, 1) = '1' THEN 'BR'
            WHEN SUBSTR(CELL_ID, 16, 1) = '2' THEN 'AR'
            WHEN SUBSTR(CELL_ID, 16, 1) = '4' THEN 'DR'
            WHEN SUBSTR(CELL_ID, 16, 1) = '5' THEN 'C2'
            WHEN SUBSTR(CELL_ID, 16, 1) = '6' THEN 'L1'
            WHEN SUBSTR(CELL_ID, 16, 1) = 'D' THEN 'LA'
            END) AS CELL_MODEL
            FROM GEIS.T_CELL_ENG_GC12_{yymm}
            WHERE LOT_NO = '{lot}'
            AND CELL_ID LIKE 'G%'
            """, con)])
            v2_data = pd.concat([v2_data, pd.read_sql(f"""
            SELECT CELL_ID, DATA_02 AS RETEST_V2, MEASURE_DATE AS V2_MEASURE_DATE,
            EQUIP_NO AS V2_MACHINE, LINE_NO, DEFECT_REASON_CD
            FROM GEIS.T_CELL_ENG_GC13_{yymm}
            WHERE LOT_NO = '{lot}'
            AND CELL_ID LIKE 'G%'
            """, con)])
        dv_data = pd.merge(v1_data, v2_data, on='CELL_ID', how='inner')
        formation_data = pd.concat([formation_data, dv_data], ignore_index=True)
    formation_data = formation_data.sort_values(by='V2_MEASURE_DATE', ascending=False, ignore_index=True)
    formation_data = formation_data.drop_duplicates(subset='CELL_ID')
    return formation_data


def get_cell_ids(lot: str, db: dict, cold_db: bool, yymm_list: list[str]) -> pd.DataFrame():
    """
    Input: lot number, database credentials, cold database boolean, and table month list
    Function: queries V2 tables for cell IDs in retest lot
    Output: dataframe of cell IDs in the given retest lot
            index: [Default]
            columns: [CELL_ID]
    """
    print('get cell ids')
    user = db['mes_user']
    pw = db['mes_pw']
    dsn = db['mes_dsn']
    if cold_db:
        user = db['cold_user']
        pw = db['cold_pw']
        dsn = db['cold_dsn']
    v2_tables = ' UNION ALL '.join([f'(SELECT CELL_ID, LOT_NO FROM GEIS.T_CELL_ENG_GC13_{yymm})' for yymm in yymm_list])
    with cx_Oracle.connect(user=user, password=pw, dsn=dsn) as con:
        ids = pd.read_sql(f"""
        SELECT CELL_ID
        FROM ({v2_tables})
        WHERE LOT_NO = '{lot}' AND CELL_ID LIKE 'G%'
        """, con)
    return ids


def check_assembly_date(cell_list: pd.DataFrame()) -> (list, bool):
    """
    Input: cell list for a given retest lot
    Function: determine assembly year & month from cell ID characters
    Output: list of assembly dates and boolean indicator if cold db is required
    """
    print('check assembly date')
    cell_list['yymm'] = cell_list.apply(lambda x: cell_id_dict[x.CELL_ID[4]] + cell_id_dict[x.CELL_ID[5]], axis=1)
    month_check_list = []
    for date in cell_list.yymm.unique():
        # condition checks return True if assembly date is older than 12 months, False otherwise
        con1 = date[:2] == str(datetime.now().year)[-2:]
        con2 = (int(date[:2]) == int(str(datetime.now().year)[-2:]) - 1) and (int(date[-2:]) > datetime.now().month)
        month_check_list.append(not(con1 or con2))
    return cell_list.yymm.unique(), any(month_check_list)


def cold_pool(query: str) -> pd.DataFrame():
    return pd.read_sql(query, cx_Oracle.SessionPool(
        'kwang', 'XV7A6GneRC2a', '10.133.200.174:1521/colddb.america.gds.panasonic.com', 1, 12, 1).acquire())


def mes_pool(query: str) -> pd.DataFrame():
    return pd.read_sql(query, cx_Oracle.SessionPool(
        'PENAENG', 'p6bru@aXE=am', '10.133.200.175/GEISDB.WORLD', 1, 12, 1).acquire())


def get_initial_cell_data(lot: str, month_list: list, cold_db: bool, cell_list: pd.DataFrame()) -> pd.DataFrame():
    """
    Input: lot number, assembly month list, cold database boolean, and retest lot cell list
    Function: Queries database for initial measure date
    Output: Retest cell IDs and their initial measure date
            index: [Default]
            columns: [CELL_ID, INITIAL_MEASURE_DATE]
    """
    print('get initial cell data')
    print('cell list - unique/total', len(cell_list.CELL_ID.unique()), len(cell_list))
    mes_queries = []
    cold_queries = []
    mes_data = pd.DataFrame()
    cold_data = pd.DataFrame()
    month_list = [_ for _ in month_list if _ in mm_ref]
    for yymm in month_list:
        usable_cells = cell_list[cell_list.yymm == yymm]
        cell_tuples = [tuple(usable_cells.CELL_ID[i * 999:(i + 1) * 999]) for i in
                       range(((len(usable_cells) // 999) + 1))]
        mes_db = datetime(int('20' + yymm[0:2]), int(yymm[2:4]), 1) > (datetime.now() - relativedelta(months=11))
        for month in [yymm, mm_ref[min(mm_ref.index(yymm) + 1, len(mm_ref)-1)]]:
            for cell_tuple in cell_tuples:
                if len(cell_tuple) == 1:
                    cell_tuple = f"('{cell_tuple[0]}')"
                if mes_db:
                    mes_queries.append(f"""
                    SELECT CELL_ID, LOT_NO, (CASE
                    WHEN MOD(SUBSTR(TRAY_POSITION,2,2),4) = 1 THEN (DATA_03 - DATA_08)*1000
                    WHEN MOD(SUBSTR(TRAY_POSITION,2,2),4) = 2 THEN (DATA_03 - DATA_16)*1000
                    WHEN MOD(SUBSTR(TRAY_POSITION,2,2),4) = 3 THEN (DATA_03 - DATA_24)*1000
                    WHEN MOD(SUBSTR(TRAY_POSITION,2,2),4) = 0 THEN (DATA_03 - DATA_32)*1000
                    ELSE NULL END) AS NORM_DV
                    FROM GEIS.T_CELL_ENG_GC13_{month}
                    WHERE CELL_ID IN {cell_tuple}
                    """)
                else:
                    cold_queries.append(f"""
                    SELECT CELL_ID, LOT_NO, (CASE
                    WHEN MOD(SUBSTR(TRAY_POSITION,2,2),4) = 1 THEN (DATA_03 - DATA_08)*1000
                    WHEN MOD(SUBSTR(TRAY_POSITION,2,2),4) = 2 THEN (DATA_03 - DATA_16)*1000
                    WHEN MOD(SUBSTR(TRAY_POSITION,2,2),4) = 3 THEN (DATA_03 - DATA_24)*1000
                    WHEN MOD(SUBSTR(TRAY_POSITION,2,2),4) = 0 THEN (DATA_03 - DATA_32)*1000
                    ELSE NULL END) AS NORM_DV
                    FROM GEIS.T_CELL_ENG_GC13_{month}
                    WHERE CELL_ID IN {cell_tuple}
                    """)
    print('starting mes pools', len(mes_queries), datetime.now())
    if len(mes_queries) != 0:
        pool1 = mp.Pool(processes=12)
        mes_data = pool1.map(mes_pool, mes_queries)
        pool1.close()
        pool1.join()
        mes_data = pd.concat(mes_data, axis=0)
    print('starting cold pools', len(cold_queries), datetime.now())
    if len(cold_queries) != 0:
        pool2 = mp.Pool(processes=12)
        cold_data = pd.DataFrame()
        for query in tqdm.tqdm(pool2.map(cold_pool, cold_queries), total=len(cold_queries)):
            cold_data = pd.concat([cold_data, query])
        print(cold_data)
        pool2.close()
        pool2.join()
        print('complete cold pools', len(cold_queries), datetime.now())
    data = pd.concat([mes_data, cold_data], ignore_index=True)
    data = data[data.apply(lambda x: x.LOT_NO[6:8] != 'HZ', axis=1)]
    data = data[['CELL_ID', 'NORM_DV']]
    return data


def compile_data(lot: str, db: dict, cold_db: bool, cell_data: pd.DataFrame()) -> pd.DataFrame():
    """
    Input: retest lot cell list, lot number, cold_db boolean indicator, and queried cell data
    Function: Calculate cell sitting time, assign voltage cutoff, assign dV cutoff, calculate meter offsets,
              calculate adjusted dV values, assign process NG codes, get impingement cells, and merge all data
    Output: Retest cell data
            index: [Default]
            columns: [CELL_ID, TRAY_NO, TRAY_POSITION, RETEST_V1, METER, V1_MEASURE_DATE, V1_MACHINE, CELL_MODEL,
                      RETEST_V2, V2_MEASURE_DATE, V2_MACHINE, LINE_NO, DEFECT_REASON_CD, INITIAL_MEASURE_DATE,
                      SITTING_TIME, VOLTAGE_CUTOFF, DV_CUTOFF, OFFSET, DT, DVDT, DEFECT_CODE, PROCESS_NG, yymm,
                      SEPARATOR, AVG_JRD, IMPINGEMENT_NG]

    """
    print('compile data')
    cell_data = cell_data.astype({'V1_MEASURE_DATE': 'datetime64[ns]',
                                  'V2_MEASURE_DATE': 'datetime64[ns]'})
    cell_data['NORM_DV'] = cell_data.NORM_DV.fillna(-999.99)
    cell_data['VOLTAGE_CUTOFF'] = volt_spec
    cell_data['INITIAL_MEASURE_DATE'] = cell_data.apply(lambda x: datetime(int('20' + cell_id_dict[x.CELL_ID[4]]),
                                                                           int(cell_id_dict[x.CELL_ID[5]]),
                                                                           int(cell_id_dict[x.CELL_ID[6]])), axis=1)
    cell_data['SITTING_TIME'] = (cell_data.V1_MEASURE_DATE - cell_data.INITIAL_MEASURE_DATE).dt.total_seconds() / 86400
    cell_data['DV_CUTOFF'] = dv_coefficient * (1 / cell_data.SITTING_TIME) + dv_offset
    offsets = getOffsets(cell_data)
    cell_data = cell_data.merge(offsets, on=['V1_MACHINE', 'V2_MACHINE', 'METER'])
    cell_data['DT'] = (cell_data.V2_MEASURE_DATE - cell_data.V1_MEASURE_DATE) / pd.Timedelta(days=1)
    cell_data['DVDT'] = (cell_data.RETEST_V2 - cell_data.RETEST_V1 + cell_data.OFFSET) / cell_data.DT

    cell_data['DEFECT_CODE'] = cell_data.apply(lambda x: '900W' if (x.DVDT > 0.00005 or x.DVDT < x.DV_CUTOFF
                                                                    or x.CELL_MODEL != model_number
                                                                    or x.SITTING_TIME == 0)
                                               else '400V' if x.RETEST_V1 < x.VOLTAGE_CUTOFF
                                               else '500W' if x.RETEST_V2 < x.VOLTAGE_CUTOFF
                                               else 'N000', axis=1)
    cell_data['PROCESS_NG'] = cell_data.apply(lambda x: x.DEFECT_CODE in ['900W', '400V', '500W'], axis=1)
    # dQ Model
    cell_data['DQDV'] = np.interp(x=cell_data.RETEST_V2, xp=dq_ref.Voltage, fp=dq_ref.dQdV, left=1e-9, right=1e-9)
    cell_data['R_SHORT'] = (24000 * cell_data.RETEST_V2) / (-cell_data.DVDT * cell_data.DQDV)
    cell_data['DQ_CODE'] = cell_data.apply(lambda x: '900Z' if (x.RETEST_V1 < volt_spec) | (x.RETEST_V2 < volt_spec)
                                           else '900Y' if x.NORM_DV < ndv_spec
                                           else '900X' if x.CELL_MODEL != model_number
                                           else '900W' if x.R_SHORT < rshort_spec or x.DVDT > 0.00005
                                           else 'N000', axis=1)
    cell_data['DQ_NG'] = cell_data.apply(lambda x: x.DQ_CODE in ['900W', '900Y', '900X', '900U', '900Z'], axis=1)
    impingement_cells = get_impingement_cells(db_info, cell_data[['CELL_ID']])
    impingement_cells['IMPINGEMENT_NG'] = True
    cell_data = cell_data.merge(impingement_cells, on='CELL_ID', how='left')
    return cell_data


def generate_pchart(data: pd.DataFrame()):
    summary = pd.DataFrame(data={
        'Missing Cell Data': len(data[data.SITTING_TIME == 0]),
        'Cell Age Min': data[data.SITTING_TIME > 0].SITTING_TIME.min(),
        'Cell Age Max': data.SITTING_TIME.max(),
        'dV Model OK': len(data[~data.PROCESS_NG]),
        'dV Model Voltage NGs': len(data[(data.RETEST_V1 < volt_spec) | (data.RETEST_V2 < volt_spec)]),
        'dV Model dV NGs': len(data[data.DVDT < data.DV_CUTOFF]),
        'dV Model Total NGs': len(data[data.PROCESS_NG]),
        'dQ Model OK': len(data[~data.DQ_NG]),
        'dQ Model NG': len(data[data.DQ_NG]),
        'Total Cells': len(data),
        'dV Model Yield': 100 * len(data[~data.PROCESS_NG]) / len(data),
        'dQ Model Yield': 100 * len(data[~data.DQ_NG]) / len(data)},
        index=[lot_number])
    summary.index.name = 'Lot'
    if os.path.exists(rf'{DIRECTORY}\dq_summary.csv'):
        summary = pd.concat([pd.read_csv(rf'{DIRECTORY}\dq_summary.csv', index_col='Lot'), summary])
        summary = summary[~summary.index.duplicated(keep='last')]
    summary.to_csv(rf'{DIRECTORY}\dq_summary.csv')
    # dv_pchart = pd.DataFrame(data={
    #     'V1_LOT_NO': lot_number,
    #     'V1_LOT_CLASSIFICATION': 10,
    #     'V1_LINE_NO': data.LINE_NO.unique(),
    #     'V1_INSPECTION_DATE': data.V1_MEASURE_DATE.max().strftime('%Y-%m-%d'),
    #     'V1_OK': len(data[data.DEFECT_CODE != '400V']),
    #     'V1_NG': len(data[data.DEFECT_CODE == '400V']),
    #     'V1_OTHER_NG': 0,
    #     'V1_VISUAL_NG': 0,
    #     'V1_ELECTROLYTE_LEAK': 0,
    #     'V2_LOT_NO': lot_number,
    #     'V2_LOT_CLASSIFICATION': 10,
    #     'V2_LINE_NO': data.LINE_NO.unique(),
    #     'V2_INSPECTION_DATE': data.V2_MEASURE_DATE.max().strftime('%Y-%m-%d'),
    #     'V2_OK': len(data[data.DEFECT_CODE == 'N000']),
    #     'V2_NG': len(data[data.DEFECT_CODE == '500W']),
    #     'DV_NG': len(data[data.DEFECT_CODE == '900W']),
    #     'V2_OTHER_NG': 0,
    #     'V2_VISUAL_NG': 0,
    #     'V2_ELECTROLYTE_LEAK': 0,
    #     'V2_NG_SUM': len(data[data.DEFECT_CODE != 'N000']),
    #     'V2_NG_RATE (%)': f"{100 * (len(data[data.DEFECT_CODE != 'N000']) / len(data)):.2f}",
    #     'V2_ALARM_THRESHOLD': 0.32,
    #     'V2_ABNORMAL_QUALITY': 0.4,
    #     'RE_DV_INSPECTION': 0.4,
    #     'OCV_JUDGMENT': 13
    #     })
    dq_pchart = pd.DataFrame(data={
        'V1_LOT_NO': lot_number,
        'V1_LOT_CLASSIFICATION': 10,
        'V1_LINE_NO': data.LINE_NO.unique(),
        'V1_INSPECTION_DATE': data.V1_MEASURE_DATE.max().strftime('%Y-%m-%d'),
        'V1_OK': len(data),
        'V1_NG': 0,
        'V1_OTHER_NG': 0,
        'V1_VISUAL_NG': 0,
        'V1_ELECTROLYTE_LEAK': 0,
        'V2_LOT_NO': lot_number,
        'V2_LOT_CLASSIFICATION': 10,
        'V2_LINE_NO': data.LINE_NO.unique(),
        'V2_INSPECTION_DATE': data.V2_MEASURE_DATE.max().strftime('%Y-%m-%d'),
        'V2_OK': len(data[data.DQ_CODE == 'N000']),
        'V2_NG': len(data[data.DQ_CODE == '900Z']),
        'DV_NG': len(data[(data.DQ_CODE == '900W') | (data.DQ_CODE == '900Y')]),
        'V2_OTHER_NG': 0,
        'V2_VISUAL_NG': 0,
        'V2_ELECTROLYTE_LEAK': 0,
        'V2_NG_SUM': len(data[data.DQ_CODE != 'N000']),
        'V2_NG_RATE (%)': f"{100 * (len(data[data.DQ_CODE != 'N000']) / len(data)):.2f}",
        'V2_ALARM_THRESHOLD': 0.32,
        'V2_ABNORMAL_QUALITY': 0.4,
        'RE_DV_INSPECTION': 0.4,
        'OCV_JUDGMENT': 13
        })
    # dv_pchart.to_csv(rf'{DIRECTORY}\{lot_number}\{lot_number}_dv_pchart.csv', index=False)
    dq_pchart.to_csv(rf'{DIRECTORY}\{lot_number}\{lot_number}_dq_pchart.csv', index=False)
    print(f'Script Start Time: {start_time}')
    print(f'Script Completion Time: {datetime.now()}')


def scan_formation_complete(user: str, pw: str, dsn: str) -> pd.DataFrame():
    with cx_Oracle.connect(user=user, password=pw, dsn=dsn) as con:
        lot_data = pd.read_sql(f"""
        SELECT
            t1.LOT_NO,
            t1.V1_TRAY_CT,
            t2.V2_TRAY_CT
        FROM (
            SELECT 
                LOT_NO,
                COUNT(UNIQUE(TRAY_NO)) AS V1_TRAY_CT
            FROM GEIS.V_CELL_ENG_GC12_EN
            WHERE MEASURE_DATE > SYSDATE - 30
            AND LOT_NO LIKE '%HZ%'
            GROUP BY LOT_NO
        ) t1
        INNER JOIN (
            SELECT
                LOT_NO, COUNT(UNIQUE(TRAY_NO)) AS V2_TRAY_CT
            FROM GEIS.V_CELL_ENG_GC13_EN
            WHERE MEASURE_DATE > SYSDATE - 30
            AND LOT_NO LIKE '%HZ%'
            GROUP BY LOT_NO
        ) t2
        ON t1.LOT_NO = t2.LOT_NO
        """, con)
        lot_data['DONE'] = lot_data.V1_TRAY_CT - lot_data.V2_TRAY_CT <= 1
        return lot_data


if __name__ == '__main__':
    pd.set_option('display.width', 200, 'display.max_columns', 10)
    config = configparser.ConfigParser()
    config.read(r'C:\Users\KW38770\Documents\WangK\Scripts\gitlab\db_cfg.ini')
    # you will have to input your own db access info
    db_info = {'mes_user': config['MES_DB']['user'],
               'mes_pw': config['MES_DB']['password'],
               'mes_dsn': config['MES_DB']['dsn'],
               'cold_user': config['COLD_DB']['user'],
               'cold_pw': config['COLD_DB']['password'],
               'cold_dsn': config['COLD_DB']['dsn']
               }
    cell_id_dict = {'1': '01', '2': '02', '3': '03', '4': '04', '5': '05', '6': '06', '7': '07', '8': '08', '9': '09',
                    'A': '10', 'B': '11', 'C': '12', 'D': '13', 'E': '14', 'F': '15', 'G': '16', 'H': '17', 'J': '18',
                    'K': '19', 'L': '20', 'M': '21', 'N': '22', 'P': '23', 'Q': '24', 'R': '25', 'S': '26', 'T': '27',
                    'U': '28', 'V': '29', 'W': '30', 'X': '31', 'Y': '32', 'Z': '33'}
    db_mon = 12*(datetime.now().year - datetime(2020, 8, 1).year) + (datetime.now().month - datetime(2020, 8, 1).month)
    mm_ref = [(datetime(2020, 8, 1) + relativedelta(months=x)).strftime('%y%m') for x in range(db_mon)]
    volt_spec = 2.5
    ndv_spec = -0.5
    rshort_spec = 89e3
    dv_offset = -0.00009763
    dv_coefficient = -0.00303104
    dq_ref = pd.read_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\21L dqdv\dq_curve.csv')
    DIRECTORY = r'C:\Users\KW38770\Documents\WangK\Formation\Retest\Processing_v3'
    model_number = 'L1'
    results = []
    # test
    # valid_lots = scan_formation_complete(db_info['mes_user'], db_info['mes_pw'], db_info['mes_dsn'])
    # print(valid_lots)
    # exit()
    # end test
    for lot_number in [
        # '220806HZ1',
        '211106HZ1',
        # '211108HZ1', issue w/ Z data - they will rerun through retest
        # '220422HZ1', flagged, pchart uploaded
        # '211024HZ1', flagged, pchart uploaded - need additional 577 uploads
        # '220502HZ1', already flagged, pchart uploaded
        # '220505HZ1', already flagged, pchart uploaded
        # '220512HZ1', already flagged, pchart uploaded
        # '220519HZ1', already flagged, pchart uploaded
        # '220525HZ1', already flagged, pchart uploaded
        # '220527HZ1', already flagged, pchart uploaded - need additional 7 uploads
        # '220615HZ1', already flagged, pchart uploaded
        # '211025HZ1', # couldn't create offset
        # '210219HZ2', flagged, pchart uploaded
        # '210326HZ1', flagged, pchart uploaded
        # '220522HZ1', already flagged, pchart uploaded
        # '210326HZ2', already flagged, pchart uploaded - need 2114 additional uploads
        # '210517HZ1', flagged, pchart uploaded - need 11923 additional uploads
        # '220104HZ1', flagged, pchart uploaded - all extractions needed
        # '210211HZ2',  # can't create offset

        ]:
        if os.path.isdir(rf'{DIRECTORY}\{lot_number}'):
            print(f'{lot_number} already ran through flagging')
            # exit()
        start_time = datetime.now()
        print(lot_number, start_time)
        if not (lot_number[6:8] == 'HZ' and len(lot_number) == 9):
            print('log error: invalid retest lot number entered')
            exit()
        f_date = get_retest_date(lot_number, db_info)
        if len(f_date) == 0:
            print('log error: lot has not finished retest')
            exit()
        # check if formation data is older than 12 months -> cold db required
        f_cold_db = any(date not in
                        [(datetime.now() - relativedelta(months=x)).strftime('%y%m') for x in range(12)] for date in f_date)
        if not check_retest_complete(lot_number, db_info, f_cold_db, f_date):
            print('log error: formation process not complete')
            exit()
        retest_data = get_retest_formation_data(lot_number, db_info, f_cold_db, f_date)
        cell_ids = get_cell_ids(lot_number, db_info, f_cold_db, f_date)
        as_date, as_cold_db = check_assembly_date(cell_ids)
        initial_cell_data = get_initial_cell_data(lot_number, as_date, as_cold_db, cell_ids)
        retest_data = pd.merge(retest_data, initial_cell_data, on='CELL_ID', how='left')
        retest_data = compile_data(lot_number, db_info, as_cold_db, retest_data)
        print(retest_data)
        retest_data['IMPINGEMENT_NG'] = retest_data.IMPINGEMENT_NG.fillna(False)
        # dv_z301 = retest_data[retest_data.PROCESS_NG | retest_data.IMPINGEMENT_NG].CELL_ID
        dq_z301 = retest_data[retest_data.DQ_NG | retest_data.IMPINGEMENT_NG].CELL_ID
        # data saving
        if not os.path.isdir(rf'{DIRECTORY}\{lot_number}'):
            os.mkdir(rf'{DIRECTORY}\{lot_number}')
        # dv_z301.to_csv(rf'{DIRECTORY}\{lot_number}\{lot_number}_dv_Z301_cells.csv', index=False, header=None)
        dq_z301.to_csv(rf'{DIRECTORY}\{lot_number}\{lot_number}_dq_Z301_cells.csv', index=False, header=None)
        retest_data.to_csv(rf'{DIRECTORY}\{lot_number}\{lot_number}_retest_data.csv', index=False)
        generate_pchart(retest_data)
