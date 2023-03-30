"""
Flagging v2.0

This program is designed to report retest loss cost

Authored by: Kong Wang
Date: 01/04/2023

Notes:

Version Updates:
v1.0 - Initial release (01/04/23)
"""

import pandas as pd
import cx_Oracle
import os

from datetime import datetime


def get_retest_yield(start: str, end: str) -> pd.DataFrame():
    """
    Input: Start and end date for query
    Function: Queries MES for processed retest lots within specified date range
    Output: Retest lot yield data
            index: [LOT_NO]
            columns: [PROCESS_DATE, NG, OK, TOTAL, LOSS]
    """
    with cx_Oracle.connect(user='PENAENG', password='p6bru@aXE=am', dsn='10.133.200.175/GEISDB.WORLD') as con:
        print('Running SQL query for defect data...')
        defect_data = pd.read_sql(f"""
        SELECT DISTINCT LOT_NO, DEFECT_REASON_CD, DEFECT_QTY AS NG, TRUNC(CREATE_DATE, 'DDD') AS PROCESS_DATE
        FROM GEIS.T_PROD_DEFECT
        WHERE PROCESS_GROUP_NO = 'CH'
        AND LOT_NO LIKE '%HZ%'
        AND DEFECT_REASON_CD IN ('X301', 'X401', 'X402')
        AND CREATE_DATE BETWEEN TO_DATE('{start}', 'MM/DD/YYYY') AND TO_DATE('{end}', 'MM/DD/YYYY')
        ORDER BY LOT_NO, TRUNC(CREATE_DATE, 'DDD')
        """, con)
        defect_ng = defect_data.drop_duplicates(subset=['LOT_NO', 'DEFECT_REASON_CD', 'NG'], keep='last').reset_index(drop=True).groupby('LOT_NO').sum()
        defect_date = defect_data.drop_duplicates(subset=['LOT_NO'], keep='last')[['LOT_NO', 'PROCESS_DATE']].set_index('LOT_NO')
        defect_data = pd.concat([defect_date, defect_ng], axis=1)
        print('Running SQL query for production data...')
        prod_data = pd.read_sql(f"""
        SELECT DISTINCT LOT_NO, PROD_QTY AS OK
        FROM GEIS.T_PROD_RESULT
        WHERE LOT_NO IN {tuple(defect_data.index)}
        AND PROCESS_GROUP_NO = 'CH'
        AND SAP_MOVE_TYPE = '101'""", con, index_col='LOT_NO')
        yield_data = pd.concat([defect_data, prod_data], axis=1)
        yield_data['TOTAL'] = yield_data.NG + yield_data.OK
        yield_data['LOSS'] = round(yield_data.NG/yield_data.TOTAL, 3)
        avg_loss = yield_data.NG.sum()/yield_data.TOTAL.sum()
        lot_count = len(yield_data)
        yield_data.loc[f'{lot_count} lots processed'] = None
        yield_data.loc[f'{avg_loss:.3f} average loss'] = None
    return yield_data


if __name__ == '__main__':
    start_date = input('Please enter start date (format: mm/dd/yyyy)')
    end_date = input('Please enter end date (format: mm/dd/yyyy)')
    retest_yield = get_retest_yield(start_date, end_date)
    print(f'Saving data file to local folder: {os.getcwd()}')
    retest_yield.to_csv(f"{os.getcwd()}/retest_loss_cost_{datetime.now().strftime('%y%m%d')}.csv")
    print('Script complete.')
