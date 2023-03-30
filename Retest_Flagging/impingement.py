"""
Impingement Criteria Module
This program is designed to identify high risk impingement cells in retest lots. Cells are considered high risk for
impingement if they have S4 separator type or average JRD greater than 20.10mm.

Authored by: Kong Wang
Date: 09/28/22

Notes:

Version Updates:
v1.0 -

"""

import pandas as pd
import cx_Oracle
from datetime import datetime
from dateutil.relativedelta import relativedelta


def get_assembly_data(db_info: dict, cell_list: pd.DataFrame()) -> pd.DataFrame():
    """
    Input: database configuration settings, list of assembly year & month, and list of cell ids
    Function: query database for jrd & separator data
    Output: assembly cell data
            index: [Default]
            column: [CELL_ID, SEPARATOR, AVG_JRD]
    """
    separator_dict = {'G': 'S4', 'M': 'S5', 'Q': 'S6', 'P': 'S7'}
    cell_id_dict = {'1': '01', '2': '02', '3': '03', '4': '04', '5': '05', '6': '06', '7': '07', '8': '08', '9': '09',
                    'A': '10', 'B': '11', 'C': '12', 'D': '13', 'E': '14', 'F': '15', 'G': '16', 'H': '17', 'J': '18',
                    'K': '19', 'L': '20', 'M': '21', 'N': '22', 'P': '23', 'Q': '24', 'R': '25', 'S': '26', 'T': '27',
                    'U': '28', 'V': '29', 'W': '30', 'X': '31', 'Y': '32', 'Z': '33'}
    cell_list['yymm'] = cell_list.apply(lambda x: cell_id_dict[x.CELL_ID[4]] + cell_id_dict[x.CELL_ID[5]], axis=1)
    assembly_data = pd.DataFrame()
    for yymm in cell_list.yymm.unique():
        usable_cells = cell_list[cell_list.yymm == yymm]
        cell_tuples = [tuple(usable_cells.CELL_ID[i * 999:(i + 1) * 999]) for i in range(((len(usable_cells) // 999) + 1))]
        cold_db = datetime(int('20' + yymm[0:2]), int(yymm[2:4]), 1) < (datetime.now() - relativedelta(months=11))
        user = db_info['mes_user']
        pw = db_info['mes_pw']
        dsn = db_info['mes_dsn']
        if cold_db:
            user = db_info['cold_user']
            pw = db_info['cold_pw']
            dsn = db_info['cold_dsn']
        with cx_Oracle.connect(user=user, password=pw, dsn=dsn) as con:
            for i, cell_tuple in enumerate(cell_tuples):
                if len(cell_tuple) == 1:
                    cell_tuple = f"('{cell_tuple[0]}')"
                print(f'{i}/{len(cell_tuples)} ({i*100/len(cell_tuples):.2f}%)', datetime.now())  # remove
                separator_data = pd.read_sql(f"""
                SELECT CELL_ID, S1_LOT_NO AS SEPARATOR
                FROM GEIS.T_CELL_ENG_GAWW00_{yymm}
                WHERE CELL_ID IN {cell_tuple}
                """, con)
                jrd_data = pd.read_sql(f"""
                SELECT CELL_ID, DATA_01 AS AVG_JRD FROM GEIS.T_CELL_ENG_GAWA38_{yymm}
                WHERE CELL_ID IN {cell_tuple}
                """, con)
                separator_data.SEPARATOR = separator_data.SEPARATOR.apply(lambda x: separator_dict[x[2]]
                                                                          if x[2] in separator_dict.keys() else 'Other')
                cell_data = pd.merge(separator_data, jrd_data, on='CELL_ID', how='outer')
                assembly_data = pd.concat([assembly_data, cell_data], ignore_index=True)
    # capture missing data rows with left join on original cell list
    assembly_data = pd.merge(cell_list, assembly_data, on='CELL_ID', how='left')
    return assembly_data


def get_impingement_cells(db_info: dict, cell_list: pd.DataFrame()) -> pd.DataFrame():
    """
    Input: retest lot number and database configuration settings
    Function: generate impingement cell list
    Output: Z301 cell list
            index: [Default]
            column: [CELL_ID]
    """
    assembly_data = get_assembly_data(db_info, cell_list)
    print(assembly_data, assembly_data.columns)
    impingement_cells = assembly_data[(assembly_data.SEPARATOR == 'S4') | (assembly_data.AVG_JRD > 20.10)]
    return impingement_cells
