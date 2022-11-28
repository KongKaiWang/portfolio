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
import configparser
from datetime import datetime


def get_cell_ids(lot: str, cfg: dict) -> pd.DataFrame():
    """
    Input: retest lot number and database configuration settings
    Function: queries V_TRAY_INFO for active cells in given retest lot
    Output: cell IDs in the given retest lot
            index: [Default]
            columns: [CELL_ID]
    """
    with cx_Oracle.connect(user=cfg['mes_user'], password=cfg['mes_pw'], dsn=cfg['mes_dsn']) as con:
        cell_ids = pd.read_sql(f"""
        SELECT CELL_ID
        FROM GEIS.V_TRAY_INFO
        WHERE LOT_NO = '{lot}'
        AND CELL_ID LIKE 'G%'""", con)
    print(f'get cell ids done: {len(cell_ids)}')
    return cell_ids


def check_assembly_date(cell_list: pd.DataFrame()) -> (list, bool):
    """
    Input: cell list for a given retest lot
    Function: determine assembly year & month from cell ID characters
    Output: list of assembly dates and boolean indicator if cold db is required
    """
    cell_id_dict = {'1': '01', '2': '02', '3': '03', '4': '04', '5': '05', '6': '06', '7': '07', '8': '08', '9': '09',
                    'A': '10', 'B': '11', 'C': '12', 'D': '13', 'E': '14', 'F': '15', 'G': '16', 'H': '17', 'J': '18',
                    'K': '19', 'L': '20', 'M': '21', 'N': '22', 'P': '23', 'Q': '24', 'R': '25', 'S': '26', 'T': '27',
                    'U': '28', 'V': '29', 'W': '30', 'X': '31', 'Y': '32', 'Z': '33'}
    cell_list['yymm'] = cell_list.apply(lambda x: cell_id_dict[x.CELL_ID[4]] + cell_id_dict[x.CELL_ID[5]], axis=1)
    month_check_list = []
    for date in cell_list.yymm.unique():
        # condition checks return True if assembly date is older than 12 months, False otherwise
        con1 = date[:2] == str(datetime.now().year)[-2:]
        con2 = (int(date[:2]) == int(str(datetime.now().year)[-2:]) - 1) and (int(date[-2:]) > datetime.now().month)
        month_check_list.append(not(con1 or con2))
    return cell_list.yymm.unique(), any(month_check_list)


def get_assembly_data(user: str,
                      pw: str,
                      dsn: str,
                      yymm_list: list[str],
                      cell_list: pd.DataFrame()) -> pd.DataFrame():
    """
    Input: database configuration settings, list of assembly year & month, and list of cell ids
    Function: query database for jrd & separator data
    Output: assembly cell data
            index: [Default]
            column: [CELL_ID, SEPARATOR, AVG_JRD]
    """
    separator_dict = {'G': 'S4', 'M': 'S5', 'Q': 'S6', 'P': 'S7'}
    cell_tuples = [tuple(cell_list.CELL_ID[i*999:(i+1)*999]) for i in range(((len(cell_list) // 999) + 1))]
    assembly_data = pd.DataFrame()
    with cx_Oracle.connect(user=user, password=pw, dsn=dsn) as con:
        for yymm in yymm_list:
            for i, cell_tuple in enumerate(cell_tuples):
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


def get_chargeback_table(lot: str, cfg: dict, cell_list: str) -> pd.DataFrame():
    # may not be needed after total summary table is generated?
    """
    Input: retest lot number and database configuration settings
    Function: generate data summary table of cell count by separator & JRD size
    Output: chargeback summary table
            index: [SEPARATOR]
            column: [SMALL, LARGE]
    """
    cell_list = get_cell_ids(lot, cfg)
    yymm_list, cold_db_bool = check_assembly_date(cell_list)
    user = cfg['cold_user'] if cold_db_bool else cfg['mes_user']
    pw = cfg['cold_pw'] if cold_db_bool else cfg['mes_pw']
    dsn = cfg['cold_dsn'] if cold_db_bool else cfg['mes_dsn']
    assembly_data = get_assembly_data(user, pw, dsn, yymm_list, cell_list)
    # cells with missing data will populate with NG data for extraction
    assembly_data = assembly_data.fillna(value={'SEPARATOR': 'S4', 'AVG_JRD': 20.11})
    assembly_data['JRD_SIZE'] = assembly_data.apply(lambda x: 'LARGE' if x.AVG_JRD > 20.10 else 'SMALL', axis=1)
    chargeback_table = assembly_data.groupby(by=['SEPARATOR', 'JRD_SIZE']).count().unstack().CELL_ID
    return chargeback_table


def get_impingement_cells(lot: str, cfg: dict, cell_list: pd.DataFrame()) -> pd.DataFrame():
    """
    Input: retest lot number and database configuration settings
    Function: generate impingement cell list
    Output: Z301 cell list
            index: [Default]
            column: [CELL_ID]
    """
    # cell_list = get_cell_ids(lot, cfg)
    yymm_list, cold_db_bool = check_assembly_date(cell_list)
    user = cfg['cold_user'] if cold_db_bool else cfg['mes_user']
    pw = cfg['cold_pw'] if cold_db_bool else cfg['mes_pw']
    dsn = cfg['cold_dsn'] if cold_db_bool else cfg['mes_dsn']
    assembly_data = get_assembly_data(user, pw, dsn, yymm_list, cell_list)
    # assembly_data.to_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\Processing\{lot}\assembly_data.csv')
    # assembly_data.to_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\v2.0 Development\Script Validation\assembly_data.csv')
    impingement_cells = assembly_data[(assembly_data.SEPARATOR == 'S4') | (assembly_data.AVG_JRD > 20.10)]
    # impingement_cells.to_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\Processing\{lot}\impingement.csv')
    # impingement_cells.to_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\v2.0 Development\Script Validation\impingement.csv')
    return impingement_cells


# if __name__ == '__main__':
#     config = configparser.ConfigParser()
#     config.read(r'C:\Users\KW38770\Documents\WangK\Scripts\db_cfg.ini')
#     # you will have to input your own db access info
#     db_info = {'mes_user': config['MES_DB']['user'],
#                'mes_pw': config['MES_DB']['password'],
#                'mes_dsn': config['MES_DB']['dsn'],
#                'cold_user': config['COLD_DB']['user'],
#                'cold_pw': config['COLD_DB']['password'],
#                'cold_dsn': config['COLD_DB']['dsn']
#                }
#     lot_number = '220322HZ1'
    # get_impingement_cells(lot_number, db_info)
