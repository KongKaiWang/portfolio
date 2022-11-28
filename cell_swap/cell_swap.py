import pandas as pd
from datetime import datetime

if __name__ == '__main__':
    pd.set_option('display.width', 200, 'display.max_columns', 20)
    df = pd.read_csv('cell_ref.csv', parse_dates=['MEASURE_DATE'])
    print(df)
    exit()
    ng_cell_ids = df['CELL_ID'].unique()
    ok_cell_ids = []
    ok_tray_pos = []
    ng_tray_pos = []
    measure_date = []
    tray_no = []
    lot_no = []


    for cell_id in ng_cell_ids:  # grab corresponding parameters and ensure they're associated correctly
        ng_tray_pos.append(df[df['CELL_ID'] == cell_id]['TRAY_POSITION'].unique()[0])
        measure_date.append(df[df['CELL_ID'] == cell_id]['MEASURE_DATE'].unique()[0])
        tray_no.append(df[df['CELL_ID'] == cell_id]['TRAY_NO'].unique()[0])
        lot_no.append(df[df['CELL_ID'] == cell_id]['LOT_NO'].unique()[0])
    res = pd.DataFrame({'MEASURE_DATE': measure_date, 'NG_CELL_ID': ng_cell_ids, 'NG_TRAY_POS': ng_tray_pos,
                        'TRAY_NO': tray_no, 'LOT_NO': lot_no})
    res['WIRE_SWAP'] = res['NG_TRAY_POS'].apply(lambda x: 'AB' if x[0] in 'AB' else 'CD')  # Create column of tray prefix
    # Time & wire swap conditions
    res['AB_CON'] = (res['MEASURE_DATE'] > datetime(2022, 6, 13, 18, 0, 0)) & (
                    res['MEASURE_DATE'] < datetime(2022, 6, 20, 9, 40, 0)) & (
                    res['WIRE_SWAP'] == 'AB')
    res['CD_CON'] = (res['MEASURE_DATE'] > datetime(2022, 6, 19, 10, 30, 0)) & (
                    res['MEASURE_DATE'] < datetime(2022, 6, 20, 9, 40, 0)) & (
                    res['WIRE_SWAP'] == 'CD')
    res = res[res['AB_CON'] | res['CD_CON']].reset_index()  # eliminate columns that don't at least meet one condition
    for index, tray_pos in enumerate(res['NG_TRAY_POS']):
        ok_pos = tray_pos
        # Swap tray prefix based on time & wire swap conditions
        if res['AB_CON'][index]:
            if tray_pos[0] == 'A':
                ok_pos = 'B' + tray_pos[-2:]
            elif tray_pos[0] == 'B':
                ok_pos = 'A' + tray_pos[-2:]
        elif res['CD_CON'][index]:
            if tray_pos[0] == 'C':
                ok_pos = 'D' + tray_pos[-2:]
            elif tray_pos[0] == 'D':
                ok_pos = 'C' + tray_pos[-2:]
        ok_tray_pos.append(ok_pos)  # append corresponding cell positions
        ok_cell_reference = (df['TRAY_POSITION'] == tray_pos) & (
                             df['OK_TRAY_POSITION'] == ok_pos) & (
                             df['TRAY_NO'] == res['TRAY_NO'][index]) & (
                             df['LOT_NO'] == res['LOT_NO'][index])  # cross-reference criteria against original df
        if len(df[ok_cell_reference]['OK_CELL_ID']) == 0:  # account for missing cell case
            ok_cell_ids.append('Missing Cell')
        else:
            ok_cell_ids.append(df[ok_cell_reference]['OK_CELL_ID'].values[0])  # append corresponding cell ID if it exists
    res['OK_CELL_ID'] = ok_cell_ids
    res['OK_TRAY_POSITION'] = ok_tray_pos
