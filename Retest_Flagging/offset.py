"""
Created on Wed Jul 15 11:32:51 2020
@author: CY17482
"""
#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import datetime as dt
import cx_Oracle


## Calculate Meter Offsets for that data
## Traditional DV Offset
def tryOffsets(day1, _first_month, _second_month, lines):
    """This function attempts to create an offset given specific information

    Parameters:
    day1 (datetime): The end day of the 2 day time period for offset calculation
    _first_month (str): the table code for the first table needed to pull MP data
    _second_month (str): the table code for the second table needed to pull MP data
    lines (str): a list of necessary lines in str(tuple) format to improve query speed

    Returns:
    Pandas Dataframe: Columns ['V1_MACHINE', 'V2_MACHINE', 'METER', 'OFFSET'] which states expected bias for each meter combination

    Notes:
    This fxn returns an empty dataframe in the case of any errors. These are intended to be caught with the validate(offset, cell_data) fxn in this module
    """
    #sets variables necessary to calculate offsets from inputs
    _end_day = str(day1)[0:10]
    _start_day = str(day1 - pd.Timedelta(days = 2))[0:10]
    sql = """SELECT MEDIAN(t2.DATA_02 - t1.DATA_01) AS MED_TRAY_METER_DV,
                t1.EQUIP_NO AS V1_MACHINE,
                t2.EQUIP_NO AS V2_MACHINE,
                MOD(SUBSTR(t1.TRAY_POSITION,2,2),4) AS METER,
                t1.TRAY_NO,
                t1.LOT_NO
            FROM (SELECT * FROM GEIS.T_CELL_ENG_GC12_{} UNION ALL SELECT * FROM GEIS.T_CELL_ENG_GC12_{}) t1
                INNER JOIN (SELECT * FROM GEIS.T_CELL_ENG_GC13_{} UNION ALL SELECT * FROM GEIS.T_CELL_ENG_GC13_{}) t2
                ON (t2.CELL_ID = t1.CELL_ID)
            WHERE t1.MEASURE_DATE BETWEEN TO_DATE('{}','YYYY-MM-DD') AND TO_DATE('{}','YYYY-MM-DD')
                AND t1.LINE_NO IN {}
                AND t2.LINE_NO IN {}
                AND SUBSTR(t1.LOT_NO,7,2) IN ('G1', 'G2', 'G3', 'G4', 'G5', 'G6', 'G7', 'G8', 'G9', 'GA', 'GB', 'GC', 'GD')
                AND SUBSTR(t1.LOT_NO,7,3) NOT IN ('GD9')
            GROUP BY t1.TRAY_NO, t1.EQUIP_NO, t2.EQUIP_NO, MOD(SUBSTR(t1.TRAY_POSITION,2,2),4), t1.LOT_NO
            HAVING MEDIAN(t2.DATA_02 - t1.DATA_01) BETWEEN -.5 AND .5
            """.format(_first_month, _second_month, _first_month, _second_month, _start_day, _end_day, lines,  lines)



    #Grab MP Data into df offset_data for processing

    con = cx_Oracle.connect(user='PENAENG', password='p6bru@aXE=am', dsn='10.133.200.175/GEISDB.WORLD')
    if _first_month not in [(datetime.now() - relativedelta(months=x)).strftime('%y%m') for x in range(12)]:
        con = cx_Oracle.connect(user='kwang', password='XV7A6GneRC2a', dsn='10.133.200.174:1521/colddb.america.gds.panasonic.com')

    print('MES Connection Established at {}'.format(str(datetime.now().time())[:-5]))
    offset_data = pd.read_sql_query(sql, con)
    con.close()
    print('MES Connection Ended at {}'.format(str(datetime.now().time())[:-5]))

    try:

        #Process Offset from MP data
        tray_medians = offset_data[['MED_TRAY_METER_DV', 'TRAY_NO']].groupby('TRAY_NO').median().reset_index().rename(columns={'MED_TRAY_METER_DV' : 'MED_TRAY_DV'})
        print('tray medians', tray_medians)
        offset_data = offset_data.merge(tray_medians, on='TRAY_NO')

        offset_data['METER_DIFF'] = offset_data['MED_TRAY_DV'] - offset_data['MED_TRAY_METER_DV']
        print('offset data', offset_data)
        offsets = offset_data.groupby(['METER', 'V1_MACHINE', 'V2_MACHINE']).mean()['METER_DIFF'].rename('OFFSET')
        offsets = offsets.to_frame().reset_index()

    except:
        print('Error with offset on ' + str(day1)[0:10])
        return pd.DataFrame(columns=['V1_MACHINE', 'V2_MACHINE', 'METER', 'OFFSET'])


    return offsets



def getOffsets(cell_data):
    """Creates a validated offset from the best availible data.

    Parameters:
    cell_data (pandas DataFrame): The Cell Data for which offsets are being created. Must include Columns = ['LINE_NO', 'V1_MEASURE_DATE', 'V2_MEASURE_DATE', 'V1_MACHINE', 'V2_MACHINE', 'METER']
        'LINE_NO' -->  contains the line on which each cell was run
        'V1_MEASURE_DATE' --> contains the time of retest V1 for each cell
        'V2_MEASURE_DATE' --> contains the time of retest V2 for each cell
        'V1_MACHINE' --> contains the V1 EQUIP_NO for each cell
        'V2_MACHINE' --> contains the V2 Equip_NO for each cell
        'METER' --> contains the location of the specfic meter used within a machine for each cell

    Returns:
    Pandas DataFrame: contains an accuracte expected bias for at least every meter combination within cell_data.
                               bias is stored in column 'OFFSET' corresponding to key values ['V1_MACHINE', 'V2_MACHINE', 'METER']
    """
    lines = str(tuple(cell_data['LINE_NO'].unique())).replace(',)', ')')


    #First Attempts to create an offset from 2 days of data before the median V1 measurement
    day1 = cell_data['V1_MEASURE_DATE'].mean()
    month1, month2 = getMonths(day1)
    offset = tryOffsets(day1, month1, month2, lines)
    if validate(offset, cell_data):
        return offset

    #if the default method doesn't result in effective offsets, using data between v1 and v2 is attempted
    else:
        day1 = day1 + pd.Timedelta(days = 3)
        month1, month2 = getMonths(day1)
        offset = tryOffsets(day1, month1, month2, lines)

        if validate(offset, cell_data):
            return offset

        #if
        else:
            for i in range(1,13):#this loop attempts to recalculate offsets from past data going back one day at a time.
                day1 = day1 - pd.Timedelta(days = 1)
                month1, month2 = getMonths(day1)
                offset = tryOffsets(day1, month1, month2, lines)
                if validate(offset, cell_data):
                    return offset
            #beyond 13 days back from the day1 value at the start of this loop, there is no guarentee that offset is more accurate than V1-V1, so an error is raised
            raise NameError('No Valid Offset Could be created. This meter may have been recently been there may not be any recent mass production data on this line')



def validate(offset, cell_data):
    """Runs checks to confirm that a given offset will reduce the variation due to meters when applied to cell_data

        Parameters:
        offset (pandas DataFrmae): conatins potential offsets to be checked; Columnns must include ['V1_MACHINE', 'V2_MACHINE', 'METER', 'OFFSET']
        cell_data (pandas DataFrame): contains cell_data which offsets must be confirmed to be effective with; Columns must include ['LINE_NO', 'V2_MEASURE_DATE', 'V1_MACHINE', 'V2_MACHINE', 'METER']

        Returns:
        Bool: If given DataFrame 'offset' will be effective in reducing variation due to meter bias when applied to the DataFrame 'cell_data'

        Notes:
        This function is intended to be called from the getOffsets functions.
        To that end, the parameters offset and cell_data can be retrieved from the output of tryOffsets and the parameter of getOffsets respectively without error.
    """
    ##OFFSET EXISTS CHECK
    if offset.size <= 0:
        return False
    print('One Check Passed')

    ##OFFSET SIZE CHECK
    if (offset.loc[offset['OFFSET'] > .000320].size > 0):
        return False
    print('Two Checks Passed')

    ##CELL_DATA PROCESSING
    v1_machines = str(tuple(cell_data['V1_MACHINE'].unique())).replace(',)', ')')
    v2_machines = str(tuple(cell_data['V2_MACHINE'].unique())).replace(',)', ')')
    lines = ["'" + d + "'" for d in cell_data['LINE_NO'].unique()]#This for loop creates an array of sql readable lines from the pandas line_no series in cell_data
    day2 = cell_data['V2_MEASURE_DATE'].mean()

    # test code
    cell_data.to_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\test_cell_data.csv')
    offset.to_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\test_offset_data.csv')
    # end test code
    ##CORRECT METERS CHECK
    for v1_machine in cell_data['V1_MACHINE'].unique():
        for v2_machine in cell_data.loc[cell_data['V1_MACHINE'] == v1_machine, 'V2_MACHINE'].unique():
            for meter in cell_data.loc[(cell_data['V1_MACHINE'] == v1_machine) & (cell_data['V2_MACHINE'] == v2_machine), 'METER'].unique():

                if (offset.loc[(offset['V1_MACHINE'] == v1_machine) & (offset['V2_MACHINE'] == v2_machine) & (offset['METER'] == meter)].size <= 0):
                    print('v1 machine', v1_machine, 'v2 machine', v2_machine, 'meter', meter)
                    return False
    print('Three Checks Passed')

    ##METER VARIABILITY CHECK
    test_data = getTestLots(day2, lines)
    # print(test_data)
    test_data = test_data.merge(offset, on = ['V1_MACHINE', 'V2_MACHINE', 'METER'])
    test_data['O_dVdt'] = (test_data['DV'] + test_data['OFFSET'])/(test_data['dT'])
    test_data = test_data.groupby(['METER', 'TRAY_NO', 'LOT_NO', 'V1_MACHINE', 'V2_MACHINE']).median().reset_index()
    test_data = test_data.drop(columns = ['V1_MACHINE', 'V2_MACHINE'])
    raw_variability = test_data.groupby('TRAY_NO').std().median()['R_dVdT']
    variability = test_data.groupby('TRAY_NO').std().median()['O_dVdt']
    if variability >= raw_variability:
        return False

    print('All Offset Validation Checks Passed')
    ##PASS
    return True



def getTestLots(day2, lines):
    """Identifies the most recently completed mass production data which is relevant to the given machine information

        Parameters:
        day2 (pandas datetime): the day at which the function starts searching for relevant MP lots (searches backwards from here)
        v1_machines (str): a string representation of the list of all relevant v1 machines
        v2_machines (str): a string representation of the list of all relevant v2 machines
        lines (str): a string representation of the list of all lines that must be included in testLots

        Returns:
        pandas DataFrame: cell data for each cell in the chosen mass production lot(s)


    """
    cell_data = pd.DataFrame()
    #this loop ensures that 1 lot is pulled from each line in lines
    for line in lines:
        #This loop looks through the 22 days before param day2 for a time period with valid lots to return
        for day in reversed(pd.date_range(day2 - pd.Timedelta(days = 20), day2)):


            ##### THIS FIRST QUERY CREATES A LIST OF THE TWO MOST RECENT LOTS IN REGARDS TO THE V2 DATE
            month1, month2 = getMonths(day)
            start_day = str(day - pd.Timedelta(days = 2))[0:10]
            end_day = str(day)[0:10]
            sql = """SELECT t1.LOT_NO, MEDIAN(t1.MEASURE_DATE) AS MEASURE_DATE
                    FROM (SELECT * FROM GEIS.T_CELL_ENG_GC13_{} UNION ALL SELECT * FROM GEIS.T_CELL_ENG_GC13_{}) t1
                    WHERE t1.LINE_NO = {}
                    AND t1.MEASURE_DATE BETWEEN TO_DATE('{}','YYYY-MM-DD') AND TO_DATE('{}','YYYY-MM-DD')
                    AND t1.LOT_NO NOT LIKE '%Z%'
                    AND t1.LOT_NO NOT LIKE '%GD9%'
                    GROUP BY t1.LOT_NO
            """.format(month1,month2,line,start_day,end_day)
            con = cx_Oracle.connect(user='PENAENG', password='p6bru@aXE=am', dsn='10.133.200.175/GEISDB.WORLD')
            if month1 not in [(datetime.now() - relativedelta(months=x)).strftime('%y%m') for x in range(12)]:
                con = cx_Oracle.connect(user='kwang', password='XV7A6GneRC2a', dsn='10.133.200.174:1521/colddb.america.gds.panasonic.com')
            print('MES Connection Established at {}'.format(str(datetime.now().time())[:-5]))
            potential_lots = pd.read_sql_query(sql, con)
            con.close()
            print('MES Connection Ended at {}'.format(str(datetime.now().time())[:-5]))


            #If there is not a significant number of mass production lots in the given time period, move to the next time period
            if potential_lots.size < 3:#size < 3 means there must be more than 1 lot because each row in this DataFrame as size 2
                continue

            #If there IS a significant amount of MP data, grab that data
            potential_lots.sort_values(by = 'MEASURE_DATE', ascending = False)
            lots = str(tuple(potential_lots['LOT_NO'][0:1])).replace(',)', ')')
            temp_data = getMPData(lots, month1, month2)

            #Once we have the data for the lot we want within a line, add that data to the final DataFrame of data and move to the next line
            print (temp_data)
            print('Adding data for ' + lots + ' to cell_data')
            cell_data = pd.concat([temp_data, cell_data])
            print(cell_data)
            break # this skips to the next lines, so the iteration doesn't continue for 20 days if a lot is found.
        print(line + ' is done.')

    try:
        if len(lines) != len(cell_data['LOT_NO'].unique()) :
            raise NameError("Insufficient Test Lots Found -- This shouldn't happen because what lots did you base the offsets on")
        return cell_data
    except KeyError:
        print("No Test Lots --This shouldn't happen because the offset is created from lots in the same range.")
        raise


# 'YYYY-MM-DD'

def getMonths(day1):
    """ Finds the relevant table codes to pull data given a date

        Parameters:
        day1 (pandas datetime): a time that is representative of the data you would like to grab

        Returns:
        str: the table code for the month of the given data
        str: the table code for the month before the month of the given data
    """
    month1 = str(day1)[2:4] + str(day1)[5:7] # changed from [0:2]
    month2 = str(day1 - pd.Timedelta(days = 2))[2:4] + str(day1 - pd.Timedelta(days = 2))[5:7] #fix indexing to actually grab yymm
    if month1 == month2:
        if int(str(day1)[5:7]) == 1:
            month2 = str(int(str(day1)[2:4])-1) + '12' # year transition
        else:
            month2 = str(int(month2) - 1) # month transition

    return month1, month2



## grab some MP Data
def getMPData(_lot_list, _start_month, _end_month):
    """ Queries MES for basic information about the cells in a given lot number.

        Parameters:
        _lot_list (str): a string representation of a tuple of lots for which to grab cell data
        _start_month (str): the table code for the first table to include in grabbing lot data
        _end_month (str): the table code for the second table to include in grabbing lot data

        Returns:
        pandas DataFrame: cell data for each cell in lots in _lot_list; Columns = ['V1_MEASURE_DATE', 'V2_MEASURE_DATE', 'DV', 'V1_MACHINE', 'V2_MACHINE', 'METER', 'TRAY_NO', 'LOT_NO', 'dT', 'R_dVdT']


    """

    #Create Query to grab MP Data
    sql = """SELECT
                t1.MEASURE_DATE AS V1_MEASURE_DATE,
                t2.MEASURE_DATE AS V2_MEASURE_DATE,
                (t2.DATA_02 - t1.DATA_01) AS DV,
                t1.EQUIP_NO AS V1_MACHINE,
                t2.EQUIP_NO AS V2_MACHINE,
                MOD(SUBSTR(t1.TRAY_POSITION,2,2),4) AS METER,
                t1.TRAY_NO,
                t1.LOT_NO
    FROM (SELECT * FROM GEIS.T_CELL_ENG_GC12_{} UNION ALL SELECT * FROM GEIS.T_CELL_ENG_GC12_{}) t1
        INNER JOIN (SELECT * FROM GEIS.T_CELL_ENG_GC13_{} UNION ALL SELECT * FROM GEIS.T_CELL_ENG_GC13_{}) t2
        ON (t2.CELL_ID = t1.CELL_ID)
    WHERE t1.LOT_NO IN {}
    """.format(_start_month, _end_month, _start_month, _end_month, _lot_list)



    #Grab MP Data into df cell_data
    con = cx_Oracle.connect(user='PENAENG', password='p6bru@aXE=am', dsn='10.133.200.175/GEISDB.WORLD')
    if _start_month not in [(datetime.now() - relativedelta(months=x)).strftime('%y%m') for x in range(12)]:
        con = cx_Oracle.connect(user='kwang', password='XV7A6GneRC2a', dsn='10.133.200.174:1521/colddb.america.gds.panasonic.com')
    print('MES Connection Established at {}'.format(str(datetime.now().time())[:-5]))
    cell_data = pd.read_sql_query(sql, con)
    con.close()
    print('MES Connection Ended at {}'.format(str(datetime.now().time())[:-5]))



    #This line removes outliers from the resultatnt data so they do not bias any tests run with this data.
    cell_data = cell_data.loc[(cell_data['DV'] < .001) & (cell_data['DV'] > -.002)]


    ##Prepares some calculated columns before returning cell_data
    converter = pd.Timedelta(days = 1)
    cell_data['dT'] = (cell_data['V2_MEASURE_DATE'] - cell_data['V1_MEASURE_DATE'])/converter # here, converter is used to ensure that the dT column is of type int instead of type pd.Timedelta
    cell_data['R_dVdT'] = cell_data['DV']/cell_data['dT']#  R_dVdT is the Raw dVdT measurement from V1 and V2 without any adjustment


    return cell_data
