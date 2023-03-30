import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

db_info = {'mes_user': 'PENAENG',
           'mes_pw': 'p6bru@aXE=am',
           'mes_dsn': '10.133.200.175/GEISDB.WORLD',
           'cold_user': 'kwang',
           'cold_pw': 'XV7A6GneRC2a',
           'cold_dsn': '10.133.200.174:1521/colddb.america.gds.panasonic.com'
           }
cell_id_dict = {'1': '01', '2': '02', '3': '03', '4': '04', '5': '05', '6': '06', '7': '07', '8': '08', '9': '09',
                'A': '10', 'B': '11', 'C': '12', 'D': '13', 'E': '14', 'F': '15', 'G': '16', 'H': '17', 'J': '18',
                'K': '19', 'L': '20', 'M': '21', 'N': '22', 'P': '23', 'Q': '24', 'R': '25', 'S': '26', 'T': '27',
                'U': '28', 'V': '29', 'W': '30', 'X': '31', 'Y': '32', 'Z': '33'}
db_mon = 12 * (datetime.now().year - datetime(2020, 8, 1).year) + (datetime.now().month - datetime(2020, 8, 1).month)
mm_ref = [(datetime(2020, 8, 1) + relativedelta(months=x)).strftime('%y%m') for x in range(db_mon)]
volt_spec = 2.5
ndv_spec = -0.5
rshort_spec = 89e3
dv_offset = -0.00009763
dv_coefficient = -0.00303104
dq_ref = pd.read_csv(rf'C:\Users\KW38770\Documents\WangK\Formation\Retest\dvdq\dq_curve.csv')
DIRECTORY = r'C:\Users\KW38770\Documents\WangK\Formation\Retest\Processing_v3'
model_number = 'L1'
