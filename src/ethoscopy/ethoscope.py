import sqlite3
from pathlib import Path
import traceback
import time
from sys import exit

import pandas as pd


def read_single_roi(meta, min_time = 0, max_time = float('inf'), reference_hour = None, cache = None):
    """
    Loads the data from a single region from an ethoscope according to inputted times
    changes time to reference hour and applies any functions added
    
    Params: 
    @ meta = row in a metadata pd.DataFrane containing a column 'path' with .db meta Location
    @ min_time = time constraint with which to query database (in hours), default is 0
    @ max_time = same as above
    @ reference_hour = the time in hours when the light begins in the experiment, i.e. the beginning of a 24 hour session
    @ cache = if not None provide path for folder with saved caches or folder to be saved to
    
    returns a pandas dataframe containing raw ethoscope dataframe
    """

    if min_time > max_time:
        exit('Error: min_time is larger than max_time')

    if cache is not None:
        cache_name = 'cached_{}_{}_{}.pkl'.format(meta['machine_id'], meta['region_id'], meta['date'])
        path = Path(cache) / Path(cache_name)
        if path.exists():
            data = pd.read_pickle(path)
            return data

    try:
        conn = sqlite3.connect(meta['path'])

        roi_df = pd.read_sql_query('SELECT * FROM ROI_MAP', conn)
        
        roi_row = roi_df[roi_df['roi_idx'] == meta['region_id']]

        if len(roi_row.index) < 1:
            print('ROI {} does not exist, skipping'.format(meta['region_id']))
            return None

        var_df = pd.read_sql_query('SELECT * FROM VAR_MAP', conn)
        date = pd.read_sql_query('SELECT value FROM METADATA WHERE field = "date_time"', conn)

        # isolate date_time string and parse to GMT with format YYYY-MM-DD HH-MM-SS
        date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(float(date.iloc[0])))      

        if max_time == float('inf'):
            max_time_condtion =  ''
        else:
            max_time_condtion = 'AND t < {}'.format(max_time * 1000) 
        
        min_time = min_time * 1000
        #sql_query takes roughyl 2.8 seconds for 2.5 days of data
        sql_query = 'SELECT * FROM ROI_{} WHERE t >= {} {}'.format(meta['region_id'], min_time, max_time_condtion)
        data = pd.read_sql_query(sql_query, conn)
        
        if 'id' in data.columns:
            data = data.drop(columns = ['id'])

        if reference_hour != None:
            t = date
            t = t.split(' ')
            hh, mm , ss = map(int, t[1].split(':'))
            hour_start = hh + mm/60 + ss/3600
            t_after_ref = ((hour_start - reference_hour) % 24) * 3600 * 1e3
            data.t = (data.t + t_after_ref) / 1e3
        
        else:
            data.t = data.t / 1e3
            
        roi_width = max(roi_row['w'].iloc[0], roi_row['h'].iloc[0])
        for var_n in var_df['var_name']:
            if var_df['functional_type'][var_df['var_name'] == var_n].iloc[0] == 'distance':
                data[var_n] = data[var_n] / roi_width

        if 'is_inferred' and 'has_interacted' in data.columns:
            data = data[(data['is_inferred'] == False) | (data['has_interacted'] == True)]
            # check if has_interacted is all false / 0, drop if so
            interacted_list = data['has_interacted'].to_numpy()
            if (0 == interacted_list[:]).all() == True:
                data = data.drop(columns = ['has_interacted'])
                # data = data.drop(columns = ['is_inferred'])
        
        elif 'is_inferred' in data.columns:
            data = data[data['is_inferred'] == False]
            data = data.drop(columns = ['is_inferred'])

        if cache is not None:
            data.to_pickle(path)

        return data
    
    except Exception as error:
        print(traceback.print_exc())
        print(error)

    finally:
        conn.close()