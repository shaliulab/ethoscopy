import traceback
import logging
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd 
import time 
import sqlite3
from sys import exit
from pathlib import Path

import numpy as np
from ethoscopy.misc.format_warning import format_warning

pd.options.mode.chained_assignment = None
warnings.formatwarning = format_warning


def read_single_roi(file,
                    min_time = 0,
                    max_time = float('inf'),
                    reference_hour = None,
                    cache=None,
):


    if min_time > max_time:
        exit('Error: min_time is larger than max_time')

    if cache is not None:
        cache_name = 'cached_{}_{}_{}.pkl'.format(file['machine_id'], file['region_id'], file['date'])
        path = Path(cache) / Path(cache_name)
        if path.exists():
            data = pd.read_pickle(path)
            return data

    try:
        conn = sqlite3.connect(file['path'])
        
        roi_df = pd.read_sql_query('SELECT * FROM ROI_MAP', conn)
        roi_row = roi_df[roi_df['roi_idx'] == 0]


        var_df = pd.read_sql_query('SELECT * FROM VAR_MAP', conn)
        date = pd.read_sql_query('SELECT value FROM METADATA WHERE field = "date_time"', conn)

        # isolate date_time string and parse to GMT with format YYYY-MM-DD HH-MM-SS
        date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(float(date.iloc[0])))      

        #sql_query takes roughyl 2.8 seconds for 2.5 days of data

        region_id = file["region_id"]
        #sql_query = 'SELECT * FROM ROI_{} WHERE t >= {} {}'.format(file['region_id'], min_time, max_time_condtion)

        if region_id == 0:
            sql_query =  """
                WITH const AS (SELECT 0 AS identity)
                SELECT
                    R0.frame_number,
                    IDX.chunk,
                    R0.in_frame_index,
                    IDX.frame_time AS t,
                    const.identity AS identity,
                    R0.x,
                    R0.y,
                    R0.modified
                FROM
                    ROI_0 AS R0, const
                    INNER JOIN STORE_INDEX AS IDX on R0.frame_number = IDX.frame_number AND IDX.half_second = 1;
                """

        else:
            sql_query = f"""
                SELECT
                    R0.frame_number,
                    IDX.chunk,
                    R0.in_frame_index,
                    IDX.frame_time AS t,
                    ID.identity,
                    R0.x,
                    R0.y,
                    R0.modified
                FROM
                    ROI_0 AS R0
                    INNER JOIN STORE_INDEX AS IDX on R0.frame_number = IDX.frame_number AND IDX.half_second = 1
                    INNER JOIN IDENTITY AS ID on R0.frame_number = ID.frame_number AND ID.in_frame_index = R0.in_frame_index AND ID.identity = {region_id};
                """
            
        logging.debug(f"Running query {sql_query}")
        before=time.time()
        data = pd.read_sql_query(sql_query, conn)
        after=time.time()
        logging.debug(f"Done in {after-before} seconds")

        # TODO Figure out how to perform the time filtering directly in SQL at a detent speed
        min_time*=1000
        data=data.loc[data["t"] >= min_time]
        if max_time == float('inf'):
            pass
        else:
            max_time*=1000
            data=data.loc[data["t"] < max_time]

        
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


        dist = np.sqrt(np.diff(data[["x", "y"]].values, axis=0).sum(axis=1))
        xy_dist_log10x1000 = np.log10(dist) * 1000
        xy_dist_log10x1000 = [0] + xy_dist_log10x1000.tolist()
        data["xy_dist_log10x1000"] = xy_dist_log10x1000
        data["phi"] = 0
        data["w"] = 0
        data["h"] = 0


        if cache is not None:
            data.to_pickle(path)

        return data
    
    except Exception as error:
        print(traceback.print_exc())
        print(error)

    finally:
        conn.close()
