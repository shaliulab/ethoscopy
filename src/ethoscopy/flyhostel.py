import traceback
import os.path
import logging
import math
import warnings
import time 
import sqlite3
from sys import exit
from pathlib import Path

warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd 

from ethoscopy.misc.xy_dist_log10x1000 import compute_xy_dist_log10x1000
from ethoscopy.misc.format_warning import format_warning

pd.options.mode.chained_assignment = None
warnings.formatwarning = format_warning

def read_qc_single_path(path, reference_hour):
    with sqlite3.connect(path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM QC;")
        records = cursor.fetchall()
        qc=pd.DataFrame.from_records(records)
        cursor.execute("PRAGMA table_info(QC);")
        meta = cursor.fetchall()
        columns = [e[1] for e in meta]
        qc.columns = columns
        
        chunk_starts=[]
        key=os.path.splitext(os.path.basename(path))[0]
        t0 = key.split("_")[3]

        h, m, s = t0.split("-")
        t0 = int(h)*3600 + int(m)*60 + int(s) # in seconds
        
        for chunk in qc["chunk"]:
            cursor.execute(f"SELECT frame_time FROM STORE_INDEX WHERE chunk = {chunk} LIMIT 1;")
            chunk_t0 = cursor.fetchone()
            if chunk_t0 is not None:
                chunk_t0=chunk_t0[0] / 1000
                t = round(chunk_t0 + t0 - reference_hour*3600)
                chunk_starts.append((chunk, t))
                
    chunk_starts=pd.DataFrame.from_records(chunk_starts, columns=["chunk", "t"])
    qc = pd.merge(qc, chunk_starts, left_on="chunk", right_on="chunk")
    qc["path"] = path
    return qc    


def load_hour_start(date):
    t = date
    t = t.split(' ')
    hh, mm, ss = map(int, t[1].split(':'))
    return hh
    #return  hh # + mm/60 + ss/3600

def read_single_roi(meta,
                    min_time = -float('inf'),
                    max_time = float('inf'),
                    reference_hour = None,
                    cache=None,
                    time_system="recording"
):
    """
    meta (pd.Series): with columns machine_id, date, path, region_id
    min_time (int): ZT time in seconds from which to load the data
    max_time (int): ZT time in seconds until which to load the data
    refererence_hour (int): Number of hours since midnight in recording computer in its timezone (GMT)
    cache (str): Path to folder where cache files may be stored
    """


    if min_time > max_time:
        exit('Error: min_time is larger than max_time')
        
    if meta["machine_name"] == "1X":
        region_id = 0
    else:
        region_id = meta["region_id"]

    if cache is not None:
        cache_name = 'cached_{}_{}_{}_t0_{}_t1_{}.pkl'.format(meta['machine_id'], region_id, meta['date'], min_time, max_time)
        path = Path(cache) / Path(cache_name)
        if path.exists():
            logging.debug(f"Loading {path}")
            data = pd.read_pickle(path)
            return data
    conn = None

    try:

        uri=f"file:{meta['path']}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        
        roi_df = pd.read_sql_query('SELECT * FROM ROI_MAP', conn)
        roi_row = roi_df[roi_df['roi_idx'] == 0]


        var_df = pd.read_sql_query('SELECT * FROM VAR_MAP', conn)
        date = pd.read_sql_query('SELECT value FROM METADATA WHERE field = "date_time"', conn)

        # isolate date_time string and parse to GMT with format YYYY-MM-DD HH-MM-SS
        date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(float(date.iloc[0])))
        if math.isinf(min_time):
            min_time = None
        if math.isinf(max_time):
            max_time = None


        if reference_hour is not None and time_system == "zt":
            offset=(load_hour_start(date) - reference_hour)*3600
            if min_time is not None:
                min_time = min_time - offset
            if max_time is not None:
                max_time = max_time - offset
        
        if min_time is None and max_time is None:
            frame_time_constraint = ""
        else:
            frame_time_constraint = """WHERE
                IDX.frame_time """
            
            if min_time is not None and max_time is not None:
                frame_time_constraint += f"BETWEEN {min_time*1000} AND {max_time*1000}"
            elif min_time is None:
                frame_time_constraint += f"< {max_time*1000}"
            else:
                frame_time_constraint += f"> {min_time*1000}"

        #sql_query takes roughly 2.8 seconds for 2.5 days of data

        if region_id == 0:
            sql_query =  f"""
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
                    INNER JOIN STORE_INDEX AS IDX on R0.frame_number = IDX.frame_number
                {frame_time_constraint};
                """
                    # INNER JOIN STORE_INDEX AS IDX on R0.frame_number = IDX.frame_number AND IDX.half_second = 1;

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
                    INNER JOIN STORE_INDEX AS IDX on R0.frame_number = IDX.frame_number
                    INNER JOIN IDENTITY AS ID on R0.frame_number = ID.frame_number AND ID.in_frame_index = R0.in_frame_index AND ID.identity = {region_id}
                {frame_time_constraint};
                """
                    # INNER JOIN STORE_INDEX AS IDX on R0.frame_number = IDX.frame_number AND IDX.half_second = 1
       
        logging.debug(f"Running query {sql_query}")
        before=time.time()
        data = pd.read_sql_query(sql_query, conn)
        after=time.time()
        logging.debug(f"Done in {after-before} seconds")
       
        # ms -> seconds
        data.t /= 1e3

        # seconds since start to seconds since zt0
        if reference_hour is not None:
            hour_start=load_hour_start(date)
            t_after_ref = ((hour_start - reference_hour) % 24) * 3600
            data.t = (data.t + t_after_ref)
    
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


        min_distance = 1 / roi_row["w"].item()
        data["xy_dist_log10x1000"] = compute_xy_dist_log10x1000(data, min_distance)
        data["phi"] = 0
        data["w"] = 0
        data["h"] = 0

        if cache is not None:
            data.to_pickle(path)

        return data
    
    except Exception as error:
        file=meta['[path]']
        print(f"Cannot load data in file {file}")
        print(traceback.print_exc())
        print(error)

    finally:
        if conn is not None:
            conn.close()


