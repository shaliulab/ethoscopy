import ftplib
import logging
import glob
import traceback
import os.path
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd 
import numpy as np
import errno
import time 
from sys import exit
from pathlib import Path, PurePosixPath
from functools import partial
from urllib.parse import urlparse
import itertools
import glob

import joblib
from ethoscopy.misc.validate_datetime import validate_datetime
from ethoscopy.misc.format_warning import format_warning
from ethoscopy.ethoscope import read_single_roi as read_ethoscope_single_roi
from ethoscopy.flyhostel import read_single_roi as read_flyhostel_single_roi
from ethoscopy.flyhostel import read_qc_single_path, load_hour_start

logger=logging.getLogger(__name__)
pd.options.mode.chained_assignment = None
warnings.formatwarning = format_warning

def update_metadata(meta_loc, prefix="ethoscope&flyhostel_metadata3"):
    """
    Puts together a single metadata.csv with all flies available in /flyhostel_metadata/ with the passed prefix
    """
    metadatas=glob.glob("/flyhostel_data/metadata/*")
    paths=[path for path in metadatas if os.path.basename(path).startswith(prefix)]
    metadatas=[pd.read_csv(path) for path in paths]
    columns, counts = np.unique(list(itertools.chain(*[metadata.columns for metadata in metadatas])), return_counts=True)
    missing_columns=columns[counts!=len(metadatas)]
    all_columns=sorted(list(set(columns)))
    metadatas2=[]
    for i, metadata in enumerate(metadatas):
        for column in columns:
            if not column in metadata.columns:
                metadata[column]="NONE"

        metadata["input_metadata"]=paths[i]
        metadatas2.append(metadata[all_columns + ["input_metadata"]])
    metadata=pd.concat(metadatas2, axis=0)
    metadata=metadata.loc[metadata["flyhostel_date"]!="NONE"]

    meta=metadata
    # x=metadata.groupby(["flyhostel_number", "flyhostel_date"]).size().reset_index()
    # x.columns=x.columns[:2].tolist() + ["number_of_animals"]
    # meta=metadata.merge(x, on=["flyhostel_number", "flyhostel_date"])
    meta["machine_id"] = [f"FlyHostel{flyhostel_number}" for flyhostel_number in meta["flyhostel_number"]]
    meta["machine_name"] = [f"{e}X" for e in meta["number_of_animals"]]
    meta["date"] = meta["flyhostel_date"]
    meta["fly_no"] = [f"{machine_id}_{machine_name}_{region_id}" for machine_id, machine_name, region_id in zip(meta["machine_id"], meta["machine_name"],meta["region_id"])]
    meta.drop(["flyhostel_date", "number_of_animals", "flyhostel_number"], axis=1, inplace=True)
    meta.to_csv(meta_loc, index=None)



def ethoscope_database_rule(path):
    return path.endswith(".db")

def flyhostel_database_rule(path):
    return os.path.basename(path).startswith("FlyHostel") and path.endswith(".db")

def list_files(remote_dir, ethoscope_list, source="ethoscope"):
    if source == "ethoscope":
        return list_ethoscope_files(remote_dir, ethoscope_list=ethoscope_list)
    elif source == "flyhostel":
        return list_flyhostel_files(remote_dir)

def list_flyhostel_files(remote_dir):

    wd = os.getcwd()

    paths = []
    check_list = []
    try:
        os.chdir(remote_dir)
        database = os.listdir()
        flyhostels = [path for path in database if os.path.isdir(path) or os.path.isdir(os.path.realpath(path))]
        for flyhostel in flyhostels:
            try:
                os.chdir(flyhostel)
                x_folders = os.listdir()
                for x_folder in x_folders:
                    if os.path.isdir(x_folder) and x_folder.endswith("X"):
                        os.chdir(x_folder)
                        exp_folders = os.listdir()
                        for exp in exp_folders:
                            # check that the found path is a folder
                            # in case there are undesired files here
                            # so that the pipeline does not error
                            # (just warn the user) 
                            if not os.path.isdir(exp):
                                print(f"{os.getcwd()}/{exp} is not a directory. Please remove it")
                                continue
                            date_time = exp.split('_')
                            os.chdir(exp)
                            files = os.listdir()
                            for path in files:
                                if flyhostel_database_rule(path):
                                    size = os.path.getsize(path)
                                    final_path = f'{flyhostel}/{x_folder}/{exp}/{path}'
                                    path_size_list = [final_path, size]
                                    paths.append(path_size_list)
                                    check_list.append([flyhostel, date_time[0]])
                            os.chdir("..")
                        os.chdir("..")
                os.chdir("..")

                                        
            except Exception as error:
                print(traceback.print_exc())
                print(f"WD: {os.getcwd()}")
                print(error)

            finally:
                os.chdir(remote_dir)
                
    finally:
        os.chdir(wd)


    return check_list, paths

def list_ethoscope_files(remote_dir, ethoscope_list):

    wd = os.getcwd()

    paths = []
    check_list = []
    try:
        os.chdir(remote_dir)
        database = os.listdir()
        dirs = [path for path in database if os.path.isdir(path) or os.path.isdir(os.path.realpath(path))]
        for dir in dirs:
            try:
                os.chdir(dir)
                directories_2 = os.listdir()
                for c, name in enumerate(ethoscope_list):
                    if name in directories_2:
                        os.chdir(name)
                        directories_3 = os.listdir()
                        for exp in directories_3:
                            date_time = exp.split('_')
                            os.chdir(exp)
                            directories_4 = os.listdir()
                            for db in directories_4:
                                if ethoscope_database_rule(db):
                                    size = os.path.getsize(db)
                                    final_path = f'{dir}/{name}/{exp}/{db}'
                                    path_size_list = [final_path, size]
                                    paths.append(path_size_list)
                                    check_list.append([name, date_time[0]])
                            os.chdir("..")
                        os.chdir("..")
                os.chdir("..")

                                        
            except Exception as error:
                print(traceback.print_exc())
                print(error)

            finally:
                os.chdir(remote_dir)
                
    finally:
        os.chdir(wd)


    return check_list, paths
    

def list_remote_files(remote_dir, ethoscope_list):
    # connect to ftp server and parse the given ftp link
    parse = urlparse(remote_dir)
    ftp = ftplib.FTP(parse.netloc)
    ftp.login()
    ftp.cwd(parse.path)
    files = ftp.nlst()

    paths = []
    check_list = []
    # iterate through the first level of directories looking for ones that match the ethoscope names given, 
    # find the susequent files that match the date and time and add to paths list
    # this is slow, should change to walk directory once, get all information and then match to csv

    for dir in files:
        temp_path = parse.path / PurePosixPath(dir)
        try:
            ftp.cwd(str(temp_path))
            directories_2 = ftp.nlst()
            for c, name in enumerate(ethoscope_list):
                if name in directories_2:
                    ftp.cwd(name)
                    directories_3 = ftp.nlst()
                    for exp in directories_3:
                        date_time = exp.split('_')

                        ftp.cwd(exp)
                        directories_4 = ftp.nlst()
                        for db in directories_4:
                            if db.endswith('.db'):
                                size = ftp.size(db)
                                final_path = f'{dir}/{name}/{exp}/{db}'
                                path_size_list = [final_path, size]
                                paths.append(path_size_list)
                                check_list.append([name, date_time[0]])
                                    
        except Exception as error:
            print(traceback.print_exc())
            print(error)
            
        finally:
            ftp.cwd("/")

        return check_list, paths
    

def download_from_remote_dir(meta, remote_dir, local_dir, source):
    """ 
    This function is used to import data from the ethoscope node platform to your local directory for later use. The ethoscope files must be saved on a
    remote FTP server and saved as .db files, see the Ethoscope manual for how to setup a node correctly
    https://www.notion.so/giorgiogilestro/Ethoscope-User-Manual-a9739373ae9f4840aa45b277f2f0e3a7
    
    Params:
    @meta = csv file containing coloumns with machine_name, date, and time if multiple files on the same day
    @remote_dir = string, containing the location of the ftp server up to the folder contain the machine id's, server must not have a username or password
    e.g. 'ftp://YOUR_SERVER//auto_generated_data//ethoscope_results'
    @local_dir = path of the local directory to save .db files to, files will be saved using the structure of the ftp server
    e.g. 'C:\\Users\\YOUR_NAME\\Documents\\ethoscope_databases'

    returns None
    """
    meta = Path(meta)
    local_dir = Path(local_dir)

    #check csv path is real and read to pandas df
    if meta.exists():
        try:
            meta_df = pd.read_csv(meta)         
        except Exception as e:
            print("An error occurred: ", e)
    else:
        raise FileNotFoundError("The metadata is not readable")

    # check and tidy df, removing un-needed columns and duplicated machine names
    if 'machine_name' not in meta_df.columns or 'date' not in meta_df.columns:
        raise KeyError("Column(s) 'machine_name' and/or 'date' missing from metadata file")

    meta_df.dropna(how = 'all', inplace = True)

    if 'time' in meta_df.columns.tolist():
        meta_df['check'] = meta_df['machine_name'] + meta_df['date'] + meta_df['time']
        meta_df.drop_duplicates(subset = ['check'], keep = 'first', inplace = True, ignore_index = False)
    else:
        meta_df['check'] = meta_df['machine_name'] + meta_df['date'] 
        meta_df.drop_duplicates(subset = ['check'], keep = 'first', inplace = True, ignore_index = False)

    # check the date format is YYYY-MM-DD, without this format the df merge will return empty
    # will correct to YYYY-MM-DD in a select few cases
    validate_datetime(meta_df)

    # extract columns as list to identify .db files from ftp server
    ethoscope_list = meta_df['machine_name'].tolist()
    date_list = meta_df['date'].tolist()

    if 'time' in meta_df.columns.tolist():
        time_list = pd.Series(meta_df['time'].tolist())
        bool_list = time_list.isna().tolist()
    else:
        nan_list = [np.nan] * len(meta_df['date'])
        time_list = pd.Series(nan_list)
        bool_list = time_list.isna().tolist()

    if remote_dir.startswith("ftp://"):
        check_list, paths = list_remote_files(remote_dir, ethoscope_list)
    else:
        check_list, paths = list_files(remote_dir, ethoscope_list, source=source)

    if len(paths) == 0:
        warnings.warn("No Ethoscope data could be found, please check the metadata file")
        exit()

    for i in zip(ethoscope_list, date_list):
        if list(i) in check_list:
            continue
        else:
            print(f'{i[0]}_{i[1]} has not been found for download')

    def download_database(remote_dir, folders, work_dir, local_dir, file_name, file_size):
        """ 
        Connects to remote FTP server and saves to designated local path, retains file name and path directory structure 
        
        Params:
        @remote_dir = ftp server netloc 
        @work_dir = ftp server path
        @local_dir = local directory path for the file and subsequent directory structure to be saved to
        @file_name = name of .db file to be download
        @file_size = size of file above in bytes

        returns None
        """
        
        #create local copy of directory tree from ftp server
        os.chdir(local_dir)

        win_path = local_dir / work_dir 
        
        try:
            os.makedirs(win_path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(win_path):
                pass
            else:
                raise

        file_path = win_path / file_name

        if os.access(file_path, os.R_OK):
            if os.path.getsize(file_path) < file_size:
                ftp = ftplib.FTP(remote_dir)
                ftp.login()
                ftp.cwd(folders + '/' + str(work_dir))

                localfile = open(file_path, 'wb')
                ftp.retrbinary('RETR ' + file_name, localfile.write)
                    
                ftp.quit()
                localfile.close()

        else:
            ftp = ftplib.FTP(remote_dir)
            ftp.login()
            ftp.cwd(folders + '/' + str(work_dir))

            localfile = open(file_path, 'wb')
            ftp.retrbinary('RETR ' + file_name, localfile.write)

            ftp.quit()
            localfile.close()

    # iterate over paths, downloading each file
    # provide estimate download time based upon average time of previous downloads in queue
    download = partial(download_database, remote_dir = parse.netloc, folders = parse.path, local_dir = local_dir)
    times = []

    for counter, j in enumerate(paths):
        print('Downloading {}... {}/{}'.format(j[0].split('/')[1], counter+1, len(paths)))
        if counter == 0:
            start = time.time()
            p = PurePosixPath(j[0])
            download(work_dir = p.parents[0], file_name = p.name, file_size = j[1])
            stop = time.time()
            t = stop - start
            times.append(t)

        else:
            av_time = round((np.mean(times)/60) * (len(paths)-(counter+1)))
            print(f'Estimated finish time: {av_time} mins')
            start = time.time()
            p = PurePosixPath(j[0])
            download(work_dir = p.parents[0], file_name = p.name, file_size = j[1])
            stop = time.time()
            t = stop - start
            times.append(t)

def link_meta_index(metadata, remote_dir, local_dir, source="ethoscope", verbose=False):
    """ A function to alter the provided metadata file with the path locations of downloaded .db files from the Ethscope experimental system. The function will check all unique machines against the orginal ftp server 
        for any errors. Errors will be ommitted from the returned metadata table without warning

        Params:
        @metadata = .csv file path, A file containing the metadata information of each ROI to be downloaded, mucst include 'ETHOSCOPE_NAME', 'date' in yyyy-mm-dd format or others (see validate_datetime), and 'region'  
        @remote_dir = ftp server location, the root directory location of the ftp server containing the .db files, should have no password or username
        @local_dir = local file path, The path where saved database files are located

        returns a pandas dataframe containing the csv file information and corresponding path for each entry in the csv 
    """
    metadata = Path(metadata)
    local_dir = Path(local_dir)
    #load metadata csv file
    #check csv path is real and read to pandas df
    if metadata.exists():
        try:
            meta_df = pd.read_csv(metadata)
        except Exception as e:
            print("An error occurred: ", e)
    else:
        raise FileNotFoundError(f"The metadata file {metadata} is not readable")

    if len(meta_df[meta_df.isna().any(axis=1)]) >= 1:
        print(meta_df[meta_df.isna().any(axis=1)])
        warnings.warn("When the metadata is read it contains NaN values (empty cells in the csv file can cause this!), please replace with an alterative")
        exit()

    # check and tidy df, removing un-needed columns and duplicated machine names
    if 'machine_name' not in meta_df.columns or 'date' not in meta_df.columns:
        raise KeyError("Column(s) 'machine_name' and/or 'date' missing from metadata file")

    meta_df.dropna(axis = 0, how = 'all', inplace = True)
    
    # check the date format is YYYY-MM-DD, without this format the df merge will return empty
    # will correct to YYYY-MM-DD in a select few cases
    meta_df = validate_datetime(meta_df)

    meta_df_original = meta_df.copy()

    if 'time' in meta_df.columns.tolist():
        meta_df['check'] = meta_df['machine_name'] + meta_df['date'] + meta_df['time']
        meta_df.drop_duplicates(subset = ['check'], keep = 'first', inplace = True, ignore_index = False)
    else:
        meta_df['check'] = meta_df['machine_name'] + meta_df['date'] 
        meta_df.drop_duplicates(subset = ['check'], keep = 'first', inplace = True, ignore_index = False)

    if "machine_id" in meta_df.columns:
        ethoscope_list = meta_df['machine_id'].tolist()
    else:
        ethoscope_list = meta_df['machine_name'].tolist()

    date_list = meta_df['date'].tolist()

    if 'time' in meta_df.columns.tolist():
        time_list = pd.Series(meta_df['time'].tolist())
        bool_list = time_list.isna().tolist()
    else:
        nan_list = [np.nan] * len(meta_df['date'])
        time_list = pd.Series(nan_list)
        bool_list = time_list.isna().tolist()

    
    if remote_dir.startswith("ftp://"):
        check_list, paths = list_remote_files(remote_dir, ethoscope_list)
    else:
        check_list, paths = list_files(remote_dir, list(set(ethoscope_list)), source=source)

    if len(paths) == 0:
        warnings.warn("No Ethoscope data could be found, please check the metadata file")
        exit()
    
    for k, i in enumerate(zip(ethoscope_list, date_list)):
        if list(i) in check_list:
            continue
        else:
            if "machine_id" in meta_df.columns:
                msg = meta_df.iloc[k]["machine_id"] + "/" + meta_df.iloc[k]["machine_name"] + "/" + i[1] + " has not been found"
            else:
                msg = f'{i[0]}/{i[1]} has not been found'

            if verbose:
                print(msg)

    # split path into parts
    database_df = pd.DataFrame()


    for path in paths:  
        split_path = path[0].split('/')
        split_series = pd.DataFrame(data = split_path).T 
        split_series.columns = ['machine_id', 'machine_name', 'date_time', 'file_name']
        split_series['file_size'] = path[1]
        database_df = pd.concat([database_df, split_series], ignore_index = True)

    #split the date_time column and add back to df
    database_df[['date', 'time']] = database_df.date_time.str.split('_', expand = True)
    database_df.drop(columns = ["date_time"], inplace = True)

    #merge df's
    if 'time' in meta_df_original.columns.tolist():
        merge_df = meta_df_original.merge(database_df, how = 'outer', on = ['machine_name', 'date', 'time'])
        merge_df.dropna(inplace = True)
    
    else:
        drop_df = database_df.sort_values(['file_size'], ascending = False)
        drop_df = drop_df.drop_duplicates(['machine_id', 'machine_name', 'date'])
        if source == "ethoscope":
            droplog = database_df[database_df.duplicated(subset=['machine_id', 'machine_name', 'date'])]
            drop_list = droplog['machine_name'].tolist()
            if len(drop_list) > 0:
                warnings.warn(f'Ethoscopes {*drop_list,} have multiple files for their day, the largest file has been kept. If you want all files for that day please add a time column')
            merge_df = meta_df_original.merge(drop_df, how = 'outer', on = ['machine_name', 'date'])
        elif source == "flyhostel":
            merge_df = meta_df_original.merge(drop_df, how = 'outer', on = ['machine_id', 'machine_name', 'date'])

        merge_df.dropna(inplace = True)

    # convert df to list and cross-reference to 'index' csv/txt to find stored paths
    path_name = merge_df['file_name'].values.tolist()
    
    # intialise path_list and populate with paths from previous
    path_list = []
    database = paths

    for i, path in enumerate(path_name):
        for j, (entry, _) in enumerate(database):
            if path in entry:
                path_list.append(entry)

    #join the db path name with the users directory
    full_path_list = []

    for j in path_list:
        win_path = Path(j)
        full_path = local_dir / win_path
        full_path_list.append(str(full_path))
    
    #create a unique id for each row, consists of first 25 char of file_name and region_id, inserted at index 0
    merge_df.insert(0, 'path', full_path_list)
    
    if source == "flyhostel":
        for i, (_, row) in enumerate(merge_df.iterrows()):
            if row["identity"] != "NONE":
                # print(merge_df[["machine_id", "machine_name", "identity"]].iloc[i])
                merge_df["region_id"].iloc[i]=row["identity"]

        
    merge_df['region_id'] = merge_df['region_id'].astype(int)
    merge_df.insert(0, 'id', merge_df['file_name'].str.slice(0,26,1) + '|' + merge_df['region_id'].map('{:02d}'.format))
    for i, id in enumerate(merge_df["id"]):
        if "1X" in id:
            merge_df["id"].iloc[i] = id.split("|")[0] + "|00"

    return merge_df

def load_qc(metadata, reference_hour=None):
    paths_ref_hours = set([tuple(e[1].tolist()) for e in list(metadata[["path", "reference_hour"]].iterrows())])
    
    qcs=[]
    for path, reference_hour_ in paths_ref_hours:
        if reference_hour_ is None:
            if reference_hour is not None:
                reference_hour_ = reference_hour
                qcs.append(read_qc_single_path(path, reference_hour=reference_hour_))
            else:
                pass
        else:
            qcs.append(read_qc_single_path(path, reference_hour=reference_hour_))

    qcs = pd.concat(qcs)
    return qcs



def load_data(i, metadata, min_time, max_time, reference_hour, cache, FUN=None, verbose=True, source="ethoscope", time_system="recording", **kwargs):


    try:
        if verbose is True:
            if metadata["machine_name"].iloc[i] == "1X":
                region_id=0
            else:
                region_id=metadata['region_id'].iloc[i]
    
            print(
                'Loading ROI_{} from {}'.format(
                    region_id,
                    f"{metadata['machine_id'].iloc[i][:10]}/{metadata['machine_name'].iloc[i]}/{metadata['date'].iloc[i]}"
                )
            )

        
        if source == "ethoscope":
            read_single_roi = read_ethoscope_single_roi
        elif source=="flyhostel":
            read_single_roi = read_flyhostel_single_roi


        meta = metadata.iloc[i,:]
        # if reference_hour is None:
        #     reference_hour=np.nan

        if np.isnan(reference_hour):
            reference_hour = meta["reference_hour"]

        data, meta_info = read_single_roi(
            meta = meta,
            min_time = min_time,
            max_time = max_time,
            reference_hour = reference_hour,
            cache = cache,
            time_system=time_system,
            **kwargs,
        )

        if data is None:
            if verbose is True:
                print('ROI_{} from {} was unable to load due to an error formatting roi'.format(metadata['region_id'].iloc[i], metadata['machine_name'].iloc[i]))
            return

        if FUN is not None:
            logger.debug("Applying function %s to centroid data", FUN)
            data = FUN(data)


        if data is None:
            if verbose is True:
                print('ROI_{} from {} was unable to load due to an error in applying the function'.format(metadata['region_id'].iloc[i], metadata['machine_name'].iloc[i]))
            return
        data.insert(0, 'id', metadata['id'].iloc[i])
        return data, meta_info
    
    except Exception as error:
        if verbose is True:
            print('ROI_{} from {} was unable to load due to an error loading roi'.format(metadata['region_id'].iloc[i], metadata['machine_name'].iloc[i]))
            print(traceback.print_exc())
            print(error)
        return

    
def load_device(metadata, min_time = 0 , max_time = float('inf'), reference_hour = None, cache = None, FUN = None, verbose = True,
                source="ethoscope", time_system="recording", n_jobs=1, **kwargs):
    """
    A wrapper function to iterate through the dataframe generated by link_meta_index() and load the corresponding database files 
    and analyse them according to the inputted fucntion.

    Params:
    metadata = pd.DataFrame object, metadata df as returned from link_meta_index function
    min_time = int, the minimum time you want to load data from with 0 being the experiment start (in seconds), for all experiments
    max_time = int, same as above
    reference_hour = int, the hour at which lights on occurs when the experiment is begun. None equals the start of the experiment
    cache = string, the local path to find and store cached versions of each ROI per database. Cached files are in a pickle format
    FUN = function, a function to apply indiviual curatation to each ROI, if None the data remains as found in the database

    returns a pandas DataFrame object containing the database data and unique ids per fly as the index
    """

    data = pd.DataFrame()
    meta_info_all=[]


    # iterate over the ROI of each ethoscope in the metadata df
    Output = joblib.Parallel(n_jobs=n_jobs)(
        joblib.delayed(
            load_data
        )(
            i, metadata, min_time, max_time, reference_hour=reference_hour, cache=cache, FUN=FUN, verbose=verbose,
            source=source, time_system=time_system, **kwargs
        )
        for i in range(len(metadata.index))
    )

    for d in Output:
        if d is not None:
            dd, meta_info = d
            data = pd.concat([data, dd], ignore_index= True)
            meta_info_all.append(meta_info)

    if source == "ethoscope":
        return data
    elif source == "flyhostel":
        return data, meta_info_all


def load_ethoscope(*args, **kwargs):
    return load_device(*args, **kwargs, source="ethoscope")

def load_flyhostel(*args, **kwargs):
    return load_device(*args, **kwargs, source="flyhostel")
