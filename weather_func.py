import os
import pandas as pd
import xarray as xr
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
import earthaccess

# Defining a function to close xarray files
@contextmanager
def close_after_use(resource):
    try:
        yield resource
    finally:
        resource.close()

# Define process_file outside of noaa_to_csv
def process_file(filename, directory, bound):
    lon_min = min(bound[0], bound[2])
    lon_max = max(bound[0], bound[2])
    lat_min = min(bound[1], bound[3])
    lat_max = max(bound[1], bound[3])
    
    file_path = os.path.join(directory, filename)
    try:
        with close_after_use(xr.open_dataset(file_path)) as ds:
            df = ds.to_dataframe().reset_index()
            df_filtered = df[(df.bnds == 0) & (df.lon < lon_max) & (df.lon > lon_min) & (df.lat > lat_min) & (df.lat < lat_max)]
            return df_filtered
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return pd.DataFrame()  

# Defining the noaa_to_csv function
def noaa_to_csv(bound, min_time, max_time, file_directory):
    auth = earthaccess.login(persist=True)
  
    # Obtaining granules 
    results = earthaccess.search_data(
        short_name='NLDAS_FORA0125_H',
        cloud_hosted=True,
        bounding_box=bound,
        temporal=(min_time, max_time),
        count=-1
    )
    
    # Retrieving links to each datafile
    data_links = [granule.data_links(access="internal") for granule in results]
  
    # Flattening the list of links
    data_links_flat = [link for sublist in data_links for link in sublist]
  
    # Filtering for only NetCDF files
    nc_links = [link for link in data_links_flat if link.endswith('.nc')]
  
    # Path for the directory to contain the netCDF files
    directory = file_directory
  
    # Downloading all files into a folder
    files = earthaccess.download(nc_links, directory)
  
    # Ensure that directory only has netCDF files within it
    nc_files = [f for f in os.listdir(directory) if f.endswith('.nc')]
  
    # Creating an empty list
    filtered_dfs = []
  
    # Mapping the "process_file" function onto each nc_file
    with ProcessPoolExecutor() as executor:
        # Pass directory and bound to process_file
        results = executor.map(process_file, nc_files, [directory]*len(nc_files), [bound]*len(nc_files)) 
        for result in results:
            if not result.empty:
                filtered_dfs.append(result)
  
    # Concatenating all files into a single dataframe
    if filtered_dfs:
        concatenated_df = pd.concat(filtered_dfs, ignore_index=True)
    else:
        concatenated_df = pd.DataFrame()
  
    return concatenated_df

# Running the function
noaa_to_csv(bound = (-74.236732, 42.044819, -73.236732, 43.044819),
            min_time = "2023-12-20 00:00:00", 
            max_time = "2023-12-21 00:00:00", 
            file_directory= "content/files")
