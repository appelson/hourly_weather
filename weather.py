# Importing Packages
import earthaccess
import pandas as pd
import os
import xarray as xr
from concurrent.futures import ProcessPoolExecutor

# Authenticating earth access login registration can be found here: 
# https://www.earthdata.nasa.gov/eosdis/science-system-description/eosdis-components/earthdata-login
auth = earthaccess.login(persist=True)

# Obtaining granules 
results = earthaccess.search_data(
    # Defining the dataset
    short_name='NLDAS_FORA0125_H',
    cloud_hosted=True,
    
    # Creating a bounding box for St. Tammany Parish
    bounding_box=(-90.258, 30.139, -89.495, 30.712),
    
    # Defining min and max dates that we want data from
    temporal=("2024-01-01 00:00:00", "2024-07-01 23:59:59"),
    
    # Retrieving all granules
    count=-1
)

# Retrieving links to each datafile
data_links = [granule.data_links(access="internal") for granule in results]

# Flattening the list of links
data_links_flat = [link for sublist in data_links for link in sublist]

# Filtering for only NetCDF files
nc_links = [link for link in data_links_flat if link.endswith('.nc')]

# Path for the directory to contain the netCDF files
directory = '/content/nc_files'

# Downloading all files into a folder titled "nc_files"
files = earthaccess.download(nc_links, directory)

# Defining a function to download the netCDF files as CSVs
def process_file(filename):
    file_path = os.path.join(directory, filename)
    ds = xr.open_dataset(file_path)
    df = ds.to_dataframe().reset_index()
    
    # Filtering data to only be within the St. Tammany Parish Sheriff's Office bounding box
    df_filtered = df[(df.bnds == 0) & (df.lon < -89.495) & (df.lon > -90.258) & (df.lat > 30.139) & (df.lat < 30.712)]
    ds.close()
    return df_filtered

# Ensure that nc_files only has netCDF files within it
nc_files = [f for f in os.listdir(directory) if f.endswith('.nc')]

# Creating an empty list
filtered_dfs = []

# Defining a function to map the "process_file" function onto each nc_file
with ProcessPoolExecutor() as executor:
    results = executor.map(process_file, nc_files)
    for result in results:
        filtered_dfs.append(result)

# Concatinating all files into a single dataframe
concatenated_df = pd.concat(filtered_dfs, ignore_index=True)

# Converting temperature in Kelvin to Fahrenheit
concatenated_df['Tair_f'] = 1.8 * (concatenated_df['Tair'] - 273.15) + 32

# Finding the mean temperature and precipitation for lat and lon's in STP
hourly_df = concatenated_df.groupby('time').agg({
    'Tair_f': 'mean',
    'CRainf_frac': 'mean',
    'Rainf': 'mean'
}).reset_index()

print(hourly_df)
