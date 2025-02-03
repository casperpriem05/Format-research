import pandas as pd
import zarr

def load_fiber_data(zarr_path, fiber_number, sensor_numbers):
    """
    Loads fiber timestamps and values for one or more sensors from a Zarr store.

    Parameters:
        zarr_path (str): Path to the Zarr store.
        fiber_number (int): Fiber group number to load.
        sensor_numbers (int or list of int): One or multiple sensor dataset numbers within the fiber group.

    Returns:
        df (pandas.DataFrame): A DataFrame with a 'timestamp' column and one column per sensor.
    """
    # Ensure sensor_numbers is a list
    if isinstance(sensor_numbers, int):
        sensor_numbers = [sensor_numbers]

    # Open the Zarr store in read-only mode
    zarr_store = zarr.open(zarr_path, mode='r')

    # Construct the group name for the fiber
    group_name = f"fibers_{fiber_number}"

    # Check if the group exists in the store
    if group_name not in zarr_store:
        raise KeyError(
            f"Group '{group_name}' not found in the Zarr store. "
            f"Available groups: {list(zarr_store.keys())}"
        )

    # Access the fiber group
    fiber_group = zarr_store[group_name]

    # Load the timestamps (dataset '0')
    if '0' not in fiber_group:
        raise ValueError(f"Dataset '0' (timestamps) not found in '{group_name}'.")
    timestamps = fiber_group['0'][:]

    # Create a dictionary for DataFrame construction
    data = {'timestamp': timestamps}

    # Load each requested sensor
    for s_num in sensor_numbers:
        s_str = str(s_num)
        if s_str not in fiber_group:
            raise ValueError(f"Dataset '{s_str}' not found in '{group_name}'.")

        values = fiber_group[s_str][:]

        # Check for alignment
        if len(values) != len(timestamps):
            raise ValueError(
                f"Length mismatch: {len(timestamps)} timestamps vs {len(values)} values "
                f"in dataset '{s_str}' of '{group_name}'."
            )

        # Add this sensor's data to the dictionary
        data[f'sensor_{s_num}'] = values

    # Convert to DataFrame
    df = pd.DataFrame(data)

    return df


def load_vibration_data(zarr_path, vibration_number):
    """
    Loads vibration timestamps and data from a Zarr store for a given vibration dataset.
    
    Parameters:
        path (str): Path to the Zarr store.
        vibration_number (int): The number identifying the vibration dataset to load.
        
    Returns:
        df (pandas.DataFrame): A DataFrame with 'timestamp' and 'data' columns.
    """
    # Open the Zarr store in read-only mode
    zarr_store = zarr.open(zarr_path, mode='r')
    
    # Construct the group name for the vibration dataset
    group_name = f"vibration_{vibration_number}"

    # Check if the group exists in the store
    if group_name not in zarr_store:
        raise KeyError(f"Group '{group_name}' not found in the Zarr store. "
                       f"Available groups: {list(zarr_store.keys())}")

    # Access the vibration group
    vib_group = zarr_store[group_name]

    # Ensure that both Timestamp and Data datasets exist
    if 'Timestamp' not in vib_group or 'Data' not in vib_group:
        raise ValueError(f"Group '{group_name}' must contain 'Timestamp' and 'Data' datasets.")

    # Load the timestamps and data arrays
    timestamps = vib_group['Timestamp'][:]
    values = vib_group['Data'][:]

    # Check alignment
    if len(timestamps) != len(values):
        raise ValueError(f"Mismatch between timestamps ({len(timestamps)}) and values ({len(values)}) in '{group_name}'.")

    # Create the DataFrame
    df = pd.DataFrame({
        'timestamp': timestamps,
        'data': values
    })

    return df


def load_other(path, group_name):
    """
    Loads data from a Zarr store for a given group, with optional timestamps.
    
    Parameters:
        path (str): Path to the Zarr store.
        group_name (str): Name of the group to load (e.g., 'environment_rpm', 'environment_temperature', 'load_temperature').
        
    Returns:
        df (pandas.DataFrame): A DataFrame with relevant columns (timestamps included if available).
    """
    # Open the Zarr store in read-only mode
    zarr_store = zarr.open(path, mode='r')
    
    # Check if the group exists in the store
    if group_name not in zarr_store:
        raise KeyError(f"Group '{group_name}' not found in the Zarr store. "
                       f"Available groups: {list(zarr_store.keys())}")

    # Access the group
    data_group = zarr_store[group_name]

    # Check for available datasets
    available_keys = list(data_group.keys())
    if not available_keys:
        raise ValueError(f"No datasets found in group '{group_name}'.")

    # Check if there is a timestamp dataset (optional)
    timestamp_column = None
    if '__time_UTC__s__' in data_group:
        timestamp_column = '__time_UTC__s__'
        timestamps = data_group[timestamp_column][:]
    else:
        timestamps = None  # No timestamps available

    # Load all other datasets dynamically
    data_columns = [key for key in available_keys if key != timestamp_column]
    if not data_columns:
        raise ValueError(f"No data columns found in group '{group_name}'.")

    # Load all data columns
    data = {col: data_group[col][:] for col in data_columns}

    # Create the DataFrame
    if timestamps is not None:
        df = pd.DataFrame({
            'timestamp': timestamps,
            **data
        })
    else:
        df = pd.DataFrame(data)

    return df


import pandas as pd

def process_fibers(fiber_1: pd.DataFrame, fiber_2: pd.DataFrame) -> pd.DataFrame:
    fiber_2 = fiber_2.rename(
        columns={
            "sensor_1": "f_sensor_2_1",
            "sensor_2": "f_sensor_2_2",
            "sensor_3": "f_sensor_2_3",
            "sensor_4": "f_sensor_2_4",
            "sensor_5": "f_sensor_2_5"
        }
    )
    fiber_1 = fiber_1.rename(
        columns={
            "sensor_1": "f_sensor_1_1",
            "sensor_2": "f_sensor_1_2",
            "sensor_3": "f_sensor_1_3",
            "sensor_4": "f_sensor_1_4",
            "sensor_5": "f_sensor_1_5"
        }
    )
    
    fiber_1['timestamp'] = pd.to_datetime(fiber_1['timestamp'], unit='ns')
    fiber_2['timestamp'] = pd.to_datetime(fiber_2['timestamp'], unit='ns')

    fiber = pd.merge_asof(
        left=fiber_1,
        right=fiber_2,
        left_on='timestamp',
        right_on='timestamp',
        direction='nearest',
        tolerance=pd.Timedelta('1ms')
    )

    fiber = fiber.drop('f_sensor_2_5', axis=1).dropna()

    return fiber



import pandas as pd

def process_vibration(
    vibration_101: pd.DataFrame, 
    vibration_102: pd.DataFrame, 
    vibration_103: pd.DataFrame,
    sample_rate: int = 25000,
) -> pd.DataFrame:

    # 1) Rename the 'data' columns
    vibration_101 = vibration_101.rename(columns={"data": "vib_101"})
    vibration_102 = vibration_102.rename(columns={"data": "vib_102"})
    vibration_103 = vibration_103.rename(columns={"data": "vib_103"})

    # 2) Combine into a single DataFrame
    vibration_101["vib_102"] = vibration_102["vib_102"]
    vibration_101["vib_103"] = vibration_103["vib_103"]

    # Rename for clarity
    vibration = vibration_101

    # 3) Convert timestamp from seconds to DateTime
    vibration['timestamp'] = pd.to_datetime(vibration['timestamp'], unit='s')

    # 4) Adjust timestamps for correct alignment using sample rate
    #    (Revised so 'sample_index' is an integer, not a datetime)
    blocks = vibration['timestamp'].notna().cumsum()

    # block_start: the first timestamp in each block
    vibration['block_start'] = vibration.groupby(blocks)['timestamp'].transform('first')

    # sample_index: counts from 0, 1, 2... for each block
    vibration['sample_index'] = vibration.groupby(blocks).cumcount()

    # Recompute the timestamp
    vibration['timestamp'] = (
        vibration['block_start'] 
        + pd.to_timedelta(vibration['sample_index'] / sample_rate, unit='s')
    )

    vibration.drop(columns=['block_start', 'sample_index'], inplace=True)

    return vibration



