# File Format Performance Benchmarking

This repository contains a Python script that benchmarks the performance of various file storage formats when reading and writing sensor data. The script is designed to evaluate how different formats perform in terms of:

- **Write Time:** How long it takes to store the data.
- **Read Time:** How long it takes to load the data back.
- **File Size:** The resulting size of the stored data on disk.

## Overview

The script performs the following steps:

1. **Data Loading:**  
   It loads multiple fiber data files (in `.npy` format) defined in the `fiber_files` list.

2. **Format Testing:**  
   For each fiber file, the script tests multiple file formats:
   - **TXT** (using `numpy.savetxt` and `numpy.loadtxt` and tab seperators)
   - **CSV** (using `pandas.DataFrame.to_csv` and `pandas.read_csv`)
   - **Parquet** (using `pandas.DataFrame.to_parquet` and `pandas.read_parquet`)
   - **Feather** (using `pyarrow.feather.write_feather` and `pyarrow.feather.read_feather`)
   - **Zarr** (using the `zarr` library)
   - **HDF5** (using `h5py`)
   - **NetCDF** (using `netCDF4`)
   - **Pickle** (using Python’s built-in `pickle` module)
   - **JSON** (using Python’s built-in `json` module)

3. **Timing Measurements:**  
   For each file and format, the script runs 15 test cycles to measure:
   - The average write time.
   - The average read time.
   - The resulting file size on disk.

4. **Cleanup:**  
   After each test run, the generated file or directory is deleted to ensure that subsequent tests start from a clean slate.

5. **Results Reporting:**  
   Finally, all the performance metrics are aggregated and displayed.

## Dependencies

The following Python packages are required:

- [numpy](https://numpy.org/)
- [pandas](https://pandas.pydata.org/)
- [zarr](https://zarr.readthedocs.io/)
- [h5py](https://www.h5py.org/)
- [netCDF4](https://unidata.github.io/netcdf4-python/)
- [pyarrow](https://arrow.apache.org/)

You can install the third-party dependencies using pip:

```bash
pip install numpy pandas zarr h5py netCDF4 pyarrow
```

*Note:* Other modules used (like `os`, `time`, `shutil`, `pickle`, and `json`) are part of the Python standard library.

## How to Use

1. **Prepare Data Files:**  
   Ensure that your fiber `.npy` files are located in the same directory as the script or update the paths in the `fiber_files` list accordingly.

2. **Run the Script:**  
   Execute the script in Jupyter with run all

3. **Review Results:**  
   After the tests complete, the script prints a summary DataFrame showing the average write times, read times, individual run times, and file sizes for each format.

## Code Structure

- **Utility Functions:**
  - `measure_time(func, *args, **kwargs)`: Measures execution time for a given function.
  - `get_file_size(filepath)`: Returns the size (in MB) of a file or directory.

- **File Format Functions:**  
  Each file format has dedicated write and read functions that handle data conversion using libraries such as `numpy`, `pandas`, `zarr`, `h5py`, `netCDF4`, etc.

- **Test Execution Loop:**  
  The main loop iterates over each fiber file and each file format. For each combination, it:
  - Writes the file multiple times to measure write performance.
  - Reads the file multiple times to measure read performance.
  - Deletes the generated file or directory after each test to ensure consistency.

- **Results Aggregation:**  
  The performance metrics for each test (including file size) are aggregated into a pandas DataFrame and printed to the console.


## Conclusion

This benchmarking script provides a robust framework to evaluate the trade-offs between various file formats in terms of speed, efficiency, and storage. It can be easily extended to test additional formats or further customized to fit specific project requirements.
