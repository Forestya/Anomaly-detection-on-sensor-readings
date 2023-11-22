# Anomaly Detection from Sensor Readings

## Project Overview
This project aims to detect anomalies in sensor readings using the MapReduce programming model. The dataset consists of sensor readings collected over time, with each record containing a station name, a date, and a numerical sensor reading value reporting the humidity.

## Problem Definition
The task is to utilize MRJob to detect anomalies from the readings for each sensor. The steps are as follows:

1. For each sensor, calculate the daily average humidity.
2. For each sensor, calculate the overall average humidity.
3. For each sensor on each day, calculate the gap between the daily and the overall average.
4. Report all the readings such that the gap between the daily average and the overall average for that sensor on that day is larger than a given threshold τ.

## Output Format
The output contains three fields: the station name, the date, and the gap, in the format of "<the station name>\t<the date>,<the gap>". The result is sorted by the station name alphabetically first and then by the date in descending order.

## Code Execution
The code should take three parameters: the input file, the output folder on HDFS, and the threshold value τ. The command to run the code is as follows:

```bash
$ python3 project1.py -r hadoop input -o hdfs_output --jobconf myjob.settings.tau=20 --jobconf mapreduce.job.reduces=2
