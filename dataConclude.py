import datetime
import glob
import os.path

import numpy as np
import pandas as pd
from netCDF4 import Dataset

# 导入文件 路径
file_path = "D:/resource/chunmei/data/"
file_path = os.path.join(file_path,'TIME_lat_lon_co2_2020.csv')

#读取文件
file = pd.read_csv(file_path)
# 对“时间”字段作类型变化
file['TIME'] = pd.to_datetime(file['TIME'])

# 根据'时间'字段 以 小时 为单位 划分表
# 在 NetCDF 文件中，通常不会直接使用时间戳作为维度，而是将时间表示为从某个起始时间点开始的时间间隔
start_time = datetime.datetime(1970,1,1)
print(start_time)
file['hours_since_start'] = (file['TIME'] - start_time).dt.total_seconds()/3600
groups = file.groupby(file['hours_since_start'].astype(int))

output_path = "D:/resource/chunmei/data/output/"
# 根据时间戳，创建并导出NC文件
for hour, group_df in groups:
    timestamp = start_time + datetime.timedelta(hours=int(hour))
    year = timestamp.year
    month = timestamp.month
    day = timestamp.day
    hour = timestamp.hour
    timestamp = datetime.datetime(year, month, day, hour).strftime('%Y_%m_%d_%H')

    nc_file_name = os.path.join(output_path, f'{"RF_"+timestamp}.nc')
    # 创建NC文件
    dataset = Dataset(nc_file_name, 'w', format = 'NETCDF4')

    # 获取维度大小
    lat_groups = group_df.groupby(group_df['lat'])
    lat_size = len(lat_groups) # 大小等于‘纬度’的条数 23

    # 创建维度
    lat_dim = dataset.createDimension('lat', lat_size)
    lon_dim = dataset.createDimension('lon', 23)
    # 创建变量
    # time_var = dataset.createVariable('time', 'f8', ('time',))
    lat_var = dataset.createVariable('lat', 'f4', ('lat',))
    lon_var = dataset.createVariable('lon', 'f4', ('lon',))
    co2_var = dataset.createVariable('co2', 'f4', ('lat','lon'))

    # 设置变量属性
    lat_var.units = 'degrees_north'
    lat_var.long_name = 'latitude'
    lon_var.units = 'degrees_east'
    lon_var.long_name = 'longitude'
    co2_var.units = 'ppm'
    co2_var.coordinates = 'lat lon'

    # 设置变量值
    # time_var[:] = np.zeros(len(group_df))
    # 去掉纬度的重复值
    lat_col = group_df['lat']
    lat_col.drop_duplicates(inplace=True)
    lat_var[:] = lat_col.values
    for i in range(lat_size):
        lon_var[:] = group_df['lon'].values[i*23:(i+1)*23]
    co2_col = np.array(group_df['co2']).reshape(23, 23)
    co2_var[:] = co2_col

    # 添加时间属性
    dataset.setncattr('time', group_df['TIME'].iloc[0].strftime('%Y-%m-%d %H:%M:%S'))
    # 关闭NetCDF文件
    dataset.close()
print("导出完成")