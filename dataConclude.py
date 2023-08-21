import glob
import os.path
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
start_time = file['TIME'].min()
file['hours_since_start'] = (file['TIME'] - start_time).dt.total_seconds()/3600
groups = file.groupby(file['hours_since_start'].astype(int))

output_path = "D:/resource/chunmei/data/output/"
# 根据时间戳，创建并导出NC文件
for hour, group_df in groups:
    nc_file_name = os.path.join(output_path, f'{hour}.nc')
    # 创建NC文件
    dataset = Dataset(nc_file_name, 'w', format = 'NETCDF4')
    # 添加维度和变量
    time_dim = dataset.createDimension('time', len(group_df))
    time_var = dataset.createVariable('time', 'f8', ('time',))
    lat_var = dataset.createVariable('lat', 'f4', ('time',))
    lon_var = dataset.createVariable('lon', 'f4', ('time',))
    co2_var = dataset.createVariable('co2', 'f4', ('time',))
    # 设置变量值
    time_var[:] = group_df['hours_since_start'].values
    lat_var[:] = group_df['lat'].values
    lon_var[:] = group_df['lon'].values
    co2_var[:] = group_df['co2'].values

    # 关闭NetCDF文件
    dataset.close()

print("导出完成")