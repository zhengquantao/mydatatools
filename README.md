### 深圳量云IB库操作组件

#### 注意
1. 需要config.ini和点表映射汇总表.csv


#### 打包流程
```bash
python setup.py sdist --format=gztar
```
*注：打包生成的包存放在dist文件夹中*

#### 安装流程
```bash
# 常规安装
python -m pip install mydatatools.tar.gz

# 指定镜像安装
python -m pip install mydatatools.tar.gz -i https://pypi.douban.com/simple
```

#### 使用方式
```python
from mydatatools.MyDataTools import MyDataTools
dt = MyDataTools()
farm_name = get_farm_name(farm_code)
all_cols = ['电网有功功率', '齿轮箱油温', '齿轮箱油泵1出口压力', '齿轮箱油泵2出口压力']

df = dt.get_data(farm_code=farm_code, turbine_code=turbine_code, col_names=all_cols,
                 start_date=start_date, end_date=end_date)

# =============================
#                      电网有功功率  齿轮箱油温  齿轮箱油泵1出口压力  齿轮箱油泵2出口压力
# 时间                                                        
# 2022-08-01 00:00:00  1130.0   52.8       13.37       13.02
# 2022-08-01 00:00:01  1134.0   52.8       13.31       12.88
# 2022-08-01 00:00:02  1120.0   52.8       13.39       12.97
# 2022-08-01 00:00:03  1130.0   52.8       13.40       12.97
# 2022-08-01 00:00:04  1116.0   52.8       13.35       12.98
# ...                     ...    ...         ...         ...
# 2022-08-30 23:59:55   191.0   50.9       11.69       11.31
# 2022-08-30 23:59:56   195.0   50.9       11.72       11.33
# 2022-08-30 23:59:57   192.0   51.0       11.69       11.26
# 2022-08-30 23:59:58   197.0   50.9       11.79       11.42
# 2022-08-30 23:59:59   200.0   51.0       11.67       11.34
# [2587697 rows x 4 columns]
```