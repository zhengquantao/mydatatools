import datetime
from urllib.parse import quote

import pandas as pd
from sqlalchemy import create_engine

from mydatatools.read_config import *


class MyDataTools(object):
    """
    明阳量云集控数据获取工具
    """
    def __init__(self, ibdb_info=None, mysqldb_info=None, **kwargs):

        if ibdb_info is None:
            self.ibdb_info = {
                'user': IB_USER,
                'password': quote(IB_PASSWORD),
                'host': IB_HOST,
                'port': IB_PORT,
            }
        else:
            self.ibdb_info = ibdb_info

        if mysqldb_info is None:
            self.mysqldb_info = {
                'user': BASE_USER,
                'password': quote(BASE_PASSWORD),
                'host': BASE_HOST,
                'port': BASE_PORT,
                'database': BASE_DB
            }
        else:
            self.mysqldb_info = mysqldb_info

        if not IB_TURBINE_TYPE:
            # 风机基础信息表
            sqlstr = "select * from tb_wind_base_wtgs;"
            engine_mysql = create_engine(
                'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s?charset=utf8' % self.mysqldb_info
            )
            self.turbine_base_info = pd.read_sql_query(sqlstr, engine_mysql)

        # 点表信息
        # 测试先按照文件读入，后续可以录入数据库进行维护
        map_table_path = os.path.join(".", '点表映射汇总表.csv')
        self.all_map_data = pd.read_csv(map_table_path)
        self.all_map_data[['FARM_CODE', 'TURBINE_CODE']] = self.all_map_data[['FARM_CODE', 'TURBINE_CODE']].astype('str')

        self.kwargs = kwargs

    def get_type_by_turbine(self, farm_code, turbine_code):
        """
        根据风机查询风机型号
        farm_code：需要查询的风场，例：'30000'
        turbine_code：需要查询的机组号，例：'30000001' 或者'001'
        return：所查询机型，例：'SE8715'
        """
        if len(turbine_code) <= 3:
            turbine_code = farm_code + turbine_code.zfill(3)

        df_turbine = self.turbine_base_info.query('CODE_ == @turbine_code')
        if len(df_turbine) == 0:
            result = '未查询到{}机组信息'.format(turbine_code)
        else:
            types = df_turbine['MODEL_'].unique().tolist()
            if len(types) > 1:
                types_str = ','.join(types)
                result = f'机组{turbine_code}存在多个机型{types_str}'
            else:
                result = types[0]
        return result

    def get_tag_map_by_type(self, turbine_type):
        """
        通过机型返回通用机型对应的点表映射dict
        """
        df_map = self.all_map_data.query('TYPE == @turbine_type')
        if len(df_map) == 0:
            print('未查询到{}机型点表信息'.format(turbine_type))
            return {}
        else:
            df_map.set_index('GENERAL_NAME_CH', inplace=True, drop=True)
            df_map = df_map[df_map['TAG_NAME_EN'].notnull()]
            return df_map['TAG_NAME_EN'].to_dict()

    def get_tag_map_by_farm_type(self, farm_code, type):
        """
        通过机型返回通用机型对应的点表映射dict
        """
        df_map = self.all_map_data.query('FARM_CODE == @farm_code')
        df_map = df_map.query('TYPE == @type')
        if len(df_map) == 0:
            print('未查询到风场{}机型{}的点表信息'.format(farm_code, type))
            return {}
        else:
            df_map.set_index('GENERAL_NAME_CH', inplace=True, drop=True)
            df_map = df_map[df_map['TAG_NAME_EN'].notnull()]
            return df_map['TAG_NAME_EN'].to_dict()

    def get_tag_map_by_turbine_code(self, turbine_code):
        """
        通过机型返回通用机型对应的点表映射dict
        """
        df_map = self.all_map_data.query('TURBINE_CODE == @turbine_code')
        if len(df_map) == 0:
            print('未查询到机组编号{}的点表信息'.format(turbine_code))
            return {}
        else:
            df_map.set_index('GENERAL_NAME_CH', inplace=True, drop=True)
            df_map = df_map[df_map['TAG_NAME_EN'].notnull()]
            return df_map['TAG_NAME_EN'].to_dict()

    def get_tag_map(self, farm_code, turbine_code):
        """
        根据风场，风机查询通用字段名与点表的对应关系dict
        """
        if len(turbine_code) <= 3:
            turbine_code = farm_code + turbine_code.zfill(3)
        # 1，优先按照自定义风机进行查询
        if turbine_code in self.all_map_data['TURBINE_CODE'].unique():
            return self.get_tag_map_by_turbine_code(turbine_code)
        # 2，优先按照自定义风场进行查询
        if farm_code in self.all_map_data['FARM_CODE'].unique():
            t_type = self.get_type_by_turbine(farm_code, turbine_code)
            return self.get_tag_map_by_farm_type(farm_code, t_type)
        # 3，最后按照通用机型查询。
        t_type = IB_TURBINE_TYPE or self.get_type_by_turbine(farm_code, turbine_code)

        return self.get_tag_map_by_type(t_type)

    def get_data_old(self, farm_code, turbine_code, col_names=None, start_date=None, end_date=None, db_yesr=None):
        """
        获取指定机组，指定数据列col，开始结束时间段对应的数据
        col_names == 'all'时，获取全部通用字段数据，
        tags_names == 'all'时，获取所有表里的字段数据。
        """
        if db_yesr is None:
            if end_date is not None:
                db_yesr = end_date[:4]
            else:
                if start_date is not None:
                    db_yesr = start_date[:4]
                else:
                    db_yesr = str(datetime.date.today().year)
        self.ibdb_info['database'] = 'db' + farm_code + '_' + db_yesr
        engine = create_engine(
            'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s?charset=utf8' % self.ibdb_info
        )
        table_name = 't' + farm_code + turbine_code + '_all'

        # 根据col_names 映射为实际的点表tags
        tag_maps_all = self.get_tag_map(farm_code, turbine_code)

        if col_names is None:
            tags_lis = list(tag_maps_all.values())
        else:
            if not isinstance(col_names, list):
                col_names = [col_names]
            tags_lis = [tag_maps_all[c] for c in col_names if c in tag_maps_all or c in tag_maps_all.values()]
            
            if len(tags_lis) < len(col_names):
                err_cols = [c for c in col_names if c not in tag_maps_all and c not in tag_maps_all.values()]
                print('warring 部分给定的字段名有误。', err_cols)
        if len(tags_lis) == 0:
            print('请输入准确的字段名称！')
            return pd.DataFrame()


        # 查询数据库已有字段名,对需要的tag进行校验
        sql_column_names = f"SELECT * FROM {table_name} limit 1;"
        df_limit1 = pd.read_sql_query(sql_column_names, engine)
        # df_limit1.to_csv(f'{table_name}_limit1.csv')
        all_exist_tags = df_limit1.columns
        valid_lis = [t for t in tags_lis if t in all_exist_tags]
        not_exist_tags = [t for t in tags_lis if t not in all_exist_tags]
        if len(not_exist_tags) > 0:
            print('warring! 部分字段对应的点表名数据库中不存在：' + '，'.join(not_exist_tags))

        tags_lis = ['real_time'] + valid_lis
        tags = ','.join(tags_lis)

        sqlstr = f"SELECT {tags} FROM {table_name}"

        # 调试用，获取所有数据
        # sqlstr = f"SELECT * FROM {table_name}"


        if start_date is not None:
            sqlstr = sqlstr + f" WHERE real_time >= '{start_date}'"
            if end_date is not None:
                sqlstr = sqlstr + f" AND real_time <= '{end_date}'"
        else:
            if end_date is not None:
                sqlstr = sqlstr + f" WHERE real_time < '{end_date}'"
        sqlstr = sqlstr + ';'
        # sqlstr = "select * from t30000001_all limit 10;"

        df = pd.read_sql_query(sqlstr, engine)
        # 这里读出来的列名是点表，需要转换为通用名称
        # map_dict = {}
        rename_dict = {v: m for m, v in tag_maps_all.items()}
        df.rename(columns=rename_dict, inplace=True)
        # 数据预处理
        # df = df[~df.duplicated(subset=['sampling_time', 'point_name', 'sampling_frequency'], keep='last')]
        # 设置时间索引
        df['real_time'] = pd.to_datetime(df['real_time'])
        df.set_index('real_time', drop=True, inplace=True)
        return df

    def field_format(self, field, str_format=IB_FIELD_FORMAT):
        return str_format.format(field) if str_format else field

    def handle_database(self, farm_code, start_date, end_date,):

        if end_date is not None:
            db_yesr = end_date[:4]
        else:
            if start_date is not None:
                db_yesr = start_date[:4]
            else:
                db_yesr = str(datetime.date.today().year)
        return 'db' + farm_code + '_' + db_yesr

    def get_data(self, farm_code, turbine_code, db_yesr=IB_DB, table_name=IB_TABLE, col_names=None, start_date=None,
                 end_date=None, field_format=IB_FIELD_FORMAT):
        """
        获取指定机组，指定数据列col，开始结束时间段对应的数据
        col_names == 'all'时，获取全部通用字段数据，
        tags_names == 'all'时，获取所有表里的字段数据。
        """
        mysqldb_info = self.ibdb_info
        mysqldb_info['database'] = db_yesr or self.handle_database(farm_code, start_date, end_date)
        engine = create_engine(
            'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s?charset=utf8' % mysqldb_info
        )
        if not table_name:
            table_name = farm_code + turbine_code

        if IB_TABLE_FORMAT:
            table_name = IB_TABLE_FORMAT.format(table_name)

        # 根据col_names 映射为实际的点表tags
        tag_maps_all = self.get_tag_map(farm_code, turbine_code)

        if col_names is None:
            tags_lis = list(tag_maps_all.values())
        else:
            if not isinstance(col_names, list):
                col_names = [col_names]
            tags_lis = [tag_maps_all[c] for c in col_names if c in tag_maps_all or c in tag_maps_all.values()]
            if len(tags_lis) < len(col_names):
                print('warring 部分给定的字段名有误。')
        if len(tags_lis) == 0:
            print('请输入准确的字段名称！')
            return pd.DataFrame()

        tags_lis = [self.field_format(i, field_format) for i in tags_lis]

        # 查询数据库已有字段名,对需要的tag进行校验
        sql_column_names = f"SELECT * FROM {table_name} limit 1;"
        df_limit1 = pd.read_sql_query(sql_column_names, engine)
        all_exist_tags = df_limit1.columns
        valid_lis = [t for t in tags_lis if t in all_exist_tags]
        not_exist_tags = [t for t in tags_lis if t not in all_exist_tags]
        if len(not_exist_tags) > 0:
            print('warring! 部分字段对应的点表名数据库中不存在：' + '，'.join(not_exist_tags))

        tag_time = self.all_map_data[(self.all_map_data["TYPE"] == IB_TURBINE_TYPE) &
                                     (self.all_map_data["GENERAL_NAME_CH"] == "时间")]["TAG_NAME_EN"].iloc[0]
        tag_turbine_id = self.all_map_data[(self.all_map_data["TYPE"] == IB_TURBINE_TYPE) &
                                           (self.all_map_data["GENERAL_NAME_CH"] == "风机编号")]["TAG_NAME_EN"].iloc[0]
        tags_lis = [tag_time] + valid_lis
        tags = ','.join(tags_lis)

        turbine_id = farm_code + turbine_code
        sqlstr = f"SELECT {tags} FROM {table_name} where {tag_turbine_id}='{turbine_id}'"

        # 调试用，获取所有数据
        # sqlstr = f"SELECT * FROM {table_name}"

        if start_date is not None:
            sqlstr = sqlstr + f" AND {tag_time} >= '{start_date}'"
        if end_date is not None:
            sqlstr = sqlstr + f" AND {tag_time} < '{end_date}'"
        sqlstr = sqlstr + ';'
        df = pd.read_sql_query(sqlstr, engine)
        # 这里读出来的列名是点表，需要转换为通用名称
        # map_dict = {}
        rename_dict = {self.field_format(v, field_format) if m not in ["时间", "风机编号"] else v: m for m, v in tag_maps_all.items()}
        df.rename(columns=rename_dict, inplace=True)
        # 数据预处理
        # df = df[~df.duplicated(subset=['sampling_time', 'point_name', 'sampling_frequency'], keep='last')]
        # 设置时间索引
        df["时间"] = pd.to_datetime(df["时间"])
        df.set_index("时间", drop=True, inplace=True)
        return df


    # def get_rated_power_by_turbine(self, farm, turbine_num):
    #     """
    #     farm：需要查询的风场，例：'TYSFCA'
    #     turbine_num：需要查询的机组号，例：'001'
    #     return：所查询机组的额定功率，例：2500
    #     """
    #
    #     df_turbine = self.df_wind_farm_turbine.query('pinyin_code == @farm & inner_turbine_name == @turbine_num')
    #     if len(df_turbine) == 0:
    #         result = '数据库表df_wind_farm_turbine中缺少 {}_{} 机组信息'.format(farm, turbine_num)
    #     else:
    #         result = df_turbine['rated_power'].unique().tolist()[0]
    #         if str(result) not in ['nan', 'None']:
    #             result = float(result)
    #         else:
    #             result = '数据库表df_wind_farm_turbine中缺少 {}_{} 机组信息'.format(farm, turbine_num)
    #
    #     return result
    #
    # def get_power_curve_by_turbine(self, farm, turbine_num):
    #     """
    #     farm：需要查询的风场，例：'TYSFCA'
    #     turbine_num：需要查询的机组号，例：'001'
    #     return：所查询机组的理论功率曲线,返回pandas.DataFrame,columns=['Wind', 'Power']
    #     """
    #
    #     df_turbine = self.df_wind_farm_turbine.query('pinyin_code == @farm & inner_turbine_name == @turbine_num')
    #     if len(df_turbine) == 0:
    #         result = '数据库表df_wind_farm_turbine中缺少 {}_{} 机组信息'.format(farm, turbine_num)
    #     else:
    #         turbine_id = df_turbine['turbine_id'].values[0]
    #         farm_id = df_turbine['farm_id'].values[0]
    #         df_power_curve = self.df_turbine_type_powercurve.query('farm_id == @farm_id & turbine_id == @turbine_id')
    #         if len(df_power_curve) == 0:
    #             result = '数据库表turbine_type_powercurve中缺少 {}_{} 机组相关id信息'.format(farm, turbine_num)
    #         else:
    #             power_curve = df_power_curve['power_curve'].unique().tolist()[0]
    #             if power_curve:
    #                 result = dict()
    #                 wind = list(json.loads(power_curve).keys())
    #                 wind = [float(x) for x in wind]
    #                 power = list(json.loads(power_curve).values())
    #                 power = [float(x) for x in power]
    #                 while power[-1] == 0:
    #                     power.pop()
    #                 wind = wind[:len(power)]
    #                 result['Wind'] = wind
    #                 result['Power'] = power
    #                 result = pd.DataFrame(result)
    #             else:
    #                 result = '数据库表turbine_type_powercurve中缺少 {}_{} 机组理论功率曲线信息'.format(farm, turbine_num)
    #
    #     return result
    #
    # def get_types_by_farm(self, farm):
    #     """
    #     farm：需要查询的风场，例：'TYSFCA'
    #     return：所查询风场的机型list
    #     """
    #
    #     df_farm = self.df_wind_farm_turbine.query('pinyin_code == @farm')
    #     if len(df_farm) == 0:
    #         result = '数据库表df_wind_farm_turbine中缺少 {} 风场信息'.format(farm)
    #     else:
    #         farm_id = df_farm['farm_id'].unique().tolist()[0]
    #         df_turbin_types = self.df_turbine_type_powercurve.query('farm_id == @farm_id')
    #         if len(df_turbin_types) == 0:
    #             result = '数据库表turbine_type_powercurve中缺少 {} 风场相关id信息'.format(farm)
    #         else:
    #             result = df_turbin_types['type_name'].unique().tolist()
    #             if str(result) in ['nan', 'None']:
    #                 result = '数据库表turbine_type_powercurve中缺少 {} 风场相关id信息'.format(farm)
    #
    #     return result
    #
    # def get_turbines_by_farm(self, farm):
    #     """
    #     farm：需要查询的风场，例：'TYSFCA'
    #     return：所查询风场下所有风机号list
    #     """
    #
    #     df_farm = self.df_wind_farm_turbine.query('pinyin_code == @farm')
    #     if len(df_farm) == 0:
    #         result = '数据库表df_wind_farm_turbine中缺少 {} 风场信息'.format(farm)
    #     else:
    #         result = df_farm['inner_turbine_name'].unique().tolist()
    #         if str(result) in ['nan', 'None']:
    #             result = '数据库表df_wind_farm_turbine中缺少 {} 风场信息'.format(farm)
    #         else:
    #             result.sort()
    #
    #     return result
    #
    # def get_turbines_by_type(self, farm, type_name):
    #     """
    #     farm：需要查询的风场，例：'TYSFCA'
    #     type_name：需要查询的机型，例：'SE8715'
    #     return：所查询风场与机型下所有风机号list
    #     """
    #
    #     df_farm = self.df_wind_farm_turbine.query('pinyin_code == @farm')
    #     if len(df_farm) == 0:
    #         result = '数据库表df_wind_farm_turbine中缺少 {} 风场信息'.format(farm)
    #     else:
    #         farm_id = df_farm['farm_id'].unique().tolist()[0]
    #         df_turbin_type = self.df_turbine_type_powercurve.query('farm_id == @farm_id & type_name == @type_name')
    #         if len(df_turbin_type) == 0:
    #             result = '数据库表turbine_type_powercurve中缺少 {} 风场相关信息'.format(farm)
    #         else:
    #             turbines = df_turbin_type['turbine_id'].unique().tolist()
    #             result = df_farm.query('turbine_id in @turbines')['inner_turbine_name'].unique().tolist()
    #             if str(result) in ['nan', 'None']:
    #                 result = '数据库表turbine_type_powercurve中缺少 {} 风场相关信息'.format(farm)
    #             else:
    #                 result.sort()
    #
    #     return result
    #
    #
    #
    # def get_power_curve_by_type(self, farm, type_name):
    #     """
    #     farm：需要查询的风场，例：'TYSFCA'
    #     tb_type：需要查询的机组型号，例：'SE8715'
    #     return：所查询机型的理论功率曲线,返回pandas.DataFrame,columns=['Wind', 'Power']
    #     """
    #
    #     df_farm = self.df_wind_farm_turbine.query('pinyin_code == @farm')
    #     if len(df_farm) == 0:
    #         result = '数据库表df_wind_farm_turbine中缺少 {}_{} 型号机组信息'.format(farm, type_name)
    #     else:
    #         farm_id = df_farm['farm_id'].values[0]
    #         df_power_curve = self.df_turbine_type_powercurve.query('farm_id == @farm_id & type_name == @type_name')
    #         if len(df_power_curve) > 0:
    #             power_curve = df_power_curve['power_curve'].unique().tolist()[0]
    #             if power_curve:
    #                 result = dict()
    #                 wind = list(json.loads(power_curve).keys())
    #                 wind = [float(x) for x in wind]
    #                 power = list(json.loads(power_curve).values())
    #                 power = [float(x) for x in power]
    #                 while power[-1] == 0:
    #                     power.pop()
    #                 wind = wind[:len(power)]
    #                 result['Wind'] = wind
    #                 result['Power'] = power
    #
    #                 result = pd.DataFrame(result)
    #             else:
    #                 result = '数据库表turbine_type_powercurve中缺少 {}_{} 型号机组理论功率曲线信息'.format(farm, type_name)
    #         else:
    #             result = '数据库表turbine_type_powercurve中缺少 {}_{} 型号机组相关信息'.format(farm, type_name)
    #
    #     return result
    #
    # def get_chinese_name_by_farm(self, farm):
    #     """
    #     farm：需要查询的风场，例：'TYSFCA'
    #     return：所查询风场的中文名，如果数据库中不存在中文名，则返回字符串'None'
    #     """
    #
    #     df_farm = self.df_wind_farm_turbine.query('pinyin_code == @farm')
    #     if len(df_farm) == 0:
    #         result = '数据库表df_wind_farm_turbine中缺少 {} 风场信息'.format(farm)
    #     else:
    #         result = str(df_farm['farm_name'].unique()[0])
    #         if str(result) in ['nan', 'None']:
    #             result = '数据库表df_wind_farm_turbine中缺少 {} 风场信息'.format(farm)
    #
    #     return result
    #
    # def get_py_code_by_farm(self, chinese_name):
    #     """
    #     chinese_name：需要查询的风场的中文名，例：'太阳山二期'
    #     return：所查询风场的拼音缩写，如果数据库中不存在拼音缩写，则返回字符串'None'
    #     """
    #
    #     df_farm = self.df_wind_farm_turbine.query('farm_name == @chinese_name')
    #     if len(df_farm) == 0:
    #         result = '数据库表df_wind_farm_turbine中缺少 {} 风场信息'.format(chinese_name)
    #     else:
    #         result = str(df_farm['pinyin_code'].unique()[0])
    #         if str(result) in ['nan', 'None']:
    #             result = '数据库表df_wind_farm_turbine中缺少 {} 风场信息'.format(chinese_name)
    #
    #     return result
    #
    # def get_etl_type_by_farm(self, farm):
    #     """
    #     farm：需要查询的风场，例：'TYSFCA'
    #     return：所查询风场下 {风机号: etl_type}
    #     """
    #
    #     df_farm = self.df_wind_farm_turbine.query('pinyin_code == @farm')
    #     if len(df_farm) == 0:
    #         type_result = '数据库表df_wind_farm_turbine中缺少 {} 风场信息'.format(farm)
    #     else:
    #         result = df_farm['inner_turbine_name'].unique().tolist()
    #         if str(result) in ['nan', 'None']:
    #             type_result = '数据库表df_wind_farm_turbine中缺少 {} 风场信息'.format(farm)
    #         else:
    #             result.sort()
    #             type_result = dict([(turbine, df_farm.loc[df_farm['inner_turbine_name'] == turbine]['etl_type'].max())
    #                                 for turbine in result])
    #
    #     return type_result
    #
    # def get_speed_by_turbine(self, farm, turbine):
    #     """
    #     farm: 需要查询的风场，例："TYSFCA"
    #     turbine: 需要查询的机组号，例："001"
    #     return：所查询机组的额定转速和并网转速，返回pandas.DataFrame, columns = ['rated_speed', 'grid_speed']
    #     """
    #
    #     df_turbine = self.df_wind_farm_turbine.query('pinyin_code == @farm & inner_turbine_name == @turbine')
    #     if len(df_turbine) == 0:
    #         result = '数据库表df_wind_farm_turbine中缺少 {}_{} 机组信息'.format(farm, turbine)
    #     else:
    #         rated_speed = df_turbine['rated_speed'].unique().tolist()[0]
    #         grid_speed = df_turbine['grid_speed'].unique().tolist()[0]
    #         if str(rated_speed) in ['nan', 'None']:
    #             result = '数据库表df_wind_farm_turbine中缺少 {}_{} 额定转速信息'.format(farm, turbine)
    #         elif str(grid_speed) in ['nan', 'None']:
    #             result = '数据库表df_wind_farm_turbine中缺少 {}_{} 并网转速信息'.format(farm, turbine)
    #         else:
    #             result = pd.DataFrame([[rated_speed, grid_speed]], columns=['rated_speed', 'grid_speed'])
    #     return result
    #
    # def get_pch2a_acc_by_turbine(self, farm, turbine):
    #     """
    #     farm: 需要查询的风场，例“TYSFCA”
    #     turbine: 需要查询的机组号，例"001"
    #     return: 所查询机组的X通道加速度信号的传感器位置，返回str,前后/左右，缺失时默认前后
    #     """
    #
    #     df_turbine = self.df_wind_farm_turbine.query('pinyin_code == @farm & inner_turbine_name == @turbine')
    #     if len(df_turbine) == 0:
    #         result = '数据库表df_wind_farm_turbine中缺少 {}_{} 机组信息'.format(farm, turbine)
    #     else:
    #         result = df_turbine['Pch2A_Acc'].unique().tolist()[0]
    #         if str(result) in ['nan', 'None']:
    #             result = '前后'
    #     return result
    #
    # def get_farm_id_by_farm(self, farm):
    #     """
    #     farm：需要查询的风场，例：'TYSFCB'
    #     return：所查询风场的风场id
    #     """
    #
    #     df_farm = self.df_wind_farm_turbine.query('pinyin_code == @farm')
    #     if len(df_farm) == 0:
    #         result = '数据库表df_wind_farm_turbine中缺少 {} 风场信息'.format(farm)
    #     else:
    #         result = str(df_farm['farm_id'].unique()[0])
    #         if str(result) in ['nan', 'None']:
    #             result = '数据库表df_wind_farm_turbine中缺少 {} 风场信息'.format(farm)
    #     return result
    #
    # def get_turbine_id_by_turbine(self, farm, turbine):
    #     """
    #     farm: 需要查询的风场，例“TYSFCA”
    #     turbine: 需要查询的机组号，例"001"
    #     return: 所查询机组风机编号
    #     """
    #
    #     df_turbine = self.df_wind_farm_turbine.query('pinyin_code == @farm & inner_turbine_name == @turbine')
    #     if len(df_turbine) == 0:
    #         result = '数据库表df_wind_farm_turbine中缺少 {}_{} 机组信息'.format(farm, turbine)
    #     else:
    #         result = df_turbine['turbine_id'].unique().tolist()[0]
    #         if str(result) in ['nan', 'None']:
    #             result = '数据库表df_wind_farm_turbine中缺少 {}_{} 机组信息'.format(farm, turbine)
    #     return result


if __name__ == '__main__':
    # import pandas as pd
    # data_con = ib_conn('db30000_2024')
    # sqlstr = "select * from t30000001_all limit 10;"
    # df = pd.read_sql(sqlstr, data_con)
    # print(df)

    # 数据获取工具
    # 输入farm_code, turbine_code, start_date, end_date, col_names, read_db
    # 返回查询的数据DataFrame

    dt = MyDataTools()
    df = dt.get_data(farm_code='30000', turbine_code='001', col_names=['风速十分钟平均值', '齿轮箱主轴承温度', '电网有功功率'],
                     start_date='2024-03-24', end_date='2024-03-25')

    # df = dt.get_data(farm_code='30000', turbine_code='001',
    #                  start_date='2024-03-24', end_date='2024-03-25')

    # 电网有功功率 与 发电机有功功率不太能确定是否是同一个，齿轮箱主轴承温度 与主轴轴承温度 应该不是同一个东西。
    print(df)
