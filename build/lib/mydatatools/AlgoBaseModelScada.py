import os
import time
import datetime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import *
from urllib.parse import quote
from loguru import logger
import json
import paramiko
from mydatatools.read_config import *

# plt.rcParams['text.color'] = 'whitesmoke'   # lightgrey
# plt.rcParams['axes.facecolor'] = 'black'

# import matplotlib.style as ms
# ms.use("seaborn-dark")
# ms.use("dark_background")
# plt.rcParams['savefig.facecolor'] = 'midnightblue'
# plt.rcParams['axes.facecolor'] = 'midnightblue'


def ftp_connect(host, port, username, password):
    """
    ftp连接
    :param host:
    :param port:
    :param username:
    :param password:
    :return:
    """
    ssh_t = paramiko.Transport((host, port))
    ssh_t.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(ssh_t)
    return sftp, ssh_t


def ftp_mkdir_new(ftp, dir_path):
    """
    sftp 文件传输在remote上递归创建多级目录
    :param ftp: sftp客户端
    :param dir_path: 需创建的目录，绝对路径
    :return:
    """
    up_dir = '/'.join(dir_path.split('/')[:-1])
    dir_name = dir_path.split('/')[-1]
    try:
        ftp.stat(up_dir)
        up_dir_exist = True
    except FileNotFoundError:
        up_dir_exist = False
    if up_dir_exist is True:
        ret = up_dir + '/' + dir_name
        ftp.mkdir(ret)
        return ret
    else:
        ret = ftp_mkdir_new(ftp, up_dir) + '/' + dir_name
        ftp.mkdir(ret)
        return ret


def ftp_mkdir(ftp, dir_path):
    """
    sftp 创建remote目录，目录存在则直接返回，不存在则新建目录
    :param ftp:
    :param dir_path:
    :return:
    """
    try:
        ftp.stat(dir_path)
        return dir_path
    except FileNotFoundError:
        return ftp_mkdir_new(ftp, dir_path)


class AlgoBaseModelScada(object):
    """
    算法模型基础类，方便后续继承
    """
    MODEL_VERSION = 1.0
    MODEL_FROM = 2   # 边缘度模型：1；云端故障诊断模型：2；云端健康值模型：3
    PROCESS_NUM = 10

    def __init__(self, data_date_range='D', alarm_date_range=None, is_to_db=False, db_info=None, dir_root='./', ftp_info=None, read_old=True,
                 update_old_file=False, **kwargs):        # /data/cms_card_collection_data
        self.MODEL_NAME = self.__class__.__name__
        logger.info('=' * 10 + str(self.MODEL_NAME) + str(self.MODEL_VERSION) + '=' * 10)
        # self.INFO_TYPE_DIR = os.path.join(dir_root, 'gearing_info_files')
        self.IMAGE_FILES_PATH = os.path.join(dir_root, "model_result", self.MODEL_NAME, "image")
        self.data_date_range = data_date_range
        self.alarm_date_range = alarm_date_range
        self.feature_result = {}
        self.is_to_db = is_to_db
        if not self.is_to_db:
            print('注意，模型结果入库开关未打开，结果无需入库！')
        # TODO 结果数据库需要修改
        if db_info is None:
            logger.info('结果写入默认数据库地址')
            self.db_info = {
                'user': RESULT_USER,
                'password': quote(RESULT_PASSWORD),
                'host': RESULT_HOST,
                'port': RESULT_PORT,
                'database': RESULT_DB,
                'table_name': RESULT_TABLE
            }
        else:
            self.db_info = db_info
        # self.csv_root = os.path.join(dir_root, 'upload_finish')
        self.pkl_root = os.path.join(dir_root, 'pkl')
        self.project_id = "scada_temp"
        self.read_old = read_old
        self.update_old_file = update_old_file
        self.kwargs = kwargs
        # TODO 结果上传的目录需要修改
        if ftp_info is None:
            # logger.info('结果写入默认数据库地址')
            self.ftp_info = {
                'host': SFTP_HOST,
                'port': SFTP_PORT,
                'username': SFTP_USER,
                'password': SFTP_PASSWORD
            }
        else:
            self.ftp_info = ftp_info
        self.upload_root_dir = f'/data/cms_card_collection_data/model_result/{self.MODEL_NAME}/image'
        if self.is_to_db:
            self.sftp, self.ssh = ftp_connect(**self.ftp_info)

    def get_args(self, data_date_range='D', alarm_date_range=None):
        """
        手动指定或者自动获取运行日期参数
        :param data_date_range: 数据获取的时间周期，D 按天为周期，新数据为昨天整天数据， M 按月调度，新数据未上月整月数据
        :param alarm_date_range: 预警数据的时间范围
        :return:
        start_date: 新数据开始时间
        end_date:  新数据结束时间
        alarm_start_date： 用于预警判断的数据起始日期。
        """
        farm_code_ = json.loads(RUN_FARM_CODES)
        turbines_ = json.loads(RUN_TURBINES)
        if turbines_ is None:
            turbines_ = [[] * len(farm_code_)]

        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)

        today_month_first = datetime.date(day=1, month=today.month, year=today.year)  # 执行当月1号
        last_month_end_date = today_month_first - datetime.timedelta(days=1)  # 上一个月最后一天
        last_month_start_date = datetime.date(day=1, month=last_month_end_date.month,
                                              year=last_month_end_date.year)  # 执行前一个月的第一天

        if data_date_range == 'D':
            start_date_default = yesterday.strftime('%Y%m%d') + '000000'
            end_date_default = today.strftime('%Y%m%d') + '000000'
        elif data_date_range == 'M':
            start_date_default = last_month_start_date.strftime('%Y%m%d') + '000000'
            end_date_default = last_month_end_date.strftime('%Y%m%d') + '235959'
        elif data_date_range == '30D':
            start_date_default = (today - datetime.timedelta(days=30)).strftime('%Y%m%d') + '000000'
            end_date_default = today.strftime('%Y%m%d') + '000000'
        else:
            start_date_default = None
            end_date_default = None

        if alarm_date_range == 'W':
            alarm_start_date_default = today - datetime.timedelta(days=7)
        elif alarm_date_range == '2W':
            alarm_start_date_default = today - datetime.timedelta(days=14)
        elif alarm_date_range == '30D':
            alarm_start_date_default = today - datetime.timedelta(days=30)
        else:
            alarm_start_date_default = None

        start_date_ = RUN_START_DATE or start_date_default
        end_date_ = RUN_END_DATE or end_date_default
        alarm_start_date_ = RUN_ALARM_START_DATE or alarm_start_date_default
        return farm_code_, turbines_, start_date_, end_date_, alarm_start_date_

    def run_turbine(self, farm_code, turbine, pic_save_dir, start_date=None, end_date=None,
                    pkl_fig=None, alarm_start_date=None):
        """
        机组具体算法函数，需要重写
        必须返回：status, comment, description, main_fig, sub_figs
        """
        raise NotImplementedError('run_service 方法未实现')

    def run_farm(self, farm_code, pic_save_dir, start_date=None, end_date=None, alarm_start_date=None):
        r = None
        return r

    def handle_model_result(self, data):
        """
        将模型结果上传数据库
        :param data:
        :return:
        """
        model_id_dict = {'mainbearing_fd': 2, 'gearing_low_state_fd': 3, 'generator_electric_fd': 4,
                         'cms_mainbearing_worsen_factor': 7, 'cms_gearingbox_worsen_factor': 9,
                         'cms_generator_worsen_factor': 10, 'GearingBoxTempModel': 18, 'GeneratorWindingTempModel': 20,
                         'GearingBoxOilFilterFDModel': 21}
        engine = create_engine(
            'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(database)s?charset=utf8'
            % self.db_info
        )
        if not isinstance(data, list):
            data = [data]
        ret_df = pd.DataFrame(data)

        if self.MODEL_NAME not in model_id_dict:
            ret_db_df = pd.read_sql(f"select id from cms_model_info where model_name_en='{self.MODEL_NAME}' limit 1",
                                    con=engine)
            model_id = ret_db_df["id"][0]
        else:
            model_id = model_id_dict[self.MODEL_NAME]

        ret_df["model_from"] = self.MODEL_FROM
        ret_df["model_id"] = model_id
        ret_df["model_name"] = self.MODEL_NAME
        ret_df["model_run_time"] = datetime.datetime.now()
        ret_df["img_sync_status"] = 0
        ret_df["project_id"] = 'scada'

        if len(ret_df) > 0:
            logger.debug(f'[{self.__class__.__name__}] 结果数据长度：{len(ret_df)}')
            logger.debug(f'[{self.__class__.__name__}] 结果数据：{ret_df.head(3)}')
            ret_df.to_sql(name=self.db_info['table_name'], con=engine, if_exists='append', index=False,
                          dtype={"sub_image_json": JSON})
        else:
            logger.info(f'[{self.__class__.__name__}] 结果数据长度为0，无需入库')
            print(ret_df)

    def upload_files(self, files, remote_dir):
        remote_dir = ftp_mkdir(self.sftp, remote_dir)
        logger.info(f'文件上传目录：{remote_dir}')
        remote_files = [remote_dir + '/' + f.split(os.path.sep)[-1] for f in files]
        for file, r_file in zip(files, remote_files):
            self.sftp.put(file, r_file)
            logger.info(f'{file}文件上传{r_file}成功！')
        return remote_files

    def run(self):
        farm_codes, turbines_lis, start_date, end_date, alarm_start_date = self.get_args(data_date_range=self.data_date_range)

        print(farm_codes)
        print(turbines_lis)
        print(start_date)
        print(end_date)

        model_run_date = datetime.datetime.now().strftime("%Y%m%d")
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(now)
        all_start_s = time.time()

        result_dicts = []
        self.IMAGE_FILES_PATH = os.path.join(self.IMAGE_FILES_PATH, model_run_date)
        self.upload_root_dir = self.upload_root_dir + f'/{model_run_date}'

        for farm_code, turbines in zip(farm_codes, turbines_lis):
            print(farm_code)
            print(turbines)
            if len(turbines) == 0:
                # 查询该风场下所有的机组
                pass

            for turbine in turbines:
                print(turbine)

                local_file_path = os.path.join(farm_code, turbine)
                one_turbine_fig = os.path.join(self.IMAGE_FILES_PATH, local_file_path)
                os.makedirs(one_turbine_fig, exist_ok=True)
                pkl_fig = os.path.join(self.pkl_root, farm_code, turbine, self.project_id)
                os.makedirs(pkl_fig, exist_ok=True)

                ret = self.run_turbine(farm_code, turbine, one_turbine_fig,
                                       start_date=start_date,
                                       end_date=end_date,
                                       pkl_fig=pkl_fig,
                                       alarm_start_date=alarm_start_date)
                if ret is not None and self.is_to_db:
                    status, comment, description, main_fig, sub_figs = ret

                    figs = [main_fig] + sub_figs
                    upload_turbine_dir = self.upload_root_dir + f'/{farm_code}/{turbine}'
                    remote_files = self.upload_files(figs, upload_turbine_dir)

                    sub_figs = {f'子图{i_name}': pic_i for i_name, pic_i in enumerate(remote_files[1:])}
                    result_dict = {'farm_code': farm_code, 'turbine_num': turbine, 'model_result': status,
                                   'model_status': status, 'model_comment': comment,
                                   'model_conclusion_description': description,
                                   'main_image': remote_files[0], 'sub_image_json': sub_figs, 'model_failure_code': 'E000'}

                    result_dicts.append(result_dict)
                # break

            local_file_path = os.path.join(farm_code, farm_code+'_all')
            one_farm_fig = os.path.join(self.IMAGE_FILES_PATH, local_file_path)
            os.makedirs(one_farm_fig, exist_ok=True)

            ret = self.run_farm(farm_code, one_farm_fig, start_date=start_date,
                                end_date=end_date, alarm_start_date=alarm_start_date)
            if ret is not None and self.is_to_db:
                status, comment, description, main_fig, sub_figs = ret

                figs = [main_fig] + sub_figs
                upload_turbine_dir = self.upload_root_dir + f'/{farm_code}/{farm_code}_all'
                remote_files = self.upload_files(figs, upload_turbine_dir)

                sub_figs = {f'子图{i_name}': pic_i for i_name, pic_i in enumerate(remote_files[1:])}
                result_dict = {'farm_code': farm_code, 'turbine_num': 'farm', 'model_result': status,
                               'model_status': status, 'model_comment': comment,
                               'model_conclusion_description': description,
                               'main_image': remote_files[0], 'sub_image_json': sub_figs, 'model_failure_code': 'E000'}

                result_dicts.append(result_dict)

        # 结果数据入库
        if self.is_to_db and len(result_dicts) > 0:
            logger.info('结果数据入库')
            self.handle_model_result(result_dicts)
            # 上传文件
            self.sftp.close()
            self.ssh.close()

        else:
            logger.info('结果无需入库')

        e = time.time()
        print(f"整个脚本运行时间：{round((e - all_start_s) / 60, 3)}分钟")
        print('================================================= 执行完毕 =======================================')
