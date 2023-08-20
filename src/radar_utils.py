#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import datetime
import fsspec
import numpy as np
import pandas as pd
import pyart
import argparse
import csv
import multiprocessing as mp
from time import time
from datetime import datetime
from config_utils import get_pars_from_ini, make_dir
from netCDF4 import num2date
from dateutil import tz

path_ini = os.path.normpath(os.getcwd() + os.sep + os.pardir)
dt_names = get_pars_from_ini('{}/config/radar_names.ini'.format(path_ini))
dt_zoom = get_pars_from_ini('{}/config/extents.ini'.format(path_ini))


def timer_func(func):
    # This function shows the execution time of
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f'Function {func.__name__!r} executed in {(t2 - t1):.2f}s')
        return result
    return wrap_func


def glob_files(path):
    fs = fsspec.filesystem("s3", anon=True)
    return fs.glob(path)


def get_radar_files(rn, years=None, months=None, days=None) -> dict:
    print(years, months, days)
    str_bucket = 's3://s3-radaresideam/'
    if months[0] is None:
        months = [i for i in range(1, 13)]
    if years[0] is None:
        years = [2018]
    if days[0] is None:
        days = [i for i in range(1, 32)]
    _time = [[[f"{str_bucket}l2_data/{year}/{i:02d}/{d:02d}/{rn}/*" for d in days]for i in months] for year in years]
    buckets = [item for sublist in _time for item in sublist]
    buckets = [item for sublist in buckets for item in sublist]
    pool = mp.Pool()
    files = pool.starmap(glob_files, zip(buckets))
    pool.close()
    pool.join()
    return [item for sublist in files for item in sublist]


def get_df_radar(rn) -> pd.DataFrame:
    try:
        df = pd.read_csv(f"/data/Estaciones_{rn.upper()}.csv", sep=';')
    except FileNotFoundError:
        df = pd.read_csv(f"../data/Estaciones_{rn.upper()}.csv", sep=';')
    df['rn'] = rn
    return df[['latitud', 'longitud', "CODIGO", "rn"]]


def read_from_aws(file) -> pyart.core.Radar:
    return pyart.io.read(pyart.io.prepare_for_read(f"s3://{file}", storage_options={"anon": True}))


def get_radar(file) -> pyart.core.Radar:
    try:
        radar = read_from_aws(file)
        if valid_task(radar):
            return radar
        else:
            pass
    except OSError:
        print(f"Corrupted File {file}")
        pass


def get_data(radar, lat, lon) -> dict:
    fields = ['total_power', 'reflectivity', 'velocity', 'spectrum_width', 'differential_reflectivity',
              'specific_differential_phase', 'differential_phase', 'normalized_coherent_power',
              'cross_correlation_ratio', 'radar_echo_classification']
    dt = {i: np.nan for i in fields}
    data = pyart.util.get_field_location(radar, latitude=lat, longitude=lon).isel(height=0)
    for field in list(radar.fields.keys()):
        dt[field] = data[field].values
    return dt


def get_test_data(rn) -> pyart.core.Radar:
    if rn == "Carimagua":
        return read_from_aws('s3-radaresideam/l2_data/2022/08/09/Carimagua/CAR220809191504.RAWDSX2')  # CARIMAGUA
    elif rn == "Guaviare":
        return read_from_aws('s3-radaresideam/l2_data/2022/10/06/Guaviare/GUA221006000012.RAWHDKV')  # GUAVIARE
    elif rn == "Munchique":
        return read_from_aws('s3-radaresideam/l2_data/2022/04/10/Munchique/CEM220410000004.RAWE4PE')  # MUNCHIQUE
    elif rn == "Barrancabermeja":
        return read_from_aws('s3-radaresideam/l2_data/2023/04/07/Barrancabermeja/BAR230407000004.RAW0LK7')  # BARRANCA
    else:
        return read_from_aws('s3-radaresideam/l2_data/2018/09/12/Guaviare/GUA180912050051.RAWFG4F')  # test


def get_time(radar) -> (time, time):
    """

    :param radar:
    :return:
    """
    try:
        time_utc = num2date(radar.time['data'][0], radar.time['units'])
    except ValueError:
        radar.time['units'] = '{} {}'.format(radar.time['units'][:13], radar.time['units'][13:])

    time_utc = datetime(*time_utc.timetuple()[:-3], tzinfo=tz.gettz('UTC'))
    to_zone = tz.gettz('America/Bogota')
    from_zone = tz.gettz('UTC')
    utc_time = time_utc.replace(tzinfo=from_zone)
    _time = utc_time.astimezone(to_zone)
    return _time, time_utc


def valid_task(radar, tasks=None) -> bool:
    if not tasks:
        tasks = ["SURV_HV_300", "SURVEILLANCE", "VOL_A", "SURVP", "PRECA"]
    _task = radar.metadata['sigmet_task_name'].decode('utf-8').replace(" ", "")
    if _task in tasks:
        return True
    else:
        return False


def get_str_file_path(filename) -> pd.to_datetime:
    return pd.to_datetime(filename.split("/")[-1].split(".")[0][3:], format="%y%m%d%H%M%S")


def check_if_exist(rn, file) -> bool:
    path = f'../results'
    _t = get_str_file_path(file)
    file_path = f"{path}/{rn}/files"
    file_name = f"{file_path}/{_t:%y%m%d}.txt"
    try:
        with open(file_name, 'r', newline='\n') as txt_file:
            lines = txt_file.readlines()
            txt_file.close()
        _file = [i for i in lines if i.replace("\n", "") == file]
        if len(_file) > 0:
            print("File already processed")
            return True
        else:
            return False
    except FileNotFoundError:
        return False


def write_file_radar(rn, file):
    path = f'../results'
    _t = get_str_file_path(file)
    file_path = f"{path}/{rn}/files"
    make_dir(file_path)
    file_name = f"{file_path}/{_t:%y%m%d}.txt"
    with open(file_name, 'a') as txt_file:
        txt_file.write(f"{file}\n")
        txt_file.close()


def write_file_sta(station, data, rn):
    path = f'../results/{rn}/data'
    make_dir(path)
    file_path = f"{path}/{station}.csv"
    with open(file_path, 'a') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', lineterminator='\n')
        csv_writer.writerow(data)
        csv_file.close()


def create_parser():
    parser = argparse.ArgumentParser(description='Descarga de datos')
    parser.add_argument('--year', nargs='+', type=str, help='Lista de años a consultar',
                        default=[None])
    parser.add_argument('--months', nargs='+', type=int, help='Lista de meses a consultar',
                        default=[None])
    parser.add_argument('--days', nargs='+', type=int, help='Lista de dias a consultar',
                        default=[None])
    parser.add_argument('--radar', nargs='+', type=int, help='radares a consultar',
                        default=['Barrancabermeja'])
    return parser.parse_args()

