#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import pyart
import fsspec
import csv
import multiprocessing as mp
from radar_utils import valid_task, get_time
from config_utils import make_dir
from time import time


def timer_func(func):
    # This function shows the execution time of
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func


@timer_func
def get_radar_files(rn, years=None):
    str_bucket = 's3://s3-radaresideam/'
    fs = fsspec.filesystem("s3", anon=True)
    if not years:
        years = [i.split('/')[-1] for i in fs.glob(f"{str_bucket}/l2_data/*")]
    return {year: fs.glob(f"{str_bucket}/l2_data/{year}/*/*/{rn}/*") for year in years}


def get_df_radar(rn):
    df = pd.read_csv(f"../data/Estaciones_{rn.upper()}.csv", sep=';')
    df['rn'] = rn
    return df[['latitud', 'longitud', "CODIGO", "rn"]]


def get_radar(file):
    return pyart.io.read(pyart.io.prepare_for_read(f"s3://{file}", storage_options={"anon": True}))


def get_ref(lat, lon):
    return pyart.util.get_field_location(radar, latitude=lat, longitude=lon).reflectivity.isel(height=0).values


def write_file_sta(station, data, rn):
    path = f'../results/{rn}'
    make_dir(path)
    file_path = f"{path}/{station}.csv"
    with open(file_path, 'a', newline='\n') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')
        csv_writer.writerow(data)
        csv_file.close()


def main_procss(lat, lon, station, rn):
    ref = get_ref(lat=lat, lon=lon)
    _, t = get_time(radar)
    data = {'date': pd.to_datetime(t, utc=True),
            'cod': str(station), 'ref': float(ref)}
    ser = pd.Series(data=data)
    write_file_sta(station=station, data=ser, rn=rn)


def get_test_data(rn):
    if rn == "Carimagua":
        return get_radar('s3-radaresideam/l2_data/2022/08/09/Carimagua/CAR220809191504.RAWDSX2') # CARIMAGUA
    elif rn == "Guaviare":
        return get_radar('s3-radaresideam/l2_data/2022/10/06/Guaviare/GUA221006000012.RAWHDKV')  # GUAVIARE

    elif rn == "Munchique":
        return get_radar('s3-radaresideam/l2_data/2022/04/10/Munchique/CEM220410000004.RAWE4PE')  # MUNCHIQUE
    elif rn == "Barrancabermeja":
        return get_radar('s3-radaresideam/l2_data/2023/04/07/Barrancabermeja/BAR230407000004.RAW0LK7')  # BARRANCA
    else:
        return get_radar('s3-radaresideam/l2_data/2018/09/12/Guaviare/GUA180912050051.RAWFG4F')  # test


@timer_func
def main():
    rn = ["Guaviare", "Barrancabermeja", "Carimagua", "Munchique"]
    rn = ["Guaviare"]
    years = ['2018']
    for rad_n in rn:
        dt_data = get_radar_files(rad_n, years=years)
        df = get_df_radar(rad_n, )
        files = dt_data[years[0]]
        print(len(files))
        for file in files[18066:]:
            global radar
            try:
                radar = get_radar(file)
                # radar = get_test_data('test') ## uncoment this for test
                if valid_task(radar):
                    args = zip(df['latitud'], df['longitud'], df['CODIGO'], df['rn'])
                    pool = mp.Pool()
                    pool.starmap(main_procss, args)
                    pool.close()
                del radar
            except OSError:
                print(f"Corrupted File {file}")

        print(1)
        pass


if __name__ == "__main__":
    main()
