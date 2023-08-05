#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import csv
import multiprocessing as mp
from config_utils import make_dir
from radar_utils import *
# import warnings
# warnings.simplefilter("error", DeprecationWarning)


def write_file_sta(station, data, rn):
    path = f'../results/{rn}'
    make_dir(path)
    file_path = f"{path}/{station}.csv"
    with open(file_path, 'a', newline='\n') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')
        csv_writer.writerow(data)
        csv_file.close()


def write_data(lat, lon, station, rn):
    ref = get_ref(radar=radar, lat=lat, lon=lon)
    _, t = get_time(radar)
    data = {'date': pd.to_datetime(t, utc=True),
            'cod': str(station), 'ref': float(ref)}
    ser = pd.Series(data=data)
    write_file_sta(station=station, data=ser, rn=rn)


@timer_func
def main():
    rn = ["Guaviare", "Barrancabermeja", "Carimagua", "Munchique"]
    rn = ["Barrancabermeja"]
    years = ['2018']
    for rad_n in rn:
        dt_data = get_radar_files(rad_n, years=years)
        df = get_df_radar(rad_n)
        files = dt_data[years[0]]
        for file in files[:100]:
            global radar
            radar = get_radar(file)
            if radar:
                args = zip(df['latitud'], df['longitud'], df['CODIGO'], df['rn'])
                pool = mp.Pool()
                pool.starmap(write_data, args)
                pool.close()
            else:
                continue
        print('Termine')
        pass


if __name__ == "__main__":
    main()
