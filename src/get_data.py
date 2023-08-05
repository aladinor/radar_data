#!/usr/bin/env python
# -*- coding: utf-8 -*-
import csv
import multiprocessing as mp
from config_utils import make_dir
from radar_utils import *
import argparse


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


def create_parser():
    parser = argparse.ArgumentParser(description='Descarga de datos')
    parser.add_argument('--year', nargs='+', type=str, help='Lista de años a consultar',
                        default=['2018'])
    parser.add_argument('--radar', nargs='+', type=str, help='radares a consultar',
                        default=['Barrancabermeja'])
    return parser.parse_args()


@timer_func
def main():
    args = create_parser()
    arg = vars(args)
    print(arg)
    for rad_n in arg['radar']:
        dt_data = get_radar_files(rad_n, years=arg['year'])
        df = get_df_radar(rad_n)
        for year in arg['year']:
            files = dt_data[year]
            print(f"Total number of files to process on {year} are {len(files)}")
            for file in files:
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
