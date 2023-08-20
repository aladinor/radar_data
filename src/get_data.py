#!/usr/bin/env python
# -*- coding: utf-8 -*-
import multiprocessing as mp
import fast_map
from radar_utils import *


def write_data(radar, lat, lon, station, rn):
    ref = get_data(radar=radar, lat=lat, lon=lon)
    _, t = get_time(radar)
    data = {'date': pd.to_datetime(t, utc=True),
            'cod': str(station)}
    data = data | ref
    ser = pd.Series(data=data)
    write_file_sta(station=station, data=ser, rn=rn)


def mp_stations(arg):
    for rad_n in arg['radar']:
        files = get_radar_files(rad_n, years=arg['year'], months=arg['months'], days=arg['days'])
        if files:
            df = get_df_radar(rad_n)
            print(f"Total number of files to process on are {len(files)}")
            for file in files:
                exist = check_if_exist(rn=rad_n, file=file)
                if not exist:
                    radar = get_radar(file)
                    if radar:
                        args = [(radar, row['latitud'], row['longitud'], row['CODIGO'], row['rn'])
                                for _, row in df.iterrows()]
                        pool = mp.Pool()
                        pool.starmap(write_data, args)
                        pool.close()
                        pool.join()
                        write_file_radar(rn=rad_n, file=file)
                    else:
                        continue
            print('Termine')
            pass


def w_data(rad_n, file):
    exist = check_if_exist(rn=rad_n, file=file)
    radar = get_radar(file)
    if not exist and radar:
        df = get_df_radar(rad_n)
        for _, row in df.iterrows():
            write_data(radar=radar, lat=row.latitud, lon=row.longitud, station=row.CODIGO, rn=row.rn)
            write_file_radar(rn=rad_n, file=file)


def mp_files(arg):
    for rad_n in arg['radar']:
        dt_data = get_radar_files(rad_n, years=arg['year'])
        for year in arg['year']:
            files = dt_data[year]
            _args = zip([rad_n] * 1, files[:1])
            _ = get_test_data('Carimagua')
            t = fast_map.fast_map_async(w_data, [rad_n] * 1, files[:1])
            t.join()

            with mp.Pool() as pool:
                pool.starmap(w_data, _args)
                pool.close()
                pool.join()


@timer_func
def main():
    args = create_parser()
    arg = vars(args)
    print(arg)
    mp_stations(arg)


if __name__ == "__main__":
    main()
