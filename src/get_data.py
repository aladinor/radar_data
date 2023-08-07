#!/usr/bin/env python
# -*- coding: utf-8 -*-
import multiprocessing as mp
from radar_utils import *


def write_data(lat, lon, station, rn):
    ref = get_ref(radar=radar, lat=lat, lon=lon)
    _, t = get_time(radar)
    data = {'date': pd.to_datetime(t, utc=True),
            'cod': str(station), 'ref': float(ref)}
    ser = pd.Series(data=data)
    write_file_sta(station=station, data=ser, rn=rn)


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
                exist = check_if_exist(rn=rad_n, file=file)
                if not exist:
                    radar = get_radar(file)
                    if radar:
                        args = zip(df['latitud'], df['longitud'], df['CODIGO'], df['rn'])
                        pool = mp.Pool()
                        pool.starmap_async(write_data, args)
                        pool.close()
                        pool.join()
                        write_file_radar(rn=rad_n, file=file)
                    else:
                        continue
            print('Termine')
            pass


if __name__ == "__main__":
    main()
