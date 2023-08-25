#!/usr/bin/env python
# -*- coding: utf-8 -*-
import multiprocessing as mpc
from radar_utils import *


def write_data(radar, lat, lon, station, rn):
    ref = get_data(radar=radar, lat=lat, lon=lon)
    _, t = get_time(radar)
    data = {'date': pd.to_datetime(t, utc=True),
            'cod': str(station)}
    data = data | ref
    ser = pd.Series(data=data)
    write_file_sta(station=station, data=ser, rn=rn)


def w_data(_obj):
    rad_n = _obj.radar_name
    file = _obj.s3_file
    exist = check_if_exist(rn=rad_n, file=file)
    radar = get_radar(file)
    df = get_df_radar(rad_n)
    if not exist and radar:
        for _, row in df.iterrows():
            write_data(radar=radar, lat=row.latitud, lon=row.longitud, station=row.CODIGO, rn=row.rn)
        write_file_radar(rn=rad_n, file=file)


class Radar_Args:
    def __init__(self, rn, s3_file):
        self.radar_name = rn
        self.s3_file = s3_file


def mp_files(arg):
    rad_n = "Barrancabermeja"
    files = get_radar_files(rad_n, years=arg['year'], months=arg['months'], days=arg['days'])
    args = [Radar_Args(rn=rad_n, s3_file=_f) for _f in files]
    pool = mpc.Pool()
    pool.map(w_data, args)
    pool.close()


@timer_func
def main():
    args = create_parser()
    arg = vars(args)
    print(arg)
    mp_files(arg)


if __name__ == "__main__":
    main()
