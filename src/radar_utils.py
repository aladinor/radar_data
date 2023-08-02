#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import datetime
from datetime import datetime
from config_utils import get_pars_from_ini
from netCDF4 import num2date
from dateutil import tz

path_ini = os.path.normpath(os.getcwd() + os.sep + os.pardir)
dt_names = get_pars_from_ini('{}/config/radar_names.ini'.format(path_ini))
dt_zoom = get_pars_from_ini('{}/config/extents.ini'.format(path_ini))


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


def get_time(radar):
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
    time = utc_time.astimezone(to_zone)
    return time, time_utc


def valid_task(radar, tasks=None):
    if not tasks:
        tasks = ['SURV_HV_300', 'SURVP', 'SURVEILLANCE']
    _task = radar.metadata['sigmet_task_name'].decode('utf-8').replace(" ", "")
    if _task in tasks:
        return True
    else:
        return False
