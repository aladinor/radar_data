#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy as np
import pyart
import pyproj
import pandas as pd
import os
import datetime
from config_utils import get_pars_from_ini, make_dir
from netCDF4 import num2date
from dateutil import tz

path_ini = os.path.normpath(os.getcwd() + os.sep + os.pardir)
dt_names = get_pars_from_ini('{}/config/radar_names.ini'.format(path_ini))
dt_zoom = get_pars_from_ini('{}/config/extents.ini'.format(path_ini))


def is_close(a, b, rel_tol=1e-03, abs_tol=0.0):
    return abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def get_time(radar):
    """

    :param radar:
    :return:
    """
    try:
        time_utc = num2date(radar.time['data'][0], radar.time['units'])
        time_utc = datetime.datetime(*time_utc.timetuple()[:-3], tzinfo=tz.gettz('UTC'))
        to_zone = tz.gettz('America/Bogota')
        from_zone = tz.gettz('UTC')
        utc_time = time_utc.replace(tzinfo=from_zone)
        time = utc_time.astimezone(to_zone)
        return time, time_utc

    except ValueError:
        time_ok = '{} {}'.format(radar.time['units'][:13], radar.time['units'][13:])
        radar.time['units'] = time_ok
        time_utc = num2date(radar.time['data'][0], radar.time['units'])
        time_utc = datetime.datetime(*time_utc.timetuple()[:-3], tzinfo=tz.gettz('UTC'))
        to_zone = tz.gettz('America/Bogota')
        from_zone = tz.gettz('UTC')
        utc_time = time_utc.replace(tzinfo=from_zone)
        time = utc_time.astimezone(to_zone)
        return time, time_utc


def valid_task(radar, tasks=None):
    if not tasks:
        tasks = ['SURV_HV_300', 'SURVP']
    _task = radar.metadata['sigmet_task_name'].decode('utf-8').replace(" ", "")
    if _task in tasks:
        return True
    else:
        return False
