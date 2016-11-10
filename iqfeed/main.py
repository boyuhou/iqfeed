#!/usr/bin/env python2.7
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""iqfeed: Data downloader for Iqfeed/DTN
      Tibor Kiss <tibor.kiss@gmail.com> - Copyright (c) 2012-2016 All rights reserved

Usage:
  iqfeed process-file <filename> <start_year> <end_year> [-d DIR] [-i CON] [-t TZ] [-D]
  iqfeed download <instrument> <start_year> <end_year> [-d DIR] [-i CON] [-t TZ] [-D]
  iqfeed -h | --help

  iqfeed --start_date 20150101 --end_date 2016101 --ticker AAPL --outdir 'C:\\temp' --seconds_per_bar 60

Commands:
  download            Download the specified instrument
  get-from-file       Download instruments listed in the specified file

Options:
  -d DIR --download-dir DIR   Directory where the files will be downloaded [default: .]
  -i CON --iqfeed CON         IQFeed host & port [default: localhost:9100]
  -t TZ --tz TZ               Time zone [default: US/Eastern]
  -D                          Debug mode
  -h                          Help screen

Note:
Date format for end_date and start_date: YYYYMMDD

"""


import os
import sys
import logging
import pytz
import click
import pandas as pd
from datetime import datetime, timedelta

from .download import get_bars
from .tools import get_instruments_from_file, bars_to_dateframe

today = datetime.now().today()
today_str = today.strftime('%Y%m%d')
eastern_tz = 'US/Eastern'


@click.command()
@click.option('--ticker', default=None, help='Ticker Symbol')
@click.option('--outdir', default=None, help='Output folder')
@click.option('--start_date', default='20140101', help='Start date default to 20140101')
@click.option('--end_date', default=today_str, help='End date')
@click.option('--debug', default=False, help='True or False to introduce debug mode')
@click.option('--universe', default=None, help='The file that contains the universe')
@click.option('--iqfeed_host', default='localhost', help='IQFeed Host default localhost')
@click.option('--iqfeed_port', default=9100, help='IQFeed Port, default 9100')
@click.option('--timezone', default=eastern_tz, help='Timezone, default US/Eastern')
@click.option('--seconds_per_bar', default=60, help='bar per seconds, default 60')
def main(ticker, outdir, start_date, end_date, debug, universe, iqfeed_host, iqfeed_port, timezone, seconds_per_bar):
    log = logging.getLogger()
    log_console = logging.StreamHandler(sys.stdout)
    log.setLevel(logging.DEBUG if debug else logging.INFO)
    log_console.setLevel(logging.DEBUG if debug else logging.INFO)
    log.addHandler(log_console)

    if ticker is not None:
        instruments = (ticker, )
    elif universe is not None:
        instruments = get_instruments_from_file(universe)
    else:
        raise NotImplementedError('No ticker or universe is specified. Not sure what to do.')

    tz = pytz.timezone(timezone)

    for (i, instrument) in enumerate(instruments):
        try:
            log.info(str.format("Processing {0} ({1} out of {2})", instrument, i+1, len(instruments)))

            instrument_path = os.path.join(outdir, instrument+'.csv')
            price_df = pd.DataFrame()
            process_start_date = start_date
            if os.path.exists(instrument_path):
                price_df = pd.read_csv(instrument_path, index_col=0, parse_dates=True)
                last_date = price_df.index[-1].date()
                process_start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')

            if int(process_start_date) > int(end_date):
                log.info('Price already in place.')
            else:
                bars = get_bars(instrument, process_start_date, end_date, tz, seconds_per_bar, iqfeed_host, iqfeed_port)
                if len(bars):
                    new_df = bars_to_dateframe(bars, tz)
                    pd.concat([price_df, new_df])[['Open', 'High', 'Low', 'Close', 'Volume']].to_csv(instrument_path, date_format='%Y%m%d %H%M%S')

        except Exception as e:
            log.error('Exception during download, continuing', exc_info=e)
