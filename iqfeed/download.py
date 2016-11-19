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

import socket
from collections import namedtuple
from datetime import datetime
import logging

from backports.functools_lru_cache import lru_cache

from .tools import retry

log = logging.getLogger(__name__)

Bar = namedtuple('IQFeedBar', ['datetime', 'open', 'high', 'low', 'close', 'volume'])


def _read_historical_data_socket(sock, recv_buffer=4096):
    """
    Read the information from the socket, in a buffered
    fashion, receiving only 4096 bytes at a time.

    Parameters:
    sock - The socket object
    recv_buffer - Amount in bytes to receive per read
    """
    buffer = ""
    data = ""
    while True:
        data = sock.recv(recv_buffer)
        buffer += data

        # Check if the end message string arrives
        if "!ENDMSG!" in buffer:
            break

    # Remove the end message string
    buffer = buffer[:-12]
    return buffer

def __download_historical_data(iqfeed_socket, chunk_size=65535):
    """
    Read the data from iqfeed_socket with the given chunk size.
    The collected data is returned as a string or exception is raised on error
    """
    buffer_ = ""
    chunk = ""
    end_msg = '\n!ENDMSG!,\r\n'

    while not chunk.endswith(end_msg):
        chunk = iqfeed_socket.recv(chunk_size)

        if chunk.startswith('E,'):  # Error condition
            if chunk.startswith('E,!NO_DATA!'):
                log.warn('No data available for the given instrument')
                break
            else:
                raise Exception(chunk)
        log.debug('New chunk: %s', " ".join("{:02x}".format(ord(c)) for c in chunk[-1*len(end_msg):]))

        buffer_ += chunk

    # Remove the end message string
    buffer_ = buffer_[:-1 * len(end_msg)]

    # Cut off CR
    buffer_ = buffer_.replace('\r', '')

    return buffer_


@lru_cache(maxsize=780000)  # 10 years worth of datetimes
def __create_datetime(datetime_str, format_str):
    # It takes a reasonable amount of time to construct the datetime fields with timezone info.
    # This function is used to cache the datetime object results.
    dt = datetime.strptime(datetime_str, format_str)
    # return timezone.localize(dt)
    return dt


@retry(5, delay=2)
def get_bars(freq, instrument, start_date, end_date, tz, seconds_per_bar,
             iqfeed_host='localhost', iqfeed_port=9100, timeout=10.0):
    """
    Returns list of Bar instances for the given instrument, time period, time zone and bar frequency (second_per_bar).
    The function is retried 5 times in 5 second intervals to alleviate IQFeed daemon's glitches.
    """
    # IQFeed accepts messages in the following format:
    #   CMD,SYM,[options]\n.
    # Notice the newline character. This must be added otherwise the request will not work.
    # The provided options are
    #   [second_per_bar],[beginning date: CCYYMMDD HHmmSS],[ending date: CCYYMMDD HHmmSS],[empty],
    #   [beginning time filter: HHmmSS],[ending time filter: HHmmSS],[old or new: 0 or 1],[empty],
    #   [second per data point].
    #
    # Source: https://github.com/bwlewis/iqfeed/blob/master/man/HIT.Rd
    begin_time_filter = '093000'
    end_time_filter = '160000'
    historical_minute_data_request = 'HIT,{0},{1},{2},{3},,{4},{5},1\n'.format(instrument, seconds_per_bar,
                                                                               start_date, end_date, begin_time_filter, end_time_filter)
    historical_daily_data_request = 'HDT,{0},{1},{2},,,,1\n'.format(instrument, start_date, end_date)
    # historical_minute_data_request = 'HTX.{0},30,,,1\n'.format(instrument)

    if freq == 'minute':
        log.info("IQFeed historical data request: %s", historical_minute_data_request.rstrip())
    elif freq == 'daily':
        log.info("IQFeed historical data request: %s", historical_daily_data_request.rstrip())
    # Open a streaming socket to the IQFeed daemon
    # with contextlib.closing(socket.create_connection((iqfeed_host, iqfeed_port))) as iqfeed_socket:
    #     iqfeed_socket.settimeout(timeout)
    #
    #     # Send the historical data request historical_minute_data_request and buffer the data
    #     iqfeed_socket.sendall(historical_minute_data_request)
    #     data = __download_historical_data(iqfeed_socket)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((iqfeed_host, iqfeed_port))

        sock.settimeout(timeout)
        if freq == 'minute':
            sock.sendall(historical_minute_data_request)
        elif freq == 'daily':
            sock.sendall(historical_daily_data_request)
        data = _read_historical_data_socket(sock)
    finally:
        sock.close()

    bars = []
    if len(data):
        for line in data.split('\n'):
            # Returned fields in data are: datetime, high, low, open, close, volume, XXX?, YYYY?
            (datetime_str, high, low, open_, close, volume, _, _) = line.split(',')
            if volume.find('.') != -1:
                raise Exception("Float as a volume, strange...: %s" % line)

            log.debug("%s open=%s high=%s low=%s close=%s volumes=%s", datetime_str, high, low, open_, close, volume)
            if freq == 'minute':
                dt = __create_datetime(datetime_str, format_str="%Y-%m-%d %H:%M:%S")
            elif freq == 'daily':
                dt = __create_datetime(datetime_str, format_str="%Y-%m-%d %H:%M:%S").date()

            (open_, high, low, close, volume) = (float(open_), float(high), float(low), float(close), int(volume))

            bar = Bar(dt, float(open_), float(high), float(low), float(close), int(volume))
            bars.append(bar)

    log.debug("Returning %d bars", len(bars))

    return bars
