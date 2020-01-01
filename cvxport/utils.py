
import pandas as pd
import time
import plotly.graph_objs as go
import itertools
import pathlib
from typing import Iterable, Awaitable, Type
import asyncio
import zmq.asyncio as azmq

from cvxport import const, JobError


def get_prices(tickers, root_dir, start_date=None, end_date=None) -> dict:
    """
    read price file with 'date', 'time', 'open', 'high', 'low', 'close', 'volume' format

    :param list tickers: 'EURUSD'
    :param str root_dir:
    :param str start_date:
    :param str end_date:
    :return: dict of open, high, low, close
    """
    ts = {}
    for ticker in tickers:
        panel = pd.read_csv(f'{root_dir}/{ticker}.csv', names=['date', 'time', 'open', 'high', 'low', 'close', 'vol'])
        panel['datetime'] = panel.date.astype('str') + ' ' + panel.time
        panel.datetime = pd.to_datetime(panel.datetime)
        panel = panel.set_index('datetime').drop(['date', 'time'], axis=1).loc[start_date: end_date]
        for col, series in panel.items():  # open, high, low, close
            ts.setdefault(col, []).append(series.rename(ticker))

    return {col: pd.concat(series, axis=1, join='inner') for col, series in ts.items()}


def plot_area(title, dfs: [list, pd.DataFrame]):
    if isinstance(dfs, pd.DataFrame):
        dfs = [dfs]

    fig = go.Figure()

    for group, df in enumerate(dfs):
        df = df.div(df.sum(axis=1), axis=0)
        for col in df.columns:
            fig.add_trace(go.Scatter(x=df.index, y=df[col], stackgroup=group, name='%s_%d' % (col, group), opacity=0.5))

    fig.update_layout(showlegend=True, xaxis={'hoverformat': '%d%b%Y'}, yaxis={'hoverformat': '.1%'})
    fig.update_layout(title=go.layout.Title(text=title))
    fig.show()


def plot_lines(df: pd.DataFrame, normalize=False):
    if normalize:
        df = df.div(df.iloc[0], axis=1)
    fig = go.Figure()
    for col in df.columns:
        fig.add_trace(go.Scatter(x=df.index, y=df[col], name=col))

    fig.update_layout(showlegend=True, xaxis={'hoverformat': '%d%b%Y'}, yaxis={'hoverformat': '.1%'})
    fig.show()


def pretty_print(df: pd.DataFrame, formats):
    new = pd.DataFrame(index=df.index)
    for col, fmt in zip(df.columns, formats):
        new[col] = df[col].apply(lambda x: fmt.format(x))
    print(new)


def run_with_status(_msg, _iterable, _size, _func, _update=1):
    _start = time.time()
    _lap = _start
    print(f'[  0%] {_msg}', end='')
    for _idx, _data in enumerate(_iterable):
        _func(_data)
        _now = time.time()
        if _now - _lap > _update:
            print(f'\r[{_idx / _size: 4.0%}] {_msg}', end='', flush=True)
            _lap = _now
    print(f'\r[100%,{time.time() - _start: 5.1f}s] {_msg}')


def get_risk_contribution(df: pd.DataFrame, weights, lookback=90):
    n_col = df.shape[1]
    cov = df.rolling(window=lookback).cov().values.reshape(-1, n_col, n_col)
    w = weights.reshape(-1, n_col, 1)
    marginal_rc = (cov * w).sum(axis=1)
    return pd.DataFrame(marginal_rc * weights, index=df.index, columns=df.columns).dropna()


# ==================== Data Structure Operation ====================
def flatten(iterable: Iterable):
    return list(itertools.chain(*iterable))


def unique(iterable: Iterable):
    return list(set(iterable))


# ==================== IO Operations ====================
async def wait_for(awaitable: Awaitable, wait_time: float, exception: Exception):
    """
    :param awaitable: coroutine, not coroutine function
    :param wait_time: in seconds
    :param exception: exception to be raised
    """
    try:
        return await asyncio.wait_for(awaitable, wait_time)
    except asyncio.TimeoutError:
        raise exception


async def wait_for_reply(socket: azmq.Socket, wait_time: float, desc: str):
    """
    we duplicate the code from "wait_for" because we intend to totally replace "wait_for"

    :param socket: socket from which we wait for reply
    :param wait_time: in seconds
    :param desc: request description, used in raising error
    """
    try:
        reply = await asyncio.wait_for(socket.recv_json(), wait_time)  # type: dict
    except asyncio.TimeoutError:
        raise JobError(f'{desc} times out')

    if reply.get('code', 0) < 0:
        raise JobError(const.CCode(reply['code']).name)

    return reply


# ==================== IO Operations ====================
def get_next_filename(pathname, file_prefix, extension='log'):
    path = pathlib.Path(pathname)
    if not path.exists():
        path.mkdir(parents=False)

    existing_files = [f.name for f in path.iterdir() if f.is_file() and f.name.startswith(file_prefix)]
    if len(existing_files) == 0:
        return (path / f'{file_prefix}_001.{extension}').as_posix()
    else:
        last_file = max(existing_files)
        next_index = int(last_file.split('.')[0].split('_')[1]) + 1
        return (path / f'{file_prefix}_{next_index:03d}.{extension}').as_posix()
