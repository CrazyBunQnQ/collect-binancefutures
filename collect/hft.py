import asyncio
import datetime
import json
import logging
import os
import signal
import sys
from multiprocessing import Process, Queue

from binance.client import Client  # pip install python-binance

from binancespot import Binance

# 若 '/training/Data/binanceKeys.json' 文件不存在则创建
file_path = '/training/Data/binanceKeys.json'
if not os.path.exists(file_path):
    # 获取文件所在的目录
    directory = os.path.dirname(file_path)
    # 如果目录不存在，则创建目录
    if not os.path.exists(directory):
        os.makedirs(directory)
        # 创建一个空的 JSON 文件
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump({
            'binance_api_key': 'your_api_key',
            'binance_api_secret': 'your_api_secret'
        }, f)

# Binance API 密钥
with open(file_path, 'r') as f:
    config = json.load(f)
api_key = config['binance_api_key']
api_secret = config['binance_api_secret']

# 创建 Binance 客户端实例
client = Client(api_key, api_secret)


def get_historical_funding_rates(symbol, limit=500):
    """
    获取指定交易对的历史资金费率。

    :param symbol: 交易对，例如 'BTCUSDT'
    :param limit: 获取记录的数量，默认为500，最大可为1000
    :return: 资金费率的历史记录
    """
    funding_rates = client.futures_funding_rate(symbol=symbol, limit=limit)
    return funding_rates


def get_high_amplitude_high_volume_tickers():
    """
    获取最近24小时高振幅且高交易量的交易对
    TODO 优化为最近 1 小时
    """
    tickers = client.get_ticker()
    usdt_pairs = [ticker for ticker in tickers if ticker['symbol'].endswith('USDT')]

    # 计算振幅并添加到ticker数据中
    for ticker in usdt_pairs:
        high_price = float(ticker['highPrice'])
        low_price = float(ticker['lowPrice'])
        open_price = float(ticker['openPrice'])
        if open_price == 0:
            continue
        price_change = high_price - low_price
        amplitude = price_change / open_price * 100
        ticker['amplitude'] = amplitude

    # 过滤交易量大于100000000且振幅超过20%的交易对
    filtered_pairs = [ticker for ticker in usdt_pairs if
                      float(ticker['quoteVolume']) > 100000000 and ticker['amplitude'] > 15]

    # 按振幅从大到小排序
    sorted_pairs = sorted(filtered_pairs, key=lambda x: x['amplitude'], reverse=True)
    # 取前三条数据
    sorted_pairs = sorted_pairs[:3]

    return sorted_pairs


def writer_proc(queue, output):
    """写入数据到文件"""
    while True:
        data = queue.get()
        if data is None:
            break
        symbol, timestamp, message = data
        date = datetime.datetime.fromtimestamp(timestamp).strftime('%Y%m%d')
        with open(os.path.join(output, '%s_%s.dat' % (symbol, date)), 'a') as f:
            f.write(str(int(timestamp * 1000000)))
            f.write(' ')
            f.write(message)
            f.write('\n')


def shutdown(s=None):
    if s is None:
        for symbol in stream_map:
            s = stream_map[symbol]
            asyncio.create_task(s.close())
    else:
        asyncio.create_task(s.close())


# 全局变量
queue = Queue()
"""数据流队列"""
stream_map = {}
"""正在运行的流"""
orders = {}
"""持有的 symbol"""


# 异步方法：更新队列和正在运行的流
async def update_queue_and_stream_map():
    global queue
    global stream_map
    global orders
    tickers_keys = []
    """最近24小时高振幅且高交易量的交易对 key"""
    keys_to_add = []
    """待加入的流"""

    # 获取最近24小时高振幅且高交易量的交易对
    tickers = get_high_amplitude_high_volume_tickers()
    """最近24小时高振幅且高交易量的交易对"""

    # 遍历 tickers, 忽略正在运行的，将新增的记录下来
    for ticker in tickers:
        symbol = ticker['symbol']
        if symbol not in stream_map:
            keys_to_add.append(symbol)
        tickers_keys.append(symbol)

    # 遍历正在运行的，删除不需要且未持有的
    for symbol in stream_map:
        if symbol not in tickers_keys and symbol not in orders:
            # 删除之前需要先关流
            shutdown(stream_map[symbol])
            # 输出日志
            logging.info('remove %s' % symbol)
            del stream_map[symbol]

    # 创建新增的数据流
    for symbol in keys_to_add:
        # 输出日志
        logging.info('add %s' % symbol)
        stream_map[symbol] = Binance(queue, symbol)


async def main():
    await update_queue_and_stream_map()
    logging.basicConfig(level=logging.DEBUG)
    writer_p = Process(target=writer_proc, args=(queue, sys.argv[1],))
    writer_p.start()
    has_connect = True
    while has_connect:
        has_connect = False
        for symbol in stream_map:
            if not stream_map[symbol].closed:
                has_connect = True
                await stream_map[symbol].connect()
        if has_connect:
            await asyncio.sleep(1)
    queue.put(None)
    writer_p.join()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, shutdown)
    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.run_until_complete(main())
