import asyncio
import json
import logging
import os
import signal
import sys
import threading
import time

from binance.client import Client  # pip install python-binance

from spot_hft import start_collector, run_collector, shutdown_event

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_api_credentials(file_path):
    """
    从 JSON 文件中加载 API key 和 secret
    :param file_path: JSON 文件路径, 若不存在将会自动创建
    :return: API key 和 secret
    """
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
    # 读取 JSON 文件并返回 API key 和 secret
    with open(file_path, 'r') as f:
        config = json.load(f)
    return config['binance_api_key'], config['binance_api_secret']


def get_high_amplitude_high_volume_tickers(min_volume=8000000, min_amplitude=5):
    """
    获取最近 1 小时高振幅且高交易量的交易对
    """
    tickers = client.get_ticker()
    usdt_pairs = [ticker for ticker in tickers if ticker['symbol'].endswith('USDT')]
    # 按照交易量排序后取前 100 条数据
    usdt_pairs = sorted(usdt_pairs, key=lambda x: float(x['quoteVolume']), reverse=True)[:100]
    # 筛选满足条件的币种并根据振幅降序排列
    selected_symbols = {}
    for ticker in usdt_pairs:
        symbol = ticker['symbol']
        try:
            # 获取该币种在指定时间内的K线数据，间隔为1小时
            klines = client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_3MINUTE, limit=20)
            if klines:
                # 跳过不足 20 条的数据
                if len(klines) < 20:
                    continue
                # 计算交易量和振幅
                open_price = float(klines[0][1])
                if open_price == 0:
                    continue
                # 计算1小时的最高价、最低价、总交易量
                high_price = float(klines[0][2])
                low_price = float(klines[0][3])
                volume = float(klines[0][5])
                for kline in klines[1:]:
                    high_price = max(high_price, float(kline[2]))
                    low_price = min(low_price, float(kline[3]))
                    volume += float(kline[5])
                # 计算振幅并保留 2 位小数
                amplitude = round((high_price - low_price) / open_price * 100, 2)
                """振幅计算公式：(最高价 - 最低价) / 开盘价 * 100%"""
                # 筛选出交易量大于10000000且振幅大于5%的币种
                if volume > min_volume and amplitude > min_amplitude and amplitude < 200:
                    print('high_price: %s, low_price: %s, volume: %s, amplitude: %s' % (
                        high_price, low_price, round(volume, 2), amplitude))
                    #
                    selected_symbols[symbol] = {'volume': volume, 'amplitude': amplitude, 'symbol': symbol}
        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")
    # 按振幅排序后取出振幅最高的三条数据
    sorted_pairs = sorted(selected_symbols.values(), key=lambda x: x['amplitude'], reverse=True)[:3]
    return sorted_pairs


def update_symbols(symbols, orders):
    """更新 symbols 集合，返回更新后的 symbols 集合"""
    if symbols is None:
        logging.warning('symbols is None')
        symbols = set()
    if len(symbols) == 0:
        logging.warning('symbols is empty')
        pre_symbols = set()
    else:
        pre_symbols = symbols.copy()
    tickers_keys = []
    keys_to_add = []
    keys_to_del = []

    tickers = get_high_amplitude_high_volume_tickers(10000000, 5)
    logging.info('更新最近 1 小时高振幅且高交易量的交易对, 数量: %s - %s' % (len(tickers), tickers))

    for ticker in tickers:
        symbol = ticker['symbol'].lower()
        if symbol not in symbols:
            keys_to_add.append(symbol)
        tickers_keys.append(symbol)

    for symbol in symbols:
        if symbol not in tickers_keys and symbol not in orders:
            logging.info('remove %s' % symbol)
            keys_to_del.append(symbol)

    symbols.difference_update(keys_to_del)
    symbols.update(keys_to_add)

    if symbols == pre_symbols:
        logging.info('symbols not changed')
        return None
    else:
        logging.info('开始采集 %s 数据' % symbols)
        return symbols


def start_collector(symbols, output):
    """启动 collector 线程"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(run_collector(symbols, output))
    finally:
        loop.close()


def shutdown_collector():
    """关闭 collector 线程"""
    shutdown_event.set()


# 管理 collector 线程和关闭事件
collector_thread = None


def manage_collector(symbols):
    """管理 collector 线程"""
    global collector_thread
    # 如果 collector 线程已经启动，关闭它
    if collector_thread is not None:
        logging.info('关闭当前采集线程...')
        shutdown_collector()  # 通过事件通知子线程关闭
        collector_thread.join()  # 等待线程完全停止
        collector_thread = None  # 重要：清除旧的线程对象引用
        logging.info('当前采集线程已成功停止')

    if len(symbols) > 0:
        # 启动新的 collector 线程
        logging.info('启动采集线程')
        collector_thread = threading.Thread(target=start_collector, args=(symbols, '/root/test'))
        collector_thread.start()


# 在主线程中监听 SIGTERM 和 SIGINT 来设置 shutdown_event
def handle_signals(signal_num, frame):
    """处理信号，关闭 collector 线程并退出"""
    shutdown_collector()
    if collector_thread is not None:
        collector_thread.join()
    sys.exit(0)  # 确保主进程也会退出


signal.signal(signal.SIGTERM, handle_signals)
signal.signal(signal.SIGINT, handle_signals)

file_path = '/training/Data/binanceKeys.json'
"""存放 API key 和 secret 的 JSON 文件路径"""

# 全局变量
api_key, api_secret = load_api_credentials(file_path)

client = Client(api_key, api_secret)
"""Binance 客户端实例"""
coins = set()
"""正在运行的 symbol"""
cur_orders = {}
"""持有的 symbol"""
keep_update = None
"""定时更新任务"""

while True:
    # 更新 coins
    coins = update_symbols(coins, cur_orders)
    if coins is not None:
        manage_collector(list(coins))
    # 延迟 30 分钟
    logging.info('30 分钟后再次更新高频交易对')
    time.sleep(1800)
