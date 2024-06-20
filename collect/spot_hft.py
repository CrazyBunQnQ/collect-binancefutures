import asyncio
import datetime
import json
import logging
import os
import time
from multiprocessing import Process, Queue

from binance import Client

from binancespot import Binance

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [PID:%(process)d] - %(message)s')


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


api_key, api_secret = load_api_credentials('/training/Data/binanceKeys.json')
client = Client(api_key, api_secret)
queue = Queue()
current_processes = {}
output_dir = '/root/test'


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


def writer_proc(queue, output):
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


def main():
    global current_processes
    while True:
        try:
            active_symbols = get_high_amplitude_high_volume_tickers()
            required_symbols = {item['symbol']: item for item in active_symbols}
            current_symbols = set(current_processes.keys())

            # 需要启动的新进程
            to_start = required_symbols.keys() - current_symbols
            # 需要停止的进程
            to_stop = current_symbols - required_symbols.keys()

            # 停止不再需要的采集进程
            for symbol in to_stop:
                proc = current_processes.pop(symbol)
                proc.terminate()
                proc.join()  # Ensure the process has exited before continuing.
                logging.info(f'Stopped collecting for {symbol}.')

            # 启动新的采集进程
            for symbol in to_start:
                p = Process(target=start_collecting, args=(symbol, queue, output_dir))
                p.start()
                current_processes[symbol] = p
                logging.info(f'Started collecting for {symbol}.')

        except Exception as e:
            logging.error(f"Error in main loop: {str(e)}")

        time.sleep(1800)


def start_collecting(symbol, queue, output):
    """启动针对特定交易对的数据采集进程"""
    logging.info(f'Starting collection for {symbol}')
    # symbol 转为小写
    binance_collector = Binance(queue, [symbol.lower()])
    asyncio.run(binance_collector.connect())


if __name__ == "__main__":
    writer_p = Process(target=writer_proc, args=(queue, output_dir))
    writer_p.start()
    main()
