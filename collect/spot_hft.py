import asyncio
import datetime
import logging
import os
import signal
from asyncio import Event
from multiprocessing import Process, Queue

from binancespot import Binance

queue = Queue()
stream = None
writer_p = None


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


def shutdown():
    asyncio.create_task(stream.close())


shutdown_event = Event()


async def run_collector(symbols, output):
    global stream, writer_p, shutdown_event
    logging.basicConfig(level=logging.DEBUG)
    stream = Binance(queue, symbols)
    writer_p = Process(target=writer_proc, args=(queue, output,))
    writer_p.start()
    try:
        while not stream.closed and not shutdown_event.is_set():
            await stream.connect()
            await asyncio.sleep(1)
    finally:
        queue.put(None)
        writer_p.join()
        if not stream.closed:
            await stream.close()


def start_collector(symbols, output):
    # 在子线程中创建一个新的事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.add_signal_handler(signal.SIGTERM, shutdown)
    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.run_until_complete(run_collector(symbols, output))
    loop.close()
