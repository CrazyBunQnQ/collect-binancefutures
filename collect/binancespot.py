import asyncio
import json
import logging
import time
import urllib.parse

import aiohttp
from aiohttp import ClientSession, WSMsgType
from yarl import URL

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Binance:
    def __init__(self, queue, symbols, timeout=7):
        self.symbols = symbols
        self.client = aiohttp.ClientSession(headers={ 'Content-Type': 'application/json' })
        self.closed = False
        self.pending_messages = {}
        self.prev_u = {}
        self.timeout = timeout
        self.keep_alive = None
        self.queue = queue

    async def __on_message(self, raw_message):
        '''
        异步处理 WebSocket 接收到的原始消息。
        使用 @ 分隔符将 stream 拆分成 tokens，例如 btcusdt@depth 会被拆分成 ['btcusdt', 'depth']
        根据消息类型和内容进行不同的处理。
        对于深度消息，它会检查消息的连续性，如果不连续则获取快照并暂存消息，否则直接处理。
        对于其他类型的消息，则直接将消息存入队列中。
        '''
        timestamp = time.time()
        message = json.loads(raw_message)
        # logging.debug(message)
        stream = message['stream']
        tokens = stream.split('@')
        if tokens[1] == 'depth':
            # 如果消息类型是 depth，表示这是一个深度消息。
            symbol = tokens[0]
            data = message['data']
            # 从数据中获取更新 ID u 和首个更新 ID U
            u = data['u']
            U = data['U']
            # 检查 prev_u（前一个更新 ID），如果是第一次接收或者 U 不是紧接在 prev_u 之后
            prev_u = self.prev_u.get(symbol)
            if prev_u is None or U != prev_u + 1:
                # 获取 pending_messages（待处理的消息队列），如果为空，记录警告日志并异步获取市场深度快照，初始化 pending_messages
                pending_messages = self.pending_messages.get(symbol)
                if pending_messages is None:
                    logging.warning('Mismatch on the book. prev_update_id=%s, U=%s' % (prev_u, U))
                    asyncio.create_task(self.__get_marketdepth_snapshot(symbol))
                    self.pending_messages[symbol] = pending_messages = []
                # 将当前消息添加到 pending_messages
                pending_messages.append((message, raw_message))
            else:
                # 如果 U 是紧接在 prev_u 之后，将消息加入队列并更新 prev_u
                self.queue.put((symbol, timestamp, raw_message))
                self.prev_u[symbol] = u
        # 对于 aggTrade（聚合交易）、trade（交易）、bookTicker（订单簿价格）、markPrice（标记价格）等其他消息类型，直接将消息加入队列 self.queue
        elif tokens[1] == 'aggTrade':
            # {
            #   "e": "aggTrade",      // 事件类型
            #   "E": 1672515782136,   // 事件时间
            #   "s": "BNBBTC",        // 交易对
            #   "a": 12345,           // 归集交易ID
            #   "p": "0.001",         // 成交价格
            #   "q": "100",           // 成交数量
            #   "f": 100,             // 被归集的首个交易ID
            #   "l": 105,             // 被归集的末次交易ID
            #   "T": 1672515782136,   // 成交时间
            #   "m": true,            // 买方是否是做市方。如true，则此次成交是一个主动卖出单，否则是一个主动买入单。
            #   "M": true             // 请忽略该字段
            # }
            symbol = tokens[0]
            self.queue.put((symbol, timestamp, raw_message))
        elif tokens[1] == 'trade':
            # {
            #   "e": "trade",        // 事件类型
            #   "E": 1672515782136,  // 事件时间
            #   "s": "BNBBTC",       // 交易对
            #   "t": 12345,          // 交易ID
            #   "p": "0.001",        // 成交价格
            #   "q": "100",          // 成交数量
            #   "T": 1672515782136,  // 成交时间
            #   "m": true,           // 买方是否是做市方。如true，则此次成交是一个主动卖出单，否则是一个主动买入单。
            #   "M": true            // 请忽略该字段
            # }
            symbol = tokens[0]
            self.queue.put((symbol, timestamp, raw_message))
        elif tokens[1] == 'bookTicker':
            # 最优挂单信息
            # {
            #   "u":400900217,     // order book updateId
            #   "s":"BNBUSDT",     // 交易对
            #   "b":"25.35190000", // 买单最优挂单价格
            #   "B":"31.21000000", // 买单最优挂单数量
            #   "a":"25.36520000", // 卖单最优挂单价格
            #   "A":"40.66000000"  // 卖单最优挂单数量
            # }
            symbol = tokens[0]
            self.queue.put((symbol, timestamp, raw_message))
        elif tokens[1] == 'markPrice':
            symbol = tokens[0]
            self.queue.put((symbol, timestamp, raw_message))
        elif tokens[1] == 'kline_1m':
            # {
            #   "e": "kline",          // 事件类型
            #   "E": 1672515782136,    // 事件时间
            #   "s": "BNBBTC",         // 交易对
            #   "k": {
            #     "t": 1672515780000,  // 这根K线的起始时间
            #     "T": 1672515839999,  // 这根K线的结束时间
            #     "s": "BNBBTC",       // 交易对
            #     "i": "1m",           // K线间隔
            #     "f": 100,            // 这根K线期间第一笔成交ID
            #     "L": 200,            // 这根K线期间末一笔成交ID
            #     "o": "0.0010",       // 这根K线期间第一笔成交价
            #     "c": "0.0020",       // 这根K线期间末一笔成交价
            #     "h": "0.0025",       // 这根K线期间最高成交价
            #     "l": "0.0015",       // 这根K线期间最低成交价
            #     "v": "1000",         // 这根K线期间成交量
            #     "n": 100,            // 这根K线期间成交数量
            #     "x": false,          // 这根K线是否完结（是否已经开始下一根K线）
            #     "q": "1.0000",       // 这根K线期间成交额
            #     "V": "500",          // 主动买入的成交量
            #     "Q": "0.500",        // 主动买入的成交额
            #     "B": "123456"        // 忽略此参数
            #   }
            # }
            symbol = tokens[0]
            self.queue.put((symbol, timestamp, raw_message))
        elif tokens[1] == 'depth20':
            # {
            #   "lastUpdateId": 160,  // 末次更新ID
            #   "bids": [             // 买单
            #     [
            #       "0.0024",         // 价
            #       "10",             // 量
            #       []                // 忽略
            #     ]
            #   ],
            #   "asks": [             // 卖单
            #     [
            #       "0.0026",         // 价
            #       "100",            // 量
            #       []                // 忽略
            #     ]
            #   ]
            # }
            symbol = tokens[0]
            self.queue.put((symbol, timestamp, raw_message))

    async def __keep_alive(self):
        '''
        使用一个无限循环和异步睡眠来实现定期发送心跳信号来保持 WebSocket 连接的活跃
        捕获并处理异常，确保在发生错误或任务被取消时能够正确终止。
        '''
        while not self.closed:
            try:
                await asyncio.sleep(5)
                await self.ws.pong()
            except asyncio.CancelledError:
                return
            except:
                logging.exception('Failed to keep alive.')
                return

    async def __curl(self, path, query=None, timeout=None, verb=None, rethrow_errors=None, max_retries=None):
        '''
        使用 aiohttp 完成 Binance API 调用请求
        调用请求时，如果返回状态码不是 200，则根据状态码进行不同的处理
        429 - 速率限制，取消订单并等待 X-RateLimit-Reset
        502 - 无法联系币安合约交易平台
        503 - 币安合约交易平台暂时停机
        '''
        if timeout is None:
            timeout = self.timeout

        # 如果附加数据则默认为 POST，否则为 GET
        if not verb:
            verb = 'POST' if query else 'GET'

        # 默认情况下不重试 POST 或 PUT。重试 GET/DELETE 是可以的，因为它们是幂等的。
        # 将来我们可以允许重试 PUT，只要不使用 'leavesQty'（不是幂等），
        # 或者您可以更改 clOrdID（设置 {"clOrdID": "new", "origClOrdID": "old"}）修正不能错误地应用两次。
        if max_retries is None:
            max_retries = 0 if verb in ['POST', 'PUT'] else 3

        if query is None:
            query = {}
        # query['timestamp'] = str(int(time.time() * 1000) - 1000)
        query = urllib.parse.urlencode(query)

        # query = query.replace('%27', '%22')

        def exit_or_throw(e):
            if rethrow_errors:
                raise e
            else:
                exit(1)

        def retry():
            self.retries += 1
            if self.retries > max_retries:
                raise Exception("Max retries on %s (%s) hit, raising." % (path, json.dumps(query or '')))
            return self.__curl(path, query, timeout, verb, rethrow_errors, max_retries)

        # Make the request
        try:
            url = URL('https://api.binance.com/api%s?%s' % (path, query), encoded=True)
            logging.info("sending req to %s: %s" % (url, json.dumps(query or query or '')))
            response = await self.client.request(verb, url, timeout=timeout)
            # Make non-200s throw
            response.raise_for_status()

        except aiohttp.ClientResponseError as e:
            # 429, 速率限制；取消订单并等待 X-RateLimit-Reset
            if e.status == 429:
                logging.error("Ratelimited on current request. Sleeping, then trying again. Try fewer " + "Request: %s \n %s" % (url, json.dumps(query)))
                logging.warning("Canceling all known orders in the meantime.")

                # logging.error("Your ratelimit will reset at %s. Sleeping for %d seconds." % (reset_str, to_sleep))
                to_sleep = 5
                logging.error("Sleeping for %d seconds." % (to_sleep))
                time.sleep(to_sleep)

                # Retry the request.
                return await retry()

            elif e.status == 502:
                logging.warning("Unable to contact the Binance Futures API (502), retrying. " + "Request: %s \n %s" % (url, json.dumps(query)))
                await asyncio.sleep(3)
                return await retry()

            # 503 - 币安合约交易平台暂时停机，可能是由于部署所致。再试一次
            elif e.status == 503:
                logging.warning("Unable to contact the Binance Futures API (503), retrying. " + "Request: %s \n %s" % (url, json.dumps(query)))
                await asyncio.sleep(3)
                return await retry()

            elif e.status == 400:
                pass
            # If we haven't returned or re-raised yet, we get here.
            logging.error("Unhandled Error: %s: %s" % (e, e.message))
            logging.error("Endpoint was: %s %s: %s" % (verb, path, json.dumps(query)))
            exit_or_throw(e)

        except asyncio.TimeoutError as e:
            # Timeout, re-run this request
            logging.warning("Timed out on request: %s (%s), retrying..." % (path, json.dumps(query or '')))
            return await retry()

        except aiohttp.ClientConnectionError as e:
            logging.warning("Unable to contact the Binance Futures API (%s). Please check the URL. Retrying. " + "Request: %s %s \n %s" % (e, url, json.dumps(query)))
            await asyncio.sleep(1)
            return await retry()

        # Reset retry counter on success
        self.retries = 0

        return await response.json()

    async def connect(self):
        '''
        异步建立与 Binance WebSocket 服务器的连接，订阅指定交易对的深度、交易和订单簿价格数据。
        在连接期间，方法处理接收到的各种类型的消息，并通过心跳信号保持连接活跃。
        包括异常处理和清理资源的机制，确保在发生错误或断开连接时能正确处理。
        '''
        try:
            # 构建 stream 字符串，包含所有需要订阅的流（深度数据、交易数据和订单簿价格数据）。
            stream = '/'.join(['%s@depth@1000ms/%s@aggTrade/%s@bookTicker/%s@kline_1m/%s@ticker_4h/%s@depth20@1000ms' % (symbol, symbol, symbol, symbol, symbol, symbol)
                               for symbol in self.symbols])
            # 构建 WebSocket URL url，格式为 wss://stream.binance.com:9443/stream?streams=%s。
            url = 'wss://stream.binance.com:9443/stream?streams=%s' % stream
            logging.info('Connecting to %s' % url)
            # 创建一个异步会话 session
            async with ClientSession() as session:
                # 建立 WebSocket 连接，返回的 WebSocket 连接对象为 ws
                async with session.ws_connect(url) as ws:
                    logging.info('%s WS Connected.' % self.symbols)
                    # 将 ws 赋值给实例变量 self.ws
                    self.ws = ws
                    # 创建一个异步任务 self.keep_alive 来保持连接的活跃，调用 self.__keep_alive() 方法。
                    self.keep_alive = asyncio.create_task(self.__keep_alive())
                    # 异步 for 循环 async for msg in ws 处理接收到的消息
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            # 调用 self.__on_message(msg.data) 处理消息数据。
                            await self.__on_message(msg.data)
                        elif msg.type == WSMsgType.BINARY:
                            pass
                        elif msg.type == WSMsgType.PING:
                            await self.ws.pong()
                        elif msg.type == WSMsgType.PONG:
                            await self.ws.ping()
                        elif msg.type == WSMsgType.ERROR:
                            exc = ws.exception()
                            raise exc if exc is not None else Exception
        except:
            logging.exception('WS Error')
        finally:
            logging.info('WS Disconnected.')
            # 即使发生异常也会执行清理操作。
            if self.keep_alive is not None:
                # 如果 self.keep_alive 不为 None，取消保持连接的任务
                self.keep_alive.cancel()
                # 等待任务完成
                await self.keep_alive
            self.ws = None
            self.keep_alive = None

    async def close(self):
        '''
        设置 self.closed 标志、关闭 WebSocket 连接和 aiohttp 客户端会话，以及异步等待一段时间，来实现关闭连接和清理资源的操作。
        '''
        self.closed = True
        if self.ws:
            await self.ws.close()
        if self.client:
            await self.client.close()
        await asyncio.sleep(1)

    async def __get_marketdepth_snapshot(self, symbol):
        '''
        异步获取市场深度的快照，并处理在此之前收到的未处理的深度更新消息。
        '''
        # 使用 /v3/depth 接口获取市场深度快照
        data = await self.__curl(verb='GET', path='/v3/depth', query={'symbol': symbol.upper(), 'limit': 1000})
        # 将获取到的市场深度数据放入队列 self.queue 中
        logging.info('Get market depth snapshot. symbol=%s %s' % (symbol, json.dumps(data)))
        self.queue.put((symbol, time.time(), json.dumps(data)))
        # 提取 lastUpdateId，这是市场深度数据的最新更新 ID
        lastUpdateId = data['lastUpdateId']
        # 初始化
        self.prev_u[symbol] = None
        # Process the pending messages.
        prev_u = None
        # 处理未处理的消息, 直到 prev_u 被设置
        while prev_u is None:
            # 获取交易对的未处理消息 pending_messages。
            pending_messages = self.pending_messages.get(symbol)
            timestamp = time.time()
            # 处理未处理的消息
            while pending_messages:
                # 从 pending_messages 中弹出消息。
                message, raw_message = pending_messages.pop(0)
                # 提取消息数据 data 以及更新 ID u 和 U
                data = message['data']
                u = data['u']
                U = data['U']
                # 根据 Binance 的 API 文档，检查 u 和 U 是否在有效范围内
                # https://binance-docs.github.io/apidocs/spot/en/#partial-book-depth-streams
                # 第一个处理的事件应具有 U <= lastUpdateId + 1 AND u >= lastUpdateId + 1
                if (u < lastUpdateId + 1 or U > lastUpdateId + 1) and prev_u is None:
                    # 如果不满足条件且 prev_u 为空，跳过当前消息。
                    continue
                if prev_u is not None and U != prev_u + 1:
                    # 如果 prev_u 不为空且 U 不等于 prev_u + 1，记录一个警告日志。
                    logging.warning('UpdateId does not match. symbol=%s, prev_update_id=%d, U=%d' % (symbol, prev_u, U))
                # 将消息放入队列 self.queue 中，并更新 self.prev_u[symbol] 和 prev_u。
                self.queue.put((symbol, timestamp, raw_message))
                self.prev_u[symbol] = prev_u = u
            if prev_u is None:
                # 如果在处理完 pending_messages 后 prev_u 仍为 None，等待 0.5 秒再重试。
                await asyncio.sleep(0.5)
        # 处理完所有未处理的消息后，将 self.pending_messages[symbol] 置为 None，表示该交易对的消息已经全部处理。
        self.pending_messages[symbol] = None
        logging.warning('The book is initialized. symbol=%s, prev_update_id=%d' % (symbol, prev_u))
