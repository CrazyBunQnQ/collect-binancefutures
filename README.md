# Binance feed 采集器

> https://developers.binance.com/docs/zh-CN/binance-spot-api-docs/web-socket-streams#%E6%8C%89symbol%E7%9A%84%E6%BB%9A%E5%8A%A8%E7%AA%97%E5%8F%A3%E7%BB%9F%E8%AE%A1

## 要求
Python3: run by `python3` command.  
aiohttp: `pip3 install aiohttp`

## Run
`collect.sh [exchange] [symbols separated by comma.] [output path]`  
示例: `collect.sh binancefutures btcusdt,ethusdt,bnbusdt /training/Data/binanceData/hft`

> `kill -9 $(ps -ef | grep collect | grep -v grep | awk '{print $2}')`
>
> `/notebook/Quantitative/collect-binancefutures/collect.sh binancefutures btcusdt,ethusdt,bnbusdt /training/Data/binanceData/hft`

## Exchanges
* **binance**: binance 现货
* **binancefutures**: binance U本位期货
* **binancefuturescoin**: binance 币本位期货
示例: `collect.sh binancefuturescoin btcusd_perp /mnt/data`


**建议 AWS 东京地区以尽量减少延迟。**


# Converter: 将数据提供给 Pandas Dataframe pickle 文件
## Requirements
Python3: run by `python3` command.  
pandas: `pip3 install pandas`

## Run
`convert.sh [-s INITIAL_MARKET_DEPTH_SNAPSHOT] [-f] [-c] src_file dst_path

option:  
with -f: 包括 mark price, funding, book ticker streams  
without -f: 仅市场深度和 trade 流  
with -c: 正确的交易时间戳单调增加  
  
example:  
`convert.sh /mnt/data/btcusdt_20220811.dat /mnt/data`  
or with the initial market depth snapshot  
`convert.sh /mnt/data/btcusdt_20220811.dat /mnt/data -s /mnt/data/btcusdt_20220810.snapshot.pkl`
  
`/mnt/data/btcusdt_20220810.snapshot.pkl` 是 20220810 的日终市场深度快照，因此它是 20220811 的初始市场深度快照。  
