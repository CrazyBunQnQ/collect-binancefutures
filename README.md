# Binance feed collector
## Requirements
Python3: run by `python3` command.  
aiohttp: `pip3 install aiohttp`

## Run
`collect.sh [exchange] [symbols separated by comma.] [output path]`  
example: `collect.sh binancefutures btcusdt,ethusdt /mnt/data`

## Exchanges  
* **binance**: binance spot  
* **binancefutures**: binance usd(s)-m futures    
* **binancefuturescoin**: binance coin-m futures
example: `collect.sh binancefuturescoin btcusd_perp /mnt/data`   
 

**AWS tokyo region is recommended to minimize latency.**


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
