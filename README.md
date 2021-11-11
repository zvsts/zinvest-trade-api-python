
# Zinvest-trade-api-python

`zinvest-trade-api-python` 尊嘉证券行情API，用户可以订阅港股、美股实时行情，包含买卖10档盘口与最新价。说明文档：(https://www.zvsts.com/api).

同时尊嘉证券提供免费实时的RESTful交易API。编程语言不限于Python/JS/JAVA/C++/Go等。说明文档：(https://www.zvsts.com/api).

## 安装 
python>=3.6
```
python-dateutil==2.8.1
msgpack==0.5.6
websockets==10.0

```
下载执行后安装
```bash
pip install -r requirements.txt 
```

## API Keys
应用市场搜索下载'尊嘉金融'APP，注册登录获取实时行情订阅权限。

| Environment                      | default                                                                                | Description                                                                                                            |
| -------------------------------- | -------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| ZVST_API_KEY_ID=<key_id>         |                                                                                        | 尊嘉金融APP用户名                                                                                                         |
| ZVST_API_SECRET_KEY=<secret_key> |                                                                                        | 尊嘉金融APP登录密码                                                                                                  |


# 服务
* 数据:
  * [实时行情订阅](https://www.zvsts.com/api)
  
## 怎样订阅实时港股、美股行情
### 实时行情包含如下类型：
* Quotes (https://www.zvsts.com/api)
* Snapshots (https://www.zvsts.com/api)

### 实时订阅行情参考：'stream_example.py'
```py
import logging

from zinvest_trade_api.stream import Stream
log = logging.getLogger(__name__)

async def print_quote(q):
    print('quote', q)

async def print_snapshots(s):
    print('snapshots', s)


def main():
    logging.basicConfig(level=logging.INFO)
    stream = Stream(key_id='test', secret_key='test')
    stream.subscribe_quotes(print_quote, 'HKEX_00700', 'HKEX_03690')
    stream.subscribe_snapshots(print_snapshots, 'HKEX_00700')
    stream.run()

if __name__ == "__main__":
    main()

```
### 更多示例可以参考example目录：
* 停止和恢复websocket连接。
* 动态订阅或取消订阅某些股票。