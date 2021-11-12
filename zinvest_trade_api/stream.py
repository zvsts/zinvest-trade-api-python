"""
Stream
Subscribe quotes and snapshot
quote {'a': [{'p': 470.0, 's': 16700}, {'p': 470.20001220703125, 's': 11900}, {'p': 470.3999938964844, 's': 6400}, {'p': 470.6000061035156, 's': 6900}, {'p': 470.79998779296875, 's': 7300}, {'p': 471.0, 's': 30800}, {'p': 471.20001220703125, 's': 8100}, {'p': 471.3999938964844, 's': 6300}, {'p': 471.6000061035156, 's': 1600}, {'p': 471.79998779296875, 's': 900}], 'b': [{'p': 469.6000061035156, 's': 4300}, {'p': 469.3999938964844, 's': 6300}, {'p': 469.20001220703125, 's': 5300}, {'p': 469.0, 's': 10700}, {'p': 468.79998779296875, 's': 4200}, {'p': 468.6000061035156, 's': 7000}, {'p': 468.3999938964844, 's': 5700}, {'p': 468.20001220703125, 's': 8400}, {'p': 468.0, 's': 37600}, {'p': 467.79998779296875, 's': 14400}], 'S': 'HKEX_00700', 'T': 'q', 't': '000000'}
snapshots: {'S': 'HKEX_00700', 'c': 469.79998779296875, 'T': 's', 't': '114212', 'v': 21382187, 'h': 480.0, 'l': 467.0, 'o': 478.0}
"""

import asyncio
import logging
import json
import msgpack
import re
import websockets
import queue

from .common import get_base_url, get_data_stream_url, get_credentials, URL

log = logging.getLogger(__name__)


def _ensure_coroutine(handler):
    if not asyncio.iscoroutinefunction(handler):
        raise ValueError('handler must be a coroutine function')


def convert(data):
    if isinstance(data, bytes):
        return data.decode('ascii')
    if isinstance(data, dict):
        return dict(map(convert, data.items()))
    if isinstance(data, tuple):
        return map(convert, data)
    if isinstance(data, list):
        return list(map(convert, data))
    return data

def convertToObj(data):
    if isinstance(data, str):
        return json.loads(data)
    else:
        return data

class _DataStream():
    def __init__(self,
                 endpoint: str,
                 key_id: str,
                 secret_key: str,
                 raw_data: bool = False) -> None:
        self._endpoint = endpoint
        self._key_id = key_id
        self._secret_key = secret_key
        self._ws = None
        self._running = False
        self._raw_data = raw_data
        self._stop_stream_queue = queue.Queue()
        self._handlers = {
            'trades':    {},
            'quotes':    {},
            'bars':      {},
            'dailyBars': {},
            'snapshots': {},
        }
        self._name = 'data'
        self._should_run = True

    async def _connect(self):
        try:
            self._ws = await websockets.connect(
                self._endpoint,
                extra_headers={'Content-Type': 'application/msgpack'})
            r = await self._ws.recv()
            msg = msgpack.unpackb(r)
            # msg = convert(msg)
            # msg = convertToObj(msg)
            if msg[0]['T'] != 'success' or msg[0]['msg'] != 'connected':
                raise ValueError('connected message not received')
        except:
            log.error(f'connected failed ，waiting 1 seconds to reconnect.')
            await asyncio.sleep(1)

    async def _auth(self):
        await self._ws.send(
            msgpack.packb({
                'action': 'auth',
                'key':    self._key_id,
                'secret': self._secret_key,
            }))
        r = await self._ws.recv()
        msg = msgpack.unpackb(r)
        # msg = convert(msg)
        # msg = convertToObj(msg)
        if msg[0]['T'] == 'error':
            raise ValueError(msg[0].get('msg', 'auth failed'))
        if msg[0]['T'] != 'success' or msg[0]['msg'] != 'authenticated':
            raise ValueError('failed to authenticate')

    async def _start_ws(self):
        await self._connect()
        if self._ws:
            await self._auth()
            log.info(f'connected to: {self._endpoint}')

    async def close(self):
        if self._ws:
            await self._ws.close()
            self._ws = None
            self._running = False

    async def stop_ws(self):
        self._should_run = False
        if self._stop_stream_queue.empty():
            self._stop_stream_queue.put_nowait({"should_stop": True})

    async def _consume(self):
        while True:
            if not self._stop_stream_queue.empty():
                self._stop_stream_queue.get(timeout=1)
                await self.close()
                break
            else:
                try:
                    r = await asyncio.wait_for(self._ws.recv(), 5)
                    msgs = msgpack.unpackb(r)
                    # msgs = convert(msgs)
                    # msgs = convertToObj(msgs)
                    for msg in msgs:
                        await self._dispatch(msg)
                except asyncio.TimeoutError:
                    # ws.recv is hanging when no data is received. by using
                    # wait_for we break when no data is received, allowing us
                    # to break the loop when needed
                    pass

    def _cast(self, msg_type, msg):
        result = msg
        if not self._raw_data:
            return result
        return result

    async def _dispatch(self, msg):
        msg_type = msg.get('T')
        symbol = msg.get('S')
        if msg_type == 't':
            handler = self._handlers['trades'].get(
                symbol, self._handlers['trades'].get('*', None))
            if handler:
                await handler(self._cast(msg_type, msg))
        elif msg_type == 'q':
            handler = self._handlers['quotes'].get(
                symbol, self._handlers['quotes'].get('*', None))
            if handler:
                await handler(self._cast(msg_type, msg))
        elif msg_type == 'b':
            handler = self._handlers['bars'].get(
                symbol, self._handlers['bars'].get('*', None))
            if handler:
                await handler(self._cast(msg_type, msg))
        elif msg_type == 'd':
            handler = self._handlers['dailyBars'].get(
                symbol, self._handlers['dailyBars'].get('*', None))
            if handler:
                await handler(self._cast(msg_type, msg))
        elif msg_type == 's':
            handler = self._handlers['snapshots'].get(
                symbol, self._handlers['snapshots'].get('*', None))
            if handler:
                await handler(self._cast(msg_type, msg))
        elif msg_type == 'subscription':
            sub = [f'{k}: {msg.get(k, [])}' for k in self._handlers]
            log.info(f'subscribed to {", ".join(sub)}')
        elif msg_type == 'success':
            log.info(f'sucess: {msg.get("msg")}')
        elif msg_type == 'error':
            log.error(f'error: {msg.get("msg")} ({msg.get("code")})')

    def _subscribe(self, handler, symbols, handlers):
        _ensure_coroutine(handler)
        for symbol in symbols:
            handlers[symbol] = handler
        if self._running:
            asyncio.get_event_loop().run_until_complete(self._subscribe_all())

    async def _subscribe_all(self):
        if any(self._handlers.values()):
            msg = {
                k: tuple(v.keys())
                for k, v in self._handlers.items()
                if v
            }
            msg['action'] = 'subscribe'
            await self._ws.send(msgpack.packb(msg))

    async def _unsubscribe(self,
                           trades=(),
                           quotes=(),
                           bars=(),
                           daily_bars=(),
                           snapshots=()):
        if trades or quotes or bars or daily_bars or snapshots:
            await self._ws.send(
                msgpack.packb({
                    'action':    'unsubscribe',
                    'trades':    trades,
                    'quotes':    quotes,
                    'bars':      bars,
                    'dailyBars': daily_bars,
                    'snapshots': snapshots,
                }))

    async def _run_forever(self):
        # do not start the websocket connection until we subscribe to something
        while not any(self._handlers.values()):
            if not self._stop_stream_queue.empty():
                # the ws was signaled to stop before starting the loop so
                # we break
                self._stop_stream_queue.get(timeout=1)
                return
            await asyncio.sleep(0.1)
        log.info(f'started {self._name} stream')
        self._should_run = True
        self._running = False
        while True:
            try:
                if not self._should_run:
                    # when signaling to stop, this is how we break run_forever
                    log.info("{} stream stopped".format(self._name))
                    return
                if not self._running:
                    log.info("starting {} websocket connection".format(
                        self._name))
                    await self._start_ws()
                    if self._ws:
                        await self._subscribe_all()
                        self._running = True
                        await self._consume()
            except websockets.WebSocketException as wse:
                await self.close()
                self._running = False
                log.warn('data websocket error, restarting connection: ' +
                         str(wse))
            except Exception as e:
                log.exception('error during websocket '
                              'communication: {}'.format(str(e)))
            finally:
                await asyncio.sleep(0.01)

    def subscribe_trades(self, handler, *symbols):
        self._subscribe(handler, symbols, self._handlers['trades'])

    def subscribe_quotes(self, handler, *symbols):
        self._subscribe(handler, symbols, self._handlers['quotes'])

    def subscribe_bars(self, handler, *symbols):
        self._subscribe(handler, symbols, self._handlers['bars'])

    def subscribe_daily_bars(self, handler, *symbols):
        self._subscribe(handler, symbols, self._handlers['dailyBars'])

    def subscribe_snapshots(self, handler, *symbols):
        self._subscribe(handler, symbols, self._handlers['snapshots'])

    def unsubscribe_trades(self, *symbols):
        if self._running:
            asyncio.get_event_loop().run_until_complete(
                self._unsubscribe(trades=symbols))
        for symbol in symbols:
            del self._handlers['trades'][symbol]

    def unsubscribe_quotes(self, *symbols):
        if self._running:
            asyncio.get_event_loop().run_until_complete(
                self._unsubscribe(quotes=symbols))
        for symbol in symbols:
            del self._handlers['quotes'][symbol]

    def unsubscribe_bars(self, *symbols):
        if self._running:
            asyncio.get_event_loop().run_until_complete(
                self._unsubscribe(bars=symbols))
        for symbol in symbols:
            del self._handlers['bars'][symbol]

    def unsubscribe_daily_bars(self, *symbols):
        if self._running:
            asyncio.get_event_loop().run_until_complete(
                self._unsubscribe(daily_bars=symbols))
        for symbol in symbols:
            del self._handlers['dailyBars'][symbol]

    def unsubscribe_snapshots(self, *symbols):
        if self._running:
            asyncio.get_event_loop().run_until_complete(
                self._unsubscribe(snapshots=symbols))
        for symbol in symbols:
            del self._handlers['snapshots'][symbol]

class DataStream(_DataStream):
    def __init__(self,
                 key_id: str,
                 secret_key: str,
                 base_url: URL,
                 raw_data: bool = True):
        base_url = re.sub(r'^http', 'ws', base_url)
        super().__init__(endpoint=base_url,
                         key_id=key_id,
                         secret_key=secret_key,
                         raw_data=raw_data,
                         )
        self._handlers['statuses'] = {}
        self._handlers['lulds'] = {}
        self._name = 'stock data'

    def _cast(self, msg_type, msg):
        result = super()._cast(msg_type, msg)
        if not self._raw_data:
            return  result
        return result

    async def _dispatch(self, msg):
        msg_type = msg.get('T')
        symbol = msg.get('S')
        if msg_type == 'ss':
            handler = self._handlers['statuses'].get(
                symbol, self._handlers['statuses'].get('*', None))
            if handler:
                await handler(self._cast(msg_type, msg))
        elif msg_type == 'l':
            handler = self._handlers['lulds'].get(
                symbol, self._handlers['lulds'].get('*', None))
            if handler:
                await handler(self._cast(msg_type, msg))
        else:
            await super()._dispatch(msg)

    async def _unsubscribe(self,
                           trades=(),
                           quotes=(),
                           bars=(),
                           daily_bars=(),
                           snapshots=(),
                           statuses=(),
                           lulds=()):
        if trades or quotes or bars or daily_bars or snapshots or statuses or lulds:
            await self._ws.send(
                msgpack.packb({
                    'action':    'unsubscribe',
                    'trades':    trades,
                    'quotes':    quotes,
                    'bars':      bars,
                    'snapshots': snapshots,
                    'dailyBars': daily_bars,
                    'statuses':  statuses,
                    'lulds':     lulds,
                }))

    def subscribe_statuses(self, handler, *symbols):
        self._subscribe(handler, symbols, self._handlers['statuses'])

    def subscribe_lulds(self, handler, *symbols):
        self._subscribe(handler, symbols, self._handlers['lulds'])

    def unsubscribe_statuses(self, *symbols):
        if self._running:
            asyncio.get_event_loop().run_until_complete(
                self._unsubscribe(statuses=symbols))
        for symbol in symbols:
            del self._handlers['statuses'][symbol]

    def unsubscribe_lulds(self, *symbols):
        if self._running:
            asyncio.get_event_loop().run_until_complete(
                self._unsubscribe(lulds=symbols))
        for symbol in symbols:
            del self._handlers['lulds'][symbol]

class Stream:
    def __init__(self,
                 key_id: str = None,
                 secret_key: str = None,
                 data_stream_url: URL = None,
                 raw_data: bool = True):
        self._key_id, self._secret_key, _ = get_credentials(key_id, secret_key)
        self._data_steam_url = data_stream_url or get_data_stream_url()

        self._data_ws = DataStream(self._key_id,
                                   self._secret_key,
                                   self._data_steam_url,
                                   raw_data)

    def subscribe_trade_updates(self, handler):
        self._trading_ws.subscribe_trade_updates(handler)

    def subscribe_trades(self, handler, *symbols):
        self._data_ws.subscribe_trades(handler, *symbols)

    def subscribe_quotes(self, handler, *symbols):
        self._data_ws.subscribe_quotes(handler, *symbols)

    def subscribe_snapshots(self, handler, *symbols):
        self._data_ws.subscribe_snapshots(handler, *symbols)

    def subscribe_bars(self, handler, *symbols):
        self._data_ws.subscribe_bars(handler, *symbols)

    def subscribe_daily_bars(self, handler, *symbols):
        self._data_ws.subscribe_daily_bars(handler, *symbols)

    def subscribe_statuses(self, handler, *symbols):
        self._data_ws.subscribe_statuses(handler, *symbols)

    def subscribe_lulds(self, handler, *symbols):
        self._data_ws.subscribe_lulds(handler, *symbols)


    def on_trade_update(self, func):
        self.subscribe_trade_updates(func)
        return func

    def on_trade(self, *symbols):
        def decorator(func):
            self.subscribe_trades(func, *symbols)
            return func

        return decorator

    def on_quote(self, *symbols):
        def decorator(func):
            self.subscribe_quotes(func, *symbols)
            return func

        return decorator

    def on_bar(self, *symbols):
        def decorator(func):
            self.subscribe_bars(func, *symbols)
            return func

        return decorator

    def on_snapshots(self, *symbols):
        def decorator(func):
            self.subscribe_snapshots(func, *symbols)
            return func

        return decorator

    def on_daily_bar(self, *symbols):
        def decorator(func):
            self.subscribe_daily_bars(func, *symbols)
            return func

        return decorator

    def on_status(self, *symbols):
        def decorator(func):
            self.subscribe_statuses(func, *symbols)
            return func

        return decorator

    def unsubscribe_trades(self, *symbols):
        self._data_ws.unsubscribe_trades(*symbols)

    def unsubscribe_quotes(self, *symbols):
        self._data_ws.unsubscribe_quotes(*symbols)

    def unsubscribe_snapshots(self, *symbols):
        self._data_ws.unsubscribe_snapshots(*symbols)

    def unsubscribe_bars(self, *symbols):
        self._data_ws.unsubscribe_bars(*symbols)

    def unsubscribe_daily_bars(self, *symbols):
        self._data_ws.unsubscribe_daily_bars(*symbols)

    def unsubscribe_statuses(self, *symbols):
        self._data_ws.unsubscribe_statuses(*symbols)

    def unsubscribe_lulds(self, *symbols):
        self._data_ws.unsubscribe_lulds(*symbols)

    async def _run_forever(self):
        await asyncio.gather(self._data_ws._run_forever())

    def run(self):
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(self._run_forever())
        except KeyboardInterrupt:
            print('keyboard interrupt, bye')
            pass

    async def stop_ws(self):
        """
        Signal the ws connections to stop listenning to api stream.
        """

        if self._data_ws:
            await self._data_ws.stop_ws()

    def is_open(self):
        """
        Checks if either of the websockets is open
        :return:
        """
        open_ws =  self._data_ws._ws # noqa
        if open_ws:
            return True
        return False
