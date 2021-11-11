"""
In this example code we will show a pattern that allows a user to change
the websocket subscriptions as they please.
"""
import logging
import threading
import asyncio
import time
from zinvest_trade_api.stream import Stream
from zinvest_trade_api.common import URL

ZVST_API_KEY = "<YOUR-API-KEY>"
ZVST_SECRET_KEY = "<YOUR-SECRET-KEY>"


async def print_trade(t):
    print('trade', t)


async def print_quote(q):
    print('quote', q)


PREVIOUS = None


def consumer_thread():
    try:
        # make sure we have an event loop, if not create a new one
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    global conn
    conn = Stream(ZVST_API_KEY, ZVST_SECRET_KEY)

    conn.subscribe_quotes(print_quote, 'HKEX_00700')
    global PREVIOUS
    PREVIOUS = "HKEX_00700"
    conn.run()

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO)
    threading.Thread(target=consumer_thread).start()

    loop = asyncio.get_event_loop()

    time.sleep(5)  # give the initial connection time to be established
    subscriptions = {"HKEX_00981": print_quote,
                     "HKEX_00388": print_quote,
                     "HKEX_03690": print_quote,
                     }

    while 1:
        for ticker, handler in subscriptions.items():
            conn.unsubscribe_quotes(PREVIOUS)
            conn.subscribe_quotes(handler, ticker)
            PREVIOUS = ticker
            time.sleep(20)
