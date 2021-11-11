"""
In this example code we will show how to shut the streamconn websocket
connection down and then up again. it's the ability to stop/start the
connection
"""
import logging
import threading
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from zinvest_trade_api.stream import Stream
from zinvest_trade_api.common import URL

ZVST_API_KEY = "<YOUR-USER-NAME>"
ZVST_SECRET_KEY = "<YOUR-PASSWORD>"

async def print_trade(t):
    print('trade', t)


async def print_quote(q):
    print('quote', q)



def consumer_thread():
    try:
        # make sure we have an event loop, if not create a new one
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    global conn
    conn = Stream(ZVST_API_KEY, ZVST_SECRET_KEY)

    conn.subscribe_quotes(print_quote, 'HKEX_00700', 'HKEX_00388')
    conn.run()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s',
                        level=logging.INFO)

    loop = asyncio.get_event_loop()
    pool = ThreadPoolExecutor(1)

    while 1:
        try:
            pool.submit(consumer_thread)
            time.sleep(20)
            loop.run_until_complete(conn.stop_ws())
            time.sleep(20)
        except KeyboardInterrupt:
            print("Interrupted execution by user")
            loop.run_until_complete(conn.stop_ws())
            exit(0)
        except Exception as e:
            print("You goe an exception: {} during execution. continue "
                  "execution.".format(e))
            # let the execution continue
            pass
