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
    stream.subscribe_quotes(print_quote, 'HKEX_00700', 'HKEX_03690', "US_DIDI")
    stream.subscribe_snapshots(print_snapshots, 'HKEX_00700')
    stream.run()

if __name__ == "__main__":
    main()
