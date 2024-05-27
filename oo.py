from binance_okx.models import Symbol, OkxSymbol, Account, Strategy, Position; import okx.PublicData; from django.db import connection;from django.core.cache import cache; from binance_okx.tasks import update_okx_symbols,update_binance_symbols; from binance.um_futures import UMFutures; from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient; client = UMFutures(key='8cdf7562faec8a4d38ce814aafcb24f478a1410d7a2dd7025391311fa75525e3', secret='609ec54e3d56ff9145440c7b22f97903af3c884a37ac18d7278871136ca7df03', show_limit_usage=True, show_header=True); import okx.MarketData,  okx.Trade; import okx.PublicData as PublicData; public = PublicData.PublicAPI(flag='0'); from binance_okx.tasks import CachePrice; from binance_okx.trade import OkxTrade; from binance_okx.helper import calc; from binance_okx.misc import convert_dict_values; s = Strategy.objects.last()
ss=Symbol.objects.get(symbol='1INCHUSDT'); t=OkxTrade(s, ss, 'long'); sy='1INCH-USDT-SWAP'

l=[]

for i in range(1000):
    if i == 0:
        d=t.account.get_account_bills()['data']
        # d.sort(key=lambda x: x['ts'])
        b=d[-1]['billId']
        e=[convert_dict_values(i) for i in d if i['type']=='2']
    else:
        d=t.account.get_account_bills(before=b)['data']
        # d.sort(key=lambda x: x['ts'])
        e=[convert_dict_values(i) for i in d if i['type']=='2']
        b=d[-1]['billId']
    print(len(e), d[-1]['ts'])
    l.extend(e)
