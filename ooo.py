import asyncio
import base64
import hmac
import json
import logging
import os
from datetime import datetime
import websockets
from binance_okx.misc import convert_dict_values

url = 'wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999'

def get_login_subscribe():
    ts = str(int(datetime.now().timestamp()))
    sign = ts + 'GET' + '/users/self/verify'
    mac = hmac.new(bytes('686AEC4B2E1EEF91A3C2E7AD6A52DEA1', encoding='utf8'), bytes(sign, encoding='utf-8'), digestmod='sha256')
    sign = base64.b64encode(mac.digest()).decode(encoding='utf-8')
    login = {
        'op': 'login',
        'args': [{
            'apiKey': '522b36ef-5b62-401a-bad0-4a41e202b1ef',
            'passphrase': 'PONYGAS5678###a',
            'timestamp': ts,
            'sign': sign
        }]
    }
    return login

orders = {
    'op': 'subscribe',
    'args': [{
        'channel': 'orders',
        'instType': 'SWAP'
    }]
}
positions = {
    'op': 'subscribe',
    'args': [{
        'channel': 'positions',
        'instType': 'SWAP'
    }]
}

async def azz_ws():
    async for ws in websockets.connect(url):
        try:
            login = get_login_subscribe()
            await ws.send(json.dumps(login))
            async for msg_string in ws:
                try:

                    m = json.loads(msg_string)
                    ev = m.get('event')
                    data = m.get('data')
                    try:
                        channel = m['arg']['channel']
                    except KeyError:
                        channel = None

                    if ev == 'error':
                        print("Error ", msg_string)
                    elif ev in ['subscribe', 'unsubscribe']:
                        print("subscribe/unsubscribe ", msg_string)
                    elif ev == 'login':
                        print('Logged in')
                        await ws.send(json.dumps(orders))
                        await ws.send(json.dumps(positions))
                    elif data:
                        d = convert_dict_values(data[0])
                        print(f'------------{channel}------------')
                        print(json.dumps(d, indent=2))

                except Exception as e:
                    print(e)

        except (websockets.ConnectionClosed, websockets.ConnectionClosedError) as e:
            print("ConnectionClosed " + datetime.now().isoformat())
            await asyncio.sleep(3)
            continue
        except asyncio.CancelledError as e:
            break


if __name__ == '__main__':
    asyncio.run(azz_ws())
