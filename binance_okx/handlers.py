import logging
from .helper import CacheOkxOrderId


logger = logging.getLogger(__name__)


def save_filled_limit_order_id(account_id: int, data: dict) -> None:
    cache_orders_ids = CacheOkxOrderId(account_id, data['instId'])
    if not data['ordType'] == 'limit':
        logger.info(f'Order {data["ordId"]} is not limit')
        return
    if not data['state'] == 'filled':
        logger.info(f'Limit order {data["ordId"]} is not filled')
        return
    if cache_orders_ids.add(data['ordId']):
        logger.info(f'Limit order {data["ordId"]} is filled and saved')
    else:
        logger.error(f'Failed to save order {data["ordId"]}')
