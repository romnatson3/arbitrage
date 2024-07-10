import logging
import time
from typing import Any, Dict, List, Optional
import okx.Account
from django.db.models import Func, Value, DateTimeField, CharField, F
from django.db.models.expressions import RawSQL
from django.conf import settings
from .models import Account, Bill, Execution, Position
from .misc import convert_dict_values
from .exceptions import GetPositionException, GetBillsException
from .trade import OkxTrade


logger = logging.getLogger(__name__)


class OkxExchange():
    def __init__(self, account: Account):
        self.account = account
        self.client = okx.Account.AccountAPI(
            account.api_key, account.api_secret, account.api_passphrase,
            flag='1' if account.testnet else '0',
            debug=False
        )

    def get_open_positions(self) -> Dict[str, Any]:
        result = self.client.get_positions(instType='SWAP')
        if result['code'] != '0':
            raise GetPositionException(f'Failed to get positions data. {result}')
        open_positions = {i['instId']: convert_dict_values(i) for i in result['data']}
        logger.info(
            f'Found {len(open_positions)} open positions in exchange for account: {self.account.name}'
        )
        logger.info(f'Symbols: {", ".join(sorted(list(open_positions)))}')
        return open_positions

    def get_bills(self, bill_id: Optional[int] = None) -> list[dict[str, Any]]:
        if not bill_id:
            result = self.client.get_account_bills(instType='SWAP', mgnMode='isolated', type=2)
        else:
            result = self.client.get_account_bills(
                instType='SWAP', mgnMode='isolated', type=2,
                before=bill_id
            )
        if result['code'] != '0':
            raise GetBillsException(result)
        bills = list(map(convert_dict_values, result['data']))
        for i in bills:
            i['subType'] = Execution.sub_type.get(i['subType'], i['subType'])
        if bill_id:
            logger.info(
                f'Found {len(bills)} new bills in exchange, before {bill_id=}, '
                f'for account: {self.account.name}'
            )
        else:
            logger.info(f'Got {len(bills)} last bills from exchange for account: {self.account.name}')
        return bills

    def get_new_executions_for_position(self, position: Position) -> Optional[List[Dict[str, Any]]]:
        end_time = time.time() + settings.RECEIVE_TIMEOUT
        last_bill_id = position.executions.values_list('bill_id', flat=True).order_by('bill_id').last()
        if not last_bill_id:
            logger.debug('No found any executions in database', extra=position.strategy.extra_log)
        while time.time() < end_time:
            if last_bill_id:
                logger.debug(
                    f'Trying to get new executions before bill_id: {last_bill_id}',
                    extra=position.strategy.extra_log
                )
                where = {
                    'account': self.account,
                    'data__instId': position.symbol.okx.inst_id,
                    'bill_id__gt': last_bill_id
                }
            else:
                logger.debug(
                    'Trying to get new executions after position creation '
                    f'time "{position.position_data["cTime"]}"',
                    extra=position.strategy.extra_log
                )
                where = {
                    'account': self.account,
                    'data__instId': position.symbol.okx.inst_id,
                    'ts__gte': F('ctime')
                }
            executions = (
                Bill.objects.annotate(
                    ctime_str=Value(position.position_data['cTime'], output_field=CharField()),
                    ctime=Func(
                        'ctime_str',
                        Value('DD-MM-YYYY HH24:MI:SS.US'),
                        function='to_timestamp',
                        output_field=DateTimeField()
                    ),
                    ts_str=RawSQL("data->>'ts'", []),
                    ts=Func(
                        'ts_str',
                        Value('DD-MM-YYYY HH24:MI:SS.US'),
                        function='to_timestamp',
                        output_field=DateTimeField()
                    )).filter(**where).values_list('data', flat=True)
            )
            if executions:
                logger.info(
                    f'Found {len(executions)} executions', extra=position.strategy.extra_log
                )
                return executions
            else:
                if position.is_open:
                    if last_bill_id:
                        logger.debug(
                            'Not found any new executions for open position',
                            extra=position.strategy.extra_log
                        )
                        return
                    else:
                        logger.warning(
                            'Not found any executions for open position',
                            extra=position.strategy.extra_log
                        )
                else:
                    logger.warning(
                        'Not found any new executions for closed position',
                        extra=position.strategy.extra_log
                    )
            time.sleep(2)
        else:
            logger.critical(
                'Failed to get executions', extra=position.strategy.extra_log
            )

    def check_and_update_single_position(self, position: Position, open_positions: Dict[str, Any]) -> None:
        try:
            if position.symbol.okx.inst_id in open_positions:
                logger.debug('Position is still open in exchange', extra=position.strategy.extra_log)
                if position.position_data['pos'] != open_positions[position.symbol.okx.inst_id]['pos']:
                    logger.info('Position is updated in exchange', extra=position.strategy.extra_log)
                    position.position_data = open_positions[position.symbol.okx.inst_id]
                    position.save()
                    logger.info('Updated position data in database', extra=position.strategy.extra_log)
            else:
                logger.warning('Position is closed in exchange', extra=position.strategy.extra_log)
                position.is_open = False
                position.save()
                logger.info('Position is closed in database', extra=position.strategy.extra_log)
            executions = self.get_new_executions_for_position(position)
            if executions:
                for execution in executions:
                    OkxTrade.save_execution(execution, position)
        except Exception as e:
            logger.exception(e, extra=position.strategy.extra_log)
