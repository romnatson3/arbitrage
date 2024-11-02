import logging
from django.db.models.signals import post_delete, post_save, pre_save, post_delete
from django.dispatch import receiver
from .models import Strategy, Position
from .helper import TaskLock


logger = logging.getLogger(__name__)


@receiver(post_delete, sender=Strategy)
def handle_delete_task(sender, **kwargs):
    strategy = kwargs['instance']
    if strategy.task:
        strategy.task.delete()
        logger.warning(f'Deleted task {strategy.task} for strategy {strategy}')


@receiver(post_save, sender=Strategy)
def handle_save_task(sender, **kwargs):
    strategy = kwargs['instance']
    if not strategy.task:
        task = strategy._create_task()
        logger.info(f'Task {task.name} successfully created', extra=strategy.extra_log)
    else:
        task = strategy._update_task()
        logger.info(f'Task {task.name} successfully updated', extra=strategy.extra_log)


@receiver(pre_save, sender=Position)
def handle_pre_save_position(sender, **kwargs):
    ...


@receiver(post_save, sender=Position)
def handle_post_save_position(sender, created, instance, **kwargs):
    if not created:
        key = f'main_lock_{instance.strategy_id}_{instance.symbol}'
        extra = (
            instance.strategy.extra_log |
            {'position': instance.id, 'symbol': instance.symbol}
        )
        if instance._is_open and not instance.is_open:
            if TaskLock(key).release():
                logger.warning('Main lock released. Position closed', extra=extra)
        if instance.mode == Strategy.Mode.trade:
            if (
                not instance._stop_loss_breakeven_set and
                instance.stop_loss_breakeven_set and
                instance.is_open
            ):
                if TaskLock(key).release():
                    logger.info(
                        'Main lock released. Stop loss breakeven set', extra=extra
                    )
    else:
        if TaskLock(
            f'main_lock_{instance.strategy.id}_{instance.symbol}', timeout=None
        ).acquire():
            logger.warning(
                'Main lock acquired before creating new position',
                extra=instance.strategy.extra_log | {'position': instance.id}
            )


@receiver(post_delete, sender=Position)
def handle_post_delete_position(sender, instance, **kwargs):
    key = f'main_lock_{instance.strategy_id}_{instance.symbol}'
    extra = (
        instance.strategy.extra_log |
        {'position': instance.id, 'symbol': instance.symbol}
    )
    if instance.is_open:
        TaskLock(key).release()
        logger.warning('Main lock released. Position deleted', extra=extra)
