import logging
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver
from .models import Strategy


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
