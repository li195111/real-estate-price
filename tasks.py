from celery import Celery
from celery.schedules import crontab
import utils
# from app import rdb

task = Celery('tasks',
             broker='redis://localhost:6379/0',
             backend='redis://localhost:6379/0')

task.conf.timezone = 'UTC'

'''
@task.task
def function():
    ... 
'''
@task.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # clean jwt every 10 seconds.
    # sender.add_periodic_task(10.0, clean_key_pairs.s(), name='clean every 10s')
    pass
