# Package Scheduler.
from apscheduler.schedulers.blocking import BlockingScheduler

# Main cronjob function.
from main import sync

# Create an instance of scheduler and add function.
scheduler = BlockingScheduler()
scheduler.add_job(sync, 'interval', days=1)
# https://apscheduler.readthedocs.io/en/stable/modules/triggers/interval.html

scheduler.start()
