# -*- coding: utf-8 -*-

from dq import DQClient, JobConfig
from time import sleep

dq = DQClient('https://app.dataquality.pl', user='<USER_EMAIL>',
              token='<API_TOKEN>')


resp = dq.list_jobs()
#for x in resp:
#    print(x)

report = dq.job_report('c3288a26-663d-4413-917e-9c9fa32b9aea')
print(report)

dq.job_results('c3288a26-663d-4413-917e-9c9fa32b9aea', 'output1.csv')

print("Processing finished")
