# -*- coding: utf-8 -*-

from time import sleep

from dq import DQClient, JobConfig

dq = DQClient('https://app.dataquality.pl', user='<USER_EMAIL>',
              token='<API_TOKEN>')

print("Account status:")
print(dq.account_status())
#
print("Listing previous jobs")
print('-'*60)
resp = dq.list_jobs()
for x in resp[-10:]:
    print(x)
print('-'*60)

input_data = '''"ID","ADRES"
1234,"02-724, Warszawa, ul. Wołodyjowskiego 38a"
'''

job_config = JobConfig('my job 1')
job_config.input_format(field_separator=';', text_delimiter='"', has_header=True)
job_config.input_column(0, name='id', function='PRZEPISZ')
job_config.input_column(1, name='adres', function='CALY_ADRES')
job_config.extend(gus=True, geocode=True, diagnostic=True)

# submit job with input data as string
job = dq.submit_job(job_config, input_data=input_data)

# submit job with input data as file in default encoding (utf-8)
#job = dq.submit_job(job_config, input_file='my_data.csv')

# submit job with input data as file in custom encoding
#job = dq.submit_job(job_config, input_file='my_file.csv', input_file_encoding='windows-1250')

print("Job Created: %s %s %s" % (job.id, job.name, job.status))
job_id = job.id

sleep(1.0)

job_state = dq.job_state(job_id)
print("Job state: %s" % job_state)

finished_ok = False

for x in range(0, int(7200/5)):

    job_state = dq.job_state(job_id)
    if job_state == 'FINISHED':
        finished_ok = True
        print('finished jobid: {}'.format(job_id))
        break

    print('status %s: %s' % (x, job_state))
    sleep(5.0)

if not finished_ok:
    print("Something went wrong during job execution")
else:
    print('Getting job results ...')

    report = dq.job_report(job_id)
    print(report)

    dq.job_results(job_id, 'output1.csv')

    print("Processing finished")
