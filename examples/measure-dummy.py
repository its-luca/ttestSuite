#!/usr/bin/python3
import requests
import time
nframes=1

acq=0
r = requests.post('http://localhost:8080/start',json={"TotalNumberOfTraceFiles":nframes})
if r.status_code != requests.codes.ok:
	print("failed to inform server of start:",r.content,' code: ',r.reason)
	exit(1)
	pass

sendingDisabled=False
while acq < nframes:
	print('simulate gathering data from scope...')
	time.sleep(2)
	fileName = 'trace (1).wfm'
	print('done\n')

	print('sending data')
	if not sendingDisabled:
		r = requests.post('http://localhost:8080/newFile',json={"FileName":fileName})
		if r.status_code != requests.codes.ok:
			print("failed to send new file to server, disabling sending:",r.content,' code: ',r.reason)
			sendingDisabled=True
			pass
		pass
	acq=acq+1



