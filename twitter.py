#!/usr/bin/env python

def stream(user, passwd, stream):
	from urllib2 import Request, urlopen
	from base64 import b64encode
	from json import loads

	url = 'http://stream.twitter.com/1/statuses/{0:s}.json'.format(stream)
	req = Request(url, headers = {
		'Authorization': 'Basic ' + b64encode(
			(user + ':' + passwd).encode('utf-8')
		).decode('ascii')
	})

	for line in urlopen(req):
		try:
			yield loads(line.decode('utf-8'))['text']
		except:
			pass


if __name__ == '__main__':
	from credentials import credentials
	c = credentials()
	for m in stream( **c.strdict('api') ):
		print m
