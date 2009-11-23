#!/usr/bin/env python

from urllib2 import Request, urlopen
from urllib import urlencode
from json import loads


class Stream():


	def __init__(self, user, passwd, stream, stalk = None, **kwargs):
		'''
		stream one of:
			sample
			(user|friends|home)_timeline
			mentions
			retweeted_(by|to|of)_me
		'''
		from base64 import b64encode
		self.headers = {
			'Authorization': 'Basic ' + b64encode(
				(user + ':' + passwd).encode('utf-8')
			).decode('ascii')
		}

		self.stream = stream
		self.stalk = stalk

	def sample(self):
		''' sample the global twitter stream. '''
		url = 'http://stream.twitter.com/1/statuses/sample.json'
		for line in urlopen(Request(url, headers = self.headers)):
			try:
				yield loads(line.decode('utf-8'))['text']
			except:
				pass


	def timeline(self):
		''' get a timeline '''
		url = 'http://twitter.com/statuses/{0:s}.json'.format(self.stream)
		pagesize = 200
		for page in range(100):
			opt = { 'count': pagesize, 'page': page }
			if( self.stalk ):
				opt['screen_name'] = self.stalk

			print 'Fetching tweets', page * pagesize, 'to', (page + 1) * pagesize

			try:
				req = Request(url + '?' + urlencode(opt), headers = self.headers) 
				js = loads( '\n'.join( urlopen(req).readlines() ) )
				if len(js) < 5:
					break
				for tweet in js:
					yield tweet['text']
			except:
				break


	def __iter__(self):
		if self.stream == 'sample':
			return self.sample()
		else:
			return self.timeline()



if __name__ == '__main__':
	from credentials import Credentials
	c = Credentials()
	for m in c.stream():
		print m
