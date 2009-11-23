#!/usr/bin/env python

class Credentials():

	def __init__(self):
		# Load settings
		import json
		try:
			self.cred = json.load( open('cred.json') )
		except:
			self.cred = {'mysql': {
				'host':   raw_input('MySQL    host: '),
				'db':     raw_input('     database: '),
				'user':   raw_input('         user: '),
				'passwd': raw_input('     password: ')
			}, 'api': {
				'stream': 'sample',
				'user':   raw_input('Twitter  user: '),
				'passwd': raw_input('     password: '),
				'parts': 8,
				'prefix': ''
			}}
			json.dump( self.cred, open('cred.json', 'w') )

		# Override with command-line settings
		from optparse import OptionParser

		parser = OptionParser()
		parser.add_option('-s', '--stream', dest='stream', help='Stream something else: sample, user_timeline, friends_timeline', default=self.cred['api'].get('stream', 'sample'))
		parser.add_option('-u', '--user', dest='stalk', help="Read this user\'s tweets instead of own", default=None)
		parser.add_option('-n', '--new', dest='new', help='Reinitialize the database table', action='store_true', default=False)
		parser.add_option('-p', '--prefix', dest='prefix', help='Write this session into a prefixed table', default=self.cred['api'].get('prefix', ''))
		options, args = parser.parse_args()

		self.cred['api']['stream'] = options.stream
		self.cred['api']['stalk'] = options.stalk
		self.cred['api']['new'] = options.new
		self.cred['api']['prefix'] = options.prefix


	def mapdict(self, f, d):
		return dict([(f(k), v) for k, v in d.items() ])

	def strdict(self, d):
		return self.mapdict(str, self.cred[d])

	def db(self):
		import MySQLdb
		# Connect to database
		con = MySQLdb.connect( **self.strdict('mysql') )
		con.set_character_set('utf8')
		cur = con.cursor()
		cur.execute('''
			set names utf8;
			set character set utf8;
			set character_set_connection=utf8;
		''')
		cur.close()
		return con

	def stream(self):
		from twitter import Stream
		return Stream( **self.strdict('api') )

