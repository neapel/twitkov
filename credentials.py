#!/usr/bin/env python

class credentials():

	def __init__(self):
		# Load settings
		import json
		self.cred = json.load( open('cred.json') )

	def strdict(self, d):
		return dict([(str(k), v) for k, v in self.cred[d].items() ])

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
		from twitter import stream
		return stream( **self.strdict('api') )

