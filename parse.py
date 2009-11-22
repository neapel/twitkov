#!/usr/bin/env python

import MySQLdb


class parser():
	def __init__(self, con, parts = 8):
		self.parts = parts
		self.con = con

		self.insert = '''
			insert into parts{0:d} (head, head_low, tail, start)
			values (%s, %s, %s, %s)
			on duplicate key update count = count + 1
		'''.format( self.parts )

		self.original = '''
			insert ignore into original{0:d} (text) value (%s)
		'''.format( self.parts )

	def create(self):
		cur = self.con.cursor()
		try:
			cur.execute('''
				create table original{0:d} (
					text char(140) not null,
					primary key (text)
				) default charset=utf8'''.format(self.parts) )
		except:
			pass
		try:
			cur.execute('''
				create table parts{0:d} (
					head char({0:d}) not null,
					head_low char({0:d}) not null,
					tail char(1) default null,
					count int not null default 1,
					start tinyint(1) not null default 0,
					unique (head_low, tail),
					index (head_low)
				) default charset=utf8'''.format(self.parts) )
		except:
			pass
		cur.close()
	
	def reset(self):
		cur = self.con.cursor()
		try:
			cur.execute('drop table original{0:d}'.format(self.parts) )
		except:
			pass
		try:
			cur.execute('drop table parts{0:d}'.format(self.parts) )
		except:
			pass
		cur.close()
		self.create()

	def analyze(self, it):
		cur = self.con.cursor()

		for message in it:
			if cur.execute(self.original, message.lower()):
				last = len(message) - self.parts
				l = list([
					(
						message[i : i + self.parts],
						message[i : i + self.parts].lower(),
						message[i + self.parts] if i < last else None,
						i == 0
					)
					for i in range(last + 1)
				])
				cur.executemany(self.insert, l)

		cur.close()



if __name__ == '__main__':
	from credentials import credentials
	import sys

	cred = credentials()
	con = cred.db()
	p = parser(con)

	if '--new' in sys.argv:
		p.reset()

	try:
		#p.analyze( ['Das ist ein Test.', 'Das ist kein Test'])
		p.analyze( cred.stream() )
	except KeyboardInterrupt:
		pass

	con.close()


