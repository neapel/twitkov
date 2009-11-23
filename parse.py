#!/usr/bin/env python

import MySQLdb


class Parser():
	def __init__(self, con, prefix, parts = 8, **kwargs):
		self.parts = parts
		self.con = con
		self.prefix = prefix

		self.insert = '''
			insert into {0:s} (head, head_low, tail, start)
			values (%s, %s, %s, %s)
			on duplicate key update count = count + 1
		'''.format(self.tab('parts'))

		self.original = '''
			insert ignore into {0:s} (text) value (%s)
		'''.format(self.tab('original'))

	def tab(self, t):
		return '`{0:s}{1:s}{2:d}`'.format(self.prefix, t, self.parts)

	def create(self):
		cur = self.con.cursor()
		cur.execute('''
			create table if not exists {0:s} (
				text char(140) not null,
				primary key (text)
			) default charset=utf8
		'''.format(self.tab('original')))
		cur.execute('''
			create table if not exists {0:s} (
				head char({1:d}) not null,
				head_low char({1:d}) not null,
				tail char(1) default null,
				count int not null default 1,
				start tinyint(1) not null default 0,
				unique (head_low, tail),
				index (head_low)
			) default charset=utf8
		'''.format(self.tab('parts'), self.parts))
		cur.close()
	
	def reset(self):
		cur = self.con.cursor()
		try:
			cur.execute('drop table {0:s}'.format(self.tab('original')) )
		except:
			pass
		try:
			cur.execute('drop table {0:s}'.format(self.tab('parts')) )
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
	from credentials import Credentials

	cred = Credentials()
	con = cred.db()
	p = Parser( con, **cred.strdict('api') )

	if cred.cred['api']['new']:
		print 'Reinitializing database.'
		p.reset()

	try:
		p.analyze( cred.stream() )
	except KeyboardInterrupt:
		pass

	con.close()


