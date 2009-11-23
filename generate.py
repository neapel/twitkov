#!/usr/bin/env python
# encoding=utf-8

from random import random
import operator

class Generator():
	stops = ['www.', 'http:']
	may_end = '.!?'

	def __init__(self, con, prefix, parts = 8, **kwargs):
		self.con = con
		self.parts = parts
		self.prefix = prefix

		self.first = '''
			select head from {0:s}
			where start and count > 0
			order by rand()
			limit 1
		'''.format(self.tab('parts'))

		self.next = '''
			select tail, count from {0:s}
			where head_low = %s and count > 0
			order by count desc
			limit 50
		'''.format(self.tab('parts'))

		self.existing = '''
			select (
				select count(*) from {0:s}
				where text = %s
			) + (
				select count(*) from {1:s}
				where text = %s
			) + (
				select count(*) from {0:s}
				where position(%s in text) > 0
			)
		'''.format(self.tab('original'), self.tab('generated'))

		self.publish = '''
			insert into {0:s} (text, prob) values (%s, %s)
		'''.format(self.tab('generated'))

	def create(self):
		cur = self.con.cursor()
		cur.execute('''
			create table if not exists {0:s} (
				text char(140) not null,
				prob float not null,
				generated timestamp not null default current_timestamp,
				posted int(1) not null default 0,
				primary key (text)
			) default charset=utf8'''.format(self.tab('generated')) )
		cur.close()
	
	def reset(self):
		cur = self.con.cursor()
		try:
			cur.execute('drop table {0:s}'.format(self.tab('generated')) )
		except:
			pass
		cur.close()
		self.create()

	def tab(self, t):
		return '`{0:s}{1:s}{2:d}`'.format(self.prefix, t, self.parts)

	def make_tweet(self):

		def fillp(k):
			return unicode(k, 'utf-8') + u' ' * (self.parts - len(k))

		def fill1(k):
			return unicode(k, 'utf-8') if len(k) > 0 else u' '

		def append(s, p = 1):
			if len(s) > 50 and s[-1] in self.may_end and random() < 0.5:
				return s, p
			if len(s) >= 140 or not self.early(s, p):
				return None
			else:
				head = s[-self.parts:]
				cur = self.con.cursor()
				cur.execute(self.next, head.lower())
				all = cur.fetchall()
				cur.close()
				if len(all) == 0:
					# something is rotten
					return None
				elif len(all) == 1 and all[0][0] == None:
					return s, p
				elif len(all) == 1:
					return append( s + fill1(all[0][0]), p )
				else:
					total = sum([ float(c) for _, c in all ])
					for tail, count in all:
						if random() < 0.2:
							p *= count / total
							if tail == None:
								return s, p
							else:
								o = append( s + fill1(tail), p )
								if o:
									return o
					return None

		while True:
			cur = self.con.cursor()
			cur.execute(self.first)
			w = cur.fetchone()
			cur.close()
			if not w:
				raise Exception('Gather more tweets first.')
			o = append( fillp( w[0] ) )
			if o:
				return o


	def early(self, s, p):
		if len(s) > 80 and p > len(s)**-0.5:
			return False
		if any([ (stop in s) for stop in self.stops ]):
			return False
		return True

	def good(self, s, p):

		if p > 0.8:
			return False

		cur = self.con.cursor()
		cur.execute(self.existing, (s.lower(),) * 3)
		c, = cur.fetchone()
		cur.close()

		return c < 1



	def tweet(self):
		while True:
			t = self.make_tweet()

			if self.good(*t):
				print u'POST\t{1:.5f}\t{0:s}'.format(*t)

				cur = self.con.cursor()
				cur.execute(self.publish, t)
				cur.close()



if __name__ == '__main__':
	from credentials import Credentials

	cred = Credentials()

	con = cred.db()
	g = Generator(con, **cred.strdict('api'))

	if cred.cred['api']['new']:
		print 'Reinitializing database.'
		g.reset()

	g.tweet()
