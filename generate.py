#!/usr/bin/env python

from random import random
import operator

class generator():
	stops = ['www.', 'http:']
	may_end = ' .!?'

	def __init__(self, con, parts = 8):
		self.con = con
		self.parts = parts

		self.first = '''
			select head from parts{0:d}
			where start and count > 2
			order by rand()
			limit 1
		'''.format(parts)

		self.next = '''
			select tail, count from parts{0:d}
			where head_low = %s and count > 2
			order by count desc
			limit 50
		'''.format(parts)

		self.existing = '''
			select (
				select count(*) from original{0:d}
				where text = %s
			) + (
				select count(*) from generated{0:d}
				where text = %s
			)
		'''.format(parts)

		self.publish = '''
			insert into generated{0:d} (text, prob) values (%s, %s)
		'''.format(parts)

	def create(self):
		cur = self.con.cursor()
		try:
			cur.execute('''
				create table generated{0:d} (
					text char(140) not null,
					prob float not null,
					generated timestamp not null default current_timestamp,
					posted int(1) not null default 0,
					primary key (text)
				) default charset=utf8'''.format(self.parts) )
		except:
			pass
		cur.close()
	
	def reset(self):
		cur = self.con.cursor()
		try:
			cur.execute('drop table generated{0:d}'.format(self.parts) )
		except:
			pass
		cur.close()
		self.create()

	def make_tweet(self):
		cur = self.con.cursor()

		def fillp(k):
			return k + ' ' * (self.parts - len(k))

		def fill1(k):
			return k if len(k) > 0 else ' '

		def append(s, p = 1):
			if len(s) > 50 and s[-1] in self.may_end and random() < 0.5:
				return s, p
			if len(s) >= 140 or not self.early(s, p):
				return None
			else:
				head = s[-self.parts:]
				cur.execute(self.next, head.lower())
				all = cur.fetchall()
				if len(all) == 0 or (len(all) == 1 and all[0][0] == None):
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
			cur.execute(self.first)
			w = cur.fetchone()
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

		cur = self.con.cursor()
		cur.execute(self.existing, (s.lower(), s.lower()))
		c = cur.fetchone()
		cur.close()
		
		return c == 0



	def tweet(self):
		while True:
			t = self.make_tweet()

			if self.good(*t):
				print 'POST\t{1:.5f}\t{0:s}'.format(*t)

				cur = self.con.cursor()
				cur.execute(self.publish, t)
				cur.close()

			#else:
			#	print 'DROP\t{1:.5f}\t{0:s}'.format(*t)


if __name__ == '__main__':
	from credentials import credentials
	import sys

	cred = credentials()

	con = cred.db()
	g = generator(con)

	if '--new' in sys.argv:
		g.reset()

	g.tweet()
