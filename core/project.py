#coding: utf-8
import os
import json
from rgit_commit import rgit_commit_csv_store as rccs
import time
import random


def cmp_second(x, y):#ascend
	if x[1]>y[1]:
		return 1
	if x[1]<y[1]:
		return -1
	return 0
	

def cmp_second2(x, y):#descend
	if x[1]>y[1]:
		return -1
	if x[1]<y[1]:
		return 1
	return 0

class PROJECT:
	def __init__(self, name, basepath, source_path='', repo_size='', head='', arr=[]):
		'''
		head is the sha
		source_path: git path
		basepath: the 'projects' path to store
		name: name of this repo
		arr: [{sha:, type:}]
		'''
		np = os.path.join(basepath, name)
		self.path = os.path.join(basepath, name)
		self.commit_path = os.path.join(basepath, name, 'commit')
		self.blob_path = os.path.join(basepath, name, 'blob')
		self.tree_path = os.path.join(basepath, name, 'tree')
		self.tag_path = os.path.join(basepath, name, 'tag')
		self.meta_path = os.path.join(basepath, name, 'meta')
		if os.path.exists(np):
			return  #already exists
		if not source_path or not repo_size or not head or not arr:
			print ("Error in PROJECT::__init__, argus are not legal")
			exit()
		os.mkdir(np)
		f1 = open(self.commit_path, 'wb')
		f2 = open(self.blob_path, 'wb')
		f3 = open(self.tree_path, 'wb')
		f4 = open(self.tag_path, 'wb')
		f5 = open(self.meta_path, 'wb')
		h = {'blob': f2, 'commit':f1, 'tree':f3, 'tag':f4}
		if arr:
			c = 0
			for i in arr:
				if i['type'] == 'commit':
					c += 1
				h[i['type']].write(i['sha']+'\n')
			mt = {'commit_num':c, 'HEAD':head, 'source_path':source_path,\
			'repo_size':repo_size}
			json.dump(mt, f5)
		f1.close()
		f2.close()
		f3.close()
		f4.close()
		f5.close()
		
	def commit_sha(self):
		r = []
		with open(self.commit_path, 'rb') as f:
			while True:
				a = f.readline()
				if not a:
					break
				r.append(a.strip())
		return r
	def cmt_num(self):
		c = 0
		with open(self.commit_path, 'rb') as f:
			while True:
				a = f.readline()
				if not a:
					return c
				c += 1
	def head(self):
		f = open(self.meta_path,'rb')
		a = json.load(f)
		return a['HEAD']
		
class Project_Store:
	def __init__(self, path):
		self.path = path
	
	def show_repos(self, args):
		pool = []
		for i in os.listdir(self.path):
			p = PROJECT(i, self.path)
			if args['commit-num'] and p.cmt_num()<args['commit-num']:
				continue
			if args['last']:
				head = p.head()
				mid = rccs.get_some_commit([head])
				sha, total, detail = mid[0]
				dt = args['last']
				timeArray = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
				timestamp = time.mktime(timeArray)
				if timestamp > detail['time']:
					continue
			pool.append(i)
		if args['random']:
			b = random.sample(pool, args['random'])
			
			for posi in b:
				try:
					print posi
				except IOError:
					print posi
		else:
			for posi in pool:
				try:
					print posi
				except IOError:
					print posi
				
			
	
	def show_commit(self, args):
		
		if args['random'] and args['recent'] or args['random'] and args['count'] or args['count'] and args['recent']:
			print ("Error from Project_Store::show_commit: random and recent cannot appear together!")
			exit()
		if args['merge'] and args['no-merge']:
			print ("Error from Project_Store::show_commit: random and recent cannot appear together!")
			exit()
		mid = []
		pool = []
		if args['repo']:
			for i in args['repo']:
				p = PROJECT(i, self.path)
				pool.extend(p.commit_sha())
			mid = rccs.get_some_commit(pool)
		else:
			mid = rccs.get_all()
		
		sha_posi = {}
		posi = -1
		pairs = []
		for sha, total, detail in mid:
			posi += 1
			if args['merge']:
				if not detail['merge']:
					continue
			else:
				if detail['merge']:
					continue
			if args['begin']:
				dt = args['begin']
				timeArray = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
				timestamp = time.mktime(timeArray)
				if args['local-time']:
					if detail['local_time'] < timestamp:
						continue
				elif detail['time'] < timestamp:
					continue
			if args['end']:
				dt = args['end']
				timeArray = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
				timestamp = time.mktime(timeArray)
				if args['local-time']:
					if detail['local_time'] > timestamp:
						continue
				elif detail['time'] > timestamp:
					continue
			if args['author']:
				flag = False
				for author in args['author']:
					if detail['author'].find(author) != -1:
						flag =True
						break
				if not flag:
					continue
			if args['committer']:
				flag = False
				for cmter in args['committer']:
					if detail['committer'].find(cmter) != -1:
						flag =True
						break
				if not flag:
					continue
			if args['key-word']:
				if args['key-word-rule'] == 'and':
					flag = True
					for word in args['key-word']:
						if detail['msg'].find(word) == -1:
							flag = False
							break
					if not flag:
						continue
				else:
					flag = False
					for word in args['key-word']:
						if detail['msg'].find(word) != -1:
							flag = True
							break
					if not flag:
						continue
			
			sha_posi[sha] = posi
			if args['recent']:
				pairs.append((posi, detail['time']))
			
		if args['count']:
			print len(sha_posi)
		elif args['random']:
			rand_cnt = args['random']
			if rand_cnt > len(sha_posi):
				print ("Warning: the number according to your query is less than random number")
				for sha in sha_posi:
					try:
						print "commit %s\n%s"%(sha, mid[sha_posi[sha]][1])
					except IOError:
						print "commit %s\n%s"%(sha, mid[sha_posi[sha]][1])
			else:
				b = random.sample(sha_posi.values(), rand_cnt)
				
				for posi in b:
					try:
						print "commit %s\n%s"%(mid[posi][0], mid[posi][1])
					except IOError:
						print "commit %s\n%s"%(mid[posi][0], mid[posi][1])
		elif args['recent']:
			recent_cnt = args['recent']
			pairs.sort(cmp_second2)
			if len(pairs)<recent_cnt:
				print ("Warning: the number according to your query is less than recent number")
				for posi, time in pairs:
					try:
						print "commit %s\n%s"%(mid[posi][0], mid[posi][1])
					except IOError:
						print "commit %s\n%s"%(mid[posi][0], mid[posi][1])
			else:
				cnt = 0
				for posi, time in pairs:
					try:
						print "commit %s\n%s"%(mid[posi][0], mid[posi][1])
					except IOError:
						print "commit %s\n%s"%(mid[posi][0], mid[posi][1])
					cnt += 1
					if cnt == recent_cnt:
						break
		else:
			for sha in sha_posi:
				try:
					print "commit %s\n%s"%(sha, mid[sha_posi[sha]][1])
				except IOError:
					print "commit %s\n%s"%(sha, mid[sha_posi[sha]][1])
			