#coding: utf-8
import os
import argparse
import sys
from rgit_commit import rgit_commit_csv_store as rccs
from rgit_blob import rgit_blob_store as rbs
from rgit_tree import rgit_tree_store as rts
from rgit_tag import rgit_tag_store as rtas
#from test import getidx


def remove_dir(dir):
	dir = dir.replace('\\', '/')
	if(os.path.isdir(dir)):
		for p in os.listdir(dir):
			remove_dir(os.path.join(dir,p))
		if(os.path.exists(dir)):
			os.rmdir(dir)
	else:
		if(os.path.exists(dir)):
			os.remove(dir)

def clear(store_path = '../'):
	check_path = [store_path+'blob_store/', store_path+'commit_store/commit/', store_path+'tree_store/', store_path+'tag_store/']
	for i in check_path:
		if not os.path.exists(i):
			print 'Cannot clear objects, because it is not a legal rgit_store'
			exit()
		remove_dir(i)
		os.mkdir(i)

def absorb(repo_path, store_path = '../'):
	rbs.rgit_blob_store(repo_path, store_path + 'blob_store/')
	rts.rgit_tree_store(repo_path, store_path + 'tree_store/')
	rccs.rgit_commit_csv_store(repo_path, store_path + 'commit_store/commit/')
	rtas.rgit_tag_store(repo_path, store_path + 'tag_store/')
	
def recover(sha, store_path = '../'):
	res = rbs.recover(sha, store_path + 'blob_store/')
	if res != -1:
		import zlib
		import hashlib
		#print res
		#print '\n\n\n'
		if hashlib.sha1("blob %d\0%s"%(len(res),res)).hexdigest() == sha:
			print 'success'
		else:
			print sha, 'failed'
		return
	res = rts.recover(sha, store_path + 'tree_store/')
	if res != -1:
		import zlib
		import hashlib
		#print res
		#print '\n\n\n'
		if hashlib.sha1("tree %d\0%s"%(len(res),res)).hexdigest() == sha:
			print 'success'
		else:
			print 'failed'
		return
	res = rccs.recover_commit(sha, store_path + 'commit_store/commit/')
	if res != -1:
		import zlib
		import hashlib
		#print res
		#print '\n\n\n'
		if hashlib.sha1("commit %d\0%s"%(len(res),res)).hexdigest() == sha:
			print 'success'
		else:
			print 'failed'
		return
	res = rtas.recover(sha, store_path + 'tag_store/')
	if res != -1:
		import zlib
		import hashlib
		#print res
		#print '\n\n\n'
		if hashlib.sha1("tag %d\0%s"%(len(res),res)).hexdigest() == sha:
			print 'success'
		else:
			print 'failed'
		return
		
	
def test_blob():
	p = 'd://deduplication/data/cpp_delta/bitcoin.csv'
	f = open(p)
	a = f.readline()
	sha_pool = []
	while 1:
		a = f.readline()
		if not a:
			break
		a = a.split(',')
		if a[1] == 'blob':
			sha_pool.append(a[0])
		
		

def initParse():

	parser = argparse.ArgumentParser()
	#absorb a raw git repository into rgit store, argu is the path
	parser.add_argument('-a', '--absorb', help = "absorb the objects of git repo into specific directories", nargs = "?")

	#recover a commit from rgit-commit to standard output
	parser.add_argument('-r', '--recover', help = 'recover an object from rgit-dedup to standard output', nargs = '?')
	
	#clear all the rgit-object-repos
	parser.add_argument('--clear-all', help = "clear all the commit stored", action = 'store_true', default = False)
	
	#print the size (after delta in pack) of commit objects take in one repository
	parser.add_argument('-i', '--info', help = "print the size (after delta in pack) of commit objects take in one repository", nargs = '?')
	
	#print the size (after delta in pack) of commit objects take in one repository
	parser.add_argument('-css', '--commit-store-stat', help = "print the size (after different methods of deduplication) of commit objects take in storage", action = 'store_true', default = False)
	
	return parser


if __name__ == '__main__':
	#parsing the argus
	parser = initParse()
	args = parser.parse_args()

	args = vars(args)
	if args['absorb']:
		absorb(os.path.abspath(args['absorb']))
		exit()
	if args['recover']:
		recover(args['recover'])
		exit()
	if args['info']:
		exit()
	if args['commit_store_stat']:
		exit()
	if args['clear_all']:
		clear()
		exit()
		