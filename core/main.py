#coding: utf-8
import os
import argparse
import sys
from rgit_commit import rgit_commit_csv_store as rccs
from rgit_blob import rgit_blob_store as rbs
from rgit_tree import rgit_tree_store as rts
from rgit_tag import rgit_tag_store as rtas
import rgit_all
import func
import project
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
	check_path = [store_path+'blob_store/', store_path+'commit_store/commit/', store_path+'tree_store/', store_path+'tag_store/tag']
	for i in check_path:
		if not os.path.exists(i):
			print 'Cannot clear objects, because it is not a legal rgit_store'
			exit()
		remove_dir(i)
		os.mkdir(i)

def absorb(repo_path, store_path = '../', type = ['blob', 'tag', 'tree', 'commit']):
	idx_pack_pairs = func.idx_pack_from_repo(repo_path)
	ret = {'blob':[], 'tag':[], 'tree': [], 'commit': []}
	for i, j in idx_pack_pairs:
		temp = rgit_all.allFromPack(i, j)
		ret['blob'].extend(temp['blob'])
		ret['tree'].extend(temp['tree'])
		ret['tag'].extend(temp['tag'])
		ret['commit'].extend(temp['commit'])
	if 'blob' in type:
		rbs.rgit_blob_store(repo_path, blob_store_path = store_path + 'blob_store/', already = ret['blob'])
	if 'tree' in type:
		rts.rgit_tree_store(repo_path, tree_store_path = store_path + 'tree_store/', already = ret['tree'])
	if 'commit' in type:
		rccs.rgit_commit_csv_store(repo_path, store_path + 'commit_store/commit/', already = ret['commit'])
	if 'tag' in type:
		rtas.rgit_tag_store(repo_path, store_path + 'tag_store/tag/', already = ret['tag'])
	st = []
	for i in ret['blob']:
		st.append({'sha':i[0], 'type':'blob'})
	for i in ret['tree']:
		st.append({'sha':i[0], 'type':'tree'})
	for i in ret['commit']:
		st.append({'sha':i[0], 'type':'commit'})
	for i in ret['tag']:
		st.append({'sha':i[0], 'type':'tag'})
	
	temp = re.sub(r'\\|/', '\n', repo_path)
	temp = temp.split('\n')
	name = temp[-1]
	
	pwd = os.getcwd()
	os.chdir(repo_path)
	head = subprocess.check_output('git log -1', shell = True)
	head = head[7:47]
	os.chdir(pwd)
	s  =func.dirSize(repo_path)
	project.PROJECT(name, os.path.join(store_path, 'projects'), repo_path, s, head, st)
	
def recover(sha, store_path = '../'):
	res = rccs.recover_commit(sha, store_path + 'commit_store/commit/')
	if res != -1:
		print res
		return
	res = rbs.recover(sha, store_path + 'blob_store/')
	if res != -1:
		print res
		return
	res = rts.recover(sha, store_path + 'tree_store/')
	if res != -1:
		import hashlib
		print hashlib.sha1(res).hexdigest()
		print (res)
		return
	res = rtas.recover_tag(sha, store_path + 'tag_store/tag')
	if res != -1:
		print res
		return
		
	
def test_type(type = ['blob','tree','commit','tag']):
	if not type:
		print ('Warning: nothing happened in test_type')
		return
	p = 'd://deduplication/data/cpp_delta/bitcoin.csv'
	f = open(p)
	a = f.readline()
	sha_pool = []
	while 1:
		a = f.readline()
		if not a:
			break
		a = a.split(',')
		if a[1] in type:
			sha_pool.append(a[0])
	return sha_pool

def initParse():

	parser = argparse.ArgumentParser()
	#absorb a raw git repository into rgit store, argu is the path
	parser.add_argument('-a', '--absorb', help = "absorb the objects of git repo into specific directories", nargs = "?")

	#recover a commit from rgit-commit to standard output
	parser.add_argument('-r', '--recover', help = 'recover an object from rgit-dedup to standard output', nargs = '?')
	
	#clear all the rgit-object-repos
	parser.add_argument('--clear-all', help = "clear all the objects stored", action = 'store_true', default = False)
	
	parser.add_argument('--no-merge', help = 'filter out the merge commits (cannot co-exists with --just-merge)', action = 'store_true', default = False)
	parser.add_argument('--merge', help = 'just reserve the merge commits (cannot co-exists with --no-merge)', action = 'store_true', default = False)
	parser.add_argument('--begin', help = 'reserve the commits after the time, format as:"2016-05-05 20:28:54"', nargs = '?', default = '')
	parser.add_argument('--end', help = 'reserve the commits before the time, format as:"2016-05-05 20:28:54"', nargs = '?', default = '')
	parser.add_argument('--count', help = 'just print the number of commits according with conditions(cannot co-exists with recent, random)', action = 'store_true', default = False)
	parser.add_argument('--repo', help = 'just search commit in specified repos', nargs = '+', default = False)
	parser.add_argument('--committer', help = 'search for commit committed by the specified guy', nargs = '+', default = False)
	parser.add_argument('--author', help = 'search for commit wrote by the specified guy', nargs = '+', default = False)
	parser.add_argument('--key-word', help = 'search for commits whose message contains ', nargs = '+', default = False)
	parser.add_argument('--key-word-rule', help = '"and" or "or" when search for key-word', nargs = '?', default = 'and')
	parser.add_argument('--random', help = 'randomly samples "arg" number of conditional commits (cannot co-exists with recent, count)', type=int, nargs = '?', default = 0)
	parser.add_argument('--recent', help = 'return most recent conditional commits (cannot co-exists with random, count)', type= int, nargs = '?', default = 0)
	parser.add_argument('--format', help = 'just print the number of commits according with conditions', nargs ="?", default = '')
	
	parser.add_argument('--local-time', help = 'if use local-time to search commits, default is use GMT', action = 'store_true', default = False)
	
	parser.add_argument('--commit-num', help = 'get repo list whose commit amount > arg', type=int, default = 0)
	parser.add_argument('--last', help = 'get repo whose last commit is after some time, format as:"2016-05-05 20:28:54"', nargs='?',default = '')
	
	return parser


if __name__ == '__main__':
	#parsing the argus
	if sys.argv[1] == 'log':
		parser = initParse()
		args = parser.parse_args(sys.argv[2:])
		h = {'no-merge':args.no_merge, 'merge':args.merge, 'begin':args.begin, 'end':args.end, 'count':args.count, 'repo':args.repo, 'committer':args.committer,'author':args.author,'key-word':args.key_word,'key-word-rule':args.key_word_rule,'random':args.random, 'recent':args.recent, 'format':args.format,'local-time':args.local_time}
			
		ps = project.Project_Store('../projects')
		ps.show_commit(h)
		exit()
	if sys.argv[1] == 'repo':
		parser = initParse()
		args = parser.parse_args(sys.argv[2:])
		h = {'random':args.random, 'commit-num':args.commit_num, 'last':args.last}
			
		ps = project.Project_Store('../projects')
		ps.show_repos(h)
		exit()
	parser = initParse()
	args = parser.parse_args()

	args = vars(args)
	if args['absorb']:
		absorb(os.path.abspath(args['absorb']))
		exit()
	if args['recover']:
		recover(args['recover'])
		exit()
	if args['clear_all']:
		clear()
		exit()
		