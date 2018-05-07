#coding: utf-8
'''
some general functions
'''
import os
import Queue
import commands as cmd

def dirSize(dirPath):
	'''
	ret in Byte
	'''
	if not os.path.isdir(dirPath):
		return -1
	q = Queue.Queue()
	q.put(dirPath)
	res = 0
	while not q.empty():
		a = q.get()
		b = os.listdir(a)
		for i in b:
			c = a+'/'+i
			if os.path.isdir(c):
				q.put(c)
			elif os.path.isfile(c):
				res += os.path.getsize(c)
			else:
				print c,'is not file or dir'
	return res

def cmpSha(sha1, sha2):
	for i in xrange(0, len(sha1)):
		a = sha1[i]
		b = sha2[i]
		if a > b:
			return 1
		elif a < b:
			return -1
	return 0


def idxAna(idxPath, detail = False):
	'''get a path of *.idx file, give all its objects'''
	if not detail:
		status, output = cmd.getstatusoutput('cat %s | git show-index'%(idxPath))
		if status != 0:
			print 'git show-index failed in idxAna'
			exit()
		ret = output.split()[1::3]
		return ret
	else:
		#analyze the index file
		pass

def getObjFromGit(repoPath):
	'''get all objects names from git repo'''
	rawPath = os.getcwd()
	os.chdir(repoPath)

	path = repoPath + '/.git/objects'
	dirs = os.listdir(path)
	ret = []
	for i in dirs:
		if i == 'info':
			continue
		elif i == 'pack':
			d = os.listdir(path + '/' + i)
			for j in d:
				if len(j) > 4 and j[-4:]=='.idx':
					ret += idxAna(path + '/pack/' + j)
		else:
			objs = os.listdir(path + '/' + i)
			ret += [i + x for x in objs]

	os.chdir(rawPath)
	return ret


def read_number_from_file(file, bytes, bigendian = True):
	a = 0
	for i in xrange(0, bytes):
		b = ord(file.read(1))
		if bigendian:
			a = a*256 + b
		else:
			a = a + b*(256**i)
	return a
	


OBJTYPE = ["not exists", "commit", "tree", "blob", "tag", "not exists", \
"ofs_delta", "ref_delta"]

def read_chunk_from_pack(file, length = -1):
	#if length == -1, read all remained data
	#have checked to be corrected
	header_len = 0 #bytes of a header of a chunk takes
	obj_type = 0
	while 1:
		header_len += 1
		a = ord(file.read(1))
		if header_len == 1:
			obj_type = OBJTYPE[(a>>4)&7]
		if not a&(0x80):
			break
	if length != -1:
		compressed_data = file.read(length - header_len)
	else:
		compressed_data = file.read()
	return obj_type, compressed_data, header_len

def idx_pack_from_repo(repo_path):
	pack_path = os.path.join(repo_path, '.git/objects/pack')
	file_list = os.listdir(pack_path)
	ret = []
	for i in file_list:
		if len(i) > 4 and i[-4:] == '.idx':
			ret.append((os.path.join(repo_path,'.git/objects/pack',i),\
			 os.path.join(repo_path,'.git/objects/pack',i[:-4] + '.pack')))
	return ret