#coding: utf-8
import os
import subprocess
import hashlib
import sys
sys.path.append('../')
from func import *
import zlib

MSBBIT = 1<<31

NUM_PER_MAIN_COMMIT = 50000

class OBJECT:
	def __init__(self, raw_data = '', type = ''):
		self.raw_data = raw_data
		self.type = type

class PARSED_COMMIT:
	def __init__(self):
		pass
	def show(self):
		print (self.parents)
		print (self.author)
		print (self.committer)

class INDEX_AND_NEW:
	def __init__(self, file, set = {}):
		self.file = file
		self.set = set

def commitFromPack(idxPath, packPath):
	f_idx = open(idxPath, 'rb') 
	f_idx.seek(4*257)

	#I dnt know if it's little endian
	obj_num = read_number_from_file(f_idx, 4)
	obj_list = []
	for i in xrange(0, obj_num):
		j = ""
		for k in xrange(0, 20):
			a = hex(ord(f_idx.read(1)))[2:]
			if len(a)==1:
				a = '0' + a
			j = j + a
		obj_list.append(j)

	f_idx.seek(4*258 + 24*obj_num, 0)
	#if offset is negative, then the offset is in layer5 not in layer4
	obj_offset_list = []
	layer5_list = []

	for i in xrange(0, obj_num):
		j = read_number_from_file(f_idx, 4)
		if not j&MSBBIT:
			obj_offset_list.append(j&(~MSBBIT))
		else:
			obj_offset_list.append(-1)
			layer5_list.append((i, j))

	def cmp_second(x, y):
		if x[1]>y[1]:
			return 1
		if x[1]<y[1]:
			return -1
		return 0
	layer5_list.sort(cmp_second)

	for index, offset_of_layer5 in layer5_list:
		obj_offset_list[index] = read_number_from_file(f_idx, 8, bigendian = False)

	f_idx.close()

	#now the offset of obj in packfile are well
	#I've prove the base object is before the deltaed object

	f_pack = open(packPath, 'rb')
	f_pack.seek(12, 0)
	
	obj_list = zip(obj_list, obj_offset_list)
	obj_list.sort(cmp_second)

	obj_hash = {}
	off2sha = {}
	for i, j in obj_list:
		#not need to store sha in offset
		obj_hash[i] = OBJECT()
		off2sha[j] = i

	def handle_delta(string, idx, base_obj):
		string = zlib.decompress(string[idx:])
		tail_idx = len(string)
		#read two var-len int first
		idx = 0
		i = 7
		a = ord(string[idx])
		idx += 1
		src_size = a&0x7f
		while a&0x80:
			a = ord(string[idx])
			src_size |= (a&0x7f)<<i
			i += 7
			idx += 1
		if src_size != len(base_obj.raw_data):
			if src_size != len(base_obj.raw_data).size - 1 or base_obj.raw_data[-1] != "\n":
				print "Error in addObjFromPack:handle_delta: src_size != input_obj_size"
				print "former is %d, latter is %d"%(src_size, len(base_obj.raw_data))
				exit()

		tar_size = 0
		i = 0
		while True:
			#read two var-len int first
			a = ord(string[idx])
			tar_size |= (a&0x7f)<<i
			i += 7
			idx += 1
			if not a&(0x80):
					break
		#now deal with copy and insert command
		tar_data = ''
		while idx < tail_idx:
			a = ord(string[idx])
			idx += 1
			if a&(0x80):#copy
				offset = 0
				copy_len = 0
				if a&(1):
					offset = ord(string[idx])
					idx += 1
				if a&(2):
					offset |= ord(string[idx])<<8
					idx += 1
				if a&(4):
					offset |= ord(string[idx])<<16
					idx += 1
				if a&(8):
					offset |= ord(string[idx])<<24
					idx += 1
				if a&(0x10):
					copy_len = ord(string[idx])
					idx += 1
				if a&(0x20):
					copy_len |= ord(string[idx])<<8
					idx += 1
				if a&(0x40):
					copy_len |= ord(string[idx])<<16
					idx += 1
				if copy_len==0:
					copy_len = 0x10000
				
				tar_data += base_obj.raw_data[offset : offset + copy_len]
			else:#insert
				tar_data += string[idx:idx+a]
				idx += a
			if idx > tail_idx:
				print 'error in handle_delta, idx is bigger than string:\
 idx is %d, tail_idx is %d'%(idx, tail_idx)
				exit()
		return tar_data

	ret = []
	for i in xrange(0, len(obj_list)):
		#the type of base object and deltaed object is the same
		compressed_data = ''
		if i != len(obj_list) - 1:
			read_len = obj_list[i+1][1] - obj_list[i][1]
		else:
			read_len = -1

		obj_type, to_process, header_len = read_chunk_from_pack(f_pack, read_len)

		if obj_type == "ofs_delta":
			j = 1
			a = ord(to_process[0])
			base_real_offset = a&0x7f
			while a&0x80:#from the source code of git
				a = ord(to_process[j])
				base_real_offset = ((base_real_offset + 1)<<7) | (a&(0x7f))
				j += 1

			base_obj_sha1 = off2sha[obj_list[i][1] - base_real_offset]
			obj_type = obj_hash[base_obj_sha1].type
			if obj_type != 'commit':
				continue
			tar_data = handle_delta(to_process, j, obj_hash[base_obj_sha1])
		elif obj_type == "ref_delta":
			base_obj_sha1 = ''
			for k in xrange(0, 20):
				a = hex(ord(to_process[k]))[2:]
				if len(a)==1:
					a = '0' + a
				base_obj_sha1 = base_obj_sha1 + a
			obj_type = obj_hash[base_obj_sha1].type
			if obj_type != 'commit':
				continue
			tar_data = handle_delta(to_process, 20, obj_hash[base_obj_sha1])
		elif obj_type == "not exists":
			print ("Error in addObjFromPack, objType is not exists")
			exit()
		else:
			if obj_type != 'commit':
				continue
			tar_data = zlib.decompress(to_process)
		obj_hash[obj_list[i][0]].raw_data = tar_data
		obj_hash[obj_list[i][0]].type = 'commit'

	f_pack.close()

	for i in obj_hash:
		if obj_hash[i].type == 'commit':
			ret.append((i, obj_hash[i].raw_data))
	return ret

def parse_commit(raw_data):
	ret = PARSED_COMMIT()
	raw_list = raw_data.split('\n')
	ret.tree = raw_list[0].split(' ')[1]
	ret.parents = []
	raw_list[1].split(' ')[1]
	temp = raw_list[2].split(' ')
	cnt = 1
	while True:
		temp = raw_list[cnt].split(' ')
		if temp[0] == 'parent':
			ret.parents.append(temp[1])
			cnt += 1
		else:
			break
	begin = raw_list[cnt].find(' ')
	end = raw_list[cnt].find('>')
	#check if only one author and committer in commit
	
	a = raw_list[cnt].split(' ')[0]
	b = raw_list[cnt+1].split(' ')[0]
	c = raw_list[cnt+2].split(' ')[0]
	if a!='author' or b!='committer' or c=='committer':
		print ('failed in checking in parse_commit')
		print ('a:',a)
		print ('b:',b)
		print ('c:',c)
		exit()
	#end check
	ret.author = raw_list[cnt][begin+1:end+1]
	ret.author_time = raw_list[cnt][end+2:]

	begin = raw_list[cnt+1].find(' ')
	end = raw_list[cnt+1].find('>')
	ret.committer = raw_list[cnt+1][begin+1:end+1]
	ret.committer_time = raw_list[cnt+1][end+2:]

	ret.msg = '\n'.join(raw_list[cnt+2:])
	return ret

def int2msb(a):
	#little endian
	s = ''
	while True:
		b = a & 0x7f
		a = a >> 7
		if a:
			s += chr(b|0x80)
		else:
			s += chr(b)
			break
	return s
	
def rgit_commit_csv_store(git_repo_path, commit_store_path = '../commit_store/commit', already = []):
	'''
	store commit objects from git_repo_path, to csv files in commit_store_path
	'''
	csv_files = os.listdir(commit_store_path)
	if 'to_write' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'to_write')
		w = open(new_file_path, 'wb')
		w.write('0,0,0') #which number of csv to write, and how many it has had, and offset it will begin
		w.close()
	if 'rgit_commit_main0' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'rgit_commit_main0')
		w = open(new_file_path, 'wb')
		w.close()
	if 'rgit_commit_hash8.csv' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'rgit_commit_hash8.csv')
		w = open(new_file_path, 'wb')
		w.write(','.join(['hash8+1','name_email_str\n']))
		w.close()
	
	if already:
		ret = already
	else:
		idx_pack_pairs = idx_pack_from_repo(git_repo_path)
		ret = []
		for i, j in idx_pack_pairs:
			ret.extend(commitFromPack(i, j))

	f = open(os.path.join(commit_store_path, 'to_write'))
	a = f.readline().split(',')
	which = int(a[0])
	already_store = int(a[1])
	offset = int(a[2])
	f.close()

	f = open(os.path.join(commit_store_path, 'rgit_commit_hash8.csv'))
	a = f.readline()
	hash8  = {}
	while True:
		a = f.readline()
		if not a:
			break
		a = a.strip().split(',')
		hash8[a[0]] = a[1]
	f.close()
	hash8_file = open(os.path.join(commit_store_path, 'rgit_commit_hash8.csv'), 'ab')

	#for now most 50000 commits in one file
	w = open(''.join([commit_store_path, '/rgit_commit_main', str(which)]), 'ab')
	h = {}
	test_cmpr = []
	for sha, raw_data in ret:
		parsed_commit = parse_commit(raw_data)

		if sha[0:2] not in h:#format: sha,which,line_number
			indexpath = ''.join(['index', sha[0:2]])
			if indexpath not in csv_files:
				temp = set()
				f = open(os.path.join(commit_store_path, indexpath), 'wb')
				f.close()
			else:
				f = open(os.path.join(commit_store_path, indexpath))
				temp  = set()
				while True:
					a = f.readline()
					if not a:
						break
					a = a.strip().split(',')
					temp.add(a[0])
				f.close()

			f = open(os.path.join(commit_store_path, indexpath), 'ab')
			h[sha[0:2]] = INDEX_AND_NEW(file = f, set = temp)
		if sha[2:] in h[sha[0:2]].set:#already exists
			continue
		h[sha[0:2]].file.write('%s,%d,%d\n'%(sha[2:], which, offset))
		h[sha[0:2]].set.add(sha[2:])

		md51 = hashlib.md5(parsed_commit.author).hexdigest()[0:8]
		temp = 0
		j1 = 0
		j2 = 0
		while True:
			j1 = md51 + str(temp) 
			if j1 in hash8:
				if hash8[j1] == parsed_commit.author:
					break
			else:
				hash8[j1] = parsed_commit.author
				hash8_file.write("%s,%s\n"%(j1, parsed_commit.author))
				break
			temp += 1

		md52 = hashlib.md5(parsed_commit.committer).hexdigest()[0:8]
		temp = 0
		while True:
			j2 = md52 + str(temp)
			if j2 in hash8:
				if hash8[j2] == parsed_commit.committer:
					break
			else:
				hash8[j2] = parsed_commit.committer
				hash8_file.write("%s,%s\n"%(j2, parsed_commit.committer))
				break
			temp += 1
		
		write_str = "%s,%s,%s,%s,%s,%s,%s"%(\
			parsed_commit.tree, ','.join(parsed_commit.parents)+'\0',\
			j1, parsed_commit.author_time, j2,\
			parsed_commit.committer_time, parsed_commit.msg)

		write_str = zlib.compress(write_str)
		
		head = int2msb(len(write_str))
		
		content = ''.join([head, write_str])

		w.write(content)
		already_store += 1
		offset += len(content)
		if already_store == NUM_PER_MAIN_COMMIT:
			already_store = 0
			which += 1
			offset = 0
			w.close()
			new_file_path = os.path.join(commit_store_path, 'rgit_commit_main%d'%(which))
			w = open(new_file_path, 'wb')
	f = open(os.path.join(commit_store_path, 'to_write'), 'wb')
	f.write("%d,%d,%d"%(which, already_store, offset))
	f.close()
	hash8_file.close()
	for i in h:
		h[i].file.close()
	w.close()
	return 1


def rgit_commit_csv_store_cmt_dup(git_repo_path, commit_store_path = '../commit_store/commit_cmt_dup'):
	'''
	a version of rgit_commit_csv_store without the deduplication of the same commit
	store commit objects from git_repo_path, to csv files in commit_store_path
	'''
	csv_files = os.listdir(commit_store_path)
	if 'to_write' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'to_write')
		w = open(new_file_path, 'wb')
		w.write('0,0,0') #which number of csv to write, and how many it has had, and offset it will begin
		w.close()
	if 'rgit_commit_main0' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'rgit_commit_main0')
		w = open(new_file_path, 'wb')
		w.close()
	if 'rgit_commit_hash8.csv' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'rgit_commit_hash8.csv')
		w = open(new_file_path, 'wb')
		w.write(','.join(['hash8+1','name_email_str\n']))
		w.close()

	idx_pack_pairs = idx_pack_from_repo(git_repo_path)
	ret = []
	for i, j in idx_pack_pairs:
		ret.extend(commitFromPack(i, j))

	f = open(os.path.join(commit_store_path, 'to_write'))
	a = f.readline().split(',')
	which = int(a[0])
	already_store = int(a[1])
	offset = int(a[2])
	f.close()

	f = open(os.path.join(commit_store_path, 'rgit_commit_hash8.csv'))
	a = f.readline()
	hash8  = {}
	while True:
		a = f.readline()
		if not a:
			break
		a = a.strip().split(',')
		hash8[a[0]] = a[1]
	f.close()
	hash8_file = open(os.path.join(commit_store_path, 'rgit_commit_hash8.csv'), 'ab')


	#each file store 50000 commits for now
	w = open(''.join([commit_store_path, '/rgit_commit_main', str(which)]), 'ab')
	h = {}
	test_cmpr = []
	for sha, raw_data in ret:
		parsed_commit = parse_commit(raw_data)

		if sha[0:2] not in h:#format: sha,which,line_number
			indexpath = ''.join(['index', sha[0:2]])
			if indexpath not in csv_files:
				temp = set()
				f = open(os.path.join(commit_store_path, indexpath), 'wb')
				f.close()
			else:
				f = open(os.path.join(commit_store_path, indexpath))
				temp  = set()
				while True:
					a = f.readline()
					if not a:
						break
					a = a.strip().split(',')
					temp.add(a[0])
				f.close()

			f = open(os.path.join(commit_store_path, indexpath), 'ab')
			h[sha[0:2]] = INDEX_AND_NEW(file = f, set = temp)
		#if sha[2:] in h[sha[0:2]].set:#already exists
		#	continue
		h[sha[0:2]].file.write('%s,%d,%d\n'%(sha[2:], which, offset))
		h[sha[0:2]].set.add(sha[2:])

		md51 = hashlib.md5(parsed_commit.author).hexdigest()[0:8]
		temp = 0
		j1 = 0
		j2 = 0
		while True:
			j1 = md51 + str(temp) 
			if j1 in hash8:
				if hash8[j1] == parsed_commit.author:
					break
			else:
				hash8[j1] = parsed_commit.author
				hash8_file.write("%s,%s\n"%(j1, parsed_commit.author))
				break
			temp += 1

		md52 = hashlib.md5(parsed_commit.committer).hexdigest()[0:8]
		temp = 0
		while True:
			j2 = md52 + str(temp) 
			if j2 in hash8:
				if hash8[j2] == parsed_commit.committer:
					break
			else:
				hash8[j2] = parsed_commit.committer
				hash8_file.write("%s,%s\n"%(j2, parsed_commit.committer))
				break
			temp += 1
		write_str = "%s,%s,%s,%s,%s,%s,%s"%(\
			parsed_commit.tree, ','.join(parsed_commit.parents)+'\0',\
			j1, parsed_commit.author_time, j2,\
			parsed_commit.committer_time, parsed_commit.msg)
		
		head = int2msb(len(write_str))
		content = ''.join([head, write_str])
		
		w.write(content)
		already_store += 1
		offset += len(content)
		if already_store == NUM_PER_MAIN_COMMIT:
			already_store = 0
			which += 1
			offset = 0
			w.close()
			new_file_path = os.path.join(commit_store_path, 'rgit_commit_main%d'%(which))
			w = open(new_file_path, 'wb')
	f = open(os.path.join(commit_store_path, 'to_write'), 'wb')
	f.write("%d,%d,%d"%(which, already_store, offset))
	f.close()
	hash8_file.close()
	for i in h:
		h[i].file.close()
	w.close()
	return 1

def rgit_commit_csv_store_struc_dup(git_repo_path, commit_store_path = '../commit_store/commit_struc_dup'):
	'''
	a version of rgit_commit_csv_store without the deduplication of structure string like 'tree','author'
	store commit objects from git_repo_path, to csv files in commit_store_path
	'''
	csv_files = os.listdir(commit_store_path)
	if 'to_write' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'to_write')
		w = open(new_file_path, 'wb')
		w.write('0,0,0') #which number of csv to write, and how many it has had, and offset it will begin
		w.close()
	if 'rgit_commit_main0' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'rgit_commit_main0')
		w = open(new_file_path, 'wb')
		w.close()
	if 'rgit_commit_hash8.csv' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'rgit_commit_hash8.csv')
		w = open(new_file_path, 'wb')
		w.write(','.join(['hash8+1','name_email_str\n']))
		w.close()

	idx_pack_pairs = idx_pack_from_repo(git_repo_path)
	ret = []
	for i, j in idx_pack_pairs:
		ret.extend(commitFromPack(i, j))

	f = open(os.path.join(commit_store_path, 'to_write'))
	a = f.readline().split(',')
	which = int(a[0])
	already_store = int(a[1])
	offset = int(a[2])
	f.close()

	f = open(os.path.join(commit_store_path, 'rgit_commit_hash8.csv'))
	a = f.readline()
	hash8  = {}
	while True:
		a = f.readline()
		if not a:
			break
		a = a.strip().split(',')
		hash8[a[0]] = a[1]
	f.close()
	hash8_file = open(os.path.join(commit_store_path, 'rgit_commit_hash8.csv'), 'ab')

	#for now 50000 commits per file
	w = open(''.join([commit_store_path, '/rgit_commit_main', str(which)]), 'ab')
	h = {}
	test_cmpr = []
	for sha, raw_data in ret:
		parsed_commit = parse_commit(raw_data)

		if sha[0:2] not in h:#format: sha,which,line_number
			indexpath = ''.join(['index', sha[0:2]])
			if indexpath not in csv_files:
				temp = set()
				f = open(os.path.join(commit_store_path, indexpath), 'wb')
				f.close()
			else:
				f = open(os.path.join(commit_store_path, indexpath))
				temp  = set()
				while True:
					a = f.readline()
					if not a:
						break
					a = a.strip().split(',')
					temp.add(a[0])
				f.close()

			f = open(os.path.join(commit_store_path, indexpath), 'ab')
			h[sha[0:2]] = INDEX_AND_NEW(file = f, set = temp)
		if sha[2:] in h[sha[0:2]].set:#already exists
			continue
		h[sha[0:2]].file.write('%s,%d,%d\n'%(sha[2:], which, offset))
		h[sha[0:2]].set.add(sha[2:])

		md51 = hashlib.md5(parsed_commit.author).hexdigest()[0:8]
		temp = 0
		j1 = 0
		j2 = 0
		while True:
			j1 = md51 + str(temp) 
			if j1 in hash8:
				if hash8[j1] == parsed_commit.author:
					break
			else:
				hash8[j1] = parsed_commit.author
				hash8_file.write("%s,%s\n"%(j1, parsed_commit.author))
				break
			temp += 1

		md52 = hashlib.md5(parsed_commit.committer).hexdigest()[0:8]
		temp = 0
		while True:
			j2 = md52 + str(temp) 
			if j2 in hash8:
				if hash8[j2] == parsed_commit.committer:
					break
			else:
				hash8[j2] = parsed_commit.committer
				hash8_file.write("%s,%s\n"%(j2, parsed_commit.committer))
				break
			temp += 1
		prt = '\nparent '.join(parsed_commit.parents)
		if prt:
			prt = 'parent ' + prt +'\0'
		else:
			prt = '\0'
		write_str = "tree %s,%s,%s,%s,%s,%s,%s"%(\
			parsed_commit.tree, prt,\
			j1, parsed_commit.author_time, j2,\
			parsed_commit.committer_time, parsed_commit.msg)
			
		write_str = zlib.compress(write_str)
		
		head = int2msb(len(write_str))
		content = ''.join([head, write_str])
		
		w.write(content)
		already_store += 1
		offset += len(content)
		if already_store == NUM_PER_MAIN_COMMIT:
			already_store = 0
			which += 1
			offset = 0
			w.close()
			new_file_path = os.path.join(commit_store_path, 'rgit_commit_main%d'%(which))
			w = open(new_file_path, 'wb')
	f = open(os.path.join(commit_store_path, 'to_write'), 'wb')
	f.write("%d,%d,%d"%(which, already_store, offset))
	f.close()
	hash8_file.close()
	for i in h:
		h[i].file.close()
	w.close()
	return 1

def rgit_commit_csv_store_developer_dup(git_repo_path, commit_store_path = '../commit_store/commit_developer_dup'):

	'''
	a version of rgit_commit_csv_store without the deduplication of developer msgs
	store commit objects from git_repo_path, to csv files in commit_store_path
	'''
	csv_files = os.listdir(commit_store_path)
	if 'to_write' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'to_write')
		w = open(new_file_path, 'wb')
		w.write('0,0,0') #which number of csv to write, and how many it has had, and offset it will begin
		w.close()
	if 'rgit_commit_main0' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'rgit_commit_main0')
		w = open(new_file_path, 'wb')
		w.close()

	idx_pack_pairs = idx_pack_from_repo(git_repo_path)
	ret = []
	for i, j in idx_pack_pairs:
		ret.extend(commitFromPack(i, j))

	f = open(os.path.join(commit_store_path, 'to_write'))
	a = f.readline().split(',')
	which = int(a[0])
	already_store = int(a[1])
	offset = int(a[2])
	f.close()

	#for now 50000 commits per file
	w = open(''.join([commit_store_path, '/rgit_commit_main', str(which)]), 'ab')
	h = {}
	test_cmpr = []
	for sha, raw_data in ret:
		parsed_commit = parse_commit(raw_data)

		if sha[0:2] not in h:#format: sha,which,line_number
			indexpath = ''.join(['index', sha[0:2]])
			if indexpath not in csv_files:
				temp = set()
				f = open(os.path.join(commit_store_path, indexpath), 'wb')
				f.close()
			else:
				f = open(os.path.join(commit_store_path, indexpath))
				temp  = set()
				while True:
					a = f.readline()
					if not a:
						break
					a = a.strip().split(',')
					temp.add(a[0])
				f.close()

			f = open(os.path.join(commit_store_path, indexpath), 'ab')
			h[sha[0:2]] = INDEX_AND_NEW(file = f, set = temp)
		if sha[2:] in h[sha[0:2]].set:#already exists
			continue
		h[sha[0:2]].file.write('%s,%d,%d\n'%(sha[2:], which, offset))
		h[sha[0:2]].set.add(sha[2:])

		write_str = "%s,%s,%s,%s,%s,%s,%s"%(\
			parsed_commit.tree, ','.join(parsed_commit.parents)+'\0',\
			parsed_commit.author, parsed_commit.author_time, parsed_commit.committer,\
			parsed_commit.committer_time, parsed_commit.msg)
		write_str = zlib.compress(write_str)
		
		head = int2msb(len(write_str))
		content = ''.join([head, write_str])
		
		w.write(content)
		already_store += 1
		offset += len(content)
		if already_store == NUM_PER_MAIN_COMMIT:
			already_store = 0
			which += 1
			offset = 0
			w.close()
			new_file_path = os.path.join(commit_store_path, 'rgit_commit_main%d'%(which))
			w = open(new_file_path, 'wb')
	f = open(os.path.join(commit_store_path, 'to_write'), 'wb')
	f.write("%d,%d,%d"%(which, already_store, offset))
	f.close()
	for i in h:
		h[i].file.close()
	w.close()
	return 1

def rgit_commit_csv_store_all_dup(git_repo_path, commit_store_path = '../commit_store/commit_all_dup'):

	'''
	a version of rgit_commit_csv_store without the deduplication of developer msgs
	store commit objects from git_repo_path, to csv files in commit_store_path
	'''
	csv_files = os.listdir(commit_store_path)
	if 'to_write' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'to_write')
		w = open(new_file_path, 'wb')
		w.write('0,0,0') #which number of csv to write, and how many it has had, and offset it will begin
		w.close()
	if 'rgit_commit_main0' not in csv_files:
		new_file_path = os.path.join(commit_store_path, 'rgit_commit_main0')
		w = open(new_file_path, 'wb')
		w.close()

	idx_pack_pairs = idx_pack_from_repo(git_repo_path)
	ret = []
	for i, j in idx_pack_pairs:
		ret.extend(commitFromPack(i, j))

	f = open(os.path.join(commit_store_path, 'to_write'))
	a = f.readline().split(',')
	which = int(a[0])
	already_store = int(a[1])
	offset = int(a[2])
	f.close()

	#for now, 50000 commits per file
	w = open(''.join([commit_store_path, '/rgit_commit_main', str(which)]), 'ab')
	h = {}
	test_cmpr = []
	for sha, raw_data in ret:
		parsed_commit = parse_commit(raw_data)

		if sha[0:2] not in h:#format: sha,which,line_number
			indexpath = ''.join(['index', sha[0:2]])
			if indexpath not in csv_files:
				temp = set()
				f = open(os.path.join(commit_store_path, indexpath), 'wb')
				f.close()
			else:
				f = open(os.path.join(commit_store_path, indexpath))
				temp  = set()
				while True:
					a = f.readline()
					if not a:
						break
					a = a.strip().split(',')
					temp.add(a[0])
				f.close()

			f = open(os.path.join(commit_store_path, indexpath), 'ab')
			h[sha[0:2]] = INDEX_AND_NEW(file = f, set = temp)
		#if sha[2:] in h[sha[0:2]].set:#already exists
		#	continue
		h[sha[0:2]].file.write('%s,%d,%d\n'%(sha[2:], which, offset))
		h[sha[0:2]].set.add(sha[2:])

		prt = '\nparent '.join(parsed_commit.parents)
		if prt:
			prt = 'parent ' + prt +'\0'
		else:
			prt = '\0'
		write_str = "tree %s, %s,%s,%s,%s,%s,%s"%(\
			parsed_commit.tree, prt,\
			parsed_commit.author, parsed_commit.author_time, parsed_commit.committer,\
			parsed_commit.committer_time, parsed_commit.msg)
		write_str = zlib.compress(write_str)
		
		head = int2msb(len(write_str))
		content = ''.join([head, write_str])
		
		w.write(content)
		already_store += 1
		offset += len(content)
		if already_store == NUM_PER_MAIN_COMMIT:
			already_store = 0
			which += 1
			offset = 0
			w.close()
			new_file_path = os.path.join(commit_store_path, 'rgit_commit_main%d'%(which))
			w = open(new_file_path, 'wb')
	f = open(os.path.join(commit_store_path, 'to_write'), 'wb')
	f.write("%d,%d,%d"%(which, already_store, offset))
	f.close()
	for i in h:
		h[i].file.close()
	w.close()
	return 1


def commit_csv_store_rate(csv_file_list, commit_store_path = '../commit_store'):
	after = dirSize(commit_store_path)
	before = 0
	no_cmpr = 0
	for i in csv_file_list:
		f = open(i)
		a = f.readline()
		while True:
			a = f.readline()
			if not a:
				break
			a = a.strip().split(',')
			t = a[1]
			if t!='commit':
				continue
			r = int(a[5])
			p = int(a[4])
			if r!=-1:
				before += r
			else:
				before += p
			no_cmpr += p
	return after,before,no_cmpr
	
def clear_all_commit(commit_store_path = '../commit_store'):
	check_path = os.listdir(commit_store_path)
	if 'commit' not in check_path:
		print 'Warning: not find directory "commit" in current directory, nothing happened'
		exit()
	for i in check_path:
		a = os.path.join(commit_store_path, i)
		for j in os.listdir(a):
			if j != '.gitkeep':
				os.remove(os.path.join(commit_store_path, i, j))

def deparse_commit(data, commit_store_path = '../commit_store/commit', detail = False, csv_file = ''):
	#do not use pd.read_csv here, bcz when ',' in developer's message, there will be error
	a = data.split(',')
	cnt = 1
	parents = []
	while True:
		if len(a[cnt]) == 40:
			parents.append(a[cnt])
			cnt += 1
		elif len(a[cnt]) == 1:#'\0'
			cnt += 1
			break
		else:
			parents.append(a[cnt][:-1])
			cnt += 1
			break
	if not csv_file:
		csv_file = open('%s/rgit_commit_hash8.csv'%(commit_store_path), 'rb')
	line = csv_file.readline()
	author = ''
	committer = ''
	while (not author) or (not committer):
		line = csv_file.readline()
		if not line:
			break
		line = line.strip().split(',')
		if not author and line[0] == a[cnt]:
			author = ','.join(line[1:])
		if not committer and line[0] == a[cnt+2]:
			committer = ','.join(line[1:])
			
	csv_file.close()
	if (not author) or (not committer):
		print 'Error in deparse_commit: not find author or committer'
		exit()

	parent_str = ''
	if parents:
		parent_str = 'parent '+ '\nparent '.join(parents) + "\n"
	msg = ','.join(a[cnt+4:])
	ret_str = "tree %s\n%sauthor %s %s\ncommitter %s %s\n%s"%(a[0], parent_str, author, a[cnt+1], committer, a[cnt+3], msg)
	
	if not detail:
		return ret_str
		
	for_case = a[cnt+3].find('>')
	if for_case!=-1:
		temp = a[cnt+3][for_case+1:].split('>')
	else:
		temp = a[cnt+3].split(' ')
	
	#some developer's name has '>' in it, so gmt may be the email thing...
	gmt = int(temp[0])
	sign = temp[1][0]
	am = int(temp[1][1:3])
	local_time = -1
	if sign == '+':
		local_time = gmt + 3600*am
	elif sign == '-':
		local_time = gmt - 3600*am
	else:
		print ("Error when processing timestamp in deparse_commit")
		exit()
	return ret_str, {'merge': (parent_str.count('parent')>1), 'time':gmt, 'local_time':local_time, 'author': author[:author.find('<')-1],'committer': committer[:committer.find('<')-1], 'msg': msg, }
	
def recover_commit(sha, commit_store_path = '../commit_store/commit'):
	store_path = '%s/index%s'%(commit_store_path, sha[0:2])
	if not os.path.exists(store_path):
		#print ('Error in recover_commit: not found sha %s'%(sha))
		return -1
	f = open(store_path)
	which = -1
	offset = -1
	while True:
		a = f.readline()
		if not a:
			#print ("Error in recover_commit: not found sha %s"%(sha))
			f.close()
			return -1
		a = a.strip().split(',')
		if a[0] == sha[2:]:
			which = a[1]
			offset = a[2]
			f.close()
			break
	main_path = '%s/rgit_commit_main%s'%(commit_store_path, which)
	f = open(main_path, 'rb')
	f.seek(int(offset))
	size = 0
	i = 0
	while True:
		a = ord(f.read(1))
		size = size + ((a & 0x7f) << (7*i))
		if a&0x80:
			i += 1
		else:
			break
	to_process = zlib.decompress(f.read(size))
	f.close()
	
	after_deparse = deparse_commit(to_process, commit_store_path)
	
	return after_deparse
	
def commit_print_stat():
	print "this program deduplicate commits with three rules:\n"
	print "\tRule 1. Remove the same commit from different repository"
	print "\tRule 2. Remove structure info in commits, such as 'author', 'committer' string in each commit"
	print '\tRule 3. Replace developer messages in commits with shorter string'
	print '\n'
	print "With all rules applied, it takes %d byte storage\n"%(dirSize('./commit_store/commit'))
	print "With rule 1, 2 applied, it takes %d byte storage\n"%(dirSize('./commit_store/commit_struc_dup'))
	print "With rule 1, 3 applied, it takes %d byte storage\n"%(dirSize('./commit_store/commit_developer_dup'))
	print "With rule 2, 3 applied, it takes %d byte storage\n"%(dirSize('./commit_store/commit_developer_dup'))
	print "Without any rules applied, it takes %d byte storage\n"%(dirSize('./commit_store/commit_all_dup'))
	
def get_all(commit_store_path = '../commit_store/commit'):
	ret = []
	suffix = get_256suffix()
	which_file = {}
	for suf in suffix:
		store_path = '%s/index%s'%(commit_store_path, suf)
		if not os.path.exists(store_path):
			#print ('Error in recover_commit: not found sha %s'%(sha))
			return -1
		f = open(store_path)
		which = -1
		offset = -1
		while True:
			a = f.readline()
			if not a:
				f.close()
				break
			a = a.strip().split(',')
			which = a[1]
			offset = a[2]
			if which not in which_file:
				which_file[which] = open('%s/rgit_commit_main%s'%(commit_store_path, which), 'rb')
			which_file[which].seek(int(offset))
			size = 0
			i = 0
			while True:
				t = ord(which_file[which].read(1))
				size = size + ((t & 0x7f) << (7*i))
				if t&0x80:
					i += 1
				else:
					break
					
			to_process = zlib.decompress(which_file[which].read(size))
			
			total, detail = deparse_commit(to_process, commit_store_path, detail = True)
			ret.append((suf+a[0], total, detail))
	
	return ret
	
def get_some_commit(pool, commit_store_path = '../commit_store/commit'):
	ret = []
	suffix = get_256suffix()
	which_file = {}
	for suf in suffix:
		store_path = '%s/index%s'%(commit_store_path, suf)
		if not os.path.exists(store_path):
			#print ('Error in recover_commit: not found sha %s'%(sha))
			return -1
		to_extract = [x[2:] for x in pool if x[0:2] == suf]
		if not to_extract:
			continue
		f = open(store_path)
		which = -1
		offset = -1
		extracted_num = 0
		while True:
			a = f.readline()
			if not a:
				f.close()
				break
			a = a.strip().split(',')
			if a[0] not in to_extract:
				continue
			which = a[1]
			offset = a[2]
			if which not in which_file:
				which_file[which] = open('%s/rgit_commit_main%s'%(commit_store_path, which), 'rb')
			which_file[which].seek(int(offset))
			size = 0
			i = 0
			while True:
				t = ord(which_file[which].read(1))
				size = size + ((t & 0x7f) << (7*i))
				if t&0x80:
					i += 1
				else:
					break
			to_process = zlib.decompress(which_file[which].read(size))
			
			total, detail = deparse_commit(to_process, commit_store_path, detail = True)
			ret.append((suf+a[0], total, detail))
			
			
			extracted_num += 1
			if extracted_num == len(to_extract):
				break
	
	return ret