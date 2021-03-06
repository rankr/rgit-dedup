#coding: utf-8
import os
import subprocess
import hashlib
import sys
sys.path.append('../')
from func import *
import zlib

MSBBIT = 1<<31


NUM_PER_MAIN_TAG = 50000

class OBJECT:
	def __init__(self, raw_data = '', type = ''):
		self.raw_data = raw_data
		self.type = type

class PARSED_TAG:
	def __init__(self):
		pass

class INDEX_AND_NEW:
	def __init__(self, file, set = {}):
		self.file = file
		self.set = set

def tagFromPack(idxPath, packPath):
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
			if obj_type != 'tag':
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
			if obj_type != 'tag':
				continue
			tar_data = handle_delta(to_process, 20, obj_hash[base_obj_sha1])
		elif obj_type == "not exists":
			print ("Error in addObjFromPack, objType is not exists")
			exit()
		else:
			if obj_type != 'tag':
				continue
			tar_data = zlib.decompress(to_process)
		obj_hash[obj_list[i][0]].raw_data = tar_data
		obj_hash[obj_list[i][0]].type = 'tag'

	f_pack.close()

	for i in obj_hash:
		if obj_hash[i].type == 'tag':
			ret.append((i, obj_hash[i].raw_data))
	return ret

s2n = {'commit':'0', 'blob':'1', 'tree':'2', 'tag':'3'}
n2s = {'0':'commit', '1':'blob', '2':'tree', '3':'tag'}
def parse_tag(raw_data):
	ret = PARSED_TAG()
	a = raw_data.split('\n')
	ret.object = a[0].split(' ')[1]
	ret.type = s2n[a[1].split(' ')[1]]
	ret.tag = ' '.join(a[2].split(' ')[1:])
	begin = a[3].find(' ')
	end = a[3].find('>')
	ret.tagger = a[3][begin+1:end+1]
	ret.time = a[3][end+2:]
	ret.msg = '\n'.join(a[4:])
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
	
def rgit_tag_store(git_repo_path, tag_store_path = '../tag_store/tag', already = []):
	'''
	store tag objects from git_repo_path, to csv files in tag_store_path
	'''
	csv_files = os.listdir(tag_store_path)
	if 'to_write' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'to_write')
		w = open(new_file_path, 'wb')
		w.write('0,0,0') #which number of csv to write, and how many it has had, and offset it will begin
		w.close()
	if 'rgit_tag_main0' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'rgit_tag_main0')
		w = open(new_file_path, 'wb')
		w.close()
	if 'rgit_tag_hash8.csv' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'rgit_tag_hash8.csv')
		w = open(new_file_path, 'wb')
		w.write(','.join(['hash8+1','name_email_str\n']))
		w.close()
	
	if already:
		ret = already
	else:
		idx_pack_pairs = idx_pack_from_repo(git_repo_path)
		ret = []
		for i, j in idx_pack_pairs:
			ret.extend(tagFromPack(i, j))

	f = open(os.path.join(tag_store_path, 'to_write'))
	a = f.readline().split(',')
	which = int(a[0])
	already_store = int(a[1])
	offset = int(a[2])
	f.close()

	f = open(os.path.join(tag_store_path, 'rgit_tag_hash8.csv'))
	a = f.readline()
	hash8  = {}
	while True:
		a = f.readline()
		if not a:
			break
		a = a.strip().split(',')
		hash8[a[0]] = a[1]
	f.close()
	hash8_file = open(os.path.join(tag_store_path, 'rgit_tag_hash8.csv'), 'ab')

	#for now most 50000 tags in one file
	w = open(''.join([tag_store_path, '/rgit_tag_main', str(which)]), 'ab')
	h = {}
	test_cmpr = []
	for sha, raw_data in ret:
		parsed_tag = parse_tag(raw_data)

		if sha[0:2] not in h:#format: sha,which,line_number
			indexpath = ''.join(['index', sha[0:2]])
			if indexpath not in csv_files:
				temp = set()
				f = open(os.path.join(tag_store_path, indexpath), 'wb')
				f.close()
			else:
				f = open(os.path.join(tag_store_path, indexpath))
				temp  = set()
				while True:
					a = f.readline()
					if not a:
						break
					a = a.strip().split(',')
					temp.add(a[0])
				f.close()

			f = open(os.path.join(tag_store_path, indexpath), 'ab')
			h[sha[0:2]] = INDEX_AND_NEW(file = f, set = temp)
		if sha[2:] in h[sha[0:2]].set:#already exists
			continue
		h[sha[0:2]].file.write('%s,%d,%d\n'%(sha[2:], which, offset))
		h[sha[0:2]].set.add(sha[2:])

		md51 = hashlib.md5(parsed_tag.tagger).hexdigest()[0:8]
		temp = 0
		j1 = 0
		while True:
			j1 = md51 + str(temp) 
			if j1 in hash8:
				if hash8[j1] == parsed_tag.tagger:
					break
			else:
				hash8[j1] = parsed_tag.tagger
				hash8_file.write("%s,%s\n"%(j1, parsed_tag.tagger))
				break
			temp += 1

		write_str = "%s,%s,%s,%s,%s,%s"%(\
			parsed_tag.object, parsed_tag.type, parsed_tag.tag,\
			j1, parsed_tag.time, parsed_tag.msg)
		write_str = zlib.compress(write_str)
		
		head = int2msb(len(write_str))
		
		content = ''.join([head, write_str])

		w.write(content)
		already_store += 1
		offset += len(content)
		if already_store == NUM_PER_MAIN_TAG:
			already_store = 0
			which += 1
			offset = 0
			w.close()
			new_file_path = os.path.join(tag_store_path, 'rgit_tag_main%d'%(which))
			w = open(new_file_path, 'wb')
	f = open(os.path.join(tag_store_path, 'to_write'), 'wb')
	f.write("%d,%d,%d"%(which, already_store, offset))
	f.close()
	hash8_file.close()
	for i in h:
		h[i].file.close()
	w.close()
	return 1


def rgit_tag_store_tag_dup(git_repo_path, tag_store_path = '../tag_store/tag_tag_dup', already = []):
	'''
	store tag objects from git_repo_path, to csv files in tag_store_path
	'''
	csv_files = os.listdir(tag_store_path)
	if 'to_write' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'to_write')
		w = open(new_file_path, 'wb')
		w.write('0,0,0') #which number of csv to write, and how many it has had, and offset it will begin
		w.close()
	if 'rgit_tag_main0' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'rgit_tag_main0')
		w = open(new_file_path, 'wb')
		w.close()
	if 'rgit_tag_hash8.csv' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'rgit_tag_hash8.csv')
		w = open(new_file_path, 'wb')
		w.write(','.join(['hash8+1','name_email_str\n']))
		w.close()
	
	if already:
		ret = already
	else:
		idx_pack_pairs = idx_pack_from_repo(git_repo_path)
		ret = []
		for i, j in idx_pack_pairs:
			ret.extend(tagFromPack(i, j))

	f = open(os.path.join(tag_store_path, 'to_write'))
	a = f.readline().split(',')
	which = int(a[0])
	already_store = int(a[1])
	offset = int(a[2])
	f.close()

	f = open(os.path.join(tag_store_path, 'rgit_tag_hash8.csv'))
	a = f.readline()
	hash8  = {}
	while True:
		a = f.readline()
		if not a:
			break
		a = a.strip().split(',')
		hash8[a[0]] = a[1]
	f.close()
	hash8_file = open(os.path.join(tag_store_path, 'rgit_tag_hash8.csv'), 'ab')

	#for now most 50000 tags in one file
	w = open(''.join([tag_store_path, '/rgit_tag_main', str(which)]), 'ab')
	h = {}
	test_cmpr = []
	for sha, raw_data in ret:
		parsed_tag = parse_tag(raw_data)

		if sha[0:2] not in h:#format: sha,which,line_number
			indexpath = ''.join(['index', sha[0:2]])
			if indexpath not in csv_files:
				temp = set()
				f = open(os.path.join(tag_store_path, indexpath), 'wb')
				f.close()
			else:
				f = open(os.path.join(tag_store_path, indexpath))
				temp  = set()
				while True:
					a = f.readline()
					if not a:
						break
					a = a.strip().split(',')
					temp.add(a[0])
				f.close()

			f = open(os.path.join(tag_store_path, indexpath), 'ab')
			h[sha[0:2]] = INDEX_AND_NEW(file = f, set = temp)
		#if sha[2:] in h[sha[0:2]].set:#already exists
		#	continue
		h[sha[0:2]].file.write('%s,%d,%d\n'%(sha[2:], which, offset))
		h[sha[0:2]].set.add(sha[2:])

		md51 = hashlib.md5(parsed_tag.tagger).hexdigest()[0:8]
		temp = 0
		j1 = 0
		while True:
			j1 = md51 + str(temp) 
			if j1 in hash8:
				if hash8[j1] == parsed_tag.tagger:
					break
			else:
				hash8[j1] = parsed_tag.tagger
				hash8_file.write("%s,%s\n"%(j1, parsed_tag.tagger))
				break
			temp += 1

		write_str = "%s,%s,%s,%s,%s,%s"%(\
			parsed_tag.object, parsed_tag.type, parsed_tag.tag,\
			j1, parsed_tag.time, parsed_tag.msg)
		write_str = zlib.compress(write_str)
		
		head = int2msb(len(write_str))
		
		content = ''.join([head, write_str])

		w.write(content)
		already_store += 1
		offset += len(content)
		if already_store == NUM_PER_MAIN_TAG:
			already_store = 0
			which += 1
			offset = 0
			w.close()
			new_file_path = os.path.join(tag_store_path, 'rgit_tag_main%d'%(which))
			w = open(new_file_path, 'wb')
	f = open(os.path.join(tag_store_path, 'to_write'), 'wb')
	f.write("%d,%d,%d"%(which, already_store, offset))
	f.close()
	hash8_file.close()
	for i in h:
		h[i].file.close()
	w.close()
	return 1

def rgit_tag_store_struc_dup(git_repo_path, tag_store_path = '../tag_store/tag_struc_dup', already = []):
	'''
	store tag objects from git_repo_path, to csv files in tag_store_path
	'''
	csv_files = os.listdir(tag_store_path)
	if 'to_write' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'to_write')
		w = open(new_file_path, 'wb')
		w.write('0,0,0') #which number of csv to write, and how many it has had, and offset it will begin
		w.close()
	if 'rgit_tag_main0' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'rgit_tag_main0')
		w = open(new_file_path, 'wb')
		w.close()
	if 'rgit_tag_hash8.csv' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'rgit_tag_hash8.csv')
		w = open(new_file_path, 'wb')
		w.write(','.join(['hash8+1','name_email_str\n']))
		w.close()
	
	if already:
		ret = already
	else:
		idx_pack_pairs = idx_pack_from_repo(git_repo_path)
		ret = []
		for i, j in idx_pack_pairs:
			ret.extend(tagFromPack(i, j))

	f = open(os.path.join(tag_store_path, 'to_write'))
	a = f.readline().split(',')
	which = int(a[0])
	already_store = int(a[1])
	offset = int(a[2])
	f.close()

	f = open(os.path.join(tag_store_path, 'rgit_tag_hash8.csv'))
	a = f.readline()
	hash8  = {}
	while True:
		a = f.readline()
		if not a:
			break
		a = a.strip().split(',')
		hash8[a[0]] = a[1]
	f.close()
	hash8_file = open(os.path.join(tag_store_path, 'rgit_tag_hash8.csv'), 'ab')

	#for now most 50000 tags in one file
	w = open(''.join([tag_store_path, '/rgit_tag_main', str(which)]), 'ab')
	h = {}
	test_cmpr = []
	for sha, raw_data in ret:
		parsed_tag = parse_tag(raw_data)

		if sha[0:2] not in h:#format: sha,which,line_number
			indexpath = ''.join(['index', sha[0:2]])
			if indexpath not in csv_files:
				temp = set()
				f = open(os.path.join(tag_store_path, indexpath), 'wb')
				f.close()
			else:
				f = open(os.path.join(tag_store_path, indexpath))
				temp  = set()
				while True:
					a = f.readline()
					if not a:
						break
					a = a.strip().split(',')
					temp.add(a[0])
				f.close()

			f = open(os.path.join(tag_store_path, indexpath), 'ab')
			h[sha[0:2]] = INDEX_AND_NEW(file = f, set = temp)
		if sha[2:] in h[sha[0:2]].set:#already exists
			continue
		h[sha[0:2]].file.write('%s,%d,%d\n'%(sha[2:], which, offset))
		h[sha[0:2]].set.add(sha[2:])

		md51 = hashlib.md5(parsed_tag.tagger).hexdigest()[0:8]
		temp = 0
		j1 = 0
		while True:
			j1 = md51 + str(temp) 
			if j1 in hash8:
				if hash8[j1] == parsed_tag.tagger:
					break
			else:
				hash8[j1] = parsed_tag.tagger
				hash8_file.write("%s,%s\n"%(j1, parsed_tag.tagger))
				break
			temp += 1

		write_str = "object %s,type %s,tag %s,tagger %s,%s,%s"%(\
			parsed_tag.object, parsed_tag.type, parsed_tag.tag,\
			j1, parsed_tag.time, parsed_tag.msg)
		write_str = zlib.compress(write_str)
		
		head = int2msb(len(write_str))
		
		content = ''.join([head, write_str])

		w.write(content)
		already_store += 1
		offset += len(content)
		if already_store == NUM_PER_MAIN_TAG:
			already_store = 0
			which += 1
			offset = 0
			w.close()
			new_file_path = os.path.join(tag_store_path, 'rgit_tag_main%d'%(which))
			w = open(new_file_path, 'wb')
	f = open(os.path.join(tag_store_path, 'to_write'), 'wb')
	f.write("%d,%d,%d"%(which, already_store, offset))
	f.close()
	hash8_file.close()
	for i in h:
		h[i].file.close()
	w.close()
	return 1

def rgit_tag_store_strlen_dup(git_repo_path, tag_store_path = '../tag_store/tag_strlen_dup', already = []):
	'''
	store tag objects from git_repo_path, to csv files in tag_store_path
	'''
	csv_files = os.listdir(tag_store_path)
	if 'to_write' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'to_write')
		w = open(new_file_path, 'wb')
		w.write('0,0,0') #which number of csv to write, and how many it has had, and offset it will begin
		w.close()
	if 'rgit_tag_main0' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'rgit_tag_main0')
		w = open(new_file_path, 'wb')
		w.close()
	if 'rgit_tag_hash8.csv' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'rgit_tag_hash8.csv')
		w = open(new_file_path, 'wb')
		w.write(','.join(['hash8+1','name_email_str\n']))
		w.close()
	
	if already:
		ret = already
	else:
		idx_pack_pairs = idx_pack_from_repo(git_repo_path)
		ret = []
		for i, j in idx_pack_pairs:
			ret.extend(tagFromPack(i, j))

	f = open(os.path.join(tag_store_path, 'to_write'))
	a = f.readline().split(',')
	which = int(a[0])
	already_store = int(a[1])
	offset = int(a[2])
	f.close()

	f = open(os.path.join(tag_store_path, 'rgit_tag_hash8.csv'))
	a = f.readline()
	hash8  = {}
	while True:
		a = f.readline()
		if not a:
			break
		a = a.strip().split(',')
		hash8[a[0]] = a[1]
	f.close()
	hash8_file = open(os.path.join(tag_store_path, 'rgit_tag_hash8.csv'), 'ab')

	#for now most 50000 tags in one file
	w = open(''.join([tag_store_path, '/rgit_tag_main', str(which)]), 'ab')
	h = {}
	test_cmpr = []
	for sha, raw_data in ret:
		parsed_tag = parse_tag(raw_data)

		if sha[0:2] not in h:#format: sha,which,line_number
			indexpath = ''.join(['index', sha[0:2]])
			if indexpath not in csv_files:
				temp = set()
				f = open(os.path.join(tag_store_path, indexpath), 'wb')
				f.close()
			else:
				f = open(os.path.join(tag_store_path, indexpath))
				temp  = set()
				while True:
					a = f.readline()
					if not a:
						break
					a = a.strip().split(',')
					temp.add(a[0])
				f.close()

			f = open(os.path.join(tag_store_path, indexpath), 'ab')
			h[sha[0:2]] = INDEX_AND_NEW(file = f, set = temp)
		if sha[2:] in h[sha[0:2]].set:#already exists
			continue
		h[sha[0:2]].file.write('%s,%d,%d\n'%(sha[2:], which, offset))
		h[sha[0:2]].set.add(sha[2:])

		md51 = hashlib.md5(parsed_tag.tagger).hexdigest()[0:8]
		temp = 0
		j1 = 0
		while True:
			j1 = md51 + str(temp) 
			if j1 in hash8:
				if hash8[j1] == parsed_tag.tagger:
					break
			else:
				hash8[j1] = parsed_tag.tagger
				hash8_file.write("%s,%s\n"%(j1, parsed_tag.tagger))
				break
			temp += 1

		write_str = "%s,%s,%s,%s,%s,%s"%(\
			parsed_tag.object, n2s[parsed_tag.type], parsed_tag.tag,\
			parsed_tag.tagger, parsed_tag.time, parsed_tag.msg)
		write_str = zlib.compress(write_str)
		
		head = int2msb(len(write_str))
		
		content = ''.join([head, write_str])

		w.write(content)
		already_store += 1
		offset += len(content)
		if already_store == NUM_PER_MAIN_TAG:
			already_store = 0
			which += 1
			offset = 0
			w.close()
			new_file_path = os.path.join(tag_store_path, 'rgit_tag_main%d'%(which))
			w = open(new_file_path, 'wb')
	f = open(os.path.join(tag_store_path, 'to_write'), 'wb')
	f.write("%d,%d,%d"%(which, already_store, offset))
	f.close()
	hash8_file.close()
	for i in h:
		h[i].file.close()
	w.close()
	return 1

def rgit_tag_store_all_dup(git_repo_path, tag_store_path = '../tag_store/tag_all_dup', already = []):
	'''
	store tag objects from git_repo_path, to csv files in tag_store_path
	'''
	csv_files = os.listdir(tag_store_path)
	if 'to_write' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'to_write')
		w = open(new_file_path, 'wb')
		w.write('0,0,0') #which number of csv to write, and how many it has had, and offset it will begin
		w.close()
	if 'rgit_tag_main0' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'rgit_tag_main0')
		w = open(new_file_path, 'wb')
		w.close()
	if 'rgit_tag_hash8.csv' not in csv_files:
		new_file_path = os.path.join(tag_store_path, 'rgit_tag_hash8.csv')
		w = open(new_file_path, 'wb')
		w.write(','.join(['hash8+1','name_email_str\n']))
		w.close()
	
	if already:
		ret = already
	else:
		idx_pack_pairs = idx_pack_from_repo(git_repo_path)
		ret = []
		for i, j in idx_pack_pairs:
			ret.extend(tagFromPack(i, j))

	f = open(os.path.join(tag_store_path, 'to_write'))
	a = f.readline().split(',')
	which = int(a[0])
	already_store = int(a[1])
	offset = int(a[2])
	f.close()

	f = open(os.path.join(tag_store_path, 'rgit_tag_hash8.csv'))
	a = f.readline()
	hash8  = {}
	while True:
		a = f.readline()
		if not a:
			break
		a = a.strip().split(',')
		hash8[a[0]] = a[1]
	f.close()
	hash8_file = open(os.path.join(tag_store_path, 'rgit_tag_hash8.csv'), 'ab')

	#for now most 50000 tags in one file
	w = open(''.join([tag_store_path, '/rgit_tag_main', str(which)]), 'ab')
	h = {}
	test_cmpr = []
	for sha, raw_data in ret:
		parsed_tag = parse_tag(raw_data)

		if sha[0:2] not in h:#format: sha,which,line_number
			indexpath = ''.join(['index', sha[0:2]])
			if indexpath not in csv_files:
				temp = set()
				f = open(os.path.join(tag_store_path, indexpath), 'wb')
				f.close()
			else:
				f = open(os.path.join(tag_store_path, indexpath))
				temp  = set()
				while True:
					a = f.readline()
					if not a:
						break
					a = a.strip().split(',')
					temp.add(a[0])
				f.close()

			f = open(os.path.join(tag_store_path, indexpath), 'ab')
			h[sha[0:2]] = INDEX_AND_NEW(file = f, set = temp)
		#if sha[2:] in h[sha[0:2]].set:#already exists
		#	continue
		h[sha[0:2]].file.write('%s,%d,%d\n'%(sha[2:], which, offset))
		h[sha[0:2]].set.add(sha[2:])

		md51 = hashlib.md5(parsed_tag.tagger).hexdigest()[0:8]
		temp = 0
		j1 = 0
		while True:
			j1 = md51 + str(temp) 
			if j1 in hash8:
				if hash8[j1] == parsed_tag.tagger:
					break
			else:
				hash8[j1] = parsed_tag.tagger
				hash8_file.write("%s,%s\n"%(j1, parsed_tag.tagger))
				break
			temp += 1

		write_str = "object %s,type %s,tag %s,tagger %s,%s,%s"%(\
			parsed_tag.object, n2s[parsed_tag.type], parsed_tag.tag,\
			parsed_tag.tagger, parsed_tag.time, parsed_tag.msg)
		write_str = zlib.compress(write_str)
		
		head = int2msb(len(write_str))
		
		content = ''.join([head, write_str])

		w.write(content)
		already_store += 1
		offset += len(content)
		if already_store == NUM_PER_MAIN_TAG:
			already_store = 0
			which += 1
			offset = 0
			w.close()
			new_file_path = os.path.join(tag_store_path, 'rgit_tag_main%d'%(which))
			w = open(new_file_path, 'wb')
	f = open(os.path.join(tag_store_path, 'to_write'), 'wb')
	f.write("%d,%d,%d"%(which, already_store, offset))
	f.close()
	hash8_file.close()
	for i in h:
		h[i].file.close()
	w.close()
	return 1


def tag_store_rate(csv_file_list, tag_store_path = '../tag_store'):
	after = dirSize(tag_store_path)
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
			if t!='tag':
				continue
			r = int(a[5])
			p = int(a[4])
			if r!=-1:
				before += r
			else:
				before += p
			no_cmpr += p
	return after,before,no_cmpr
	
def clear_all_tag(tag_store_path = '../tag_store'):
	check_path = os.listdir(tag_store_path)
	if 'tag' not in check_path:
		print 'Warning: not find directory "tag" in current directory, nothing happened'
		exit()
	for i in check_path:
		a = os.path.join(tag_store_path, i)
		for j in os.listdir(a):
			if j != '.gitkeep':
				os.remove(os.path.join(tag_store_path, i, j))

def deparse_tag(data, tag_store_path = '../tag_store/tag'):
	#do not use pd.read_csv here, bcz when ',' in developer's message, there will be error
	a = data.split(',')
	csv_file = open('%s/rgit_tag_hash8.csv'%(tag_store_path), 'rb')
	line = csv_file.readline()
	tagger = ''
	while (not tagger):
		line = csv_file.readline()
		if not line:
			break
		line = line.strip().split(',')
		if not tagger and line[0] == a[3]:
			tagger = ','.join(line[1:])
			
	csv_file.close()
	if (not tagger):
		print 'Error in deparse_tag: not find author or tagger'
		exit()
	
	return "object %s\ntype %s\ntag %s\ntagger %s %s\n%s"%(a[0], n2s[a[1]], a[2], tagger, a[4], ','.join(a[5:]))
	
def recover_tag(sha, tag_store_path = '../tag_store/tag'):
	store_path = '%s/index%s'%(tag_store_path, sha[0:2])
	if not os.path.exists(store_path):
		#print 'Error 1 in recover_tag: not found sha %s'%(sha)
		return -1
	f = open(store_path)
	which = -1
	offset = -1
	while True:
		a = f.readline()
		if not a:
			#print "Error 2 in recover_tag: not found sha %s"%(sha)
			f.close()
			return -1
		a = a.strip().split(',')
		if a[0] == sha[2:]:
			which = a[1]
			offset = a[2]
			f.close()
			break
	main_path = '%s/rgit_tag_main%s'%(tag_store_path, which)
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
	
	after_deparse = deparse_tag(to_process, tag_store_path)
	
	return after_deparse
	
	
def tag_print_stat():
	print "this program deduplicate tags with three rules:\n"
	print "\tRule 1. Remove the same tag from different repository"
	print "\tRule 2. Remove structure info in tags, such as 'author', 'tagger' string in each tag"
	print '\tRule 3. Replace developer messages in tags with shorter string'
	print '\n'
	print "With all rules applied, it takes %d byte storage\n"%(dirSize('./tag_store/tag'))
	print "With rule 1, 2 applied, it takes %d byte storage\n"%(dirSize('./tag_store/tag_struc_dup'))
	print "With rule 1, 3 applied, it takes %d byte storage\n"%(dirSize('./tag_store/tag_developer_dup'))
	print "With rule 2, 3 applied, it takes %d byte storage\n"%(dirSize('./tag_store/tag_developer_dup'))
	print "Without any rules applied, it takes %d byte storage\n"%(dirSize('./tag_store/tag_all_dup'))
	
