#coding: utf-8
import os
import zlib
import sys
sys.path.append('../')

NUM_PER_MAIN_TAG = 50000
def get_256suffix():
	#get hex form of 256 numbers
	suffix = []
	for i in xrange(0, 256):
		temp = hex(i)
		if len(temp)<4:
			suffix.append('0' + temp[2])
		else:
			suffix.append(temp[2:])
	return suffix
	
def int2msb_tag(a, is_delta):
	#little endian
	b = a & 0xf
	a = a >> 4
	if is_delta:
		b |= 0x40
	if not a:
		return chr(b)
	b |= 0x80
	s = chr(b)
	while True:
		b = a & 0x7f
		a = a >> 7
		if a:
			s += chr(b|0x80)
		else:
			s += chr(b)
			break
	return s
	
def read_main_tag(f):
	#arg: file to be read
	#ret: the compressed data and the base for delta obj
	
	is_delta = False
	temp = ord(f.read(1))
	if temp & 0x40:
		is_delta = True
	
	size = temp & 0xf
	if temp & 0x80:
		i = 0
		while True:
			a = ord(f.read(1))
			size |= (a & 0x7f) << (4 + 7*i)
			if a&0x80:
				i += 1
			else:
				break
	base = ''
	if is_delta:
		base = f.read(40)
	data = f.read(size)
	while len(data) < size:
		data += (f.read(size - len(data)))
	ret = [data, base]
	return ret

class TagStore:
	'''
	256 sub-directories in each tag_store_path
	256 index files in each sub-directories
	one to_write file in each sub-directories which indicate where to write next
	many main files to store tags
	
	structure of to_write: which,offset,already (which main file it should write next,the size already wrote in the main file,how many tags it had write in the main file)
	
	each line structure of index files: sha[4:],which,offset\n (sha except first 4 byte,which main file it stays,where it is in the main file)
	'''
	def __init__(self, tag_store_path):
		self.base_path = os.path.abspath(tag_store_path)
		self.tag_init()
	
	def find_sha(self, sha):
		#return (which mainfile it is, the offset it begins)
		p = os.path.join(self.base_path, sha[0:2], 'index%s'%(sha[2:4]))
		with open(p, 'rb') as f:
			while True:
				a = f.readline()
				if not a:
					break
				if a[0:36] == sha[4:]:
					a = a.strip().split(',')
					return (int(a[1]), int(a[2]))
		return (-1, -1)
	
	def absorb(self, triples):
		suffix = get_256suffix()
		write_list = {}#store the sha_which_offset to write in main file, and the which should be monotonic increasing
		to_writes = {}
		for i in suffix:
			write_list[i] = []
			with open(os.path.join(self.base_path, i, 'to_write'), 'rb') as f:
				a = f.readline().split(',')
				to_writes[i] = [int(a[0]), int(a[1]), int(a[2])]#which, ofset, already
		for i in xrange(0, len(triples)):
			sha, base, data = triples[i]
			if self.find_sha(sha)[0] != -1:
				continue
			if base == '':#it is not delta
				triples[i][2] = int2msb_tag(len(data), False) + data
			else:#it is delta
				triples[i][2] = int2msb_tag(len(data), True) + base + data
			
			which, offset, already = to_writes[sha[0:2]]
			
			write_list[sha[0:2]].append((i, sha, which, offset))
			already += 1
			offset += len(triples[i][2])
			
			if already == NUM_PER_MAIN_TAG:
				which += 1
				already = 0
				offset = 0
			to_writes[sha[0:2]] = [which, offset, already]
		for i in suffix:
			last_which = -1
			idxs = {}
			if not write_list[i]:
				continue
			main_file = open(os.path.join(self.base_path, i, 'rgit_tag_main%d'%(write_list[i][0][2])), 'ab')
			for posi, sha, which, offset in write_list[i]:
				if last_which != which:
					last_which = which
					main_file.close()
					main_file = open(os.path.join(self.base_path, i, 'rgit_tag_main%d'%(which)), 'ab')
				if sha[2:4] not in idxs:
					idxs[sha[2:4]] = open(os.path.join(self.base_path, i, 'index%s'%(sha[2:4])), 'ab')
				idxs[sha[2:4]].write(','.join([sha[4:], str(which), str(offset)+'\n']))
				main_file.write(triples[posi][2])
			main_file.close()
			for f in idxs.values():
				f.close()
			to_write_path = os.path.join(self.base_path, i, 'to_write')
			f = open(to_write_path, 'w')
			f.write('%d,%d,%d'%(to_writes[i][0], to_writes[i][1], to_writes[i][2]))
			f.close()
		return 1
		
	def tag_init(self):
		suffix = get_256suffix()
		sub_dirs = os.listdir(self.base_path)
		for i in suffix:
			dir_path = os.path.join(self.base_path, i)
			if i not in sub_dirs:
				os.mkdir(dir_path)
			files = os.listdir(dir_path)
			if 'to_write' not in files:
				w = open(os.path.join(dir_path, 'to_write'), 'wb')
				w.write('0,0,0')#which, num_already_store, offset
				w.close()
			if 'rgit_tag_main0' not in files:
				w = open(os.path.join(dir_path, 'rgit_tag_main0'), 'wb')
				w.close()
			for j in suffix:
				if 'index' + j not in files:
					w = open(os.path.join(dir_path, 'index' + j), 'wb')
					w.close()
					
	def cat_tag(self, sha):
		'''
		return '': failed
		'''
		which, offset = self.find_sha(sha)
		if which == -1:
			return -1
		f = open(os.path.join(self.base_path, sha[0:2], 'rgit_tag_main%d'%(which)), 'rb')
		f.seek(offset)
		compressed, base = read_main_tag(f)
		f.close()
		if not base:#not delta
			return zlib.decompress(compressed)
		else:
			father_data = self.cat_tag(base)
			string = zlib.decompress(compressed)
			tar_data = ''
			idx = 0
			tail_idx = len(string)
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
					
					tar_data += father_data[offset : offset + copy_len]
				else:#insert
					tar_data += string[idx:idx+a]
					idx += a
			return tar_data