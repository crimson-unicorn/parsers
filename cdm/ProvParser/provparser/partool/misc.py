#!/usr/bin/python
import xxhash

def sortfilenames(names):
	"""Tar file sometimes contains multiple gzip'ed files.
	These files need to be processed in order based on their names.

	For example,
	"cadets.json.1" needs to be processed before "cadets.json.2", and so on.

	Arguments:
	names - a list of file names that may or may not be in the desired order

	Return:
	a list of ordered file names
	"""

	return sorted(names, key=lambda item: (int(item.split('.')[-1]) 
		if item[-1].isdigit() else 0, item))

def hashgen(vlist):
	"""Generate a single hash value from a list.

	Arguments:
	vlist - a list of string values, which can be properties of a node/edge.

	Return:
	a single hashed integer value
	"""
	hasher = xxhash.xxh64()
	for v in vlist:
		hasher.update(v)
	return hasher.intdigest()