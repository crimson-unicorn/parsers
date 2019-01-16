#!/usr/bin/python
from constants import *
from misc import hashgen

def parsecf(parser, db):
	"""Parse CamFlow trace data.
	"""
	raise NotImplementedError("currently we cannot parse CamFlow data in this module")

def parsedp(parser, db):
	"""Parse DARPA trace data.
	We store non-Event data (i.e., node) in a database.
	The database is a key-value store where:
	key - UUID of the data record
	value - integer hash of attributes

	Arguments:
	parser - ijson parser that feeds JSON objects
	db - database
	"""
	for cdmrec in parser:
		cdmrectype = cdmrec['datum'].keys()[0]
		cdmrecval = cdmrec['datum'][cdmrectype]

		if cdmrectype == CDM_TYPE_EVENT:
			pass

		else:
			cdmkey = cdmrecval['uuid']
			cdmval = valgendp(cdmrectype, cdmrecval)
			db.put(cdmkey, cdmval)

def valgendp(cdmrectype, cdmrecval):
	"""Generate a single value for a DARPA CDM record.

	Currently, only type information is used.

	Arguments:
	cdmrectype - CDM record type
	cdmrecval - CDM record

	Return:
	a single integer value of the record
	"""
	val = list()

	if cdmrectype == CDM_TYPE_SOCK:
		val.append('NET_FLOW_OBJECT')
	elif cdmrectype == CDM_TYPE_PIPE:
		val.append('UNNAMED_PIPE_OBJECT')
	elif cdmrectype == CDM_TYPE_MEMORY:
		val.append('MEMORY_OBJECT')
	elif cdmrectype == CDM_TYPE_HOST:
		val.append(cdmrecval['hostType'])
	else:
		val.append(cdmrecval['type'])

	return hashgen(val)
