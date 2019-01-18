#!/usr/bin/python
from prepare import *
from misc import hashgen
import logging
import time
import tqdm

def parsecf(parser, ds, desc):
	"""Parse CamFlow trace data.
	"""
	raise NotImplementedError("currently we cannot parse CamFlow data in this module")

def parsedp(parser, ds, desc):
	"""Parse DARPA trace data.
	We store non-Event data (i.e., node) in a database.
	The database is a key-value store where:
	key - UUID of the data record
	value - integer hash of attributes

	Arguments:
	parser - ijson parser that feeds JSON objects
	ds - database system
	desc - description of the process
	"""
	description = '\x1b[6;30;43m[i]\x1b[0mProgress of File \x1b[6;30;42m{}\x1b[0m'.format(desc)
	procnum = 0
	if desc.split('.')[-1].isdigit():
		procnum = int(desc.split('.')[-1])
	pb = tqdm.tqdm(desc=description, mininterval=5.0, unit="recs", position=procnum)

	for cdmrec in parser:
		pb.update()

		cdmrectype = cdmrec['datum'].keys()[0]
		cdmrecval = cdmrec['datum'][cdmrectype]

		if cdmrectype == CDM_TYPE_EVENT:
			pass

		else:
			cdmkey = cdmrecval['uuid']
			cdmval = str(valgendp(cdmrectype, cdmrecval))
			ds.put(cdmkey, cdmval)
	pb.close()

def cgencf(parser, db, out):
	"""Generate CamFlow outputs from compressed file.
	"""
	raise NotImplementedError("currently we cannot generate CamFlow output data in this module")

def gencf(parser, i, dbs, ofile):
	"""Generate CamFlow outputs using a list of databases.
	"""
	raise NotImplementedError("currently we cannot generate CamFlow output data in this module")

def cgendp(parser, db, out):
	"""Generate DARPA outputs from compressed file.

	Arguments:
	parser - ijson parser that feeds JSON objects
	db - database
	out - output file object
	"""
	logging.basicConfig(filename='error.log',level=logging.DEBUG)

	for cdmrec in parser:
		cdmrectype = cdmrec['datum'].keys()[0]
		cdmrecval = cdmrec['datum'][cdmrectype]

		if cdmrectype == CDM_TYPE_EVENT:
			if 'type' not in cdmrecval:
				logging.debug('CDM_TYPE_EVENT: type is missing. Event UUID: ' + repr(cdmrecval['uuid']))
				continue
			else:
				edgetype = valgendp(cdmrectype, cdmrecval)

			if 'timestampNanos' not in cdmrecval:
				logging.debug('CDM_TYPE_EVENT: timestamp is missing. Event UUID: ' + repr(cdmrecval['uuid']))
				continue
			else:
				timestamp = cdmrecval['timestampNanos']

			srcUUID, dstUUID, bidirection = processevent(cdmrecval)

			if srcUUID == None or dstUUID == None:
				continue

			srcVal = db.get(srcUUID)
			if srcVal == None:
				logging.error('An unmatched srcUUID from edge (' + repr(cdmrecval['uuid']) + ') of type: ' + cdmrecval['type'])
				continue

			dstVal = db.get(dstUUID)
			if dstVal == None:
				logging.error('An unmatched dstUUID from edge (' + repr(cdmrecval['uuid']) + ') of type: ' + cdmrecval['type'])
				continue

			out.write(str(hashgen([srcUUID])) + '\t' \
					+ str(hashgen([dstUUID])) + '\t' \
					+ str(srcVal) + ':' + str(dstVal) \
					+ ':' + str(edgetype) \
					+ ':' + str(timestamp) + '\t' + '\n')

			if bidirection:
				out.write(str(hashgen([dstUUID])) + '\t' \
					+ str(hashgen([srcUUID])) + '\t' \
					+ str(dstVal) + ':' + str(srcVal) \
					+ ':' + str(edgetype) \
					+ ':' + str(timestamp) + '\t' + '\n')
		else:
			pass
	return

def gendp(parser, i, dbs, out):
	"""Generate DARPA outputs using a list of databases.

	Arguments:
	parser - ijson parser that feeds JSON objects
	i - the start index of the database list
	dbs - a list of database
	out - output file object
	"""
	logging.basicConfig(filename='error.log',level=logging.DEBUG)

	description = '\x1b[6;30;43m[i]\x1b[0m Progress of Generating Output'
	pb = tqdm.tqdm(desc=description, mininterval=5.0, unit="recs")
	for cdmrec in parser:
		pb.update()
		cdmrectype = cdmrec['datum'].keys()[0]
		cdmrecval = cdmrec['datum'][cdmrectype]

		if cdmrectype == CDM_TYPE_EVENT:
			if 'type' not in cdmrecval:
				logging.debug('CDM_TYPE_EVENT: type is missing. Event UUID: ' + repr(cdmrecval['uuid']))
				continue
			else:
				edgetype = valgendp(cdmrectype, cdmrecval)

			if 'timestampNanos' not in cdmrecval:
				logging.debug('CDM_TYPE_EVENT: timestamp is missing. Event UUID: ' + repr(cdmrecval['uuid']))
				continue
			else:
				timestamp = cdmrecval['timestampNanos']

			srcUUID, dstUUID, bidirection = processevent(cdmrecval)

			if srcUUID == None or dstUUID == None:
				continue

			srcVal = getfromdb(dbs, i, srcUUID)
			if srcVal == None:
				logging.error('An unmatched srcUUID from edge (' + repr(cdmrecval['uuid']) + ') of type: ' + cdmrecval['type'])
				continue

			dstVal = getfromdb(dbs, i, dstUUID)
			if dstVal == None:
				logging.error('An unmatched dstUUID from edge (' + repr(cdmrecval['uuid']) + ') of type: ' + cdmrecval['type'])
				continue

			out.write(str(hashgen([srcUUID])) + '\t' \
					+ str(hashgen([dstUUID])) + '\t' \
					+ str(srcVal) + ':' + str(dstVal) \
					+ ':' + str(edgetype) \
					+ ':' + str(timestamp) + '\t' + '\n')

			if bidirection:
				out.write(str(hashgen([dstUUID])) + '\t' \
					+ str(hashgen([srcUUID])) + '\t' \
					+ str(dstVal) + ':' + str(srcVal) \
					+ ':' + str(edgetype) \
					+ ':' + str(timestamp) + '\t' + '\n')
		else:
			pass
	pb.close()
	return

def getfromdb(dbs, i, uuid):
	"""Wrapper function to get value from an uuid.

	Arguments:
	dbs - a list of databases
	i - start index
	uuid - key
	"""
	val = None
	for ind in range(i, -1, -1):
		val = dbs[ind].get(uuid)
		if not val == None:
			break
	if val == None:
		for ind in range(len(dbs)-1, i, -1):
			val = dbs[ind].get(uuid)
			if not val == None:
				break
	return val

def processevent(cdmrecval):
	"""Process an event based on its type.

	Arguments:
	cdmrecval - value of the event
	"""
	srcUUID = None
	dstUUID = None
	bidirection = False
	edgetype = cdmrecval['type']
	# edges: Subject -> Object
	if edgetype == 'EVENT_CLOSE' or \
		edgetype == 'EVENT_CREATE_OBJECT' or \
		edgetype == 'EVENT_FORK' or \
		edgetype == 'EVENT_OPEN' or \
		edgetype == 'EVENT_LSEEK' or \
		edgetype == 'EVENT_CHANGE_PRINCIPAL' or \
		edgetype == 'EVENT_LOGIN' or \
		edgetype == 'EVENT_MODIFY_PROCESS' or \
		edgetype == 'EVENT_EXECUTE' or \
		edgetype == 'EVENT_CONNECT' or \
		edgetype == 'EVENT_SENDTO' or \
		edgetype == 'EVENT_WRITE' or \
		edgetype == 'EVENT_MODIFY_FILE_ATTRIBUTES' or \
		edgetype == 'EVENT_TRUNCATE' or \
		edgetype == 'EVENT_UNLINK' or \
		edgetype == 'EVENT_SIGNAL' or \
		edgetype == 'EVENT_MPROTECT' or \
		edgetype == 'EVENT_SENDMSG' or \
		edgetype == 'EVENT_BIND' or \
		edgetype == 'EVENT_WRITE_SOCKET_PARAMS' or \
		edgetype == 'EVENT_CREATE_THREAD' or \
		edgetype == 'EVENT_LOGOUT' or \
		edgetype == 'EVENT_CLONE' or \
		edgetype == 'EVENT_UNIT' or \
		edgetype == 'EVENT_LOGCLEAR' or \
		edgetype == 'EVENT_MOUNT' or \
		edgetype == 'EVENT_SERVICEINSTALL' or \
		edgetype == 'EVENT_STARTSERVICE' or \
		edgetype == 'EVENT_UMOUNT':
		(srcUUID, dstUUID) = subobjrel(cdmrecval)
	# edges: Subject <-> Object
	elif edgetype == 'EVENT_FCNTL' or \
			edgetype == 'EVENT_MMAP' or \
			edgetype == 'EVENT_OTHER':
		(srcUUID, dstUUID) = subobjrel(cdmrecval)
		bidirection = True
	# edges: Object -> Subject
	elif edgetype == 'EVENT_ACCEPT' or \
			edgetype == 'EVENT_READ' or \
			edgetype == 'EVENT_RECVFROM' or \
			edgetype == 'EVENT_RECVMSG' or \
			edgetype == 'EVENT_CHECK_FILE_ATTRIBUTES' or \
			edgetype == 'EVENT_READ_SOCKET_PARAMS' or \
			edgetype == 'EVENT_LOADLIBRARY' or \
			edgetype == 'EVENT_WAIT':
		(dstUUID, srcUUID) = subobjrel(cdmrecval) 
	# edges: Object1 -> Object2
	elif edgetype == 'EVENT_ADD_OBJECT_ATTRIBUTE' or \
			edgetype == 'EVENT_LINK' or \
			edgetype == 'EVENT_RENAME' or \
			edgetype == 'EVENT_FLOWS_TO' or \
			edgetype == 'EVENT_UPDATE' or \
			edgetype == 'EVENT_SHM' or \
			edgetype == 'EVENT_CORRELATION':
		(srcUUID, dstUUID) = objobjrel(cdmrecval)
	# edges non-directional
	elif edgetype == 'EVENT_EXIT' or \
			edgetype == 'EVENT_DUP' or \
			edgetype == 'EVENT_BOOT' or \
			edgetype == 'EVENT_BLIND':
		pass
	else:
		logging.error('CDM_TYPE_EVENT: event type is unexpected. Event UUID: ' + repr(cdmrecval['uuid']))

	return srcUUID, dstUUID, bidirection


def subobjrel(cdmrecval):
	"""Relations between a Subject and an Object.

	Arguments:
	cdmrecval - CDM JSON record

	Return:
	a tuple: (subjectUUID, objectUUID)
	"""
	subj = cdmrecval['subject']
	subjectUUID = None
	if type(subj).__name__ == 'NoneType':
		logging.debug("CDM_TYPE_EVENT: subject does not exist. Event UUID: " + repr(cdmrecval['uuid']))
	else:
		subjectUUID = subj[CDM_UUID]

	obj = cdmrecval['predicateObject']
	objectUUID = None
	if type(obj).__name__ == 'NoneType':
		logging.debug("CDM_TYPE_EVENT: object does not exist. Event UUID: " + repr(cdmrecval['uuid']))
	else:
		objectUUID = obj[CDM_UUID]

	return (subjectUUID, objectUUID)

def objobjrel(cdmrecval):
	"""Relations between an Object and another Object.

	Arguments:
	cdmrecval - CDM JSON record

	Return:
	a tuple: (objectUUID1, objectUUID2)
	"""
	obj1 = cdmrecval['predicateObject']
	objectUUID1 = None
	if type(obj1).__name__ == 'NoneType':
		logging.debug("CDM_TYPE_EVENT: object (1) does not exist. Event UUID: " + repr(cdmrecval['uuid']))
	else:
		objectUUID1 = obj1[CDM_UUID]

	obj2 = cdmrecval['predicateObject2']
	objectUUID2 = None
	if type(obj2).__name__ == 'NoneType':
		logging.debug("CDM_TYPE_EVENT: object (2) does not exist. Event UUID: " + repr(cdmrecval['uuid']))
	else:
		objectUUID2 = obj2[CDM_UUID]

	return (objectUUID1, objectUUID2)

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
