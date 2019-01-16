#!/usr/bin/env python

import os, sys, argparse, json, hashlib
import tarfile as tf
import logging
import ijson.backends.yajl2_cffi as ijson

from sqlitedict import SqliteDict
from constants import *

parser = argparse.ArgumentParser(description='Convert CDM data to Unicorn')
parser.add_argument('--source', help='Input data folder', required=True)
parser.add_argument('--system', help='Tracing system used', choices=['cadets', 'clearscope', 'fivedirections', 'theia', 'trace'], required=True)
parser.add_argument('--format', help='Consume Avro/JSON serialised CDM (Only JSON is supported as of 01-04-19)', default='json',
                    choices=['avro', 'json'], required=False)
parser.add_argument('--save', help='Output data filename', required=True)
args = vars(parser.parse_args())

input_source = args['source']
system = args['system']
input_format = args['format']
output_locat = args['save']

next_id = 1

def print_all_cdm_record_type_in_json(fp):
	"""Util: print existing CDM record type information to the console.

	This routine only shows the types that are in the file @fp.
	The records must be in JSON format.
	There may exist other types not in the file.
	Check CDM specs for an exhaustive list.

	* For testing purposes. DO NOT USE THIS FUNCTION IN DEPLOYMENT.

	"""
	f = open(fp, 'r')
	types = set()
	for line in f:
		cdm_record = json.loads(line.strip())
		cdm_record_type = cdm_record['datum'].keys()[0]
		if cdm_record_type not in types:
			types.add(cdm_record_type)
			print(cdm_record_type)
	f.close()

# Test 1: What kind of CDM records do we have?
# print_all_cdm_record_type_in_json(input_source)

def read_field(object, format):
	"""Read a field in the record.

	"""
	if format == 'avro':
		raise NotImplementedError('CDM avro format is not supported as of 01-04-09.')
	elif format == 'json':
		if type(object) is int:
			return object
		return object.encode('utf-8')
	else:
		raise NotImplementedError('Unknown format as of 01-04-09.')

def idgen(hashstr):
	"""Generate UID for vertices.

	"""
	hasher = hashlib.md5()
	hasher.update(hashstr)
	return long(hasher.hexdigest()[:16], 32)

def labelgen(values):
	"""Create labels based on nodes' information @values.

	"""
	hasher = hashlib.md5()
	if isinstance(values, dict):
		hasher.update(values['type'])
		return hasher.hexdigest()
	elif isinstance(values, list):
		hasher.update(values[0])
		return hasher.hexdigest()
	else:
		raise TypeError('Unknown type of the input.')

def process_cdm_srcsink(record_value, input_format, nid):
	"""Process CDM record typed CDM_TYPE_SRCSINK.

	values = {'nid', 'type'}
	"""
	values = dict()
	values['nid'] = nid

	if 'type' not in record_value:
		raise KeyError('CDM_TYPE_SRCSINK: type is missing.')
	type_value = read_field(record_value['type'], input_format)
	values['type'] = type_value

	# Currently, no other type-specific or type-general values to be appended as of 01-04-19.
	if type_value == 'SRCSINK_IPC':
		pass
	# fivedirections
	elif type_value == 'SRCSINK_DATABASE':
		pass
	elif type_value == 'SRCSINK_PROCESS_MANAGEMENT':
		pass
	# trace
	elif type_value == 'SRCSINK_UNKNOWN':
		pass
	# clearscope
	elif type_value == 'SRCSINK_BINDER':
		pass
	elif type_value == 'SRCSINK_SERVICE_MANAGEMENT':
		pass
	elif type_value == 'SRCSINK_POSIX':
		pass
	elif type_value == 'SRCSINK_POWER_MANAGEMENT':
		pass
	elif type_value == 'SRCSINK_CONTENT_PROVIDER':
		pass
	elif type_value == 'SRCSINK_SYNC_FRAMEWORK':
		pass
	elif type_value == 'SRCSINK_PERMISSIONS':
		pass
	elif type_value == 'SRCSINK_ACTIVITY_MANAGEMENT':
		pass
	elif type_value == 'SRCSINK_BROADCAST_RECEIVER_MANAGEMENT':
		pass
	elif type_value == 'SRCSINK_INSTALLED_PACKAGES':
		pass
	elif type_value == 'SRCSINK_DISPLAY':
		pass
	elif type_value == 'SRCSINK_NETWORK_MANAGEMENT':
		pass
	elif type_value == 'SRCSINK_DEVICE_ADMIN':
		pass
	elif type_value == 'SRCSINK_DEVICE_USER':
		pass
	elif type_value == 'SRCSINK_WEB_BROWSER':
		pass
	else:
		pass
		# raise KeyError('CDM_TYPE_SRCSINK: type is undefined.')

	return values

def process_cdm_subject(record_value, input_format, nid):
	"""Process CDM record typed CDM_TYPE_SUBJECT.

	values = {'nid', 'type'}
	"""
	values = dict()
	values['nid'] = nid

	if 'type' not in record_value:
		raise KeyError('CDM_TYPE_SUBJECT: type is missing.')
	type_value = read_field(record_value['type'], input_format)
	values['type'] = type_value

	# Currently, no other type-specific or type-general values to be appended as of 01-04-19.
	if type_value == 'SUBJECT_PROCESS':
		pass
	# fivedirections
	elif type_value == 'SUBJECT_THREAD':
		pass
	# trace
	elif type_value == 'SUBJECT_UNIT':
		pass
	else:
		pass
		# raise KeyError('CDM_TYPE_SUBJECT: type is undefined.')

	return values

def process_cdm_file(record_value, input_format, nid):
	"""Process CDM record typed CDM_TYPE_FILE.

	values = {'nid', 'uuid', type'}
	"""
	values = dict()
	values['nid'] = nid
	values['uuid'] = record_value['uuid'] # we store UUID to detect identical records
	if 'type' not in record_value:
		raise KeyError('CDM_TYPE_FILE: type is missing.')
	type_value = read_field(record_value['type'], input_format)
	values['type'] = type_value

	# Currently, no other type-specific or type-general values to be appended as of 01-04-19.
	if type_value == 'FILE_OBJECT_UNIX_SOCKET':
		pass
	elif type_value == 'FILE_OBJECT_FILE':
		pass
	elif type_value == 'FILE_OBJECT_DIR':
		pass
	# five directions
	elif type_value == 'FILE_OBJECT_PEFILE':
		pass
	elif type_value == 'FILE_OBJECT_CHAR':
		pass
	elif type_value == 'FILE_OBJECT_BLOCK':
		pass
	elif type_value == 'FILE_OBJECT_NAMED_PIPE':
		pass
	# trace
	elif type_value == 'FILE_OBJECT_LINK':
		pass
	else:
		pass
		# raise KeyError('CDM_TYPE_FILE: type is undefined.')

	return values

def process_cdm_sock(record_value, input_format, nid):
	"""Process CDM record typed CDM_TYPE_SOCK.

	values = {'nid', 'type', 'localAddress', 'localPort', 'remoteAddress', 'remotePort'}
	"""
	values = dict()
	values['nid'] = nid
	# type must be a NET_FLOW_OBJECT
	values['type'] = 'NET_FLOW_OBJECT'

	if 'localAddress' not in record_value:
		logging.debug('CDM_TYPE_SOCK: localAddress is missing.')
	localAddress = read_field(record_value['localAddress'], input_format)
	values['localAddress'] = localAddress

	if 'localPort' not in record_value:
		logging.debug('CDM_TYPE_SOCK: localPort is missing.')
	localPort = read_field(record_value['localPort'], input_format)
	values['localPort'] = localPort

	if 'remoteAddress' not in record_value:
		logging.debug('CDM_TYPE_SOCK: remoteAddress is missing.')
	remoteAddress = read_field(record_value['remoteAddress'], input_format)
	values['remoteAddress'] = remoteAddress

	if 'remotePort' not in record_value:
		logging.debug('CDM_TYPE_SOCK: remotePort is missing.')
	remotePort = read_field(record_value['remotePort'], input_format)
	values['remotePort'] = remotePort

	# Currently, no other values to be appended as of 01-04-19.

	return values

def process_cdm_pipe(record_value, input_format, nid):
	"""Process CDM record type CDM_TYPE_PIPE.

	values = {'nid', 'type', 'sourceUUID', 'sinkUUID'}
	"""
	values = dict()
	values['nid'] = nid
	# type must be a UNNAMED_PIPE_OBJECT
	values['type'] = 'UNNAMED_PIPE_OBJECT'

	if 'sourceUUID' not in record_value:
		logging.debug('CDM_TYPE_PIPE: sourceUUID is missing.')
	sourceUUID = record_value['sourceUUID']
	if type(sourceUUID).__name__ == 'NoneType':
		# in trace data, NoneType exists
		values['sourceUUID'] = 'None'
	else:
		UUID = sourceUUID[CDM_UUID]
		values['sourceUUID'] = UUID

	if 'sinkUUID' not in record_value:
		logging.debug('CDM_TYPE_PIPE: sinkUUID is missing.')
	sinkUUID = record_value['sinkUUID']
	if type(sinkUUID).__name__ == 'NoneType':
		values['sinkUUID'] = 'None'
	else:
		UUID = sinkUUID[CDM_UUID]
		values['sinkUUID'] = UUID

	# Currently, no other values to be appended as of 01-04-19.

	return values

def process_cdm_event(record_value, input_format):
	"""Process CDM record typed CDM_TYPE_EVENT.

	value = {'uuid', type', 'srcUUID', 'dstUUID', 'bidirectional', 'timestamp'}

	Depending on the type of the events, we obtain srcUUID and dst UUID from different fields of the record.
	Refer to CDM19.avdl for semantics.
	"""
	values = dict()
	# We save 'uuid' of the edge in edge values for debugging purposes.
	values['uuid'] = record_value['uuid']

	if 'type' not in record_value:
		logging.debug('CDM_TYPE_EVENT: type is missing. Event UUID: ' + repr(record_value['uuid']))
	type_value = read_field(record_value['type'], input_format)
	values['type'] = type_value
	values['srcUUID'] = None
	values['dstUUID'] = None


	if 'timestampNanos' not in record_value:
		logging.debug('CDM_TYPE_EVENT: timestamp is missing. Event UUID: ' + repr(record_value['uuid']))
	timestamp = read_field(record_value['timestampNanos'], input_format)
	values['timestamp'] = timestamp

	# Currently, no other type-specific or type-general values to be appended as of 01-04-19.
	if type_value == 'EVENT_CLOSE':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_CLOSE: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_CLOSE: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_FCNTL':
		# Subject <-> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_FCNTL: subject does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_FCNTL: subject must exist.")
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_FCNTL: object does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_FCNTL: object must exist.")
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = True

	elif type_value == 'EVENT_CREATE_OBJECT':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_CREATE_OBJECT: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_CREATE_OBJECT: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_ACCEPT':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_ACCEPT: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_ACCEPT: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_FORK':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_FORK: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_FORK: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_OPEN':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_OPEN: subject does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_OPEN: subject must exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_OPEN: object does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_OPEN: object must exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_READ':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_READ: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_READ: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_LSEEK':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_LSEEK: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_LSEEK: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_CHANGE_PRINCIPAL':
		# TODO: 
		# Did not see CDM_TYPE_PRINCIPAL
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_CHANGE_PRINCIPAL: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_CHANGE_PRINCIPAL: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_LOGIN':
		# Subject -> Object
		# TODO:
		# The relation needs confirmation.
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_LOGIN: subject does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_LOGIN: subject must exist.")
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_LOGIN: object does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_LOGIN: object must exist.")
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_MODIFY_PROCESS':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_MODIFY_PROCESS: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_MODIFY_PROCESS: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_EXECUTE':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_EXECUTE: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:	
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_EXECUTE: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_MMAP':
		# Subject <-> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_MMAP: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_MMAP: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = True

	elif type_value == 'EVENT_CONNECT':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_CONNECT: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_CONNECT: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_SENDTO':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_SENDTO: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_SENDTO: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_RECVFROM':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_RECVFROM: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_RECVFROM: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_WRITE':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_WRITE: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_WRITE: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_ADD_OBJECT_ATTRIBUTE':
		# Object1 -> Object2
		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_ADD_OBJECT_ATTRIBUTE: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		object2 = record_value['predicateObject2']
		if type(object2).__name__ == 'NoneType':
			logging.debug("EVENT_ADD_OBJECT_ATTRIBUTE: object2 does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object2UUID = object2[CDM_UUID]
			values['dstUUID'] = object2UUID

		values['bidirectional'] = False
			
	elif type_value == 'EVENT_MODIFY_FILE_ATTRIBUTES':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_MODIFY_FILE_ATTRIBUTES: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_MODIFY_FILE_ATTRIBUTES: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_TRUNCATE':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_TRUNCATE: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_TRUNCATE: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_EXIT':
		# non-directional
		pass
	elif type_value == 'EVENT_LINK':
		# Object1 -> Object2
		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_LINK: object1 does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_LINK: object1 must exist.")
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		object2 = record_value['predicateObject2']
		if type(object2).__name__ == 'NoneType':
			logging.debug("EVENT_LINK: object2 does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_LINK: object2 must exist.")
		else:
			object2UUID = object2[CDM_UUID]
			values['dstUUID'] = object2UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_UNLINK':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_UNLINK: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_UNLINK: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_RECVMSG':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_RECVMSG: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_RECVMSG: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_RENAME':
		# Object1 -> Object2
		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_RENAME: object1 does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		object2 = record_value['predicateObject2']
		if type(object2).__name__ == 'NoneType':
			logging.debug("EVENT_RENAME: object2 does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object2UUID = object2[CDM_UUID]
			values['dstUUID'] = object2UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_SIGNAL':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_SIGNAL: subject does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_SIGNAL: subject must exist.")
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_SIGNAL: object does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_SIGNAL: object must exist.")
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_MPROTECT':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_MPROTECT: subject does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_MPROTECT: subject must exist.")
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_MPROTECT: object does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_MPROTECT: object must exist.")
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_SENDMSG':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_SENDMSG: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_SENDMSG: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_OTHER':
		# Subject <-> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_OTHER: subject does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_OTHER: subject must exist.")
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_OTHER: object does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_OTHER: object must exist.")
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = True
		
	elif type_value == 'EVENT_FLOWS_TO':
		# Object1 -> Object2
		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_FLOWS_TO: object1 does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		object2 = record_value['predicateObject2']
		if type(object2).__name__ == 'NoneType':
			logging.debug("EVENT_FLOWS_TO: object2 does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object2UUID = object2[CDM_UUID]
			values['dstUUID'] = object2UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_BIND':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_BIND: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_BIND: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	# clearscope
	elif type_value == 'EVENT_DUP':
		# non-directional
		pass
	elif type_value == 'EVENT_CHECK_FILE_ATTRIBUTES':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_CHECK_FILE_ATTRIBUTES: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_CHECK_FILE_ATTRIBUTES: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_WRITE_SOCKET_PARAMS':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_WRITE_SOCKET_PARAMS: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_WRITE_SOCKET_PARAMS: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_READ_SOCKET_PARAMS':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_READ_SOCKET_PARAMS: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_READ_SOCKET_PARAMS: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		values['bidirectional'] = False

	# fivedirections
	elif type_value == 'EVENT_UPDATE':
		# Object1 -> Object2
		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_UPDATE: object1 does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		object2 = record_value['predicateObject2']
		if type(object2).__name__ == 'NoneType':
			logging.debug("EVENT_UPDATE: object2 does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object2UUID = object2[CDM_UUID]
			values['dstUUID'] = object2UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_LOADLIBRARY':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_LOADLIBRARY: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_LOADLIBRARY: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_CREATE_THREAD':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_CREATE_THREAD: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_CREATE_THREAD: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_LOGOUT':
		# Subject -> Object
		# TODO:
		# The relation needs confirmation.
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_LOGOUT: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_LOGOUT: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	# theia
	elif type_value == 'EVENT_BOOT':
		# non-directional
		pass
	elif type_value == 'EVENT_CLONE':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_CLONE: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_CLONE: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_SHM':
		# Object1 -> Object2
		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_SHM: object1 does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		object2 = record_value['predicateObject2']
		if type(object2).__name__ == 'NoneType':
			logging.debug("EVENT_SHM: object2 does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object2UUID = object2[CDM_UUID]
			values['dstUUID'] = object2UUID

		values['bidirectional'] = False

	# trace
	elif type_value == 'EVENT_UNIT':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_UNIT: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_UNIT: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False
	# others
	elif type_value == 'EVENT_BLIND':
		pass

	elif type_value == 'EVENT_CORRELATION':
		# Object1 -> Object2
		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_CORRELATION: object1 does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		object2 = record_value['predicateObject2']
		if type(object2).__name__ == 'NoneType':
			logging.debug("EVENT_CORRELATION: object2 does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object2UUID = object2[CDM_UUID]
			values['dstUUID'] = object2UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_LOGCLEAR':
		# Subject -> Object
		# TODO:
		# The relation needs confirmation.
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_LOGCLEAR: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_LOGCLEAR: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_MOUNT':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_MOUNT: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_MOUNT: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_SERVICEINSTALL':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_SERVICEINSTALL: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_SERVICEINSTALL: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_STARTSERVICE':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_STARTSERVICE: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_STARTSERVICE: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_UMOUNT':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_UMOUNT: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_UMOUNT: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_WAIT':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			logging.debug("EVENT_WAIT: subject does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			logging.debug("EVENT_WAIT: object does not exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		values['bidirectional'] = False

	else:
		print(record_value)
		raise KeyError('CDM_TYPE_EVENT: type is undefined.')

	return values

def process_cdm_principal(record_value, input_format, nid):
	"""Process CDM record typed CDM_TYPE_PRINCIPAL.

	values = {'nid', 'type'}
	"""
	values = dict()
	values['nid'] = nid

	if 'type' not in record_value:
		raise KeyError('CDM_TYPE_PRINCIPAL: type is missing.')
	type_value = read_field(record_value['type'], input_format)
	values['type'] = type_value

	# Currently, no other type-specific or type-general values to be appended as of 01-04-19.
	if type_value == 'PRINCIPAL_LOCAL':
		pass
	elif type_value == 'PRINCIPAL_REMOTE':
		pass
	else:
		pass
		# raise KeyError('CDM_TYPE_PRINCIPAL: type is undefined.')

	return values

def process_cdm_host(record_value, input_format, nid):
	"""Process CDM record typed CDM_TYPE_HOST.

	values = {'nid', 'type'}
	"""
	values = dict()
	values['nid'] = nid

	if 'hostType' not in record_value:
		raise KeyError('CDM_TYPE_HOST: host type is missing.')
	type_value = read_field(record_value['hostType'], input_format)
	values['type'] = type_value
	
	# Currently, no other type-specific or type-general values to be appended as of 01-05-19.
	if type_value == 'HOST_DESKTOP':
		pass
	elif type_value == 'HOST_MOBILE':
		pass
	else:
		pass
		# raise KeyError('CDM_TYPE_HOST: host type is undefined.')

	return values

def process_cdm_memory(record_value, input_format, nid):
	"""Process CDM record typed CDM_TYPE_MEMORY.

	values = {'nid', 'type'}
	"""
	values = dict()
	values['nid'] = nid
	# type must be a MEMORY_OBJECT
	values['type'] = 'MEMORY_OBJECT'
	
	# Currently, no other type-specific or type-general values to be appended as of 01-05-19.
	return values

def generate_output(nodes, parser, fp):
	"""Output parsed file to the file path @fp.

	"""

	for cdm_record in parser:
		if input_format == 'avro':
			raise NotImplementedError('CDM avro format is not supported as of 01-04-09.')
		elif input_format == 'json':
			cdm_record_type = cdm_record['datum'].keys()[0]
			cdm_record_value = cdm_record['datum'][cdm_record_type]

		if cdm_record_type == CDM_TYPE_EVENT:
			# We don't really care about UUIDs of the edges.
			# But for debugging purposes, we put them in a set
			# to make sure we don't see two edges with the same UUIDs
			
			uuid = cdm_record_value['uuid']
			# if uuid in edgeUUID:
			#	logging.debug('CDM_TYPE_EVENT: UUID is not unique. UUID: ' + repr(uuid))
			# edgeUUID.add(uuid)

			values = process_cdm_event(cdm_record_value, input_format)

			# TODO:
			# Verify that: within a single tracing file, timestamps seem to be non-decreasing.
			# This means that appending edges to the list @edges preserve the order of the events.

			if values['srcUUID'] == None or values['dstUUID'] == None:
				continue

			if values['srcUUID'] not in nodes:
				# raise ValueError('An unmatched subject UUID from edge (' + str(edge['uuid']) + ') of type: ' + edge['type'] + '.')
				logging.debug('An unmatched source UUID from edge (' + repr(values['uuid']) + ') of type: ' + values['type'] + '.')
				continue
			else:
				srcNodeID = values['srcUUID']
				srcNode = nodes[srcNodeID]
				srcNodeLabel = labelgen(srcNode)

			if values['dstUUID'] not in nodes:
				# raise ValueError('An unmatched object1 UUID from edge (' + str(edge['uuid']) + ') of type: ' + edge['type'] + '.')
				logging.debug('An unmatched destination UUID from edge (' + repr(values['uuid']) + ') of type: ' + values['type'] + '.')
				continue
			else:
				dstNodeID = values['dstUUID']
				dstNode = nodes[dstNodeID]
				dstNodeLabel = labelgen(dstNode)

			f.write(str(srcNode['nid']) + '\t' + str(dstNode['nid']) + '\t' + srcNodeLabel + ":" + dstNodeLabel + ":" + labelgen(values) + ":" + str(values['timestamp']) + "\t" + "\n")
		
			if values['bidirectional']:
				f.write(str(dstNode['nid']) + '\t' + str(srcNode['nid']) + '\t' + dstNodeLabel + ":" + srcNodeLabel + ":" + labelgen(values) + ":" + str(values['timestamp']) + "\t" + "\n")

	f.close()

logging.basicConfig(filename=system + '-error.log',level=logging.DEBUG)

# uuid -> ['type', ...]
nodes = SqliteDict('./' + system + '-nodes.sqlite', autocommit=True)
# nodes = dict()

# for debugging: make sure no two edges have the same UUIDs
# edgeUUID = set()

if input_format == 'avro':
	raise NotImplementedError('CDM avro format is not supported as of 01-04-09.')
elif input_format == 'json':
	files = os.listdir(input_source)	# all the dataset files needede to be parsed together

# Start processing CDM records
for data_file in files:
	with tf.open(os.path.join(input_source, data_file), 'r:gz') as f:
		names = f.getnames()
		sorted_files = sorted(names, key=lambda item: (int(item.split('.')[-1]) if item[-1].isdigit() else int(0), item))

		for sorted_file in sorted_files:
			file_obj = f.extractfile(f.getmember(sorted_file))
			parser = ijson.common.items(ijson.parse(file_obj, multiple_values=True), '')
			for cdm_record in parser:
				if input_format == 'avro':
					raise ValueError('This is a streaming JSON parser implementation.')
				elif input_format == 'json':
					# cdm_record = json.loads(line.strip())
					cdm_record_type = cdm_record['datum'].keys()[0]
					cdm_record_value = cdm_record['datum'][cdm_record_type]
				
				if cdm_record_type == CDM_TYPE_SRCSINK:
					uuid = cdm_record_value['uuid']
					values = process_cdm_srcsink(cdm_record_value, input_format, next_id)

					if uuid in nodes:
						logging.debug('CDM_TYPE_SRCSINK: UUID is not unique. UUID: ' + repr(uuid))
					nodes[uuid] = values
					next_id += 1

				elif cdm_record_type == CDM_TYPE_SUBJECT:
					uuid = cdm_record_value['uuid']
					values = process_cdm_subject(cdm_record_value, input_format, next_id)

					if uuid in nodes:
						logging.debug('CDM_TYPE_SUBJECT: UUID is not unique. UUID: ' + repr(uuid))
					nodes[uuid] = values
					next_id += 1

				elif cdm_record_type == CDM_TYPE_FILE:
					uuid = cdm_record_value['uuid']
					values = process_cdm_file(cdm_record_value, input_format, next_id)
				
					if uuid in nodes:
						# clearscope contain identical records
						# check if the original UUIDs are the same
						node = nodes[uuid]
						nodeUUID = node['uuid']
						if nodeUUID == cdm_record_value['uuid']:
							continue	# if simply because identical records, we just drop the record
						else:	# if collision occurs
							logging.debug('CDM_TYPE_FILE: UUID is not unique. UUID: ' + repr(uuid))
					nodes[uuid] = values
					next_id += 1

				elif cdm_record_type == CDM_TYPE_SOCK:
					uuid = cdm_record_value['uuid']
					values = process_cdm_sock(cdm_record_value, input_format, next_id)

					if uuid in nodes:
						logging.debug('CDM_TYPE_SOCK: UUID is not unique. UUID: ' + repr(uuid))
					nodes[uuid] = values
					next_id += 1

				elif cdm_record_type == CDM_TYPE_PIPE:
					uuid = cdm_record_value['uuid']
					values = process_cdm_pipe(cdm_record_value, input_format, next_id)

					if uuid in nodes:
						logging.debug('CDM_TYPE_PIPE: UUID is not unique. UUID: ' + repr(uuid))
					nodes[uuid] = values
					next_id += 1

					# TODO:
					# Do we consider PIPE an edge or a vertex?

				elif cdm_record_type == CDM_TYPE_EVENT:
					pass

				elif cdm_record_type == CDM_TYPE_PRINCIPAL:
					uuid = cdm_record_value['uuid']
					values = process_cdm_principal(cdm_record_value, input_format, next_id)

					if uuid in nodes:
						logging.debug('CDM_TYPE_PRINCIPAL: UUID is not unique. UUID: ' + repr(uuid))
					nodes[uuid] = values
					next_id += 1
				# clearscope
				elif cdm_record_type == CDM_TYPE_TAG:
					pass
				# fivedirections
				elif cdm_record_type == CDM_TYPE_STARTMARKER:
					pass
				elif cdm_record_type == CDM_TYPE_TIMEMARKER:
					pass
				elif cdm_record_type == CDM_TYPE_HOST:
					uuid = cdm_record_value['uuid']
					values = process_cdm_host(cdm_record_value, input_format, next_id)

					if uuid in nodes:
						logging.debug('CDM_TYPE_HOST: UUID is not unique. UUID: ' + repr(uuid))
					nodes[uuid] = values
					next_id += 1

				elif cdm_record_type == CDM_TYPE_KEY:
					pass
				elif cdm_record_type == CDM_TYPE_MEMORY:
					uuid = cdm_record_value['uuid']
					values = process_cdm_memory(cdm_record_value, input_format, next_id)

					if uuid in nodes:
						logging.debug('CDM_TYPE_MEMORY: UUID is not unique. UUID: ' + repr(uuid))
					nodes[uuid] = values
					next_id += 1
				elif cdm_record_type == CDM_TYPE_ENDMARKER:
					pass
				# trace
				elif cdm_record_type == CDM_TYPE_UNITDEPENDENCY:
					# TODO
					# Do we consider this record a type of edge? If so, we should not pass.
					pass
				else:
					print(cdm_record)
					raise KeyError('CDM record type is undefined.')

		f.close()

# go through the files again to output edge lists
if system == 'cadets':
	first = ['cadets-e3-0.json', 'cadets-e3-1.json', 'cadets-e3-2.json']
	second = ['cadets-e3-1-0.json', 'cadets-e3-1-1.json', 'cadets-e3-1-2.json', 'cadets-e3-1-3.json', 'cadets-e3-1-4.json']
	third = ['cadets-e3-2-0.json', 'cadets-e3-2-1.json']

	out_first = '0-' + output_locat
	out_second = '1-' + output_locat
	out_third = '2-' + output_locat

	for data_file in first:
		with open(os.path.join(input_source, data_file), 'r') as fh:
			generate_output(nodes, fh, out_first)
		fh.close()
	for data_file in second:
		with open(os.path.join(input_source, data_file), 'r') as fh:
			generate_output(nodes, fh, out_second)
		fh.close()
	for data_file in third:
		with open(os.path.join(input_source, data_file), 'r') as fh:
			generate_output(nodes, fh, out_third)
		fh.close()
elif system == 'clearscope':
	first = ['clearscope-e3-0.json', 'clearscope-e3-1.json']

	out_first = '0-' + output_locat

	for data_file in first:
		with open(os.path.join(input_source, data_file), 'r') as fh:
			generate_output(nodes, fh, out_first)
		fh.close()
elif system == 'theia':
	# first = ['theia-e3-1-0.json', 'theia-e3-1-1.json', 'theia-e3-1-2.json', 'theia-e3-1-3.json', 'theia-e3-1-4.json', 'theia-e3-1-5.json', 'theia-e3-1-6.json', 'theia-e3-1-7.json', 'theia-e3-1-8.json', 'theia-e3-1-9.json']
	# second = ['theia-e3-3-0.json']
	# third = ['theia-e3-5-0.json']

	# out_first = '0-' + output_locat
	# out_second = '1-' + output_locat
	out_third = '2-' + output_locat

#	for data_file in first:
#		with open(os.path.join(input_source, data_file), 'r') as fh:
#			generate_output(nodes, fh, out_first)
#		fh.close()
#	for data_file in second:
#		with open(os.path.join(input_source, data_file), 'r') as fh:
#			generate_output(nodes, fh, out_second)
#		fh.close()
#	for data_file in third:
#		with open(os.path.join(input_source, data_file), 'r') as fh:
#			generate_output(nodes, fh, out_third)
#		fh.close()
	for data_file in files:
		with tf.open(os.path.join(input_source, data_file), 'r:gz') as f:
			names = f.getnames()
			sorted_files = sorted(names, key=lambda item: (int(item.split('.')[-1]) if item[-1].isdigit() else int(0), item))

			for sorted_file in sorted_files:
				file_obj = f.extractfile(f.getmember(sorted_file))
				parser = ijson.common.items(ijson.parse(file_obj, multiple_values=True), '')
				generate_output(nodes, parser, out_third)

elif system == 'fivedirections':
	third = ['fivedirections-e3-3.json']

	out_third = '1-' + output_locat

	for data_file in third:
		with open(os.path.join(input_source, data_file), 'r') as fh:
			generate_output(nodes, fh, out_third)
		fh.close()


# nodes.close()















