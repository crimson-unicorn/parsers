#!/usr/bin/env python

import sys, argparse, json, hashlib

from constants import *

parser = argparse.ArgumentParser(description='Convert CDM data to Unicorn')
parser.add_argument('--source', help='Input data filename', required=True)
parser.add_argument('--format', help='Consume Avro/JSON serialised CDM (Only JSON is supported as of 01-04-19)', default='json',
                    choices=['avro', 'json'], required=False)
parser.add_argument('--save', help='Output data filename', required=True)
args = vars(parser.parse_args())

input_source = args['source']
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
	else:
		print(record_value)
		raise KeyError('CDM_TYPE_SRCSINK: type is undefined.')

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
		print(record_value)
		raise KeyError('CDM_TYPE_SUBJECT: type is undefined.')

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
		print(record_value)
		raise KeyError('CDM_TYPE_FILE: type is undefined.')

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
		raise KeyError('CDM_TYPE_SOCK: localAddress is missing.')
	localAddress = read_field(record_value['localAddress'], input_format)
	values['localAddress'] = localAddress

	if 'localPort' not in record_value:
		raise KeyError('CDM_TYPE_SOCK: localPort is missing.')
	localPort = read_field(record_value['localPort'], input_format)
	values['localPort'] = localPort

	if 'remoteAddress' not in record_value:
		raise KeyError('CDM_TYPE_SOCK: remoteAddress is missing.')
	remoteAddress = read_field(record_value['remoteAddress'], input_format)
	values['remoteAddress'] = remoteAddress

	if 'remotePort' not in record_value:
		raise KeyError('CDM_TYPE_SOCK: remotePort is missing.')
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
		raise KeyError('CDM_TYPE_PIPE: sourceUUID is missing.')
	sourceUUID = record_value['sourceUUID']
	if type(sourceUUID).__name__ == 'NoneType':
		# in trace data, NoneType exists
		values['sourceUUID'] = 'None'
	else:
		UUID = sourceUUID[CDM_UUID]
		values['sourceUUID'] = UUID

	if 'sinkUUID' not in record_value:
		raise KeyError('CDM_TYPE_PIPE: sinkUUID is missing.')
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
		raise KeyError('CDM_TYPE_EVENT: type is missing.')
	type_value = read_field(record_value['type'], input_format)
	values['type'] = type_value
	values['srcUUID'] = None
	values['dstUUID'] = None


	if 'timestampNanos' not in record_value:
		raise KeyError('CDM_TYPE_EVENT: timestamp is missing.')
	timestamp = read_field(record_value['timestampNanos'], input_format)
	values['timestamp'] = timestamp

	# Currently, no other type-specific or type-general values to be appended as of 01-04-19.
	if type_value == 'EVENT_CLOSE':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_CLOSE: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_CLOSE: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_FCNTL':
		# Subject <-> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			print("EVENT_FCNTL: subject does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_FCNTL: subject must exist.")
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			print("EVENT_FCNTL: object does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_FCNTL: object must exist.")
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = True

	elif type_value == 'EVENT_CREATE_OBJECT':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_CREATE_OBJECT: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_CREATE_OBJECT: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_ACCEPT':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_ACCEPT: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_ACCEPT: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['srcUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_FORK':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_FORK: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_FORK: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_OPEN':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			print("EVENT_OPEN: subject does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_OPEN: subject must exist. Event ID: " + repr(record_value['uuid']))
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			print("EVENT_OPEN: object does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_OPEN: object must exist. Event ID: " + repr(record_value['uuid']))
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_READ':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_READ: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_READ: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['srcUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_LSEEK':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_LSEEK: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_LSEEK: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_CHANGE_PRINCIPAL':
		# TODO: 
		# Did not see CDM_TYPE_PRINCIPAL
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_CHANGE_PRINCIPAL: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_CHANGE_PRINCIPAL: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_LOGIN':
		# Subject -> Object
		# TODO:
		# The relation needs confirmation.
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			print("EVENT_LOGIN: subject does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_LOGIN: subject must exist.")
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			print("EVENT_LOGIN: object does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_LOGIN: object must exist.")
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_MODIFY_PROCESS':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_MODIFY_PROCESS: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_MODIFY_PROCESS: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_EXECUTE':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_EXECUTE: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_EXECUTE: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_MMAP':
		# Subject <-> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_MMAP: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_MMAP: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = True

	elif type_value == 'EVENT_CONNECT':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_CONNECT: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_CONNECT: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_SENDTO':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_SENDTO: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_SENDTO: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_RECVFROM':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_RECVFROM: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_RECVFROM: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['srcUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_WRITE':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_WRITE: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_WRITE: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_ADD_OBJECT_ATTRIBUTE':
		# Object1 -> Object2
		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_ADD_OBJECT_ATTRIBUTE: object1 must exist.")
		object1UUID = object1[CDM_UUID]
		values['srcUUID'] = object1UUID

		object2 = record_value['predicateObject2']
		if type(object2).__name__ == 'NoneType':
			raise ValueError("EVENT_ADD_OBJECT_ATTRIBUTE: object2 must exist.")
		object2UUID = object2[CDM_UUID]
		values['dstUUID'] = object2UUID

		values['bidirectional'] = False
			
	elif type_value == 'EVENT_MODIFY_FILE_ATTRIBUTES':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_MODIFY_FILE_ATTRIBUTES: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_MODIFY_FILE_ATTRIBUTES: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_TRUNCATE':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_TRUNCATE: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_TRUNCATE: object must exist.")
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
			print("EVENT_LINK: object1 does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_LINK: object1 must exist.")
		else:
			object1UUID = object1[CDM_UUID]
			values['srcUUID'] = object1UUID

		object2 = record_value['predicateObject2']
		if type(object2).__name__ == 'NoneType':
			print("EVENT_LINK: object2 does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_LINK: object2 must exist.")
		else:
			object2UUID = object2[CDM_UUID]
			values['dstUUID'] = object2UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_UNLINK':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_UNLINK: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_UNLINK: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_RECVMSG':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_RECVMSG: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_RECVMSG: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['srcUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_RENAME':
		# Object1 -> Object2
		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_RENAME: object1 must exist.")
		object1UUID = object1[CDM_UUID]
		values['srcUUID'] = object1UUID

		object2 = record_value['predicateObject2']
		if type(object2).__name__ == 'NoneType':
			raise ValueError("EVENT_RENAME: object2 must exist.")
		object2UUID = object2[CDM_UUID]
		values['dstUUID'] = object2UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_SIGNAL':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			print("EVENT_SIGNAL: subject does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_SIGNAL: subject must exist.")
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			print("EVENT_SIGNAL: object does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_SIGNAL: object must exist.")
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_MPROTECT':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			print("EVENT_MPROTECT: subject does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_MPROTECT: subject must exist.")
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			print("EVENT_MPROTECT: object does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_MPROTECT: object must exist.")
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_SENDMSG':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_SENDMSG: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_SENDMSG: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_OTHER':
		# Subject <-> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			print("EVENT_OTHER: subject does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_OTHER: subject must exist.")
		else:
			subjectUUID = subject[CDM_UUID]
			values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			print("EVENT_OTHER: object does not exist. Event ID: " + repr(record_value['uuid']))
			# raise ValueError("EVENT_OTHER: object must exist.")
		else:
			object1UUID = object1[CDM_UUID]
			values['dstUUID'] = object1UUID

		values['bidirectional'] = True
		
	elif type_value == 'EVENT_FLOWS_TO':
		# Object1 -> Object2
		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_FLOWS_TO: object1 must exist.")
		object1UUID = object1[CDM_UUID]
		values['srcUUID'] = object1UUID

		object2 = record_value['predicateObject2']
		if type(object2).__name__ == 'NoneType':
			raise ValueError("EVENT_FLOWS_TO: object2 must exist.")
		object2UUID = object2[CDM_UUID]
		values['dstUUID'] = object2UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_BIND':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_BIND: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_BIND: object must exist.")
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
			raise ValueError("EVENT_CHECK_FILE_ATTRIBUTES: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_CHECK_FILE_ATTRIBUTES: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['srcUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_WRITE_SOCKET_PARAMS':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_WRITE_SOCKET_PARAMS: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_WRITE_SOCKET_PARAMS: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_READ_SOCKET_PARAMS':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_READ_SOCKET_PARAMS: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_READ_SOCKET_PARAMS: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['srcUUID'] = object1UUID

		values['bidirectional'] = False

	# fivedirections
	elif type_value == 'EVENT_UPDATE':
		# Object1 -> Object2
		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_UPDATE: object1 must exist.")
		object1UUID = object1[CDM_UUID]
		values['srcUUID'] = object1UUID

		object2 = record_value['predicateObject2']
		if type(object2).__name__ == 'NoneType':
			raise ValueError("EVENT_UPDATE: object2 must exist.")
		object2UUID = object2[CDM_UUID]
		values['dstUUID'] = object2UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_LOADLIBRARY':
		# Object -> Subject
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_LOADLIBRARY: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['dstUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_LOADLIBRARY: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['srcUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_CREATE_THREAD':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_CREATE_THREAD: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_CREATE_THREAD: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False
		
	elif type_value == 'EVENT_LOGOUT':
		# Subject -> Object
		# TODO:
		# The relation needs confirmation.
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_LOGOUT: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_LOGOUT: object must exist.")
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
			raise ValueError("EVENT_CLONE: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_CLONE: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

		values['bidirectional'] = False

	elif type_value == 'EVENT_SHM':
		# Object1 -> Object2
		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_SHM: object1 must exist.")
		object1UUID = object1[CDM_UUID]
		values['srcUUID'] = object1UUID

		object2 = record_value['predicateObject2']
		if type(object2).__name__ == 'NoneType':
			raise ValueError("EVENT_SHM: object2 must exist.")
		object2UUID = object2[CDM_UUID]
		values['dstUUID'] = object2UUID

		values['bidirectional'] = False

	# trace
	elif type_value == 'EVENT_UNIT':
		# Subject -> Object
		subject = record_value['subject']
		if type(subject).__name__ == 'NoneType':
			raise ValueError("EVENT_UNIT: subject must exist.")
		subjectUUID = subject[CDM_UUID]
		values['srcUUID'] = subjectUUID

		object1 = record_value['predicateObject']
		if type(object1).__name__ == 'NoneType':
			raise ValueError("EVENT_UNIT: object must exist.")
		object1UUID = object1[CDM_UUID]
		values['dstUUID'] = object1UUID

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
		print(record_value)
		raise KeyError('CDM_TYPE_PRINCIPAL: type is undefined.')

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
	else:
		print(record_value)
		raise KeyError('CDM_TYPE_HOST: host type is undefined.')

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

def generate_output(nodes, edges, fp):
	"""Output parsed file to the file path @fp.

	"""
	f = open(fp, 'w')
	for edge in edges:
		if edge['srcUUID'] == None or edge['dstUUID'] == None:
			continue

		if edge['srcUUID'] not in nodes:
			# raise ValueError('An unmatched subject UUID from edge (' + str(edge['uuid']) + ') of type: ' + edge['type'] + '.')
			print('An unmatched source UUID from edge (' + repr(edge['uuid']) + ') of type: ' + edge['type'] + '.')
			continue
		else:
			srcNodeID = edge['srcUUID']
			srcNode = nodes[srcNodeID]
			srcNodeLabel = labelgen(srcNode)

		if edge['dstUUID'] not in nodes:
			# raise ValueError('An unmatched object1 UUID from edge (' + str(edge['uuid']) + ') of type: ' + edge['type'] + '.')
			print('An unmatched destination UUID from edge (' + repr(edge['uuid']) + ') of type: ' + edge['type'] + '.')
			continue
		else:
			dstNodeID = edge['dstUUID']
			dstNode = nodes[dstNodeID]
			dstNodeLabel = labelgen(dstNode)

		f.write(str(srcNode['nid']) + '\t' + str(dstNode['nid']) + '\t' + srcNodeLabel + ":" + dstNodeLabel + ":" + labelgen(edge) + ":" + str(edge['timestamp']) + "\t" + "\n")
	
	f.close()

# uuid -> ['type', ...]
nodes = dict()

# ['type', ...]
edges = list()

# for debugging: make sure no two edges have the same UUIDs
edgeUUID = set()

if input_format == 'avro':
	raise NotImplementedError('CDM avro format is not supported as of 01-04-09.')
elif input_format == 'json':
	f = open(input_source, 'r')

# Start processing CDM records
for line in f:
	if input_format == 'avro':
		raise NotImplementedError('CDM avro format is not supported as of 01-04-09.')
	elif input_format == 'json':
		cdm_record = json.loads(line.strip())
		cdm_record_type = cdm_record['datum'].keys()[0]
		cdm_record_value = cdm_record['datum'][cdm_record_type]
		
	if cdm_record_type == CDM_TYPE_SRCSINK:
		uuid = cdm_record_value['uuid']
		values = process_cdm_srcsink(cdm_record_value, input_format, next_id)

		if uuid in nodes:
			raise ValueError('CDM_TYPE_SRCSINK: UUID is not unique.')
		nodes[uuid] = values
		next_id += 1

	elif cdm_record_type == CDM_TYPE_SUBJECT:
		uuid = cdm_record_value['uuid']
		values = process_cdm_subject(cdm_record_value, input_format, next_id)

		if uuid in nodes:
			raise ValueError('CDM_TYPE_SUBJECT: UUID is not unique.')
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
				print(uuid)
				raise ValueError('CDM_TYPE_FILE: UUID is not unique.')
		nodes[uuid] = values
		next_id += 1

	elif cdm_record_type == CDM_TYPE_SOCK:
		uuid = cdm_record_value['uuid']
		values = process_cdm_sock(cdm_record_value, input_format, next_id)

		if uuid in nodes:
			raise ValueError('CDM_TYPE_SOCK: UUID is not unique.')
		nodes[uuid] = values
		next_id += 1

	elif cdm_record_type == CDM_TYPE_PIPE:
		uuid = cdm_record_value['uuid']
		values = process_cdm_pipe(cdm_record_value, input_format, next_id)

		if uuid in nodes:
			raise ValueError('CDM_TYPE_PIPE: UUID is not unique.')
		nodes[uuid] = values
		next_id += 1

		# TODO:
		# Do we consider PIPE an edge or a vertex?

	elif cdm_record_type == CDM_TYPE_EVENT:
		# We don't really care about UUIDs of the edges.
		# But for debugging purposes, we put them in a set
		# to make sure we don't see two edges with the same UUIDs
		uuid = cdm_record_value['uuid']
		if uuid in edgeUUID:
			raise ValueError('CDM_TYPE_EVENT: UUID is not unique.')
		edgeUUID.add(uuid)

		values = process_cdm_event(cdm_record_value, input_format)
		# TODO:
		# Verify that: within a single tracing file, timestamps seem to be non-decreasing.
		# This means that appending edges to the list @edges preserve the order of the events.
		edges.append(values)

	elif cdm_record_type == CDM_TYPE_PRINCIPAL:
		uuid = cdm_record_value['uuid']
		values = process_cdm_principal(cdm_record_value, input_format, next_id)

		if uuid in nodes:
			raise ValueError('CDM_TYPE_PRINCIPAL: UUID is not unique.')
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
			raise ValueError('CDM_TYPE_HOST: UUID is not unique.')
		nodes[uuid] = values
		next_id += 1

	elif cdm_record_type == CDM_TYPE_KEY:
		pass
	elif cdm_record_type == CDM_TYPE_MEMORY:
		uuid = cdm_record_value['uuid']
		values = process_cdm_memory(cdm_record_value, input_format, next_id)

		if uuid in nodes:
			raise ValueError('CDM_TYPE_MEMORY: UUID is not unique.')
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

generate_output(nodes, edges, output_locat)

















