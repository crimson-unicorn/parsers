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
	return long(hasher.hexdigest()[:16], 16) # make sure it is only 32 bits

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

def process_cdm_srcsink(record_value, input_format):
	"""Process CDM record typed CDM_TYPE_SRCSINK.

	values = {'type'}
	"""
	values = dict()
	if 'type' not in record_value:
		raise KeyError('CDM_TYPE_SRCSINK: type is missing.')
	type_value = read_field(record_value['type'], input_format)
	values['type'] = type_value

	# Currently, no other type-specific or type-general values to be appended as of 01-04-19.
	if type_value == 'SRCSINK_IPC':
		pass
	else:
		print(record_value)
		raise KeyError('CDM_TYPE_SUBJECT: type is undefined.')

	return values

def process_cdm_subject(record_value, input_format):
	"""Process CDM record typed CDM_TYPE_SUBJECT.

	values = {'type'}
	"""
	values = dict()
	if 'type' not in record_value:
		raise KeyError('CDM_TYPE_SUBJECT: type is missing.')
	type_value = read_field(record_value['type'], input_format)
	values['type'] = type_value

	# Currently, no other type-specific or type-general values to be appended as of 01-04-19.
	if type_value == 'SUBJECT_PROCESS':
		pass
	else:
		print(record_value)
		raise KeyError('CDM_TYPE_SUBJECT: type is undefined.')

	return values

def process_cdm_file(record_value, input_format):
	"""Process CDM record typed CDM_TYPE_FILE.

	values = {'type'}
	"""
	values = dict()
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
	else:
		print(record_value)
		raise KeyError('CDM_TYPE_SUBJECT: type is undefined.')

	return values

def process_cdm_sock(record_value, input_format):
	"""Process CDM record typed CDM_TYPE_SOCK.

	values = ['type', 'localAddress', 'localPort', 'remoteAddress', 'remotePort']
	"""
	values = list()
	# type must be a NET_FLOW_OBJECT
	values.append('NET_FLOW_OBJECT')

	if 'localAddress' not in record_value:
		raise KeyError('CDM_TYPE_SOCK: localAddress is missing.')
	localAddress = read_field(record_value['localAddress'], input_format)
	values.append(localAddress)

	if 'localPort' not in record_value:
		raise KeyError('CDM_TYPE_SOCK: localPort is missing.')
	localPort = read_field(record_value['localPort'], input_format)
	values.append(localPort)

	if 'remoteAddress' not in record_value:
		raise KeyError('CDM_TYPE_SOCK: remoteAddress is missing.')
	remoteAddress = read_field(record_value['remoteAddress'], input_format)
	values.append(remoteAddress)

	if 'remotePort' not in record_value:
		raise KeyError('CDM_TYPE_SOCK: remotePort is missing.')
	remotePort = read_field(record_value['remotePort'], input_format)
	values.append(remotePort)

	# Currently, no other values to be appended as of 01-04-19.

	return values

def process_cdm_pipe(record_value, input_format):
	"""Process CDM record type CDM_TYPE_PIPE.

	values = ['type', 'sourceUUID', 'sinkUUID']
	"""
	values = list()
	# type must be a UNNAMED_PIPE_OBJECT
	values.append('UNNAMED_PIPE_OBJECT')

	if 'sourceUUID' not in record_value:
		raise KeyError('CDM_TYPE_PIPE: sourceUUID is missing.')
	sourceUUID = record_value['sourceUUID']
	UUID = read_field(sourceUUID[CDM_UUID], input_format)
	values.append(idgen(UUID))

	if 'sinkUUID' not in record_value:
		raise KeyError('CDM_TYPE_PIPE: sinkUUID is missing.')
	sinkUUID = record_value['sinkUUID']
	UUID = read_field(sinkUUID[CDM_UUID], input_format)
	values.append(idgen(UUID))

	# Currently, no other values to be appended as of 01-04-19.

	return values

def process_cdm_event(record_value, input_format):
	"""Process CDM record typed CDM_TYPE_EVENT.

	value = {'uuid', type', 'subjectUUID', 'object1UUID', 'object2UUID', 'timestamp'}
	"""
	values = dict()
	# We save 'uuid' of the edge in edge values for debugging purposes.
	values['uuid'] = idgen(read_field(record_value['uuid'], input_format))

	if 'type' not in record_value:
		raise KeyError('CDM_TYPE_EVENT: type is missing.')
	type_value = read_field(record_value['type'], input_format)
	values['type'] = type_value

	subject = record_value['subject']
	if type(subject).__name__ == 'NoneType':
		pass
	else:
		subjectUUID = read_field(subject[CDM_UUID], input_format)
		values['subjectUUID'] = idgen(subjectUUID)

	object1 = record_value['predicateObject']
	if type(object1).__name__ == 'NoneType':
		pass
	else:
		object1UUID = read_field(object1[CDM_UUID], input_format)
		values['object1UUID'] = idgen(object1UUID)

	object2 = record_value['predicateObject2']
	if type(object2).__name__ == 'NoneType':
		pass
	else:
		object2UUID = read_field(object2[CDM_UUID], input_format)
		values['object2UUID'] = idgen(object2UUID)

	timestamp = read_field(record_value['timestampNanos'], input_format)
	values['timestamp'] = timestamp

	# Currently, no other type-specific or type-general values to be appended as of 01-04-19.
	if type_value == 'EVENT_CLOSE':
		pass
	elif type_value == 'EVENT_FCNTL':
		pass
	elif type_value == 'EVENT_CREATE_OBJECT':
		pass
	elif type_value == 'EVENT_ACCEPT':
		pass
	elif type_value == 'EVENT_FORK':
		pass
	elif type_value == 'EVENT_OPEN':
		pass
	elif type_value == 'EVENT_READ':
		pass
	elif type_value == 'EVENT_LSEEK':
		pass
	elif type_value == 'EVENT_CHANGE_PRINCIPAL':
		# TODO: 
		# Did not see CDM_TYPE_PRINCIPAL
		pass
	elif type_value == 'EVENT_LOGIN':
		pass
	elif type_value == 'EVENT_MODIFY_PROCESS':
		pass
	elif type_value == 'EVENT_EXECUTE':
		pass
	elif type_value == 'EVENT_MMAP':
		pass
	elif type_value == 'EVENT_CONNECT':
		pass
	elif type_value == 'EVENT_SENDTO':
		pass
	elif type_value == 'EVENT_RECVFROM':
		pass
	elif type_value == 'EVENT_WRITE':
		pass
	elif type_value == 'EVENT_ADD_OBJECT_ATTRIBUTE':
		pass
	elif type_value == 'EVENT_MODIFY_FILE_ATTRIBUTES':
		pass
	elif type_value == 'EVENT_TRUNCATE':
		pass
	elif type_value == 'EVENT_EXIT':
		pass
	elif type_value == 'EVENT_LINK':
		pass
	elif type_value == 'EVENT_UNLINK':
		pass
	elif type_value == 'EVENT_RECVMSG':
		pass
	elif type_value == 'EVENT_RENAME':
		pass
	elif type_value == 'EVENT_SIGNAL':
		pass
	elif type_value == 'EVENT_MPROTECT':
		pass
	elif type_value == 'EVENT_SENDMSG':
		pass
	elif type_value == 'EVENT_OTHER':
		pass
	elif type_value == 'EVENT_FLOWS_TO':
		pass
	elif type_value == 'EVENT_BIND':
		pass
	else:
		print(record_value)
		raise KeyError('CDM_TYPE_EVENT: type is undefined.')

	return values

def process_cdm_principal(record_value, input_format):
	"""Process CDM record typed CDM_TYPE_PRINCIPAL.

	values = {'type'}
	"""
	values = dict()
	if 'type' not in record_value:
		raise KeyError('CDM_TYPE_PRINCIPAL: type is missing.')
	type_value = read_field(record_value['type'], input_format)
	values['type'] = type_value

	# Currently, no other type-specific or type-general values to be appended as of 01-04-19.
	if type_value == 'PRINCIPAL_LOCAL':
		pass
	else:
		print(record_value)
		raise KeyError('CDM_TYPE_PRINCIPAL: type is undefined.')

	return values

def generate_output(nodes, edges, fp):
	"""Output parsed file to the file path @fp.

	"""
	f = open(fp, 'w')
	for edge in edges:
		if 'subjectUUID' not in edge:
			print('This edge (' + str(edge['uuid']) + ') of type: ' + edge['type'] + ' does not have Subject node.')
			continue
		if 'object1UUID' not in edge:
			print('This edge (' + str(edge['uuid']) + ') of type: ' + edge['type'] + ' does not have Object node.')
			continue			# Note that if the edge does not have object1UUID
								# it shall not have object2UUID.
		# In the above two cases, we will dimiss the edge.
		if edge['subjectUUID'] not in nodes:
			# raise ValueError('An unmatched subject UUID from edge (' + str(edge['uuid']) + ') of type: ' + edge['type'] + '.')
			print('An unmatched subject UUID from edge (' + str(edge['uuid']) + ') of type: ' + edge['type'] + '.')
			continue
		else:
			srcNodeID = edge['subjectUUID']
			srcNode = nodes[srcNodeID]
			srcNodeLabel = labelgen(srcNode)

		if edge['object1UUID'] not in nodes:
			# raise ValueError('An unmatched object1 UUID from edge (' + str(edge['uuid']) + ') of type: ' + edge['type'] + '.')
			print('An unmatched object1 UUID from edge (' + str(edge['uuid']) + ') of type: ' + edge['type'] + '.')
			continue
		else:
			dstNodeID = edge['object1UUID']
			dstNode = nodes[dstNodeID]
			dstNodeLabel = labelgen(dstNode)

		f.write(str(srcNodeID) + '\t' + str(dstNodeID) + '\t' + srcNodeLabel + ":" + dstNodeLabel + ":" + labelgen(edge) + ":" + str(edge['timestamp']) + "\t" + "\n")


		if 'object2UUID' in edge:
			if edge['object2UUID'] not in nodes:
				# raise ValueError('An unmatched object2 UUID from edge (' + str(edge['uuid']) + ') of type: ' + edge['type'] + '.')
				print('An unmatched object2 UUID from edge (' + str(edge['uuid']) + ') of type: ' + edge['type'] + '.')
				continue
			else:
				dstNode2ID = edge['object2UUID']
				dstNode2 = nodes[dstNode2ID]
				dstNode2Label = labelgen(dstNode2)

			f.write(str(srcNodeID) + '\t' + str(dstNode2ID) + '\t' + srcNodeLabel + ":" + dstNode2Label + ":" + labelgen(edge) + ":" + str(edge['timestamp']) + "\t" + "\n")
	
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
		uuid = idgen(read_field(cdm_record_value['uuid'], input_format))
		values = process_cdm_srcsink(cdm_record_value, input_format)

		if uuid in nodes:
			raise ValueError('CDM_TYPE_SRCSINK: UUID is not unique.')
		nodes[uuid] = values

	elif cdm_record_type == CDM_TYPE_SUBJECT:
		uuid = idgen(read_field(cdm_record_value['uuid'], input_format))
		values = process_cdm_subject(cdm_record_value, input_format)

		if uuid in nodes:
			raise ValueError('CDM_TYPE_SUBJECT: UUID is not unique.')
		nodes[uuid] = values

	elif cdm_record_type == CDM_TYPE_FILE:
		uuid = idgen(read_field(cdm_record_value['uuid'], input_format))
		values = process_cdm_file(cdm_record_value, input_format)

		if uuid in nodes:
			raise ValueError('CDM_TYPE_FILE: UUID is not unique.')
		nodes[uuid] = values

	elif cdm_record_type == CDM_TYPE_SOCK:
		uuid = idgen(read_field(cdm_record_value['uuid'], input_format))
		values = process_cdm_sock(cdm_record_value, input_format)

		if uuid in nodes:
			raise ValueError('CDM_TYPE_SOCK: UUID is not unique.')
		nodes[uuid] = values

	elif cdm_record_type == CDM_TYPE_PIPE:
		uuid = idgen(read_field(cdm_record_value['uuid'], input_format))
		values = process_cdm_pipe(cdm_record_value, input_format)

		if uuid in nodes:
			raise ValueError('CDM_TYPE_PIPE: UUID is not unique.')
		nodes[uuid] = values

		# TODO:
		# Do we consider PIPE an edge or a vertex?

	elif cdm_record_type == CDM_TYPE_EVENT:
		# We don't really care about UUIDs of the edges.
		# But for debugging purposes, we put them in a set
		# to make sure we don't see two edges with the same UUIDs
		uuid = idgen(read_field(cdm_record_value['uuid'], input_format))
		if uuid in edgeUUID:
			print(cdm_record_value)
			raise ValueError('CDM_TYPE_EVENT: UUID is not unique.')
		edgeUUID.add(uuid)

		values = process_cdm_event(cdm_record_value, input_format)
		# TODO:
		# Verify that: within a single tracing file, timestamps seem to be non-decreasing.
		# This means that appending edges to the list @edges preserve the order of the events.
		edges.append(values)

	elif cdm_record_type == CDM_TYPE_PRINCIPAL:
		uuid = idgen(read_field(cdm_record_value['uuid'], input_format))
		values = process_cdm_principal(cdm_record_value, input_format)

		if uuid in nodes:
			raise ValueError('CDM_TYPE_PRINCIPAL: UUID is not unique.')
		nodes[uuid] = values

	else:
		print(cdm_record)
		raise KeyError('CDM record type is undefined.')

f.close()

generate_output(nodes, edges, output_locat)

















