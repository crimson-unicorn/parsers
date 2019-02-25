#!/usr/bin/env python
from __future__ import print_function
import rocksdb

# CDM record type constants
CDM_TYPE_EVENT = 'com.bbn.tc.schema.avro.cdm18.Event'
CDM_TYPE_FILE = 'com.bbn.tc.schema.avro.cdm18.FileObject'
CDM_TYPE_SOCK = 'com.bbn.tc.schema.avro.cdm18.NetFlowObject'
CDM_TYPE_SUBJECT = 'com.bbn.tc.schema.avro.cdm18.Subject'
CDM_TYPE_SRCSINK = 'com.bbn.tc.schema.avro.cdm18.SrcSinkObject'
CDM_TYPE_PIPE = 'com.bbn.tc.schema.avro.cdm18.UnnamedPipeObject'
CDM_TYPE_PRINCIPAL = 'com.bbn.tc.schema.avro.cdm18.Principal'
CDM_TYPE_TAG = 'com.bbn.tc.schema.avro.cdm18.ProvenanceTagNode'
CDM_TYPE_STARTMARKER = 'com.bbn.tc.schema.avro.cdm18.StartMarker'
CDM_TYPE_TIMEMARKER = 'com.bbn.tc.schema.avro.cdm18.TimeMarker'
CDM_TYPE_HOST = 'com.bbn.tc.schema.avro.cdm18.Host'
CDM_TYPE_KEY = 'com.bbn.tc.schema.avro.cdm18.RegistryKeyObject'
CDM_TYPE_MEMORY = 'com.bbn.tc.schema.avro.cdm18.MemoryObject'
CDM_TYPE_ENDMARKER = 'com.bbn.tc.schema.avro.cdm18.EndMarker'
CDM_TYPE_UNITDEPENDENCY = 'com.bbn.tc.schema.avro.cdm18.UnitDependency'

# CDM UUID constant
CDM_UUID = 'com.bbn.tc.schema.avro.cdm18.UUID'

# CDM_19 record type constants
CDM19_TYPE_EVENT = 'com.bbn.tc.schema.avro.cdm19.Event'
CDM19_TYPE_FILE = 'com.bbn.tc.schema.avro.cdm19.FileObject'
CDM19_TYPE_SOCK = 'com.bbn.tc.schema.avro.cdm19.NetFlowObject'
CDM19_TYPE_SUBJECT = 'com.bbn.tc.schema.avro.cdm19.Subject'
CDM19_TYPE_SRCSINK = 'com.bbn.tc.schema.avro.cdm19.SrcSinkObject'
CDM19_TYPE_PIPE = 'com.bbn.tc.schema.avro.cdm19.UnnamedPipeObject'
CDM19_TYPE_PRINCIPAL = 'com.bbn.tc.schema.avro.cdm19.Principal'
CDM19_TYPE_TAG = 'com.bbn.tc.schema.avro.cdm19.ProvenanceTagNode'
CDM19_TYPE_STARTMARKER = 'com.bbn.tc.schema.avro.cdm19.StartMarker'
CDM19_TYPE_TIMEMARKER = 'com.bbn.tc.schema.avro.cdm19.TimeMarker'
CDM19_TYPE_HOST = 'com.bbn.tc.schema.avro.cdm19.Host'
CDM19_TYPE_KEY = 'com.bbn.tc.schema.avro.cdm19.RegistryKeyObject'
CDM19_TYPE_MEMORY = 'com.bbn.tc.schema.avro.cdm19.MemoryObject'
CDM19_TYPE_ENDMARKER = 'com.bbn.tc.schema.avro.cdm19.EndMarker'
CDM19_TYPE_UNITDEPENDENCY = 'com.bbn.tc.schema.avro.cdm19.UnitDependency'

# CDM_19 UUID constant
CDM19_UUID = 'com.bbn.tc.schema.avro.cdm19.UUID'

# Cadets-E2/FiveDirections record type constants
CD2_TYPE_EVENT = 'Event'
CD2_TYPE_FILE = 'FileObject'
CD2_TYPE_SOCK = 'NetFlowObject'
CD2_TYPE_SUBJECT = 'Subject'
CD2_TYPE_SRCSINK = 'SrcSinkObject'
CD2_TYPE_PIPE = 'UnnamedPipeObject'
CD2_TYPE_PRINCIPAL = 'Principal'
CD2_TYPE_TAG = 'ProvenanceTagNode'
CD2_TYPE_STARTMARKER = 'StartMarker'
CD2_TYPE_TIMEMARKER = 'TimeMarker'
CD2_TYPE_HOST = 'Host'
CD2_TYPE_KEY = 'RegistryKeyObject'
CD2_TYPE_MEMORY = 'MemoryObject'
CD2_TYPE_ENDMARKER = 'EndMarker'
CD2_TYPE_UNITDEPENDENCY = 'UnitDependency'
CD2_TYPE_IPC = 'IpcObject'

# Cadets-E2/FiveDirections UUID constant
CD2_UUID = 'UUID'

def initdb(fn):
	"""Initialize a database with a given name

	Arguments:
	fn - name of the database
	"""
	# create database
	opts = rocksdb.Options()
	opts.create_if_missing = True
	opts.max_open_files = 300000
	opts.write_buffer_size = 67108864
	opts.max_write_buffer_number = 3
	opts.target_file_size_base = 67108864

	opts.table_factory = rocksdb.BlockBasedTableFactory(
		filter_policy=rocksdb.BloomFilterPolicy(10),
		block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
		block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)))

	db = rocksdb.DB(fn + '.db', opts)
	print("\x1b[6;30;42m[+]\x1b[0m setting up database {}.db in current directory...".format(fn))
	return db