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
# CDM_UUID = 'com.bbn.tc.schema.avro.cdm18.UUID'
CDM_UUID = 'UUID'

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