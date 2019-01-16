#!/usr/bin/python
from __future__ import print_function
from constants import *

def sanitycheckcf(parser, logfile):
	"""Sanity checks the following items from CamFlow trace data.

	"""
	raise NotImplementedError("we have confidence in CamFlow data. No checking is implemented yet.")

def sanitycheckdp(parser, logfile):
	"""Sanity checks the following items from DARPA trace data.

	1. CDM record type is recognizable.
	2. Subtype within each record exists.

	Arguments:
	parser - ijson parser that feeds JSON objects
	logfile - file object where non-compliance is recorded
	"""
	for cdmrec in parser:
		cdmrectype = cdmrec['datum'].keys()[0]

		if not cdmrectype == CDM_TYPE_EVENT and \
			not cdmrectype == CDM_TYPE_FILE and \
			not cdmrectype == CDM_TYPE_SOCK and \
			not cdmrectype == CDM_TYPE_SUBJECT and \
			not cdmrectype == CDM_TYPE_SRCSINK and \
			not cdmrectype == CDM_TYPE_PIPE and \
			not cdmrectype == CDM_TYPE_PRINCIPAL and \
			not cdmrectype == CDM_TYPE_TAG and \
			not cdmrectype == CDM_TYPE_STARTMARKER and \
			not cdmrectype == CDM_TYPE_TIMEMARKER and \
			not cdmrectype == CDM_TYPE_HOST and \
			not cdmrectype == CDM_TYPE_KEY and \
			not cdmrectype == CDM_TYPE_MEMORY and \
			not cdmrectype == CDM_TYPE_ENDMARKER and \
			not cdmrectype == CDM_TYPE_UNITDEPENDENCY:
			print("unregistered CDM type: {}".format(cdmrectype), file=logfile)

		if cdmrectype == CDM_TYPE_SOCK or \
			cdmrectype == CDM_TYPE_PIPE or \
			cdmrectype == CDM_TYPE_MEMORY:
			pass
		elif cdmrectype == CDM_TYPE_HOST:
			cdmrecval = cdmrec['datum'][cdmrectype]
			if 'hostType' not in cdmrecval:
				print("Subtype does not exist in this host record: {}".format(cdmrec), file=logfile)
		else:
			cdmrecval = cdmrec['datum'][cdmrectype]
			if 'type' not in cdmrecval:
				print("Subtype does not exist in this record: {}".format(cdmrec), file=logfile)

	return