#!/usr/bin/python
from prepare import *
from misc import hashgen
import logging
import time
import tqdm

def parsecf(parser, ds, desc):
	"""Parse CamFlow trace data.

	Arguments:
	parser - ijson parser that feeds JSON objects
	ds - database system
	desc - description of the process
	"""
	logging.basicConfig(filename='parse-error-camflow-' + desc + '.log', level=logging.DEBUG)

	description = '\x1b[6;30;43m[i]\x1b[0mProgress of File \x1b[6;30;42m{}\x1b[0m'.format(desc)	
	procnum = 0
	if desc.split('.')[-1].isdigit():
		procnum = int(desc.split('.')[-1])
	pb = tqdm.tqdm(desc=description, mininterval=5.0, unit="recs", position=procnum)

	for cfrec in parser:
		pb.update()

		if "activity" in cfrec:
			activity = cfrec["activity"]
			for uid in activity:
				if "prov:type" not in activity[uid]:
					logging.debug("Parsing ERROR - Activity (node) record without type. UUID: %s", uid)
				else:
					cfkey = uid
					cfval = str(valgencf(activity[uid]))
					ds.put(cfkey, cfval)

		if "entity" in cfrec:
			entity = cfrec["entity"]
			for uid in entity:
				if "prov:type" not in entity[uid]:
					logging.debug("Parsing ERROR - Entity (node) record without type. UUID: %s", uid)
				else:
					cfkey = uid
					cfval = str(valgencf(entity[uid]))
					ds.put(cfkey, cfval)

	pb.close()


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
	logging.basicConfig(filename='parse-error-' + desc + '.log', level=logging.DEBUG)

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
			try:
				cdmkey = cdmrecval['uuid'].encode('utf-8')
				cdmval = str(valgendp(cdmrectype, cdmrecval))
				ds.put(cdmkey, cdmval)
			except:
				logging.debug('Parsing ERROR - Record without UUID: ' + repr(cdmrecval))
				
	pb.close()

def parsecd(parser, ds, desc):
	"""Parse Cadets E2/FiveDirections trace data.
	We store non-Event data (i.e., node) in a database.
	The database is a key-value store where:
	key - UUID of the data record
	value - integer hash of attributes

	Note: Only one subset of FiveDirections dataset uses this function. Other subsets use regular DARPA functions.

	Arguments:
	parser - ijson parser that feeds JSON objects
	ds - database system
	desc - description of the process
	"""
	logging.basicConfig(filename='parse-error-' + desc + '.log', level=logging.DEBUG)

	description = '\x1b[6;30;43m[i]\x1b[0mProgress of File \x1b[6;30;42m{}\x1b[0m'.format(desc)
	procnum = 0
	if desc.split('.')[-1].isdigit():
		procnum = int(desc.split('.')[-1])
	pb = tqdm.tqdm(desc=description, mininterval=5.0, unit="recs", position=procnum)

	for cdmrec in parser:
		pb.update()

		cdmrectype = cdmrec['datum'].keys()[0]
		cdmrecval = cdmrec['datum'][cdmrectype]

		if cdmrectype == CD2_TYPE_EVENT:
			pass

		else:
			try:
				cdmkey = cdmrecval['uuid'].encode('utf-8')
				cdmval = str(valgendp(cdmrectype, cdmrecval))
				ds.put(cdmkey, cdmval)
			except:
				logging.debug('Parsing ERROR - Record without UUID: ' + repr(cdmrecval))
	pb.close()

def cgencf(parser, db, out):
	"""Generate CamFlow outputs from compressed/single file.
	"""
	logging.basicConfig(filename='error.log',level=logging.DEBUG)

	description = '\x1b[6;30;43m[i]\x1b[0m Progress of Generating Output'
	pb = tqdm.tqdm(desc=description, mininterval=5.0, unit="recs")
	
	for cfrec in parser:
		pb.update()

		if "used" in cfrec:
			used = cfrec["used"]
			for uid in used:
				if "prov:type" not in used[uid]:
					logging.debug("Edge (used) record without type. UUID: %s", uid)
					continue
				else:
					edgetype = valgencf(used[uid])

				if "cf:id" not in used[uid]:	# Can be used as timestamp
					logging.debug("Edge (used) record without timestamp. UUID: %s", uid)
					continue
				else:
					timestamp = used[uid]["cf:id"]

				if "prov:entity" not in used[uid]:
					logging.debug("Edge (used) record without srcUUID. UUID: %s", uid)
					continue

				if "prov:activity" not in used[uid]:
					logging.debug("Edge (used) record without dstUUID. UUID: %s", uid)
					continue

				srcUUID = used[uid]["prov:entity"]
				dstUUID = used[uid]["prov:activity"]

				srcVal = db.get(srcUUID)
				if srcVal == None:
					logging.debug("Edge (used) record with an unmatched srcUUID. UUID: %s", uid)
					continue

				dstVal = db.get(dstUUID)
				if dstVal == None:
					logging.debug("Edge (used) record with an unmatched dstUUID. UUID: %s", uid)
					continue

				out.write(str(hashgen([srcUUID])) + '\t' \
					+ str(hashgen([dstUUID])) + '\t' \
					+ str(srcVal) + ':' + str(dstVal) \
					+ ':' + str(edgetype) \
					+ ':' + str(timestamp) + '\t' + '\n')

		if "wasGeneratedBy" in cfrec:
			wasGeneratedBy = cfrec["wasGeneratedBy"]
			for uid in wasGeneratedBy:
				if "prov:type" not in wasGeneratedBy[uid]:
					logging.debug("Edge (wasGeneratedBy) record without type. UUID: %s", uid)
					continue
				else:
					edgetype = valgencf(wasGeneratedBy[uid])

				if "cf:id" not in wasGeneratedBy[uid]:	# Can be used as timestamp
					logging.debug("Edge (wasGeneratedBy) record without timestamp. UUID: %s", uid)
					continue
				else:
					timestamp = wasGeneratedBy[uid]["cf:id"]

				if "prov:entity" not in wasGeneratedBy[uid]:
					logging.debug("Edge (used) record without srcUUID. UUID: %s", uid)
					continue

				if "prov:activity" not in wasGeneratedBy[uid]:
					logging.debug("Edge (used) record without dstUUID. UUID: %s", uid)
					continue

				srcUUID = wasGeneratedBy[uid]["prov:activity"]
				dstUUID = wasGeneratedBy[uid]["prov:entity"]

				srcVal = db.get(srcUUID)
				if srcVal == None:
					logging.debug("Edge (wasGeneratedBy) record with an unmatched srcUUID. UUID: %s", uid)
					continue

				dstVal = db.get(dstUUID)
				if dstVal == None:
					logging.debug("Edge (wasGeneratedBy) record with an unmatched dstUUID. UUID: %s", uid)
					continue

				out.write(str(hashgen([srcUUID])) + '\t' \
					+ str(hashgen([dstUUID])) + '\t' \
					+ str(srcVal) + ':' + str(dstVal) \
					+ ':' + str(edgetype) \
					+ ':' + str(timestamp) + '\t' + '\n')

		if "wasInformedBy" in cfrec:
			wasInformedBy = cfrec["wasInformedBy"]
			for uid in wasInformedBy:
				if "prov:type" not in wasInformedBy[uid]:
					logging.debug("Edge (wasInformedBy) record without type. UUID: %s", uid)
					continue
				else:
					edgetype = valgencf(wasInformedBy[uid])

				if "cf:id" not in wasInformedBy[uid]:	# Can be used as timestamp
					logging.debug("Edge (wasInformedBy) record without timestamp. UUID: %s", uid)
					continue
				else:
					timestamp = wasInformedBy[uid]["cf:id"]

				if "prov:informant" not in wasInformedBy[uid]:
					logging.debug("Edge (wasInformedBy) record without srcUUID. UUID: %s", uid)
					continue

				if "prov:informed" not in wasInformedBy[uid]:
					logging.debug("Edge (wasInformedBy) record without dstUUID. UUID: %s", uid)
					continue

				srcUUID = wasInformedBy[uid]["prov:informant"]
				dstUUID = wasInformedBy[uid]["prov:informed"]

				srcVal = db.get(srcUUID)
				if srcVal == None:
					logging.debug("Edge (wasInformedBy) record with an unmatched srcUUID. UUID: %s", uid)
					continue

				dstVal = db.get(dstUUID)
				if dstVal == None:
					logging.debug("Edge (wasInformedBy) record with an unmatched dstUUID. UUID: %s", uid)
					continue

				out.write(str(hashgen([srcUUID])) + '\t' \
					+ str(hashgen([dstUUID])) + '\t' \
					+ str(srcVal) + ':' + str(dstVal) \
					+ ':' + str(edgetype) \
					+ ':' + str(timestamp) + '\t' + '\n')

		if "wasDerivedFrom" in cfrec:
			wasDerivedFrom = cfrec["wasDerivedFrom"]
			for uid in wasDerivedFrom:
				if "prov:type" not in wasDerivedFrom[uid]:
					logging.debug("Edge (wasDerivedFrom) record without type. UUID: %s", uid)
					continue
				else:
					edgetype = valgencf(wasDerivedFrom[uid])

				if "cf:id" not in wasDerivedFrom[uid]:	# Can be used as timestamp
					logging.debug("Edge (wasDerivedFrom) record without timestamp. UUID: %s", uid)
					continue
				else:
					timestamp = wasDerivedFrom[uid]["cf:id"]

				if "prov:usedEntity" not in wasDerivedFrom[uid]:
					logging.debug("Edge (wasDerivedFrom) record without srcUUID. UUID: %s", uid)
					continue

				if "prov:generatedEntity" not in wasDerivedFrom[uid]:
					logging.debug("Edge (wasDerivedFrom) record without dstUUID. UUID: %s", uid)
					continue

				srcUUID = wasDerivedFrom[uid]["prov:usedEntity"]
				dstUUID = wasDerivedFrom[uid]["prov:generatedEntity"]

				srcVal = db.get(srcUUID)
				if srcVal == None:
					logging.debug("Edge (wasDerivedFrom) record with an unmatched srcUUID. UUID: %s", uid)
					continue

				dstVal = db.get(dstUUID)
				if dstVal == None:
					logging.debug("Edge (wasDerivedFrom) record with an unmatched dstUUID. UUID: %s", uid)
					continue

				out.write(str(hashgen([srcUUID])) + '\t' \
					+ str(hashgen([dstUUID])) + '\t' \
					+ str(srcVal) + ':' + str(dstVal) \
					+ ':' + str(edgetype) \
					+ ':' + str(timestamp) + '\t' + '\n')
				
	pb.close()
	return

def cgendp(parser, db, out):
	"""Generate DARPA outputs from compressed/single file.

	Arguments:
	parser - ijson parser that feeds JSON objects
	db - database
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

			srcUUID, dstUUID, bidirection = processevent(cdmrecval, 'darpa')

			if srcUUID == None or dstUUID == None:
				continue

			srcVal = db.get(srcUUID.encode('utf-8'))
			if srcVal == None:
				logging.error('An unmatched srcUUID from edge (' + repr(cdmrecval['uuid']) + ') of type: ' + cdmrecval['type'])
				continue

			dstVal = db.get(dstUUID.encode('utf-8'))
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

def cgencd(parser, db, out):
	"""Generate Cadets E2/FiveDirections outputs from compressed/single file.

	Note: Only one subset of FiveDirections dataset uses this function. Other subsets use regular DARPA functions.

	Arguments:
	parser - ijson parser that feeds JSON objects
	db - database
	out - output file object
	"""
	logging.basicConfig(filename='error.log',level=logging.DEBUG)

	description = '\x1b[6;30;43m[i]\x1b[0m Progress of Generating Output'
	pb = tqdm.tqdm(desc=description, mininterval=5.0, unit="recs")

	for cdmrec in parser:
		pb.update()
		cdmrectype = cdmrec['datum'].keys()[0]
		cdmrecval = cdmrec['datum'][cdmrectype]

		if cdmrectype == CD2_TYPE_EVENT:
			if 'type' not in cdmrecval:
				logging.debug('CD2_TYPE_EVENT: type is missing. Event UUID: ' + repr(cdmrecval['uuid']))
				continue
			else:
				edgetype = valgendp(cdmrectype, cdmrecval)

			if 'timestampNanos' not in cdmrecval:
				logging.debug('CD2_TYPE_EVENT: timestamp is missing. Event UUID: ' + repr(cdmrecval['uuid']))
				continue
			else:
				timestamp = cdmrecval['timestampNanos']

			srcUUID, dstUUID, bidirection = processevent(cdmrecval, 'cadets2')

			if srcUUID == None or dstUUID == None:
				continue

			srcVal = db.get(srcUUID.encode('utf-8'))
			if srcVal == None:
				logging.error('An unmatched srcUUID from edge (' + repr(cdmrecval['uuid']) + ') of type: ' + cdmrecval['type'])
				continue

			dstVal = db.get(dstUUID.encode('utf-8'))
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

def gencf(parser, i, dbs, out):
	"""Generate CamFlow outputs using a list of databases.

	Arguments:
	parser - ijson parser that feeds JSON objects
	i - the start index of the database list
	dbs - a list of database
	out - output file object
	"""
	logging.basicConfig(filename='error.log',level=logging.DEBUG)

	description = '\x1b[6;30;43m[i]\x1b[0m Progress of Generating Output from File \x1b[6;30;42m{}\x1b[0m'.format(i)
	pb = tqdm.tqdm(desc=description, mininterval=5.0, unit="recs", position=i)
	
	# for camflow, each file is independent
	db = dbs[i]

	for cfrec in parser:
		pb.update()

		if "used" in cfrec:
			used = cfrec["used"]
			for uid in used:
				if "prov:type" not in used[uid]:
					logging.debug("Edge (used) record without type. UUID: %s", uid)
					continue
				else:
					edgetype = valgencf(used[uid])

				if "cf:id" not in used[uid]:	# Can be used as timestamp
					logging.debug("Edge (used) record without timestamp. UUID: %s", uid)
					continue
				else:
					timestamp = used[uid]["cf:id"]

				if "prov:entity" not in used[uid]:
					logging.debug("Edge (used) record without srcUUID. UUID: %s", uid)
					continue

				if "prov:activity" not in used[uid]:
					logging.debug("Edge (used) record without dstUUID. UUID: %s", uid)
					continue

				srcUUID = used[uid]["prov:entity"]
				dstUUID = used[uid]["prov:activity"]

				srcVal = db.get(srcUUID)
				if srcVal == None:
					logging.debug("Edge (used) record with an unmatched srcUUID. UUID: %s", uid)
					continue

				dstVal = db.get(dstUUID)
				if dstVal == None:
					logging.debug("Edge (used) record with an unmatched dstUUID. UUID: %s", uid)
					continue

				out.write(str(hashgen([srcUUID])) + '\t' \
					+ str(hashgen([dstUUID])) + '\t' \
					+ str(srcVal) + ':' + str(dstVal) \
					+ ':' + str(edgetype) \
					+ ':' + str(timestamp) + '\t' + '\n')

		if "wasGeneratedBy" in cfrec:
			wasGeneratedBy = cfrec["wasGeneratedBy"]
			for uid in wasGeneratedBy:
				if "prov:type" not in wasGeneratedBy[uid]:
					logging.debug("Edge (wasGeneratedBy) record without type. UUID: %s", uid)
					continue
				else:
					edgetype = valgencf(wasGeneratedBy[uid])

				if "cf:id" not in wasGeneratedBy[uid]:	# Can be used as timestamp
					logging.debug("Edge (wasGeneratedBy) record without timestamp. UUID: %s", uid)
					continue
				else:
					timestamp = wasGeneratedBy[uid]["cf:id"]

				if "prov:entity" not in used[uid]:
					logging.debug("Edge (used) record without srcUUID. UUID: %s", uid)
					continue

				if "prov:activity" not in used[uid]:
					logging.debug("Edge (used) record without dstUUID. UUID: %s", uid)
					continue

				srcUUID = wasGeneratedBy[uid]["prov:activity"]
				dstUUID = wasGeneratedBy[uid]["prov:entity"]

				srcVal = db.get(srcUUID)
				if srcVal == None:
					logging.debug("Edge (wasGeneratedBy) record with an unmatched srcUUID. UUID: %s", uid)
					continue

				dstVal = db.get(dstUUID)
				if dstVal == None:
					logging.debug("Edge (wasGeneratedBy) record with an unmatched dstUUID. UUID: %s", uid)
					continue

				out.write(str(hashgen([srcUUID])) + '\t' \
					+ str(hashgen([dstUUID])) + '\t' \
					+ str(srcVal) + ':' + str(dstVal) \
					+ ':' + str(edgetype) \
					+ ':' + str(timestamp) + '\t' + '\n')

		if "wasInformedBy" in cfrec:
			wasInformedBy = cfrec["wasInformedBy"]
			for uid in wasInformedBy:
				if "prov:type" not in wasInformedBy[uid]:
					logging.debug("Edge (wasInformedBy) record without type. UUID: %s", uid)
					continue
				else:
					edgetype = valgencf(wasInformedBy[uid])

				if "cf:id" not in wasInformedBy[uid]:	# Can be used as timestamp
					logging.debug("Edge (wasInformedBy) record without timestamp. UUID: %s", uid)
					continue
				else:
					timestamp = wasInformedBy[uid]["cf:id"]

				if "prov:informant" not in wasInformedBy[uid]:
					logging.debug("Edge (wasInformedBy) record without srcUUID. UUID: %s", uid)
					continue

				if "prov:informed" not in wasInformedBy[uid]:
					logging.debug("Edge (wasInformedBy) record without dstUUID. UUID: %s", uid)
					continue

				srcUUID = wasInformedBy[uid]["prov:informant"]
				dstUUID = wasInformedBy[uid]["prov:informed"]

				srcVal = db.get(srcUUID)
				if srcVal == None:
					logging.debug("Edge (wasInformedBy) record with an unmatched srcUUID. UUID: %s", uid)
					continue

				dstVal = db.get(dstUUID)
				if dstVal == None:
					logging.debug("Edge (wasInformedBy) record with an unmatched dstUUID. UUID: %s", uid)
					continue

				out.write(str(hashgen([srcUUID])) + '\t' \
					+ str(hashgen([dstUUID])) + '\t' \
					+ str(srcVal) + ':' + str(dstVal) \
					+ ':' + str(edgetype) \
					+ ':' + str(timestamp) + '\t' + '\n')

		if "wasDerivedFrom" in cfrec:
			wasDerivedFrom = cfrec["wasDerivedFrom"]
			for uid in wasDerivedFrom:
				if "prov:type" not in wasDerivedFrom[uid]:
					logging.debug("Edge (wasDerivedFrom) record without type. UUID: %s", uid)
					continue
				else:
					edgetype = valgencf(wasDerivedFrom[uid])

				if "cf:id" not in wasDerivedFrom[uid]:	# Can be used as timestamp
					logging.debug("Edge (wasDerivedFrom) record without timestamp. UUID: %s", uid)
					continue
				else:
					timestamp = wasDerivedFrom[uid]["cf:id"]

				if "prov:usedEntity" not in wasDerivedFrom[uid]:
					logging.debug("Edge (wasDerivedFrom) record without srcUUID. UUID: %s", uid)
					continue

				if "prov:generatedEntity" not in wasDerivedFrom[uid]:
					logging.debug("Edge (wasDerivedFrom) record without dstUUID. UUID: %s", uid)
					continue

				srcUUID = wasDerivedFrom[uid]["prov:usedEntity"]
				dstUUID = wasDerivedFrom[uid]["prov:generatedEntity"]

				srcVal = db.get(srcUUID)
				if srcVal == None:
					logging.debug("Edge (wasDerivedFrom) record with an unmatched srcUUID. UUID: %s", uid)
					continue

				dstVal = db.get(dstUUID)
				if dstVal == None:
					logging.debug("Edge (wasDerivedFrom) record with an unmatched dstUUID. UUID: %s", uid)
					continue

				out.write(str(hashgen([srcUUID])) + '\t' \
					+ str(hashgen([dstUUID])) + '\t' \
					+ str(srcVal) + ':' + str(dstVal) \
					+ ':' + str(edgetype) \
					+ ':' + str(timestamp) + '\t' + '\n')
				
	pb.close()
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

	description = '\x1b[6;30;43m[i]\x1b[0m Progress of Generating Output from File \x1b[6;30;42m{}\x1b[0m'.format(i)
	pb = tqdm.tqdm(desc=description, mininterval=5.0, unit="recs", position=i)
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

			srcUUID, dstUUID, bidirection = processevent(cdmrecval, 'darpa')

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

def gencd(parser, i, dbs, out):
	"""Generate CADETS2/FiveDirections outputs using a list of databases.

	Arguments:
	parser - ijson parser that feeds JSON objects
	i - the start index of the database list
	dbs - a list of database
	out - output file object
	"""
	logging.basicConfig(filename='error.log',level=logging.DEBUG)

	description = '\x1b[6;30;43m[i]\x1b[0m Progress of Generating Output from File \x1b[6;30;42m{}\x1b[0m'.format(i)
	pb = tqdm.tqdm(desc=description, mininterval=5.0, unit="recs", position=i)
	for cdmrec in parser:
		pb.update()
		cdmrectype = cdmrec['datum'].keys()[0]
		cdmrecval = cdmrec['datum'][cdmrectype]

		if cdmrectype == CD2_TYPE_EVENT:
			if 'type' not in cdmrecval:
				logging.debug('CD2_TYPE_EVENT: type is missing. Event UUID: ' + repr(cdmrecval['uuid']))
				continue
			else:
				edgetype = valgendp(cdmrectype, cdmrecval)

			if 'timestampNanos' not in cdmrecval:
				logging.debug('CD2_TYPE_EVENT: timestamp is missing. Event UUID: ' + repr(cdmrecval['uuid']))
				continue
			else:
				timestamp = cdmrecval['timestampNanos']

			srcUUID, dstUUID, bidirection = processevent(cdmrecval, 'cadets2')

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
		val = dbs[ind].get(uuid.encode('utf-8'))
		if not val == None:
			break
	if val == None:
		for ind in range(len(dbs)-1, i, -1):
			val = dbs[ind].get(uuid.encode('utf-8'))
			if not val == None:
				break
	return val

def processevent(cdmrecval, trace):
	"""Process an event based on its type.

	Arguments:
	cdmrecval - value of the event
	trace - the tracing system used (Cadets2 and FiveDirections belong to the same category)
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
		(srcUUID, dstUUID) = subobjrel(cdmrecval, trace)
	# edges: Subject <-> Object
	elif edgetype == 'EVENT_FCNTL' or \
			edgetype == 'EVENT_MMAP' or \
			edgetype == 'EVENT_OTHER':
		(srcUUID, dstUUID) = subobjrel(cdmrecval, trace)
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
		(dstUUID, srcUUID) = subobjrel(cdmrecval, trace) 
	# edges: Object1 -> Object2
	elif edgetype == 'EVENT_ADD_OBJECT_ATTRIBUTE' or \
			edgetype == 'EVENT_LINK' or \
			edgetype == 'EVENT_RENAME' or \
			edgetype == 'EVENT_FLOWS_TO' or \
			edgetype == 'EVENT_UPDATE' or \
			edgetype == 'EVENT_SHM' or \
			edgetype == 'EVENT_CORRELATION':
		(srcUUID, dstUUID) = objobjrel(cdmrecval, trace)
	# edges non-directional
	elif edgetype == 'EVENT_EXIT' or \
			edgetype == 'EVENT_DUP' or \
			edgetype == 'EVENT_BOOT' or \
			edgetype == 'EVENT_BLIND':
		pass
	else:
		logging.error('CDM_TYPE_EVENT: event type is unexpected. Event UUID: ' + repr(cdmrecval['uuid']))

	return srcUUID, dstUUID, bidirection


def subobjrel(cdmrecval, trace):
	"""Relations between a Subject and an Object.

	Arguments:
	cdmrecval - CDM JSON record
	trace - the tracing system used

	Return:
	a tuple: (subjectUUID, objectUUID)
	"""
	subj = cdmrecval['subject']
	subjectUUID = None
	if type(subj).__name__ == 'NoneType':
		logging.debug("CDM_TYPE_EVENT: subject does not exist. Event UUID: " + repr(cdmrecval['uuid']))
	else:
		if trace == 'darpa':
			subjectUUID = subj[CDM_UUID]
		elif trace == 'cadets2':
			subjectUUID = subj[CD2_UUID]

	obj = cdmrecval['predicateObject']
	objectUUID = None
	if type(obj).__name__ == 'NoneType':
		logging.debug("CDM_TYPE_EVENT: object does not exist. Event UUID: " + repr(cdmrecval['uuid']))
	else:
		if trace == 'darpa':
			objectUUID = obj[CDM_UUID]
		elif trace == 'cadets2':
			objectUUID = obj[CD2_UUID]

	return (subjectUUID, objectUUID)

def objobjrel(cdmrecval, trace):
	"""Relations between an Object and another Object.

	Arguments:
	cdmrecval - CDM JSON record
	trace - the tracing system used

	Return:
	a tuple: (objectUUID1, objectUUID2)
	"""
	obj1 = cdmrecval['predicateObject']
	objectUUID1 = None
	if type(obj1).__name__ == 'NoneType':
		logging.debug("CDM_TYPE_EVENT: object (1) does not exist. Event UUID: " + repr(cdmrecval['uuid']))
	else:
		if trace == 'darpa':
			objectUUID1 = obj1[CDM_UUID]
		elif trace == 'cadets2':
			objectUUID1 = obj1[CD2_UUID]

	obj2 = cdmrecval['predicateObject2']
	objectUUID2 = None
	if type(obj2).__name__ == 'NoneType':
		logging.debug("CDM_TYPE_EVENT: object (2) does not exist. Event UUID: " + repr(cdmrecval['uuid']))
	else:
		if trace == 'darpa':
			objectUUID2 = obj2[CDM_UUID]
		elif trace == 'cadets2':
			objectUUID2 = obj2[CD2_UUID]

	return (objectUUID1, objectUUID2)

def valgendp(cdmrectype, cdmrecval):
	"""Generate a single value for a DARPA CDM/Cadets E2 record.

	Currently, only type information is used.

	Arguments:
	cdmrectype - CDM record type
	cdmrecval - CDM record

	Return:
	a single integer value of the record
	"""
	val = list()

	if cdmrectype == CDM_TYPE_SOCK or \
		cdmrectype == CD2_TYPE_SOCK:
		val.append('NET_FLOW_OBJECT')
	elif cdmrectype == CDM_TYPE_PIPE or \
		cdmrectype == CD2_TYPE_PIPE:
		val.append('UNNAMED_PIPE_OBJECT')
	elif cdmrectype == CDM_TYPE_MEMORY or \
		cdmrectype == CD2_TYPE_MEMORY:
		val.append('MEMORY_OBJECT')
	elif cdmrectype == CDM_TYPE_HOST or \
		cdmrectype == CD2_TYPE_HOST:
		val.append(cdmrecval['hostType'])
	else:
		val.append(cdmrecval['type'])

	return hashgen(val)

def valgencf(cfrecval):
	"""Generate a single value for a CamFlow record.

	Currently, only type information is used.

	Arguments:
	cfrecval - CamFlow record

	Return:
	a single integer value of the record
	"""
	val = list()
	val.append(cfrecval["prov:type"])
	return hashgen(val)
