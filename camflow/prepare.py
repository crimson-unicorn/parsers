##############################################
# Preprocess CamFlow W3C format provenance log
# To be parsed into a format for Unicorn
# data parser.
##############################################

import os, sys, argparse
import json
import logging
import xxhash
import time
import datetime
import tqdm

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

def parse_nodes(json_string, node_map):
	json_object = json.loads(json_string)
	if "activity" in json_object:
		activity = json_object["activity"]
		for uid in activity:
			if uid in node_map:
				logging.debug("The ID of the activity node shows up more than once: %s", uid)
			else:
				if "prov:type" not in activity[uid]:
					logging.debug("Skipping a problematic activity node with no prov:type. UUID: %s", uid)	# A node must have a type.
				else:
					node_map[uid] = str(valgencf(activity[uid]))

	if "entity" in json_object:
		entity = json_object["entity"]
		for uid in entity:
			if uid in node_map:
				logging.debug("The ID of the entity node shows up more than once: %s", uid)
			else:
				if "prov:type" not in entity[uid]:
					logging.debug("Skipping a problematic entity node with no prov:type. UUID: %s", uid)
				else:
					node_map[uid] = str(valgencf(entity[uid]))

def parse_all_nodes(filename, node_map):
	description = '\x1b[6;30;43m[i]\x1b[0m Node Parsing Progress of File: \x1b[6;30;42m{}\x1b[0m'.format(filename)
	pb = tqdm.tqdm(desc=description, mininterval=1.0, unit="recs")
	with open(filename, 'r') as f:
		for line in f:
			pb.update()
			parse_nodes(line, node_map)
	f.close()
	pb.close()

def parse_all_edges(inputfile, outputfile, node_map):
	"""
	Parse all edges with the timestamp to a file.
	Format: <source_node_id> \t <destination_node_id> \t <hashed_source_type>:<hashed_destination_type>:<hashed_edge_type>:<edge_timestamp>
	"""
	# Scan through the file to validate edges (i.e., both end nodes must exist) and find the smallest timestamp.
	description = '\x1b[6;30;43m[i]\x1b[0m Edge Scanning Progress of File: \x1b[6;30;42m{}\x1b[0m'.format(inputfile)
	pb = tqdm.tqdm(desc=description, mininterval=1.0, unit="recs")
	total_edges = 0
	smallest_timestamp = None
	with open(inputfile, 'r') as f:
		for line in f:
			pb.update()
			json_object = json.loads(line)

			if "used" in json_object:
				used = json_object["used"]
				for uid in used:
					if "prov:type" not in used[uid]:
						logging.debug("Edge (used) record without type. UUID: %s", uid)
						continue
					if "cf:date" not in used[uid]:
						logging.debug("Edge (used) record without date. UUID: %s", uid)
						continue
					if "prov:entity" not in used[uid]:
						logging.debug("Edge (used/{}) record without srcUUID. UUID: {}".format(used[uid]["prov:type"], uid))
						continue
					if "prov:activity" not in used[uid]:
						logging.debug("Edge (used/{}) record without dstUUID. UUID: {}".format(used[uid]["prov:type"], uid))
						continue
					srcUUID = used[uid]["prov:entity"]
					dstUUID = used[uid]["prov:activity"]
					if srcUUID not in node_map:
						logging.debug("Edge (used/{}) record with an unmatched srcUUID. UUID: {}".format(used[uid]["prov:type"], uid))
						continue
					if dstUUID not in node_map:
						logging.debug("Edge (used/{}) record with an unmatched dstUUID. UUID: {}".format(used[uid]["prov:type"], uid))
						continue
					total_edges += 1
					timestamp_str = used[uid]["cf:date"]
					ts = time.mktime(datetime.datetime.strptime(timestamp_str, "%Y:%m:%dT%H:%M:%S").timetuple())
					if smallest_timestamp == None or ts < smallest_timestamp:
						smallest_timestamp = ts
			if "wasGeneratedBy" in json_object:
				wasGeneratedBy = json_object["wasGeneratedBy"]
				for uid in wasGeneratedBy:
					if "prov:type" not in wasGeneratedBy[uid]:
						logging.debug("Edge (wasGeneratedBy) record without type. UUID: %s", uid)
						continue
					if "cf:date" not in wasGeneratedBy[uid]:
						logging.debug("Edge (wasGeneratedBy) record without date. UUID: %s", uid)
						continue
					if "prov:entity" not in wasGeneratedBy[uid]:
						logging.debug("Edge (wasGeneratedBy/{}) record without srcUUID. UUID: {}".format(wasGeneratedBy[uid]["prov:type"], uid))
						continue
					if "prov:activity" not in wasGeneratedBy[uid]:
						logging.debug("Edge (wasGeneratedBy/{}) record without dstUUID. UUID: {}".format(wasGeneratedBy[uid]["prov:type"], uid))
						continue
					srcUUID = wasGeneratedBy[uid]["prov:activity"]
					dstUUID = wasGeneratedBy[uid]["prov:entity"]
					if srcUUID not in node_map:
						logging.debug("Edge (wasGeneratedBy/{}) record with an unmatched srcUUID. UUID: {}".format(wasGeneratedBy[uid]["prov:type"], uid))
						continue
					if dstUUID not in node_map:
						logging.debug("Edge (wasGeneratedBy/{}) record with an unmatched dstUUID. UUID: {}".format(wasGeneratedBy[uid]["prov:type"], uid))
						continue
					total_edges += 1
					timestamp_str = wasGeneratedBy[uid]["cf:date"]
					ts = time.mktime(datetime.datetime.strptime(timestamp_str, "%Y:%m:%dT%H:%M:%S").timetuple())
					if smallest_timestamp == None or ts < smallest_timestamp:
						smallest_timestamp = ts
			if "wasInformedBy" in json_object:
				wasInformedBy = json_object["wasInformedBy"]
				for uid in wasInformedBy:
					if "prov:type" not in wasInformedBy[uid]:
						logging.debug("Edge (wasInformedBy) record without type. UUID: %s", uid)
						continue
					if "cf:date" not in wasInformedBy[uid]:
						logging.debug("Edge (wasInformedBy) record without date. UUID: %s", uid)
						continue
					if "prov:informant" not in wasInformedBy[uid]:
						logging.debug("Edge (wasInformedBy/{}) record without srcUUID. UUID: {}".format(wasInformedBy[uid]["prov:type"], uid))
						continue
					if "prov:informed" not in wasInformedBy[uid]:
						logging.debug("Edge (wasInformedBy/{}) record without dstUUID. UUID: {}".format(wasInformedBy[uid]["prov:type"], uid))
						continue
					srcUUID = wasInformedBy[uid]["prov:informant"]
					dstUUID = wasInformedBy[uid]["prov:informed"]
					if srcUUID not in node_map:
						logging.debug("Edge (wasInformedBy/{}) record with an unmatched srcUUID. UUID: {}".format(wasInformedBy[uid]["prov:type"], uid))
						continue
					if dstUUID not in node_map:
						logging.debug("Edge (wasInformedBy/{}) record with an unmatched dstUUID. UUID: {}".format(wasInformedBy[uid]["prov:type"], uid))
						continue
					total_edges += 1
					timestamp_str = wasInformedBy[uid]["cf:date"]
					ts = time.mktime(datetime.datetime.strptime(timestamp_str, "%Y:%m:%dT%H:%M:%S").timetuple())
					if smallest_timestamp == None or ts < smallest_timestamp:
						smallest_timestamp = ts
			if "wasDerivedFrom" in json_object:
				wasDerivedFrom = json_object["wasDerivedFrom"]
				for uid in wasDerivedFrom:
					if "prov:type" not in wasDerivedFrom[uid]:
						logging.debug("Edge (wasDerivedFrom) record without type. UUID: %s", uid)
						continue
					if "cf:date" not in wasDerivedFrom[uid]:
						logging.debug("Edge (wasDerivedFrom) record without date. UUID: %s", uid)
						continue
					if "prov:usedEntity" not in wasDerivedFrom[uid]:
						logging.debug("Edge (wasDerivedFrom/{}) record without srcUUID. UUID: {}".format(wasDerivedFrom[uid]["prov:type"], uid))
						continue
					if "prov:generatedEntity" not in wasDerivedFrom[uid]:
						logging.debug("Edge (wasDerivedFrom/{}) record without dstUUID. UUID: {}".format(wasDerivedFrom[uid]["prov:type"], uid))
						continue
					srcUUID = wasDerivedFrom[uid]["prov:usedEntity"]
					dstUUID = wasDerivedFrom[uid]["prov:generatedEntity"]
					if srcUUID not in node_map:
						logging.debug("Edge (wasDerivedFrom/{}) record with an unmatched srcUUID. UUID: {}".format(wasDerivedFrom[uid]["prov:type"], uid))
						continue
					if dstUUID not in node_map:
						logging.debug("Edge (wasDerivedFrom/{}) record with an unmatched dstUUID. UUID: {}".format(wasDerivedFrom[uid]["prov:type"], uid))
						continue
					total_edges += 1
					timestamp_str = wasDerivedFrom[uid]["cf:date"]
					ts = time.mktime(datetime.datetime.strptime(timestamp_str, "%Y:%m:%dT%H:%M:%S").timetuple())
					if smallest_timestamp == None or ts < smallest_timestamp:
						smallest_timestamp = ts
	f.close()
	pb.close()

	output = open(outputfile, "w+")
	description = '\x1b[6;30;43m[i]\x1b[0m Progress of Generating Output of File: \x1b[6;30;42m{}\x1b[0m'.format(inputfile)
	pb = tqdm.tqdm(desc=description, mininterval=1.0, unit="recs")
	with open(inputfile, 'r') as f:
		for line in f:
			pb.update()
			json_object = json.loads(line)
			
			if "used" in json_object:
				used = json_object["used"]
				for uid in used:
					if "prov:type" not in used[uid]:
						continue
					else:
						edgetype = valgencf(used[uid])
					if "cf:id" not in used[uid]:
						logging.debug("Edge (used) record without timestamp. UUID: %s", uid)
						continue
					else:
						timestamp = used[uid]["cf:id"]  # Can be used as timestamp
					if "prov:entity" not in used[uid]:
						continue
					if "prov:activity" not in used[uid]:
						continue
					srcUUID = used[uid]["prov:entity"]
					dstUUID = used[uid]["prov:activity"]
					if srcUUID not in node_map:
						continue
					else:
						srcVal = node_map[srcUUID]
					if dstUUID not in node_map:
						continue
					else:
						dstVal = node_map[dstUUID]

					ts_str = used[uid]["cf:date"]
					ts = time.mktime(datetime.datetime.strptime(ts_str, "%Y:%m:%dT%H:%M:%S").timetuple())
					adjusted_ts = ts - smallest_timestamp

					output.write(str(hashgen([srcUUID])) + '\t' \
						+ str(hashgen([dstUUID])) + '\t' \
						+ str(srcVal) + ':' + str(dstVal) \
						+ ':' + str(edgetype) + ':' + str(timestamp) \
						+ ':' + str(adjusted_ts) + '\t' + '\n')
			if "wasGeneratedBy" in json_object:
				wasGeneratedBy = json_object["wasGeneratedBy"]
				for uid in wasGeneratedBy:
					if "prov:type" not in wasGeneratedBy[uid]:
						continue
					else:
						edgetype = valgencf(wasGeneratedBy[uid])
					if "cf:id" not in wasGeneratedBy[uid]:
						logging.debug("Edge (wasGeneratedBy) record without timestamp. UUID: %s", uid)
						continue
					else:
						timestamp = wasGeneratedBy[uid]["cf:id"]
					if "prov:entity" not in wasGeneratedBy[uid]:
						continue
					if "prov:activity" not in wasGeneratedBy[uid]:
						continue
					srcUUID = wasGeneratedBy[uid]["prov:activity"]
					dstUUID = wasGeneratedBy[uid]["prov:entity"]
					if srcUUID not in node_map:
						continue
					else:
						srcVal = node_map[srcUUID]
					if dstUUID not in node_map:
						continue
					else:
						dstVal = node_map[dstUUID]

					ts_str = wasGeneratedBy[uid]["cf:date"]
					ts = time.mktime(datetime.datetime.strptime(ts_str, "%Y:%m:%dT%H:%M:%S").timetuple())
					adjusted_ts = ts - smallest_timestamp

					output.write(str(hashgen([srcUUID])) + '\t' \
						+ str(hashgen([dstUUID])) + '\t' \
						+ str(srcVal) + ':' + str(dstVal) \
						+ ':' + str(edgetype) + ':' + str(timestamp) \
						+ ':' + str(adjusted_ts) + '\t' + '\n')
			if "wasInformedBy" in json_object:
				wasInformedBy = json_object["wasInformedBy"]
				for uid in wasInformedBy:
					if "prov:type" not in wasInformedBy[uid]:
						continue
					else:
						edgetype = valgencf(wasInformedBy[uid])
					if "cf:id" not in wasInformedBy[uid]:
						logging.debug("Edge (wasInformedBy) record without timestamp. UUID: %s", uid)
						continue
					else:
						timestamp = wasInformedBy[uid]["cf:id"]
					if "prov:informant" not in wasInformedBy[uid]:
						continue
					if "prov:informed" not in wasInformedBy[uid]:
						continue
					srcUUID = wasInformedBy[uid]["prov:informant"]
					dstUUID = wasInformedBy[uid]["prov:informed"]
					if srcUUID not in node_map:
						continue
					else:
						srcVal = node_map[srcUUID]
					if dstUUID not in node_map:
						continue
					else:
						dstVal = node_map[dstUUID]

					ts_str = wasInformedBy[uid]["cf:date"]
					ts = time.mktime(datetime.datetime.strptime(ts_str, "%Y:%m:%dT%H:%M:%S").timetuple())
					adjusted_ts = ts - smallest_timestamp

					output.write(str(hashgen([srcUUID])) + '\t' \
						+ str(hashgen([dstUUID])) + '\t' \
						+ str(srcVal) + ':' + str(dstVal) \
						+ ':' + str(edgetype) + ':' + str(timestamp) \
						+ ':' + str(adjusted_ts) + '\t' + '\n')
			if "wasDerivedFrom" in json_object:
				wasDerivedFrom = json_object["wasDerivedFrom"]
				for uid in wasDerivedFrom:
					if "prov:type" not in wasDerivedFrom[uid]:
						continue
					else:
						edgetype = valgencf(wasDerivedFrom[uid])
					if "cf:id" not in wasDerivedFrom[uid]:
						logging.debug("Edge (wasDerivedFrom) record without timestamp. UUID: %s", uid)
						continue
					else:
						timestamp = wasDerivedFrom[uid]["cf:id"]
					if "prov:usedEntity" not in wasDerivedFrom[uid]:
						continue
					if "prov:generatedEntity" not in wasDerivedFrom[uid]:
						continue
					srcUUID = wasDerivedFrom[uid]["prov:usedEntity"]
					dstUUID = wasDerivedFrom[uid]["prov:generatedEntity"]
					if srcUUID not in node_map:
						continue
					else:
						srcVal = node_map[srcUUID]
					if dstUUID not in node_map:
						continue
					else:
						dstVal = node_map[dstUUID]

					ts_str = wasDerivedFrom[uid]["cf:date"]
					ts = time.mktime(datetime.datetime.strptime(ts_str, "%Y:%m:%dT%H:%M:%S").timetuple())
					adjusted_ts = ts - smallest_timestamp
					
					output.write(str(hashgen([srcUUID])) + '\t' \
						+ str(hashgen([dstUUID])) + '\t' \
						+ str(srcVal) + ':' + str(dstVal) \
						+ ':' + str(edgetype) + ':' + str(timestamp) \
						+ ':' + str(adjusted_ts) + '\t' + '\n')
	f.close()
	output.close()
	pb.close()
	return total_edges


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Convert CamFlow JSON datasets to Unicorn edgelist datasets for runtime performance evaluation.')
	parser.add_argument('-i', '--input', help='input data file path', required=True)
	parser.add_argument('-o', '--output', help='output destination file path', required=True)
	args = parser.parse_args()

	logging.basicConfig(filename='error.log',level=logging.DEBUG)

	node_map = {}
	parse_all_nodes(args.input, node_map)
	total_edges = parse_all_edges(args.input, args.output, node_map)

	total_nodes = len(node_map)
	stats = open("stats.csv", "a+")
	stats_str = args.input + "," + str(total_nodes) + "," + str(total_edges) + "\n"
	stats.write(stats_str)
