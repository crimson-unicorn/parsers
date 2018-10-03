##############################################
# Preprocess CamFlow W3C format provenance log
# To be parsed into a format for Unicorn
# data parser.
##############################################

import sys
import json
import logging
import hashlib

class Node:
	def __init__(self, nid, ntype, secctx, mode, name):
		self.nid = nid
		self.ntype = ntype
		self.secctx = secctx
		self.mode = mode
		self.name = name

	def getnid(self):
		return self.nid

	def getntype(self):
		return self.ntype

	def getsecctx(self):
		return self.secctx

	def getmode(self):
		return self.mode

	def getname(self):
		return self.name

def hashgen(hashstr):
	hasher = hashlib.md5()
	hasher.update(hashstr)
	return hasher.hexdigest()

def nodeidgen(hashstr):
	hasher = hashlib.md5()
	hasher.update(hashstr)
	return int(hasher.hexdigest()[:8], 16) # make sure it is only 32 bits

def parse_nodes(json_string, node_map):
	json_object = json.loads(json_string)
	if "activity" in json_object:
		activity = json_object["activity"]
		for uid in activity:
			if uid in node_map:
				logging.debug("The ID of the activity node shows up more than once: %s", uid)
			else:
				if "prov:type" not in activity[uid]:
					logging.debug("Skipping a problematic activity node with no 'prov:type'. ID: %s", uid)	# A node must have a type.
				else:
					secctx = "N/A"
					if "cf:secctx" in activity[uid]:
						secctx = activity[uid]["cf:secctx"]
					secctx = hashgen(secctx)

					mode = "N/A"
					if "cf:mode" in activity[uid]:
						mode = activity[uid]["cf:mode"]
					mode = hashgen(mode)

					name = "N/A"
					if "cf:name" in activity[uid]:
						name = activity[uid]["cf:name"]
					name = hashgen(name)

					node_map[uid] = Node(nodeidgen(uid), hashgen(activity[uid]["prov:type"]), secctx, mode, name)

	if "entity" in json_object:
		entity = json_object["entity"]
		for uid in entity:
			if uid in node_map:
				logging.debug("The ID of the entity node shows up more than once: %s", uid)
			else:
				if "prov:type" not in entity[uid]:
					logging.debug("Skipping a problematic entity node with no 'prov:type'. ID: %s", uid)
				else:
					secctx = "N/A"
					if "cf:secctx" in entity[uid]:
						secctx = entity[uid]["cf:secctx"]
					secctx = hashgen(secctx)

					mode = "N/A"
					if "cf:mode" in entity[uid]:
						mode = entity[uid]["cf:mode"]
					mode = hashgen(mode)

					name = "N/A"
					if "cf:name" in entity[uid]:
						name = entity[uid]["cf:name"]
					name = hashgen(name)

					node_map[uid] = Node(nodeidgen(uid), hashgen(entity[uid]["prov:type"]), secctx, mode, name)

def parse_all_nodes(filename, node_map):
	with open(filename) as f:
		for line in f:
			parse_nodes(line, node_map)
		f.close()

def parse_all_edges(inputfile, outputfile, node_map):
	"""
	Parse all edges with the timestamp to a file.
	Format: <source_node_id> \t <destination_node_id> \t <hashed_source_type>:<hashed_destination_type>:<hashed_edge_type>:<edge_timestamp>
	"""
	# Go through the file to find the smallest timestamp and the also count the total number of edges.
	total_edges = 0
	smallest_timestamp = None
	with open(inputfile) as f:
		for line in f:
			json_object = json.loads(line)

			if "used" in json_object:
				used = json_object["used"]
				for uid in used:
					from_id = used[uid]["prov:entity"]
					to_id = used[uid]["prov:activity"]
					if from_id not in node_map:
						continue
					if to_id not in node_map:
						continue
					total_edges += 1
					timestamp = long(used[uid]["cf:jiffies"])
					if smallest_timestamp == None or timestamp < smallest_timestamp:
						smallest_timestamp = timestamp

			if "wasGeneratedBy" in json_object:
				wasGeneratedBy = json_object["wasGeneratedBy"]
				for uid in wasGeneratedBy:
					from_id = wasGeneratedBy[uid]["prov:activity"]
					to_id = wasGeneratedBy[uid]["prov:entity"]
					if from_id not in node_map:
						continue
					if to_id not in node_map:
						continue
					total_edges += 1
					timestamp = long(wasGeneratedBy[uid]["cf:jiffies"])
					if smallest_timestamp == None or timestamp < smallest_timestamp:
						smallest_timestamp = timestamp

			if "wasInformedBy" in json_object:
				wasInformedBy = json_object["wasInformedBy"]
				for uid in wasInformedBy:
					from_id = wasInformedBy[uid]["prov:informant"]
					to_id = wasInformedBy[uid]["prov:informed"]
					if from_id not in node_map:
						continue
					if to_id not in node_map:
						continue
					total_edges += 1
					timestamp = long(wasInformedBy[uid]["cf:jiffies"])
					if smallest_timestamp == None or timestamp < smallest_timestamp:
						smallest_timestamp = timestamp

			if "wasDerivedFrom" in json_object:
				wasDerivedFrom = json_object["wasDerivedFrom"]
				for uid in wasDerivedFrom:
					from_id = wasDerivedFrom[uid]["prov:usedEntity"]
					to_id = wasDerivedFrom[uid]["prov:generatedEntity"]
					if from_id not in node_map:
						continue
					if to_id not in node_map:
						continue	
					total_edges += 1
					timestamp = long(wasDerivedFrom[uid]["cf:jiffies"])
					if smallest_timestamp == None or timestamp < smallest_timestamp:
						smallest_timestamp = timestamp
	f.close()			

	output = open(outputfile, "w+")
	with open(inputfile) as f:
		for line in f:
			json_object = json.loads(line)
			
			if "used" in json_object:
				used = json_object["used"]
				for uid in used:
					from_id = used[uid]["prov:entity"]
					to_id = used[uid]["prov:activity"]
					if from_id not in node_map:
						logging.debug("Skipping an edge in 'used' because we cannot find the source node. Node ID: %s", from_id)
						continue
					if to_id not in node_map:
						logging.debug("Skipping an edge in 'used' because we cannot find the destination node. Node ID: %s", to_id)
						continue

					from_node = node_map[from_id]
					from_type = hashgen(from_node.getntype() + from_node.getsecctx() + from_node.getmode() + from_node.getname())

					to_node = node_map[to_id]
					to_type = hashgen(to_node.getntype() + to_node.getsecctx() + to_node.getmode() + to_node.getname())
					
					ts = used[uid]["cf:jiffies"]
					adjusted_ts = str(long(ts) - smallest_timestamp)
					
					edge_flags = "N/A"
					if "cf:flags" in used[uid]:
						edge_flags = used[uid]["cf:flags"]
					edge_flags = hashgen(edge_flags)
					edge_type = hashgen(hashgen(used[uid]["prov:type"]) + edge_flags)

					output.write(str(from_node.getnid()) + '\t' + str(to_node.getnid()) + '\t' + from_type + ':' + to_type + ':' + edge_type + ':' + adjusted_ts + '\t' + '\n')
			
			if "wasGeneratedBy" in json_object:
				wasGeneratedBy = json_object["wasGeneratedBy"]
				for uid in wasGeneratedBy:
					from_id = wasGeneratedBy[uid]["prov:activity"]
					to_id = wasGeneratedBy[uid]["prov:entity"]
					if from_id not in node_map:
						logging.debug("Skipping an edge in 'wasGeneratedBy' because we cannot find the source node. Node ID: %s", from_id)
						continue
					if to_id not in node_map:
						logging.debug("Skipping an edge in 'wasGeneratedBy' because we cannot find the destination node. Node ID: %s", to_id)
						continue

					from_node = node_map[from_id]
					from_type = hashgen(from_node.getntype() + from_node.getsecctx() + from_node.getmode() + from_node.getname())

					to_node = node_map[to_id]
					to_type = hashgen(to_node.getntype() + to_node.getsecctx() + to_node.getmode() + to_node.getname())

					ts = wasGeneratedBy[uid]["cf:jiffies"]
					adjusted_ts = str(long(ts) - smallest_timestamp)

					edge_flags = "N/A"
					if "cf:flags" in wasGeneratedBy[uid]:
						edge_flags = wasGeneratedBy[uid]["cf:flags"]
					edge_flags = hashgen(edge_flags)
					edge_type = hashgen(hashgen(wasGeneratedBy[uid]["prov:type"]) + edge_flags)
					
					output.write(str(from_node.getnid()) + '\t' + str(to_node.getnid()) + '\t' + from_type + ':' + to_type + ':' + edge_type + ':' + adjusted_ts + '\t' + '\n')
			
			if "wasInformedBy" in json_object:
				wasInformedBy = json_object["wasInformedBy"]
				for uid in wasInformedBy:
					from_id = wasInformedBy[uid]["prov:informant"]
					to_id = wasInformedBy[uid]["prov:informed"]
					if from_id not in node_map:
						logging.debug("Skipping an edge in 'wasInformedBy' because we cannot find the source node. Node ID: %s", from_id)
						continue
					if to_id not in node_map:
						logging.debug("Skipping an edge in 'wasInformedBy' because we cannot find the destination node. Node ID: %s", to_id)
						continue

					from_node = node_map[from_id]
					from_type = hashgen(from_node.getntype() + from_node.getsecctx() + from_node.getmode() + from_node.getname())

					to_node = node_map[to_id]
					to_type = hashgen(to_node.getntype() + to_node.getsecctx() + to_node.getmode() + to_node.getname())

					ts = wasInformedBy[uid]["cf:jiffies"]
					adjusted_ts = str(long(ts) - smallest_timestamp)

					edge_flags = "N/A"
					if "cf:flags" in wasInformedBy[uid]:
						edge_flags = wasInformedBy[uid]["cf:flags"]
					edge_flags = hashgen(edge_flags)
					edge_type = hashgen(hashgen(wasInformedBy[uid]["prov:type"]) + edge_flags)

					output.write(str(from_node.getnid()) + '\t' + str(to_node.getnid()) + '\t' + from_type + ':' + to_type + ':' + edge_type + ':' + adjusted_ts + '\t' + '\n')
					
			if "wasDerivedFrom" in json_object:
				wasDerivedFrom = json_object["wasDerivedFrom"]
				for uid in wasDerivedFrom:
					from_id = wasDerivedFrom[uid]["prov:usedEntity"]
					to_id = wasDerivedFrom[uid]["prov:generatedEntity"]
					if from_id not in node_map:
						logging.debug("Skipping an edge in 'wasDerivedFrom' because we cannot find the source node. Node ID: %s", from_id)
						continue
					if to_id not in node_map:
						logging.debug("Skipping an edge in 'wasDerivedFrom' because we cannot find the destination node. Node ID: %s", to_id)
						continue

					from_node = node_map[from_id]
					from_type = hashgen(from_node.getntype() + from_node.getsecctx() + from_node.getmode() + from_node.getname())
					
					to_node = node_map[to_id]
					to_type = hashgen(to_node.getntype() + to_node.getsecctx() + to_node.getmode() + to_node.getname())

					ts = wasDerivedFrom[uid]["cf:jiffies"]
					adjusted_ts = str(long(ts) - smallest_timestamp)

					edge_flags = "N/A"
					if "cf:flags" in wasDerivedFrom[uid]:
						edge_flags = wasDerivedFrom[uid]["cf:flags"]
					edge_flags = hashgen(edge_flags)
					edge_type = hashgen(hashgen(wasDerivedFrom[uid]["prov:type"]) + edge_flags)
					
					output.write(str(from_node.getnid()) + '\t' + str(to_node.getnid()) + '\t' + from_type + ':' + to_type + ':' + edge_type + ':' + adjusted_ts + '\t' + '\n')
	
	f.close()
	output.close()
	return total_edges


if __name__ == "__main__":
	if (len(sys.argv) < 3):
		print("""
			Usage: python prepare.py <input_file> <output_file_with_timestamp>
		"""
		)
		sys.exit(1)
	logging.basicConfig(filename='error.log',level=logging.DEBUG)

	node_map = {}
	parse_all_nodes(sys.argv[1], node_map)
	total_edges = parse_all_edges(sys.argv[1], sys.argv[2], node_map)
	total_nodes = len(node_map)
	stats = open("stats.csv", "a+")
	stats_str = sys.argv[1] + "," + str(total_nodes) + "," + str(total_edges) + "\n"
	stats.write(stats_str)
