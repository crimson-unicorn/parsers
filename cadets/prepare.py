import csv
import os, sys
import hashlib
from collections import OrderedDict

edge_map = {}
node_map = {}

def hashgen(hashstr):
	hasher = hashlib.md5()
	hasher.update(hashstr)
	return hasher.hexdigest()

class Edge:
	def __init__(self, from_id, to_id, etype, generating_call):
		self.from_id = from_id
		self.to_id = to_id
		self.etype = etype
		self.gc = generating_call

	def getfid(self):
		return self.from_id

	def gettid(self):
		return self.to_id

	def getetype(self):
		return self.etype

	def getgc(self):
		return self.gc

class Node:
	def __init__(self, label, ty):
		self.label = label
		self.ty = ty

	def getlabel(self):
		return self.label

	def getty(self):
		return self.ty

def parse_single_file(csvfile):
	reader = csv.DictReader(csvfile)
	if ':START_ID' in reader.fieldnames: # reading an egde csv file
		for row in reader:
			edge_id = int(row["db_id"])	# edge id is also its timestamp
			from_id = row[":START_ID"]
			to_id = row[":END_ID"]
			etype = row[":TYPE"]
			generating_call = row["generating_call"]
			if edge_id in edge_map:
				print "EdgeError: Repeated db_id: " + edge_id
			edge_map[edge_id] = Edge(from_id, to_id, etype, generating_call)
	else: # reading a node csv file
		for row in reader:
			node_id = row["db_id:ID"]	# this ID enables edges to find it
			label = "N/A"
			if ":LABEL" in reader.fieldnames:
				label = row[":LABEL"]
			ty = "N/A"
			if "ty" in reader.fieldnames:
				ty = row["ty"]
			if node_id in node_map:
				print "NodeError: Repeated db_id: " + node_id
			node_map[node_id] = Node(label, ty)

def write_edgelist_file(file_path):
	output = open(file_path, "w+")
	ordered_map = OrderedDict(sorted(edge_map.items()))
	for key, edge in ordered_map.items():
		from_id = edge.getfid()
		from_node = node_map[from_id]
		from_node_hashed_type = hashgen(from_node.getlabel() + from_node.getty())
		to_id = edge.gettid()
		to_node = node_map[to_id]
		to_node_hashed_type = hashgen(to_node.getlabel() + to_node.getty())
		edge_hashed_type = hashgen(edge.getetype() + edge.getgc())

		output.write(from_id + '\t' + to_id + '\t' + from_node_hashed_type + ":" + to_node_hashed_type + ":" + edge_hashed_type + ":" + str(key) + "\t" + "\n")

if __name__ == "__main__":
	if (len(sys.argv) < 3):
		print("""
			Usage: python parser.py <input_directory> <output_file_with_timestamp>
		"""
		)
		sys.exit(1)

	files_to_parse = os.listdir(sys.argv[1] + '/db')
	for input_file in files_to_parse:
		if input_file == "dbinfo.csv" or input_file == "hydrate.sh" or input_file == "types.csv":
			pass
		else:
			with open(os.path.join(sys.argv[1] + '/db', input_file), 'r') as f:
				parse_single_file(f)

	write_edgelist_file(sys.argv[2])

