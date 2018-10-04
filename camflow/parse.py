import sys
import math

def compare_edges(a, b):
	if long(a[7]) > long(b[7]):
		return 1
	else:
		return -1

def read_single_graph(file_name):
	'''
	edge format: [source_node_id, destination_node_id, source_node_type, source_node_seen, destination_node_seen, destination_node_type, edge_type, edge_order, timestamp]
	'''
	map_id = {}	# maps original ID to new ID
	new_id = 0
	graph = []
	with open(file_name) as f:
		for line in f:
			edge = line.strip().split("\t")

			if edge[0] in map_id:	# Check if source ID has been seen before.
				edge[0] = map_id[edge[0]]
				edge.append("0")
			else:
				edge.append("1")
				map_id[edge[0]] = str(new_id)
				edge[0] = str(new_id)
				new_id = new_id + 1

			if edge[1] in map_id:	# Check if destination ID has been seen before.
				edge[1] = map_id[edge[1]]
				edge.append("0")
			else:
				edge.append("1")
				map_id[edge[1]] = str(new_id)
				edge[1] = str(new_id)
				new_id = new_id + 1

			attributes = edge[2].strip().split(":")
			source_node_type = attributes[0]
			destination_node_type = attributes[1]
			edge_type = attributes[2]
			edge_order = attributes[3]
			ts = attributes[4]
			
			edge[2] = source_node_type
			edge.append(destination_node_type)
			edge.append(edge_type)
			edge.append(edge_order)
			edge.append(ts)

			graph.append(edge)
	
	f.close()
	graph.sort(compare_edges)
	return graph

def print_instruction():
	print(
		"Usage: python parse.py <input_file_path> <base_graph_file_path> <stream_file_path>\n"
		"The first 10% of the edges in the graph will be considered as the basis of the streaming graph, stored in <base_graph_file_path>.\n"
		"The rest of the edges will be streamed in, stored in <stream_file_path>.\n")


if __name__ == "__main__":
	if (len(sys.argv) < 4):
		sys.exit(1)

	graph = read_single_graph(sys.argv[1])
	# base_graph_size = math.ceil(len(graph) * 0.1)	# The size of base graph.
	base_graph_size = 1 	# For evalution, we only care about streaming graph.
	stream_graph_size = len(graph) - base_graph_size

	base_file = open(sys.argv[2], "w")
	stream_file = open(sys.argv[3], "w")

	cnt = 0
	for edge in graph:
		if cnt < base_graph_size:
			cnt = cnt + 1
			base_file.write(str(edge[0]) + " " + str(edge[1]) + " " + edge[2] + ":" + edge[5] + ":" + edge[6] + ":" + edge[7] + "\n")
		else:
			stream_file.write(str(edge[0]) + " " + str(edge[1]) + " " + edge[2] + ":" + edge[5] + ":" + edge[6] + ":" + edge[3] + ":" + edge[4] + ":" + edge[7] + ":" + edge[8] + "\n")

	print "[success] processing of " + sys.argv[1] + " is done. Data now can be accepted by the graph processing framework."
	
	base_file.close()
	stream_file.close()