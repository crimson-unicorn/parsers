import sys, argparse
import math
import tqdm

def compare_edges(a, b):
	if long(a[5]) > long(b[5]):
		return 1
	else:
		return -1

def read_single_graph(file_name):
	'''
	edge format: [source_node_id, destination_node_id, source_node_type, source_node_seen, destination_node_seen, destination_node_type, edge_type, timestamp]
	'''
	map_id = {}	# maps original ID to new ID
	new_id = 0
	graph = []
	
	description = '\x1b[6;30;43m[i]\x1b[0m Sorting Progress of file: {}'.format(file_name)
	pb = tqdm.tqdm(desc=description, mininterval=1.0, unit="recs")
	
	with open(file_name) as f:
		for line in f:
			pb.update()
			edge = line.strip().split("\t")

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
	pb.close()
	graph.sort(compare_edges)

	description = '\x1b[6;30;43m[i]\x1b[0m Final Processing Progress of file: {}'.format(file_name)
	pb = tqdm.tqdm(desc=description, mininterval=1.0, unit="recs")
	for edge in graph:
		pb.update()
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

	pb.close()
	return graph

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-s', '--size', help='the interval to record time', required=True, type=int)
	parser.add_argument('-b', '--bsize', help='the size of the base graph', type=int)
	parser.add_argument('-i', '--input', help='input data file path', required=True)
	parser.add_argument('-B', '--base', help='output destination file path of base graph', required=True)
	parser.add_argument('-S', '--stream', help='output destination file path of stream graph', required=True)
	args = parser.parse_args()

	graph = read_single_graph(args.input)
	
	if args.bsize is not None:
		base_graph_size = args.bsize
	else:
		base_graph_size = math.ceil(len(graph) * 0.1)	# default to 10% of the total edges in the graph
	stream_graph_size = len(graph) - base_graph_size

	base_file = open(args.base, "w")
	stream_file = open(args.stream, "w")

	# For runtime performance eval.
	ts_file = open("ts.txt", "w")
	# We use this flag to make sure we record the time it takes to create base graph only once.
	recorded_once = False
	edge_cnt = 0
	for num, edge in enumerate(graph):
		if num < base_graph_size:
			base_file.write(str(edge[0]) + " " + str(edge[1]) + " " + edge[2] + ":" + edge[3] + ":" + edge[4] + ":" + edge[5] + "\n")
		else:
			stream_file.write(str(edge[0]) + " " + str(edge[1]) + " " + edge[2] + ":" + edge[3] + ":" + edge[4] + ":" + edge[7] + ":" + edge[8] + ":" + edge[5] + ":" + edge[6] + "\n")
		edge_cnt += 1
		if not recorded_once and edge_cnt == base_graph_size:
			ts_file.write(str(edge[6]) + '\n')
			edge_cnt = 0
			recorded_once = True
		if edge_cnt == args.size:
			ts_file.write(str(edge[6]) + '\n')
			edge_cnt = 0
		# record time for the last round of edges
		if num == len(graph) - 1:
			ts_file.write(str(edge[6]) + '\n')

	print ("[success] processing of " + args.input + " is done. Data now can be accepted by the graph processing framework.")
	
	base_file.close()
	stream_file.close()
	ts_file.close()
