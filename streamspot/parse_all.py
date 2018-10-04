####################################################
## A faster parser that parses all streamspot graphs
#################################################### 

import sys

def parse_all(parent_dir, streamspot_file_path):
	"""
	This function parses all graphs from StreamSpot @streamspot_file_path.
	The common parent directory is given by @parent_dir.
	All the rest must be in this format, based on graph ID:
	0 - 99: <parent_dir>/youtube_data/base_train/base-youtube-<graph_id>.txt, <parent_dir>/youtube_data/stream_train/stream-youtube-<graph_id>.txt
	100 - 199: <parent_dir>/gmail_data/base_train/base-gmail-<graph_id>.txt, <parent_dir>/gmail_data/stream_train/stream-gmail-<graph_id>.txt
	200 - 299: <parent_dir>/vgame_data/base_train/base-vgame-<graph_id>.txt, <parent_dir>/vgame_data/stream_train/stream-vgame-<graph_id>.txt
	300 - 399: <parent_dir>/attack_data/base_train/base-attack-<graph_id>.txt, <parent_dir>/attack_data/stream_train/stream-attack-<graph_id>.txt
	400 - 499: <parent_dir>/download_data/base_train/base-download-<graph_id>.txt, <parent_dir>/download_data/stream_train/stream-download-<graph_id>.txt
	500 - 599: <parent_dir>/cnn_data/base_train/base-cnn-<graph_id>.txt, <parent_dir>/cnn_data/stream_train/stream-cnn-<graph_id>.txt
	"""
	# opening all the files to write:
	graph = {}
	node_seen = {}
	cnt = {}
	for i in range(0, 600):
		graph[i] = []
		node_seen[i] = []
		cnt[i] = 1
	# Read through the StreamSpot file
	with open(streamspot_file_path) as f:
		for line in f:
			edge = line.strip().split("\t")
			graph_id = int(edge[5])
			cur_graph = graph[graph_id]
			node_seen_list = node_seen[graph_id]
			cur_cnt = cnt[graph_id]

			if edge[0] in node_seen_list:	# Check if source ID has been seen before.
				edge.append("0")
			else:
				edge.append("1")
				node_seen_list.append(edge[0])
			if edge[2] in node_seen_list:	# Check if destination ID has been seen before.
				edge.append("0")
			else:
				edge.append("1")
				node_seen_list.append(edge[2])
			edge.append(cur_cnt)
			cur_cnt += 1
			cur_graph.append(edge)
	f.close()
	
	for i in range(0, 600):
		if i < 100:
			base_file_path = parent_dir + "/youtube_data/base_train/base-youtube-" + str(i) + ".txt"
			stream_file_path = parent_dir + "/youtube_data/stream_train/stream-youtube-" + str(i) + ".txt"
			base_fh = open(base_file_path, "a+")
			stream_fh = open(stream_file_path, "a+")
		elif i < 200:
			base_file_path = parent_dir + "/gmail_data/base_train/base-gmail-" + str(i) + ".txt"
			stream_file_path = parent_dir + "/gmail_data/stream_train/stream-gmail-" + str(i) + ".txt"
			base_fh = open(base_file_path, "a+")
			stream_fh = open(stream_file_path, "a+")
		elif i < 300:
			base_file_path = parent_dir + "/vgame_data/base_train/base-vgame-" + str(i) + ".txt"
			stream_file_path = parent_dir + "/vgame_data/stream_train/stream-vgame-" + str(i) + ".txt"
			base_fh = open(base_file_path, "a+")
			stream_fh = open(stream_file_path, "a+")
		elif i < 400:
			base_file_path = parent_dir + "/attack_data/base_train/base-attack-" + str(i) + ".txt"
			stream_file_path = parent_dir + "/attack_data/stream_train/stream-attack-" + str(i) + ".txt"
			base_fh = open(base_file_path, "a+")
			stream_fh = open(stream_file_path, "a+")
		elif i < 500:
			base_file_path = parent_dir + "/download_data/base_train/base-download-" + str(i) + ".txt"
			stream_file_path = parent_dir + "/download_data/stream_train/stream-download-" + str(i) + ".txt"
			base_fh = open(base_file_path, "a+")
			stream_fh = open(stream_file_path, "a+")
		elif i < 600:
			base_file_path = parent_dir + "/cnn_data/base_train/base-cnn-" + str(i) + ".txt"
			stream_file_path = parent_dir + "/cnn_data/stream_train/stream-cnn-" + str(i) + ".txt"
			base_fh = open(base_file_path, "a+")
			stream_fh = open(stream_file_path, "a+")
		base_graph_size = math.ceil(len(graph[i]) * 0.1)
		for edge in graph[i]:
			if edge[8] <= base_graph_size:
				base_fh.write(str(edge[0]) + " " + str(edge[2]) + " " + edge[1] + ":" + edge[3] + ":" + edge[4] + ":" + str(edge[8]) + "\n")
			else:
				stream_fh.write(str(edge[0]) + " " + str(edge[2]) + " " + edge[1] + ":" + edge[3] + ":" + edge[4] + ":" + edge[6] + ":" + edge[7] + ":" + str(edge[8]) + "\n")
		base_fh.close()
		stream_fh.close()
		print "[progress] Graph " + str(i) + " is parsed..." 

if __name__ == "__main__":
	if (len(sys.argv) < 3):
		print("""
			Usage: python parse_all.py <parent_dir> <streamspot_file_path>
		"""
		)
		sys.exit(1)

	print "Parsing a large file. Please wait patiently..."
	parse_all(sys.argv[1], sys.argv[2])	
	print "[success] All graphs have been successfully parsed."




