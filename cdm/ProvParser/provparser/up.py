#!/usr/bin/env python
from __future__ import print_function
import os, sys, argparse
import tqdm

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Convert edgelist datasets to Unicorn Stream datasets.')
	parser.add_argument('-v', '--verbose', help='increase console verbosity', action='store_true')
	parser.add_argument('-m', '--memory', help='use in-memory dictionary instead of RocksDB', action='store_true')
	parser.add_argument('-S', '--size', help='size of base output file in # of edges (set automatically if not given)', type=int)
	parser.add_argument('-i', '--input', help='input file path', required=True)
	parser.add_argument('-b', '--base', help='base output file path', required=True)
	parser.add_argument('-s', '--stream', help='stream output file path', required=True)
	parser.add_argument('-I', '--information', help='print out the graph statistics to stats.txt', action='store_true', required=False)
	global args
	args = parser.parse_args()

	if not args.memory:
		import rocksdb
		# create database for nodes
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

		db = rocksdb.DB('nodes.db', opts)
		if args.verbose:
			print("\x1b[6;30;42m[+]\x1b[0m setting up database nodes.db in current directory...")
	else:
		db = dict()

	if args.size is None:
		# auto set base graph size in # of edges
		total = 0
		with open(args.input) as f:
			for line in f:
				total += 1
		f.close()
		args.size = total * 0.1

	bf = open(args.base, "w")
	if args.verbose:
		print("\x1b[6;30;42m[+]\x1b[0m opening base output file {} to write...".format(args.base))
	sf = open(args.stream, "w")
	if args.verbose:
		print("\x1b[6;30;42m[+]\x1b[0m opening stream output file {} to write...".format(args.stream))

	# node ID starts from 1
	nid = 1
	cnt = 0

	description = '\x1b[6;30;43m[i]\x1b[0m Progress'
	pb = tqdm.tqdm(desc=description, mininterval=5.0, unit="recs")

	with open(args.input) as f:
		for line in f:
			pb.update()
			edge = line.strip().split("\t")
			try:
				srcID = db.get(edge[0])
				srcBool = None
				if srcID != None:	# Check if source ID has been seen before.
					edge[0] = srcID
					srcBool = "0"
				else:
					srcBool = "1"
					if args.memory:
						db[edge[0]] = str(nid)
					else:
						db.put(edge[0], str(nid))
					edge[0] = str(nid)
					nid = nid + 1

				dstID = db.get(edge[1])
				dstBool = None
				if dstID != None:	# Check if destination ID has been seen before.
					edge[1] = dstID
					dstBool = "0"
				else:
					dstBool = "1"
					if args.memory:
						db[edge[1]] = str(nid)
					else:
						db.put(edge[1], str(nid))
					edge[1] = str(nid)
					nid = nid + 1

				attributes = edge[2].strip().split(":")
				srctype = attributes[0]
				dsttype = attributes[1]
				edgetype = attributes[2]
				timestamp = attributes[3]
				
				if cnt < args.size:
					cnt = cnt + 1
					bf.write(edge[0] + ' ' + edge[1] + ' ' + srctype + ':' + dsttype + ':' + edgetype + ':' + timestamp + '\n')
				else:
					cnt = cnt + 1
					sf.write(edge[0] + ' ' + edge[1] + ' ' + srctype + ':' + dsttype + ':' + edgetype + ':' + srcBool + ":" + dstBool + ":" + timestamp + '\n')
			except:
				print("\x1b[6;30;41m\n[error]\x1b[0m  skipping this problematic line:{}".format(line))


	f.close()
	bf.close()
	sf.close()
	if args.information:
		stats = open("stats.txt", "a+")
		stats.write(str(nid) + '\t' + str(cnt) + '\n')
		stats.close()
	print("\x1b[6;30;42m\n[success]\x1b[0m  processing of {} is done. Data now can be accepted by the graph processing framework.".format(args.input))
