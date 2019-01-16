#!/usr/bin/env python

import os, sys, argparse, hashlib, logging
import tarfile as tf
import ijson.backends.yajl2_cffi as ijson
import partool.misc as ptm
import partool.check as ptc
import partool.jparse as ptj
import multiprocessing as mp
import yappi
import rocksdb

def parsefile(fileobj, args, db):
	"""Iteratively process an file object based on given arguments.

	Arguments:
	fileobj - an file object
	args - arguments given
	db - database
	"""
	parser = ijson.common.items(ijson.parse(fileobj, multiple_values=True), '')
	if args.trace == 'camflow':
		if args.verbose:
			print "parsing in CAMFLOW mode..."
		
		ptj.parsecf(parser, db)

	elif args.trace == 'darpa':
		if args.verbose:
			print "parsing in DARPA mode..."

		ptj.parsedp(parser, db)

	else:
		raise NotImplementedError("cannot run traces from an unknown system")

	fileobj.close()
	return

def scanfile(fileobj, args, sanitylog):
	"""Scan a file to perform sanity check.

	Arguments:
	fileobj - an file object
	args - arguments given
	sanitylot - log to write the results to
	"""
	parser = ijson.common.items(ijson.parse(fileobj, multiple_values=True), '')
	if args.trace == 'camflow':
		if args.verbose:
			print "scanning in CAMFLOW mode..."
		ptc.sanitycheckcf(parser, sanitylog)

	elif args.trace == 'darpa':
		if args.verbose:
			print "scanning in DARPA mode..."
		ptc.sanitycheckdp(parser, sanitylog)
	else:
		raise NotImplementedError("cannot scan traces from an unknown system")
	
	fileobj.close()
	return


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Convert JSON datasets to Unicorn edgelist datasets.')
	parser.add_argument('-v', '--verbose', action='store_true', help='increase console verbosity')
	parser.add_argument('-t', '--trace', help='tracing system that generates the input', 
		choices=['camflow', 'darpa'], required=True)
	parser.add_argument('-i', '--input', help='input data folder', required=True)
	parser.add_argument('-c', '--compact', help='input data is compressed',  action='store_true')
	parser.add_argument('-s', '--scan', help='scan input data for sanity check', action='store_true')
	parser.add_argument('-p', '--profile', help='profile the code for performance analysis', action='store_true')
	parser.add_argument('-o', '--output', help='output data path', required=True)
	args = parser.parse_args()

	if args.scan:
		print "scanning files one at a time to peform sanity check only. Turn on verbosity for more details..."
		sanitylog = open('sanity.log', 'a+')
	else:
		# create process pool
		nprocs = mp.cpu_count()
		pool = mp.Pool(nprocs)

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

		db = rocksdb.DB("nodes.db", opts)
	
	if args.compact:
		if args.verbose:
			print "processing compressed JSON tar files in directory {}...".format(args.input)

		fns = os.listdir(args.input)
		# For compressed JSON tar file,
		# we only process one tar file at a time.
		if len(fns) != 1:
			raise ValueError("{} directory must contain one and only one tar file".format(args.input))
		with tf.open(os.path.join(args.input, fns[0]), 'r:gz') as gzf:
			filenames = gzf.getnames()
			sortedfilenames = ptm.sortfilenames(filenames)

			if not args.scan:
				funcargs = list()

			for sfn in sortedfilenames:
				fileobj = gzf.extractfile(gzf.getmember(sfn))
				if args.scan:
					if args.verbose:
						print "start scanning compressed JSON gzip file {}...".format(sfn)
					scanfile(fileobj, args, sanitylog)
				else:
					funcargs.append((fileobj, args, db))
		if not args.scan:
			pool.map(parsefile, funcargs)
		gzf.close()
	else:
		if args.verbose:
			print "processing regular JSON files in directory {}...".format(args.input)

		fns = os.listdir(args.input)
		sortedfilenames = ptm.sortfilenames(fns)
		funcargs = list()
		yappi.start()
		for sfn in sortedfilenames: 
			fileobj = open(os.path.join(args.input, sfn), 'r')
			if args.scan:
				if args.verbose:
					print "start scanning regular JSON file {}...".format(sfn)
				scanfile(fileobj, args, sanitylog)
			else:
				funcargs.append((fileobj, args, db))
		if not args.scan:
			pool.map(parsefile, funcargs)
		yappi.get_func_stats().print_all()
		yappi.get_thread_stats().print_all()
	if args.scan:
		sanitylog.close()
		print "sanity checking is done"
		print "check sanity.log in the current directory for results"
	else:
		print "parsing is done"
