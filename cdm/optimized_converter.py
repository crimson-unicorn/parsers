#!/usr/bin/env python
from __future__ import print_function
import os, sys, argparse, hashlib
import tarfile as tf
import ijson.backends.yajl2_cffi as ijson
import partool.misc as ptm
import partool.check as ptc
import partool.jparse as ptj
from partool.prepare import *
import multiprocessing as mp
import yappi

def cprocess(fileobj, ds, fn):
	"""Iteratively process/scan an file object.

	Arguments:
	fileobj - file object
	ds - a database (for node parsing) or a sanitylog (for scanning)
	fn - file name
	"""
	parser = ijson.common.items(ijson.parse(fileobj, multiple_values=True), '')

	if args.trace == 'camflow':
		if args.scan:
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m scanning compressed file in CAMFLOW mode...")
			ptc.sanitycheckcf(parser, ds)

		else:
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m parsing compressed file in CAMFLOW mode...")
			ptj.parsecf(parser, ds, fn)

	elif args.trace == 'darpa':
		if args.scan:
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m scanning compressed file in DARPA mode...")
			ptc.sanitycheckdp(parser, ds)

		else:
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m parsing compressed file in DARPA mode...")
			ptj.parsedp(parser, ds, fn)

	else:
		raise NotImplementedError("cannot run traces from an unknown system")

	fileobj.close()
	return

def process(fn, out=None):
	"""Iteratively process an file object.

	Arguments:
	fn - file name
	args - arguments given
	ds - for parsing nodes/scanning; it can be a database (parsing) or a sanitylog (scanning)
	out - None for parsing nodes/scanning and output file object for output generation
	"""
	if args.profile:
			if args.verbose:
				print("\x1b[6;30;43m[i]\x1b[0m profiling is on...")
			yappi.clear_stats()
			yappi.set_clock_type('cpu')
			yappi.start(builtins=True)

	db = initdb(fn)

	with open(os.path.join(args.input, fn), 'r') as fileobj:
		parser = ijson.common.items(ijson.parse(fileobj, multiple_values=True), '')

		if args.trace == 'camflow':
			if out == None:
				if args.verbose:
					print("\x1b[6;30;42m[+]\x1b[0m parsing file {} in CAMFLOW mode...".format(fn))
				ptj.parsecf(parser, db, fn)

			else:
				if args.verbose:
					print("\x1b[6;30;42m[+]\x1b[0m generating output in CAMFLOW mode...")
					print("\x1b[6;30;43m[i]\x1b[0m initiating logging. Check error.log afterwards...")
				ptj.gencf(parser, out)

		elif args.trace == 'darpa':
			if out == None:
				if args.verbose:
					print("\x1b[6;30;42m[+]\x1b[0m parsing file {} in DARPA mode...".format(fn))
				ptj.parsedp(parser, db, fn)

			else:
				if args.verbose:
					print("\x1b[6;30;42m[+]\x1b[0m generating output in DARPA mode...")
					print("\x1b[6;30;43m[i]\x1b[0m initiating logging. Check error.log afterwards...")
				ptj.gendp(parser, out)

		else:
			raise NotImplementedError("cannot run traces from an unknown system")

	if args.profile:
		yappi.stop()
		if args.verbose:
			print("\x1b[6;30;43m[i]\x1b[0m profiling is done...")
		stat = yappi.get_func_stats()
		stat.save(fn + '.prof', type='callgrind')

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
	global args
	args = parser.parse_args()

	if args.scan:
		print("\x1b[6;30;42m[+]\x1b[0m creating sanity.log in current directory...")
		sanitylog = open('sanity.log', 'a+')
	else:
		print("\x1b[6;30;42m[+]\x1b[0m initiating parser...")
		# create process pool
		procs = list()
		nprocs = mp.cpu_count()
	
	if args.compact:
		# CURRENTLY, NO SUPPORT FOR MULTIPROCESSING IN COMPACT FILES
		if args.verbose:
			print("\x1b[6;30;43m[i]\x1b[0m no support for multiprocessing in compact files at the moment")
			print("\x1b[6;30;42m[+]\x1b[0m processing compressed JSON tar file in directory {}...".format(args.input))

		fns = os.listdir(args.input)
		# For compressed JSON tar file,
		# we only process one tar file at a time.
		if len(fns) != 1:
			raise ValueError("{} directory must contain one and only one tar file".format(args.input))

		if args.profile:
			if args.verbose:
				print("\x1b[6;30;43m[i]\x1b[0m profiling is on...")
			yappi.clear_stats()
			yappi.set_clock_type('cpu')
			yappi.start(builtins=True)

		with tf.open(os.path.join(args.input, fns[0]), 'r:gz') as gzf:
			filenames = gzf.getnames()
			sortedfilenames = ptm.sortfilenames(filenames)

			for sfn in sortedfilenames:
				fileobj = gzf.extractfile(gzf.getmember(sfn))
				if args.scan:
					if args.verbose:
						print("\x1b[6;30;42m[+]\x1b[0m start scanning compressed JSON gzip file {}...".format(sfn))
					cprocess(fileobj, sanitylog, sfn)
				else:
					if args.verbose:
						print("\x1b[6;30;42m[+]\x1b[0m start sequentially processing compressed JSON gzip file {}...".format(sfn))
					cprocess(fileobj, db, sfn)

		if args.profile:
			yappi.stop()
			if args.verbose:
				print("\x1b[6;30;43m[i]\x1b[0m profiling is done...")
			yappi.get_func_stats().print_all()
			yappi.get_thread_stats().print_all()

		gzf.close()

	else:
		if args.verbose:
			print("\x1b[6;30;43m[i]\x1b[0m multiprocessing support is on for processing but not scanning")
			print("\x1b[6;30;42m[+]\x1b[0m processing regular JSON files in directory {}...".format(args.input))

		fns = os.listdir(args.input)
		sortedfilenames = ptm.sortfilenames(fns)

		if args.scan:
			for sfn in sortedfilenames: 
				if args.verbose:
					print("\x1b[6;30;42m[+]\x1b[0m start scanning regular JSON gzip file {}...".format(sfn))
				fileobj = open(os.path.join(args.input, sfn), 'r')
				cprocess(fileobj, sanitylog, sfn)
		else:
			if args.verbose:
				print("\x1b[6;30;43m[i]\x1b[0m multiprocesses processing regular JSON files")
			for i in range(len(sortedfilenames)):
				p = mp.Process(target=process, \
								args=(sortedfilenames[i],))
				procs.append(p)
				p.start()

			for p in procs:
				p.join()

	if args.scan:
		sanitylog.close()
		print("\x1b[6;30;42m[+]\x1b[0m sanity checking is done")
		print("\x1b[6;30;43m[i]\x1b[0m check sanity.log in the current directory for results")
	else:
		print("\x1b[6;30;42m[+]\x1b[0m node parsing is done")

#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#

	print "parsing files again sequentially to output final results at {}".format(args.output)
	if args.verbose:
		print "opening output file {} for writing...".format(args.output)
	ofile = open(args.output, 'os.O_WRONLY | os.O_APPEND | os.O_CREAT')

	# if args.compact:
	# 	if args.verbose:
	# 		print "processing compressed JSON tar file in directory {}...".format(args.input)

	# 	fns = os.listdir(args.input)
	# 	# For compressed JSON tar file,
	# 	# we only process one tar file at a time.
	# 	if len(fns) != 1:
	# 		raise ValueError("{} directory must contain one and only one tar file".format(args.input))

	# 	if args.profile:
	# 		if args.verbose:
	# 			print "profiling scanning/parsing simultaneously..."
	# 		yappi.clear_stats()
	# 		yappi.set_clock_type('cpu')
	# 		yappi.start(builtins=True)

	# 	with tf.open(os.path.join(args.input, fns[0]), 'r:gz') as gzf:
	# 		filenames = gzf.getnames()
	# 		sortedfilenames = ptm.sortfilenames(filenames)

	# 		for sfn in sortedfilenames:
	# 			fileobj = gzf.extractfile(gzf.getmember(sfn))
				
	# 			if args.verbose:
	# 				print "start parsing compressed JSON gzip file {} again and generating outputs...".format(sfn)
	# 			parsefile(fileobj, db, ofile)

	# 	if args.profile:
	# 		yappi.stop()
	# 		yappi.get_func_stats().print_all()
	# 		yappi.get_thread_stats().print_all()

	# 	gzf.close()
	# else:
	# 	if args.verbose:
	# 		print "processing regular JSON files in directory {}...".format(args.input)

	# 	fns = os.listdir(args.input)
	# 	sortedfilenames = ptm.sortfilenames(fns)
		
	# 	if args.profile:
	# 		if args.verbose:
	# 			print "profiling scanning/parsing simultaneously..."
	# 		yappi.clear_stats()
	# 		yappi.set_clock_type('cpu')
	# 		yappi.start(builtins=True)

	# 	for sfn in sortedfilenames: 
	# 		fileobj = open(os.path.join(args.input, sfn), 'r')
	# 		if args.verbose:
	# 			print "start parsing regular JSON file {} again and generating outputs...".format(sfn)
	# 		parsefile(fileobj, db, ofile)

	# 	if args.profile:
	# 		yappi.stop()
	# 		yappi.get_func_stats().print_all()
	# 		yappi.get_thread_stats().print_all()
	
	# ofile.close()
	# print "all done"

