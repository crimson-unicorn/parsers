#!/usr/bin/env python
from __future__ import print_function
import os, sys, argparse
import tarfile as tf
import ijson.backends.yajl2_cffi as ijson
import partool.misc as ptm
import partool.check as ptc
import partool.jparse as ptj
from partool.prepare import *
import multiprocessing as mp
import yappi
import rocksdb

def cprocess(fileobj, ds, fn, out=None):
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
				print("\x1b[6;30;42m[+]\x1b[0m scanning file {} in CAMFLOW mode...".format(fn))
			ptc.sanitycheckcf(parser, ds)

		elif out == None:
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m parsing compressed file {} in CAMFLOW mode...".format(fn))
			ptj.parsecf(parser, ds, fn)

		else:
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m generating output for file {} in CAMFLOW mode...".format(fn))
				print("\x1b[6;30;43m[i]\x1b[0m initiating logging. Check error.log afterwards...")
			ptj.cgencf(parser, ds, out)


	elif args.trace == 'darpa':
		if args.scan:
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m scanning file {} in DARPA mode...".format(fn))
			ptc.sanitycheckdp(parser, ds)

		elif out == None:
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m parsing compressed file {} in DARPA mode...".format(fn))
			ptj.parsedp(parser, ds, fn)

		else:
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m generating output for file {} in DARPA mode...".format(fn))
				print("\x1b[6;30;43m[i]\x1b[0m initiating logging. Check error.log afterwards...")
			ptj.cgendp(parser, ds, out)

	elif args.trace == 'cadets2' or args.trace == 'fivedirections':
		if args.scan:
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m scanning file {} in CADETS2/FIVEDIRECTIONS mode...".format(fn))
			ptc.sanitycheckcd(parser, ds)

		elif out == None:
			raise NotImplementedError("no support for processing {} compact files at the moment".format(args.trace))

		else:
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m generating output for file {} in CADETS2/FIVEDIRECTIONS mode...".format(fn))
				print("\x1b[6;30;43m[i]\x1b[0m initiating logging. Check error.log afterwards...")
			ptj.cgencd(parser, ds, out)

	else:
		raise NotImplementedError("cannot run traces from an unknown system")

	fileobj.close()
	return

def process(fn):
	"""Iteratively process an file object.

	Arguments:
	fn - file name
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
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m parsing file {} in CAMFLOW mode...".format(fn))
			ptj.parsecf(parser, db, fn)

		elif args.trace == 'darpa':
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m parsing file {} in DARPA mode...".format(fn))
			ptj.parsedp(parser, db, fn)

		elif args.trace == 'cadets2' or args.trace == 'fivedirections':
			if args.verbose:
				print("\x1b[6;30;42m[+]\x1b[0m parsing file {} in CADETS2/FIVEDIRECTIONS mode...".format(fn))
			ptj.parsecd(parser, db, fn)

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


def gprocess(i, fns):
	"""Iteratively parse the file object and generate the output

	Arguments:
	i: the index of the file to process
	fns: a list of filenames
	"""
	dbs = list()
	for fn in fns:
		try:
			db = rocksdb.DB(fn + '.db', rocksdb.Options(create_if_missing=False), read_only=True)
			dbs.append(db)
		except:
			raise ValueError("Given DB: {}.db does not exist. Are you sure the name is correct?".format(fn))
	
	fileobj = open(os.path.join(args.input, fns[i]), 'r')
	
	ofilename = args.trace + '-out.' + str(i) + '.txt'
	if args.verbose:
		print("\x1b[6;30;43m[i]\x1b[0m opening output file {} for writing...".format(ofilename))
	ofile = open(ofilename, 'a+')

	parser = ijson.common.items(ijson.parse(fileobj, multiple_values=True), '')

	if args.trace == 'camflow':
		if args.verbose:
			print("\x1b[6;30;42m[+]\x1b[0m parsing file {} in CAMFLOW mode...".format(i))
		ptj.gencf(parser, i, dbs, ofile)

	elif args.trace == 'darpa':
		if args.verbose:
			print("\x1b[6;30;42m[+]\x1b[0m parsing file {} in DARPA mode...".format(i))
		ptj.gendp(parser, i, dbs, ofile)

	elif args.trace == 'cadets2' or args.trace == 'fivedirections':
		if args.verbose:
			print("\x1b[6;30;42m[+]\x1b[0m parsing file {} in CADETS2/FIVEDIRECTIONS mode...".format(i))
		ptj.gencd(parser, i, dbs, ofile)

	else:
		raise NotImplementedError("cannot run traces from an unknown system")

	fileobj.close()
	ofile.close()
	return

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Convert JSON datasets to Unicorn edgelist datasets.')
	parser.add_argument('-v', '--verbose', action='store_true', help='increase console verbosity')
	parser.add_argument('-t', '--trace', help='tracing system that generates the input', 
		choices=['camflow', 'darpa', 'cadets2', 'fivedirections'], required=True)
	parser.add_argument('-i', '--input', help='input data folder', required=True)
	parser.add_argument('-c', '--compact', help='input data is compressed',  action='store_true')
	parser.add_argument('-s', '--scan', help='scan input data for sanity check', action='store_true')
	parser.add_argument('-p', '--profile', help='profile the code for performance analysis', action='store_true')
	parser.add_argument('-C', '--comma', help='use coma as a separator to parse JSON objects (for FiveDirections datasets)', action='store_true')
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

	if args.comma:
		print("\x1b[6;30;41m[x]\x1b[0m Trailing comas are not supported. Run the following code to remove the tailing coma at the end of each line first:")
		print("\x1b[6;30;43m[i]\x1b[0m sed 's/.$//' < INPUT_FILE > OUTPUT_FILE")
		raise NotImplementedError("no support for malformatted FiveDirections datasets at the moment; a preprocessing step is needed to be implemented.")
	
	if args.compact:
		# CURRENTLY, NO SUPPORT FOR MULTIPROCESSING IN COMPACT FILES
		# CURRENTLY, NO SUPPORT FOR CADETS2 DATASETS
		if args.trace == 'cadets2' or args.trace == 'fivedirections':
			raise NotImplementedError("no support for processing {} compact files at the moment".format(args.trace))

		if args.verbose:
			print("\x1b[6;30;43m[i]\x1b[0m no support for multiprocessing in compact files at the moment")
			print("\x1b[6;30;42m[+]\x1b[0m processing compressed JSON tar file in directory {}...".format(args.input))

		fns = os.listdir(args.input)
		# For compressed JSON tar file,
		# we only process one tar file at a time.
		if len(fns) != 1:
			raise ValueError("{} directory must contain one and only one tar file".format(args.input))

		if not args.scan:
			db = initdb(fns[0])

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
					print("\x1b[6;30;42m[+]\x1b[0m start scanning regular JSON file {}...".format(sfn))
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

#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#
#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#+#

	print("\x1b[6;30;42m[+]\x1b[0m parsing files again to output final results.")

	if args.compact:
		if args.trace == 'cadets2' or args.trace == 'fivedirections':
			raise NotImplementedError("no support for processing {} compact files at the moment".format(args.trace))

		ofilename = args.trace + '-out.txt'
		if args.verbose:
			print("\x1b[6;30;43m[i]\x1b[0m opening output file {} for writing...".format(ofilename))
		ofile = open(ofilename, 'a+')

		if args.verbose:
			print("\x1b[6;30;43m[i]\x1b[0m no support for multiprocessing in compact files at the moment")
			print("\x1b[6;30;42m[+]\x1b[0m processing compressed JSON tar file in directory {}...".format(args.input))

		fns = os.listdir(args.input)
		if len(fns) != 1:
			raise ValueError("{} directory must contain one and only one tar file".format(args.input))

		db = rocksdb.DB(fns[0] + '.db')

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
				
				if args.verbose:
					print("\x1b[6;30;43m[i]\x1b[0m start parsing compressed JSON gzip file {} again and generating outputs...".format(sfn))
				cprocess(fileobj, db, sfn, ofile)

		if args.profile:
			yappi.stop()
			if args.verbose:
				print("\x1b[6;30;43m[i]\x1b[0m profiling is done...")
			yappi.get_func_stats().print_all()
			yappi.get_thread_stats().print_all()

		gzf.close()
		ofile.close()

	else:
		if args.verbose:
			print("\x1b[6;30;43m[i]\x1b[0m multiprocessing support is on...")
			print("\x1b[6;30;42m[+]\x1b[0m processing regular JSON files in directory {}...".format(args.input))

		fns = os.listdir(args.input)
		sortedfilenames = ptm.sortfilenames(fns)

		if len(sortedfilenames) > 1:
			gprocs = list()

		elif len(sortedfilenames) == 1:
			ofilename = args.trace + '-out.txt'
			if args.verbose:
				print("\x1b[6;30;43m[i]\x1b[0m opening output file {} for writing...".format(ofilename))
			ofile = open(ofilename, 'a+')
		else:
			raise ValueError("at least one file is needed as an input")

		if args.profile:
			if args.verbose:
				print("\x1b[6;30;43m[i]\x1b[0m profiling is on...")
			yappi.clear_stats()
			yappi.set_clock_type('cpu')
			yappi.start(builtins=True)

		if len(sortedfilenames) == 1:
			try:
				db = rocksdb.DB(sortedfilenames[0] + '.db', rocksdb.Options(create_if_missing=False))
			except:
				raise ValueError("Given DB: {}.db does not exist. Are you sure the name is correct?".format(sortedfilenames[0]))
				
			fileobj = open(os.path.join(args.input, sortedfilenames[0]), 'r')
			if args.verbose:
				print("\x1b[6;30;43m[i]\x1b[0m start parsing regular JSON file {} again and generating outputs...".format(sortedfilenames[0]))
			cprocess(fileobj, db, sortedfilenames[0], ofile)
			ofile.close()
		else:
			if args.verbose:
				print("\x1b[6;30;43m[i]\x1b[0m multiprocesses processing regular JSON files")
			for i in range(len(sortedfilenames)):
				gp = mp.Process(target=gprocess, \
							args=(i, sortedfilenames,))
				gprocs.append(gp)
				gp.start()

			for gp in gprocs:
				gp.join()				

		if args.profile:
			yappi.stop()
			if args.verbose:
				print("\x1b[6;30;43m[i]\x1b[0m profiling is done...")
			stat = yappi.get_func_stats()
			stat.save('gen.prof', type='callgrind')
	
	print("\x1b[6;30;42m[+]\x1b[0m finished")

