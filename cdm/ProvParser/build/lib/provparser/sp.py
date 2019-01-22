#!/usr/bin/env python
from __future__ import print_function
import os, sys, argparse
import tarfile as tf
import partool.misc as ptm

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Decompress selected files within a tar file.')
	parser.add_argument('-v', '--verbose', action='store_true', help='increase console verbosity')
	parser.add_argument('-i', '--input', help='input data file', required=True)
	parser.add_argument('-s', '--size', help='number of files to decompress', type=int, required=True)
	parser.add_argument('-f', '--front', help='starting index of the file to decompress (0 - Max)', type=int, required=True)
	global args
	args = parser.parse_args()

	if args.verbose:
		print("\x1b[6;30;43m[i]\x1b[0m use this program to extract a subset of gzip files as input for pp.py program.")
		print("\x1b[6;30;43m[i]\x1b[0m files are extracted in the desired order starting from the start index gived by -f flag.")
		print("\x1b[6;30;43m[i]\x1b[0m suppress verbosity by omitting -v flag.")

	with tf.open(args.input, 'r:gz') as gzf:
		if args.verbose:
			print("\x1b[6;30;42m[+]\x1b[0m opening gzip tar file {} to prepare for extraction...".format(args.input))

		filenames = gzf.getnames()
		sortedfilenames = ptm.sortfilenames(filenames)

		maxnum = len(sortedfilenames)
		if args.verbose:
			print("\x1b[6;30;42m[+]\x1b[0m there are {} files in the gzip tar file {}...".format(maxnum, args.input))

		startind = args.front
		if startind >= maxnum:
			print("\x1b[6;30;43m[i]\x1b[0m the starting point is larger than the total number of files ({}). Are you sure you are not done yet?".format(maxnum))
			print("\x1b[6;30;41m[x]\x1b[0m nothing to do")
			exit(0)
		elif startind < 0:
			print("\x1b[6;30;43m[i]\x1b[0m bad starting input: {}".format(startind))
			print("\x1b[6;30;41m[x]\x1b[0m exit the program because of bad input")
			exit(0)

		endind = startind + args.size
		if endind > maxnum:
			if args.verbose:
				print("\x1b[6;30;43m[i]\x1b[0m the ending index ({}) will exceed the number of files ({}) in the tar file. You need not run this program again to extract more files".format(endind, maxnum))
		
		for sfn in sortedfilenames[startind:endind]:
			gzf.extract(gzf.getmember(sfn))

	gzf.close()
	print("\x1b[6;30;42m[+]\x1b[0m this round of extraction is completed")
