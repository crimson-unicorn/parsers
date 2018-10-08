import sys, os
import math

def truncate_run(fh):
	"""
	This is a generic function that truncates the json file and runs libpvm on the truncated data.
	@fh is the handle of the generated Makefile 
	"""
	fh.write("define truncate_data\n")
	fh.write("\tsed -n '$(1),$(2)p;$(3)q' $(4).json | ../../libpvm-rs/build/pvm2csv - ../../../data/$(6)/$(4)_$(5).zip ; \\\n")
	fh.write("\tmkdir ../../../data/$(6)/$(4)_$(5) ; \\\n")
	fh.write("\tunzip ../../../data/$(6)/$(4)_$(5).zip -d ../../../data/$(6)/$(4)_$(5) ; \\\n")
	fh.write("\trm ../../../data/$(6)/$(4)_$(5).zip\n")
	fh.write("endef\n")

def gen(filepath, name, chunks, ty, fh):
	"""
	This function takes a file @filepath with shortened name @name, calculate the places to truncate the file based on the number of chunks @chunks.
	Then it writes to the output file using its handle @fh.
	@ty determines it is "benign" or "attack"
	"""
	with open(filepath, "r") as f:
		for i, l in enumerate(f):
			pass
	lc = i + 1	# total line counts of the file
	f.close()
	fh.write(name + ":\n")

	interval = int(math.ceil(lc / int(chunks)))
	start = 1
	end = lc
	cnt = 0
	while start + interval < end:
		fh.write("\t$(call truncate_data,")
		fh.write(str(start) + "," + str(start + interval) + ",")
		start = start + interval + 1
		fh.write(str(start) + "," + name + "," + str(cnt) + "," + ty + ")\n")
		cnt += 1
	fh.write("\t$(call truncate_data,")
	fh.write(str(start) + "," + str(end) + "," + str(end + 1) + "," + name + "," + str(cnt) + "," + ty + ")\n")


if __name__ == "__main__":
	if (len(sys.argv) < 1):
		print("""
			Usage: python make_gen.py
		"""
		)
		sys.exit(1)

	makefile = open("Makefile", "a+")
	truncate_run(makefile)
	makefile.write("\n")
	gen("../../../data/benign/benign1.json", "benign1", 50, "benign", makefile)
	makefile.write("\n")
	gen("../../../data/benign/benign2.json", "benign2", 10, "benign", makefile)
	makefile.write("\n")
	gen("../../../data/benign/benign3.json", "benign3", 50, "benign", makefile)
	makefile.write("\n")
	gen("../../../data/attack/pandex.json", "pandex", 25, "attack", makefile)
	
	
	