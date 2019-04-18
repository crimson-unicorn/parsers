camflow-apt:
	number=0 ; while [ $$number -le 124 ] ; do \
		python up.py -v -m -i benign/camflow-benign.txt.$$number -b base.txt -s stream.txt -I; \
		number=`expr $$number + 1` ; \
	done