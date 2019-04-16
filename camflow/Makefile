eval_camflow_apt:
	number=0 ; while [ $$number -le 0 ] ; do \
		cd ../../../data/camflow-apt/benign && mkdir camflow-benign-$$number && tar zxvf camflow-benign-$$number.gz.tar -C camflow-benign-$$number && mv camflow-benign-$$number/camflow-benign.txt.* ../edgelists_benign ; \
		cd ../../../data/camflow-apt/benign && rm -f camflow-benign-$$number.gz.tar && rm -rf camflow-benign-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 0 ] ; do \
		python prepare.py -i ../../../data/camflow-apt/edgelists_benign/camflow-benign.txt.$$number -o camflow-benign-$$number.txt ; \
		python parse.py -s $(INTERVAL) -b $(WINDOW) -i camflow-benign-$$number.txt -B ../../../data/camflow-apt/train/base/base-camflow-benign-$$number.txt -S ../../../data/camflow-apt/train/stream/stream-camflow-benign-$$number.txt ; \
		mv ts.txt ../../../output/ts-w-$(WINDOW)-i-$(INTERVAL).txt ; \
		number=`expr $$number + 1` ; \
	done