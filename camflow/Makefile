eval_camflow_apt_prepare:
	number=0 ; while [ $$number -le 0 ] ; do \
		cd ../../../data/camflow-apt && mkdir camflow-benign-$$number && tar zxvf wget-normal-raw-$$number.gz.tar -C camflow-benign-$$number && mv camflow-benign-$$number/normal-camflow-*.data edgelists_benign/ ; \
		cd ../../../data/camflow-apt && rm -f wget-normal-raw-$$number.gz.tar && rm -rf camflow-benign-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 0 ] ; do \
		python prepare.py -i ../../../data/camflow-apt/edgelists_benign/normal-camflow-$$number.data -o camflow-benign-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

eval_camflow_apt:
	number=0 ; while [ $$number -le 0 ] ; do \
		python parse.py -s $(INTERVAL) -b $(WINDOW) -i camflow-benign-$$number.txt -B ../../../data/camflow-apt/train/base/base-camflow-benign-$$number.txt -S ../../../data/camflow-apt/train/stream/stream-camflow-benign-$$number.txt ; \
		mv ts.txt ../../../output/ts-camflow-benign-$$number-w-$(WINDOW)-i-$(INTERVAL).txt ; \
		number=`expr $$number + 1` ; \
	done