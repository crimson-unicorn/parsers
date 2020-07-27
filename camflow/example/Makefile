example:
	test -f venv/bin/activate || virtualenv -p $(shell which python) venv
	. venv/bin/activate ; \
		pip install xxhash tqdm ; \
		python ../prepare.py -i data.txt -o preprocessed.txt ; \
		python ../parse.py -i preprocessed.txt -B base.txt -S stream.txt

stats:
	test -f venv/bin/activate || virtualenv -p $(shell which python) venv
	. venv/bin/activate ; \
		pip install xxhash tqdm ; \
		python ../prepare.py -s -v -i data.txt -o preprocessed.txt ; \
		python ../parse.py -i preprocessed.txt -B base.txt -S stream.txt -s -I 10

clean:
	rm -rf preprocessed.txt debug.log stats.csv base.txt stream.txt ts.txt
	rm -rf venv
