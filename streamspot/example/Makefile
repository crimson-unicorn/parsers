example:
	test -f venv/bin/activate || virtualenv -p $(shell which python) venv
	. venv/bin/activate ; \
		pip install tqdm ; \
		python ../parse.py -g 0 -i data.txt -b base.txt -S stream.txt

clean:
	rm -f base.txt stream.txt
	rm -rf venv
