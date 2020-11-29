toy:
	cd ../../data && mkdir -p toy_data
	cd ../../data/toy_data && mkdir -p base_train && mkdir -p stream_train
	cd ../../data/toy_data && mkdir -p base_test && mkdir -p stream_test
	test -f venv/bin/activate || virtualenv -p $(shell which python) venv
	. venv/bin/activate ; \
		pip install tqdm pandas ; \
		number=0 ; while [ $$number -le 99 ] ; do \
			python streamspot/parse_fast.py -g $$number -a -i ../../data/all.tsv -b ../../data/toy_data/base_train/base-toy-$$number.txt -S ../../data/toy_data/stream_train/stream-toy-$$number.txt ; \
			number=`expr $$number + 4` ; \
			rm -f tmp.csv ; \
		done ; \
		number=300 ; while [ $$number -le 399 ] ; do \
			python streamspot/parse_fast.py -g $$number -a -i ../../data/all.tsv -b ../../data/toy_data/base_test/base-attack-$$number.txt -S ../../data/toy_data/stream_test/stream-attack-$$number.txt ; \
			number=`expr $$number + 16` ; \
			rm -f tmp.csv ; \
		done

youtube:
	cd ../../data && mkdir -p youtube_data
	cd ../../data/youtube_data && mkdir -p base_train && mkdir -p stream_train
	number=0 ; while [ $$number -le 99 ] ; do \
		python streamspot/parse.py $$number ../../data/all.tsv ../../data/youtube_data/base_train/base-youtube-$$number.txt ../../data/youtube_data/stream_train/stream-youtube-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

gmail:
	cd ../../data && mkdir -p gmail_data
	cd ../../data/gmail_data && mkdir -p base_train && mkdir -p stream_train
	number=100 ; while [ $$number -le 199 ] ; do \
		python streamspot/parse.py $$number ../../data/all.tsv ../../data/gmail_data/base_train/base-gmail-$$number.txt ../../data/gmail_data/stream_train/stream-gmail-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

vgame:
	cd ../../data && mkdir -p vgame_data
	cd ../../data/vgame_data && mkdir -p base_train && mkdir -p stream_train
	number=200 ; while [ $$number -le 299 ] ; do \
		python streamspot/parse.py $$number ../../data/all.tsv ../../data/vgame_data/base_train/base-vgame-$$number.txt ../../data/vgame_data/stream_train/stream-vgame-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

download:
	cd ../../data && mkdir -p download_data
	cd ../../data/download_data && mkdir -p base_train && mkdir -p stream_train
	number=400 ; while [ $$number -le 499 ] ; do \
		python streamspot/parse.py $$number ../../data/all.tsv ../../data/download_data/base_train/base-download-$$number.txt ../../data/download_data/stream_train/stream-download-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

cnn:
	cd ../../data && mkdir -p cnn_data
	cd ../../data/cnn_data/ && mkdir -p base_train && mkdir -p stream_train
	number=500 ; while [ $$number -le 599 ] ; do \
		python streamspot/parse.py $$number ../../data/all.tsv ../../data/cnn_data/base_train/base-cnn-$$number.txt ../../data/cnn_data/stream_train/stream-cnn-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

attack:
	cd ../../data && mkdir -p attack_data
	cd ../../data/attack_data && mkdir -p base_train && mkdir -p stream_train
	test -f venv/bin/activate || virtualenv -p $(shell which python) venv
	 . venv/bin/activate ; \
		pip install tqdm ; \
		number=300 ; while [ $$number -le 399 ] ; do \
			python streamspot/parse.py -g $$number -i ../../data/all.tsv -b ../../data/attack_data/base_train/base-attack-$$number.txt -S ../../data/attack_data/stream_train/stream-attack-$$number.txt ; \
			number=`expr $$number + 1` ; \
		done

evasion:
	cd ../../data && mkdir -p evasion_data
	cd ../../data/evasion_data && mkdir -p base_train && mkdir -p stream_train
	test -f venv/bin/activate || virtualenv -p $(shell which python) venv
	. venv/bin/activate ; \
		pip install tqdm ; \
		number=0 ; while [ $$number -le 99 ] ; do \
			python streamspot/parse.py -g 0 -i ../../data/evasion_raw/dispersedGraph-579-300-targeted-$$number.csv -b ../../data/evasion_data/base_train/base-evasion-$$number.txt -S ../../data/evasion_data/stream_train/stream-evasion-$$number.txt ; \
			number=`expr $$number + 1` ; \
		done

camflow_toy:
	cd ../../data && mkdir -p camflow_train_toy
	cd ../../data/camflow_train_toy/ && mkdir -p base && mkdir -p stream
	cd ../../data && mkdir -p camflow_test_toy
	cd ../../data/camflow_test_toy/ && mkdir -p base && mkdir -p stream
	test -f venv/bin/activate || virtualenv -p $(shell which python) venv ; \
		. venv/bin/activate ; \
		pip install xxhash tqdm ; \
		python camflow/prepare.py -i ../../data/camflow_new_raw/attack1.data -o preprocessed.txt ; \
		python camflow/parse.py -b 4000 -i preprocessed.txt -B ../../data/camflow_test_toy/base/base-camflow-1.txt -S ../../data/camflow_test_toy/stream/stream-camflow-1.txt ; \
		rm preprocessed.txt ; \
		python camflow/prepare.py -i ../../data/camflow_new_raw/attack2.data -o preprocessed.txt ; \
		python camflow/parse.py -b 4000 -i preprocessed.txt -B ../../data/camflow_test_toy/base/base-camflow-2.txt -S ../../data/camflow_test_toy/stream/stream-camflow-2.txt ; \
		rm preprocessed.txt ; \
		python camflow/prepare.py -i ../../data/camflow_new_raw/attack3.data -o preprocessed.txt ; \
		python camflow/parse.py -b 4000 -i preprocessed.txt -B ../../data/camflow_test_toy/base/base-camflow-3.txt -S ../../data/camflow_test_toy/stream/stream-camflow-3.txt ; \
		rm preprocessed.txt ; \
		python camflow/prepare.py -i ../../data/camflow_new_raw/attack5.data -o preprocessed.txt ; \
		python camflow/parse.py -b 4000 -i preprocessed.txt -B ../../data/camflow_test_toy/base/base-camflow-5.txt -S ../../data/camflow_test_toy/stream/stream-camflow-5.txt ; \
		rm preprocessed.txt ; \
		python camflow/prepare.py -i ../../data/camflow_new_raw/attack6.data -o preprocessed.txt ; \
		python camflow/parse.py -b 4000 -i preprocessed.txt -B ../../data/camflow_test_toy/base/base-camflow-6.txt -S ../../data/camflow_test_toy/stream/stream-camflow-6.txt ; \
		rm preprocessed.txt ; \
		python camflow/prepare.py -i ../../data/camflow_new_raw/normal0.data -o preprocessed.txt ; \
		python camflow/parse.py -b 4000 -i preprocessed.txt -B ../../data/camflow_train_toy/base/base-camflow-0.txt -S ../../data/camflow_train_toy/stream/stream-camflow-0.txt ; \
		rm preprocessed.txt ; \
		python camflow/prepare.py -i ../../data/camflow_new_raw/normal1.data -o preprocessed.txt ; \
		python camflow/parse.py -b 4000 -i preprocessed.txt -B ../../data/camflow_train_toy/base/base-camflow-1.txt -S ../../data/camflow_train_toy/stream/stream-camflow-1.txt ; \
		rm preprocessed.txt ; \
		python camflow/prepare.py -i ../../data/camflow_new_raw/attack0.data -o preprocessed.txt ; \
		python camflow/parse.py -b 4000 -i preprocessed.txt -B ../../data/camflow_test_toy/base/base-camflow-0.txt -S ../../data/camflow_test_toy/stream/stream-camflow-0.txt ; \
		rm preprocessed.txt ; \
		python camflow/prepare.py -i ../../data/camflow_new_raw/attack4.data -o preprocessed.txt ; \
		python camflow/parse.py -b 4000 -i preprocessed.txt -B ../../data/camflow_test_toy/base/base-camflow-4.txt -S ../../data/camflow_test_toy/stream/stream-camflow-4.txt ; \
		rm preprocessed.txt ; \

prepare_camflow:
	number=0 ; while [ $$number -le 12 ] ; do \
		cd ../../data/camflow-apt-raw/ && tar xzf wget-normal-raw-$$number.gz.tar ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 2 ] ; do \
		cd ../../data/camflow-apt-raw/ && tar xzf wget-attack-raw-$$number.gz.tar ; \
		number=`expr $$number + 1` ; \
	done

camflow_train_full:
	cd ../../data && mkdir -p camflow_train_full
	cd ../../data/camflow_train_full/ && mkdir -p base && mkdir -p stream
	number=0 ; while [ $$number -le 124 ] ; do \
		test -f venv/bin/activate || virtualenv -p $(shell which python) venv ; \
		. venv/bin/activate ; \
		pip install xxhash tqdm ; \
		python camflow/prepare.py -i ../../data/camflow-apt-raw/normal-camflow-$$number.data -o preprocessed.txt ; \
                python camflow/parse.py -b 1 -i preprocessed.txt -B ../../data/camflow_train_full/base/base-camflow-$$number.txt -S ../../data/camflow_train_full/stream/stream-camflow-$$number.txt ; \
		rm preprocessed.txt ; \
		number=`expr $$number + 1` ; \
	done

camflow_test_full:
	cd ../../data && mkdir -p camflow_test_full
	cd ../../data/camflow_test_full/ && mkdir -p base && mkdir -p stream
	number=0 ; while [ $$number -le 24 ] ; do \
	       test -f venv/bin/activate || virtualenv -p $(shell which python) venv ; \
	       . venv/bin/activate ; \
	       pip install xxhash tqdm ; \
	       python camflow/prepare.py -i ../../data/camflow-apt-raw/camflow-attack-$$number.log -o preprocessed.txt ; \
	       python camflow/parse.py -b 1 -i preprocessed.txt -B ../../data/camflow_test_full/base/base-camflow-$$number.txt -S ../../data/camflow_test_full/stream/stream-camflow-$$number.txt ; \
	       rm preprocessed.txt ; \
	       number=`expr $$number + 1` ; \
	done

wget_train:
	cd ../../data/benign && mkdir -p base && mkdir -p stream
	number=0 ; while [ $$number -le 109 ] ; do \
		python camflow/prepare.py ../../data/benign/wget-normal-$$number.log wget-normal-preprocessed-$$number.txt ; \
		python camflow/parse.py wget-normal-preprocessed-$$number.txt ../../data/benign/base/base-wget-$$number.txt ../../data/benign/stream/stream-wget-$$number.txt ; \
		rm error.log ; \
		rm wget-normal-preprocessed-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

wget_baseline_attack:
	cd ../../data/attack_baseline && mkdir -p base && mkdir -p stream
	number=0 ; while [ $$number -le 24 ] ; do \
		python camflow/prepare.py ../../data/attack_baseline/wget-baseline-attack-$$number.log wget-baseline-attack-preprocessed-$$number.txt ; \
		python camflow/parse.py wget-baseline-attack-preprocessed-$$number.txt ../../data/attack_baseline/base/base-wget-attack-baseline-$$number.txt ../../data/attack_baseline/stream/stream-wget-attack-baseline-$$number.txt ; \
		rm error.log ; \
		rm wget-baseline-attack-preprocessed-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

wget_interval_attack:
	cd ../../data/attack_interval && mkdir -p base && mkdir -p stream
	number=0 ; while [ $$number -le 24 ] ; do \
		python camflow/prepare.py ../../data/attack_interval/wget-interval-attack-$$number.log wget-interval-attack-preprocessed-$$number.txt ; \
		python camflow/parse.py wget-interval-attack-preprocessed-$$number.txt ../../data/attack_interval/base/base-wget-attack-interval-$$number.txt ../../data/attack_interval/stream/stream-wget-attack-interval-$$number.txt ; \
		rm error.log ; \
		rm wget-interval-attack-preprocessed-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

cadets_prepare:
	python cadets/make_gen.py
	cd cadets && make benign1 && make benign2 && make benign3 && make pandex

cadets_train:
	cd ../../data/benign && mkdir -p base && mkdir -p stream
	number=0 ; while [ $$number -le 49 ] ; do \
		python cadets/prepare.py ../../data/benign/benign1_$$number benign1-$$number.txt ; \
		python camflow/parse.py benign1-$$number.txt ../../data/benign/base/base-benign1-$$number.txt ../../data/benign/stream/stream-benign1-$$number.txt ; \
		rm -rf ../../data/benign/benign1_$$number ; \
		rm benign1-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 9 ] ; do \
		python cadets/prepare.py ../../data/benign/benign2_$$number benign2-$$number.txt ; \
		python camflow/parse.py benign2-$$number.txt ../../data/benign/base/base-benign2-$$number.txt ../../data/benign/stream/stream-benign2-$$number.txt ; \
		rm -rf ../../data/benign/benign2_$$number ; \
		rm benign2-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 49 ] ; do \
		python cadets/prepare.py ../../data/benign/benign3_$$number benign3-$$number.txt ; \
		python camflow/parse.py benign3-$$number.txt ../../data/benign/base/base-benign3-$$number.txt ../../data/benign/stream/stream-benign3-$$number.txt ; \
		rm -rf ../../data/benign/benign3_$$number ; \
		rm benign3-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

cadets_attack:
	cd ../../data/attack && mkdir -p base && mkdir -p stream
	number=0 ; while [ $$number -le 2 ] ; do \
		python cadets/prepare.py ../../data/attack/pandex_$$number attack-$$number.txt ; \
		python camflow/parse.py attack-$$number.txt ../../data/attack/base/base-attack-$$number.txt ../../data/attack/stream/stream-attack-$$number.txt ; \
		rm -rf ../../data/attack/pandex_$$number ; \
		rm attack-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

wget_long_train:
	cd ../../data/benign && mkdir -p base && mkdir -p stream
	number=0 ; while [ $$number -le 99 ] ; do \
		python camflow/prepare.py ../../data/benign/wget-normal-$$number.log wget-normal-preprocessed-$$number.txt ; \
		python camflow/parse.py wget-normal-preprocessed-$$number.txt ../../data/benign/base/base-wget-$$number.txt ../../data/benign/stream/stream-wget-$$number.txt ; \
		rm error.log ; \
		rm wget-normal-preprocessed-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

wget_long_test:
	cd ../../data/attack && mkdir -p base && mkdir -p stream
	number=0 ; while [ $$number -le 4 ] ; do \
		python camflow/prepare.py ../../data/attack/wget-attack-$$number.log wget-attack-preprocessed-$$number.txt ; \
		python camflow/parse.py wget-attack-preprocessed-$$number.txt ../../data/attack/base/base-wget-attack-$$number.txt ../../data/attack/stream/stream-wget-attack-$$number.txt ; \
		rm error.log ; \
		rm wget-attack-preprocessed-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done
