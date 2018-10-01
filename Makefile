youtube:
	cd ../../data && mkdir -p youtube_data
	cd ../../data/youtube_data && mkdir -p base_train && mkdir -p stream_train
	number=0 ; while [ $$number -le 99 ] ; do \
		python streamspot/parse.py $$number ../../data/all.tsv ../../data/youtube_data/base_train/base-youtube-v2-$$number.txt ../../data/youtube_data/stream_train/stream-youtube-v2-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

gmail:
	cd ../../data && mkdir -p gmail_data
	cd ../../data/gmail_data && mkdir -p base_train && mkdir -p stream_train
	number=100 ; while [ $$number -le 199 ] ; do \
		python streamspot/parse.py $$number ../../data/all.tsv ../../data/gmail_data/base_train/base-gmail-v2-$$number.txt ../../data/gmail_data/stream_train/stream-gmail-v2-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

vgame:
	cd ../../data && mkdir -p vgame_data
	cd ../../data/vgame_data && mkdir -p base_train && mkdir -p stream_train
	number=200 ; while [ $$number -le 299 ] ; do \
		python streamspot/parse.py $$number ../../data/all.tsv ../../data/vgame_data/base_train/base-vgame-v2-$$number.txt ../../data/vgame_data/stream_train/stream-vgame-v2-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

attack:
	cd ../../data && mkdir -p attack_data
	cd ../../data/attack_data && mkdir -p base_train && mkdir -p stream_train
	number=300 ; while [ $$number -le 399 ] ; do \
		python streamspot/parse.py $$number ../../data/all.tsv ../../data/attack_data/base_train/base-attack-v2-$$number.txt ../../data/attack_data/stream_train/stream-attack-v2-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

download:
	cd ../../data && mkdir -p download_data
	cd ../../data/download_data && mkdir -p base_train && mkdir -p stream_train
	number=400 ; while [ $$number -le 499 ] ; do \
		python streamspot/parse.py $$number ../../data/all.tsv ../../data/download_data/base_train/base-download-v2-$$number.txt ../../data/download_data/stream_train/stream-download-v2-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

cnn:
	cd ../../data && mkdir -p cnn_data
	cd ../../data/cnn_data/ && mkdir -p base_train && mkdir -p stream_train
	number=500 ; while [ $$number -le 599 ] ; do \
		python streamspot/parse.py $$number ../../data/all.tsv ../../data/cnn_data/base_train/base-cnn-v2-$$number.txt ../../data/cnn_data/stream_train/stream-cnn-v2-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done
