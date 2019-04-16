cadets-e3-official=1DpyVpK9JWNlXzhlzzeuKO_gD8BSosn--
cadets-e3-official-json=1AcWrYiBmgAqp7DizclKJYYJJBQbnDMfb
cadets-e3-official-1=1sIpBepnKw0kz4yZviUD_dPZWz0EAfrAS
cadets-e3-official-1-json=1XLCEhf5DR8xw3S-Fimcj32IKnfzHFPJW
cadets-e3-official-2=159Enm1TChO9nUvUE2UhcAlZH5nv9CZ2e
cadets-e3-official-2-json=1EycO23tEvZVnN3VxOHZ7gdbSCwqEZTI1

clearscope-e3-official=1XVdgTyu2Lr8K13XUuLZ0B09BjbTwXGen
clearscope-e3-official-json=1z1KtBvS-XBwCt3DlkI6yS1UO7_1sVvB6
clearscope-e3-official-1=1Mz0lqI2iUu4RyM8WY_n8-0wf7HBtdILx
clearscope-e3-official-1-json=1cEdDdmDACkeTmC04vUwW8WXqn36t4DE4
clearscope-e3-official-2=17UJnLTwJQFfaVsnuKT27WiNIXSMxAbhv
clearscope-e3-official-2-json=11bVukedxRn8lVPmbBD2S6juUlqL4cI6b

fivedirections-e3-official=1JnoL7X2h1JIciox9OXPCXivBQVZAZXZy
fivedirections-e3-official-json=1XjX2Tlb3H0XEHggF3IM0vDIsV_lSlcBN
fivedirections-e3-official-2=1IHlz1s94ckTSaVWw9iyTeQ8-e6NNZDLq
fivedirections-e3-official-2-json=1BeP80zUUmm4eZl0UuU43PsKNkl_xgskj
fivedirections-e3-official-3=1s5AgodIRlcp7bD-sRQzKpi6VFvNOk1GH
fivedirections-e3-official-3-json=13OvCFAxy23D9CZuheT5l7BhNhZN_5kE-

theia-e3-official-1r=1yksk4I9DBnl0Tca_Y0eaiNcpRNccB9sF
theia-e3-official-1r-json=10cecNtR3VsHfV0N-gNEeoVeB89kCnse5
theia-e3-official-3=10KZG-8ET9CRG9qc8OmEQMaCgf6uSEylD
theia-e3-official-3-json=1dWJecuLXZMksKAPo8348Q6L5DiccsS1u
theia-e3-official-5m=1krhA9Rco7W5hZG6md3TqB1aH2FOjxBn3
theia-e3-official-5m-json=1zbgWJgF7F0fI6JhViqQZoo6AWdoV5YFK
theia-e3-official-6r=13rgPgHunDV1dSNX9U8bSOem56StlUmqF
theia-e3-official-6r-json=1Kadc6CUTb4opVSDE4x6RFFnEy0P1cRp0

trace-e3-official=1HyJ8du-nJcls91Yz1jDSMIJJ_ih6n_Ui
trace-e3-official-json=1sfIbavsUFwmB-irSGY1TZZ0Sq1dZqF9G
trace-e3-official-1=1N7qMHTRKJRyg7TkalORb48TrXDZTK8lY
trace-e3-official-1-json=1GG1aUnPjjzzdbxznVTN8X6oVfA-K4oIV

prepare:
	wget http://apache.claz.org/avro/stable/java/avro-tools-1.8.2.jar

download:
	curl -c ./cookie -s -L "https://drive.google.com/uc?export=download&id=$(fivedirections-e3-official)" > /dev/null
	curl -Lb ./cookie "https://drive.google.com/uc?export=download&confirm=`awk '/download/ {print $NF}' ./cookie`&id=$(fivedirections-e3-official)" -o fivedirections-e3-official.bin.tar.gz
	tar xvzf fivedirections-e3-official.bin.tar.gz

tojson:
	java -jar avro-tools-1.8.2.jar tojson avro/cadets-e3/ta1-cadets-e3-official-1.bin > json/cadets-e3/cadets-e3-1-0.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/cadets-e3/ta1-cadets-e3-official-1.bin.1 > json/cadets-e3/cadets-e3-1-1.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/cadets-e3/ta1-cadets-e3-official-1.bin.2 > json/cadets-e3/cadets-e3-1-2.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/cadets-e3/ta1-cadets-e3-official-1.bin.3 > json/cadets-e3/cadets-e3-1-3.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/cadets-e3/ta1-cadets-e3-official-1.bin.4 > json/cadets-e3/cadets-e3-1-4.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/cadets-e3/ta1-cadets-e3-official.bin > json/cadets-e3/cadets-e3-0.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/cadets-e3/ta1-cadets-e3-official.bin.1 > json/cadets-e3/cadets-e3-1.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/cadets-e3/ta1-cadets-e3-official.bin.2 > json/cadets-e3/cadets-e3-2.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/cadets-e3/ta1-cadets-e3-official-2.bin > json/cadets-e3/cadets-e3-2-0.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/cadets-e3/ta1-cadets-e3-official-2.bin.1 > json/cadets-e3/cadets-e3-2-1.json 2>/dev/null
	
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin > json/clearscope-e3/clearscope-e3-1-0.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.1 > json/clearscope-e3/clearscope-e3-1-1.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.2 > json/clearscope-e3/clearscope-e3-1-2.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.3 > json/clearscope-e3/clearscope-e3-1-3.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.4 > json/clearscope-e3/clearscope-e3-1-4.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.5 > json/clearscope-e3/clearscope-e3-1-5.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.6 > json/clearscope-e3/clearscope-e3-1-6.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.7 > json/clearscope-e3/clearscope-e3-1-7.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.8 > json/clearscope-e3/clearscope-e3-1-8.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.9 > json/clearscope-e3/clearscope-e3-1-9.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.10 > json/clearscope-e3/clearscope-e3-1-10.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.11 > json/clearscope-e3/clearscope-e3-1-11.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.12 > json/clearscope-e3/clearscope-e3-1-12.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.13 > json/clearscope-e3/clearscope-e3-1-13.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.14 > json/clearscope-e3/clearscope-e3-1-14.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.15 > json/clearscope-e3/clearscope-e3-1-15.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.16 > json/clearscope-e3/clearscope-e3-1-16.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.17 > json/clearscope-e3/clearscope-e3-1-17.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.18 > json/clearscope-e3/clearscope-e3-1-18.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official-1.bin.19 > json/clearscope-e3/clearscope-e3-1-19.json 2>/dev/null


	java -jar avro-tools-1.8.2.jar tojson avro/theia-e3/ta1-theia-e3-official-1r.bin > json/theia-e3/theia-e3-1-0.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/theia-e3/ta1-theia-e3-official-1r.bin.1 > json/theia-e3/theia-e3-1-1.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/theia-e3/ta1-theia-e3-official-1r.bin.2 > json/theia-e3/theia-e3-1-2.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/theia-e3/ta1-theia-e3-official-1r.bin.3 > json/theia-e3/theia-e3-1-3.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/theia-e3/ta1-theia-e3-official-1r.bin.4 > json/theia-e3/theia-e3-1-4.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/theia-e3/ta1-theia-e3-official-1r.bin.5 > json/theia-e3/theia-e3-1-5.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/theia-e3/ta1-theia-e3-official-1r.bin.6 > json/theia-e3/theia-e3-1-6.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/theia-e3/ta1-theia-e3-official-1r.bin.7 > json/theia-e3/theia-e3-1-7.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/theia-e3/ta1-theia-e3-official-1r.bin.8 > json/theia-e3/theia-e3-1-8.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/theia-e3/ta1-theia-e3-official-1r.bin.9 > json/theia-e3/theia-e3-1-9.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/theia-e3/ta1-theia-e3-official-3.bin > json/theia-e3/theia-e3-3-0.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/theia-e3/ta1-theia-e3-official-5m.bin > json/theia-e3/theia-e3-5-0.json 2>/dev/null

	java -jar avro-tools-1.8.2.jar tojson avro/fivedirections-e3/ta1-fivedirections-e3-official-3.bin > json/fivedirections-e3/fivedirections-e3-3.json 2>/dev/null
	

	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.1 > json/trace-e3-1-1.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.2 > json/trace-e3-1-2.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.3 > json/trace-e3-1-3.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.4 > json/trace-e3-1-4.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.5 > json/trace-e3-1-5.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.6 > json/trace-e3-1-6.json 2>/dev/null

cadets_e3:
	number=0 ; while [ $$number -le 8 ] ; do \
		cd ../../../data/cadets-e3/benign && mkdir cadets-e3-benign-$$number && tar zxvf cadets-e3-benign-$$number.gz.tar -C cadets-e3-benign-$$number && mv cadets-e3-benign-$$number/cadets-e3-benign.txt.* ../edgelists_benign ; \
		cd ../../../data/cadets-e3/benign && rm -f cadets-e3-benign-$$number.gz.tar && rm -rf cadets-e3-benign-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 65 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 5000 -i ../../../data/cadets-e3/edgelists_benign/cadets-e3-benign.txt.$$number -b ../../../data/cadets-e3/train/base/base-cadets-e3-benign-$$number.txt -s ../../../data/cadets-e3/train/stream/stream-cadets-e3-benign-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 2 ] ; do \
		cd ../../../data/cadets-e3/attack && mkdir cadets-e3-official-$$number && tar zxvf cadets-e3-official-$$number.gz.tar -C cadets-e3-official-$$number && mv cadets-e3-official-$$number/cadets-e3-attack.txt.* ../edgelists_attack ; \
		cd ../../../data/cadets-e3/attack && rm -f cadets-e3-official-$$number.gz.tar && rm -rf cadets-e3-official-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 7 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 5000 -i ../../../data/cadets-e3/edgelists_attack/cadets-e3-attack.txt.$$number -b ../../../data/cadets-e3/test/base/base-cadets-e3-attack-$$number.txt -s ../../../data/cadets-e3/test/stream/stream-cadets-e3-attack-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

theia_e3:
	cd ../../../data/theia-e3/benign && mkdir theia-e3-benign && tar zxvf theia-e3-benign.gz.tar -C theia-e3-benign && mv theia-e3-benign/theia-e3-benign.txt.* ../edgelists_benign
	cd ../../../data/theia-e3/benign && rm -f theia-e3-benign.gz.tar && rm -rf theia-e3-benign
	number=0 ; while [ $$number -le 1 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 10000 -i ../../../data/theia-e3/edgelists_benign/theia-e3-benign.txt.$$number -b ../../../data/theia-e3/train/base/base-theia-e3-benign-$$number.txt -s ../../../data/theia-e3/train/stream/stream-theia-e3-benign-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done
	cd ../../../data/theia-e3/attack && mkdir theia-e3-official-1r && tar zxvf theia-e3-official-1r.gz.tar -C theia-e3-official-1r && mv theia-e3-official-1r/theia-e3-official.txt.* ../edgelists_attack
	cd ../../../data/theia-e3/attack && rm -f theia-e3-official-1r.gz.tar && rm -rf theia-e3-official-1r
	cd ../../../data/theia-e3/attack && mkdir theia-e3-official-3 && tar zxvf theia-e3-official-3.gz.tar -C theia-e3-official-3 && mv theia-e3-official-3/theia-e3-official.txt.* ../edgelists_attack
	cd ../../../data/theia-e3/attack && rm -f theia-e3-official-3.gz.tar && rm -rf theia-e3-official-3
	cd ../../../data/theia-e3/attack && mkdir theia-e3-official-5m && tar zxvf theia-e3-official-5m.gz.tar -C theia-e3-official-5m && mv theia-e3-official-5m/theia-e3-official.txt.* ../edgelists_attack
	cd ../../../data/theia-e3/attack && rm -f theia-e3-official-5m.gz.tar && rm -rf theia-e3-official-5m
	cd ../../../data/theia-e3/attack && mkdir theia-e3-official-6r && tar zxvf theia-e3-official-6r.gz.tar -C theia-e3-official-6r && mv theia-e3-official-6r/theia-e3-official.txt.* ../edgelists_attack
	cd ../../../data/theia-e3/attack && rm -f theia-e3-official-6r.gz.tar && rm -rf theia-e3-official-6r
	number=0 ; while [ $$number -le 24 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 10000 -i ../../../data/theia-e3/edgelists_attack/theia-e3-official.txt.$$number -b ../../../data/theia-e3/test/base/base-theia-e3-attack-$$number.txt -s ../../../data/theia-e3/test/stream/stream-theia-e3-attack-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

fivedirections_e3:
	number=0 ; while [ $$number -le 15 ] ; do \
		cd ../../../data/fivedirections-e3/benign && mkdir fivedirections-e3-benign-$$number && tar zxvf fivedirections-e3-benign-$$number.gz.tar -C fivedirections-e3-benign-$$number && mv fivedirections-e3-benign-$$number/fivedirections-e3-benign.txt.* ../edgelists_benign ; \
		cd ../../../data/fivedirections-e3/benign && rm -f fivedirections-e3-benign-$$number.gz.tar && rm -rf fivedirections-e3-benign-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 112 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 10000 -i ../../../data/fivedirections-e3/edgelists_benign/fivedirections-e3-benign.txt.$$number -b ../../../data/fivedirections-e3/train/base/base-fivedirections-e3-benign-$$number.txt -s ../../../data/fivedirections-e3/train/stream/stream-fivedirections-e3-benign-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done
	cd ../../../data/fivedirections-e3/attack && mkdir fivedirections-e3-official && tar zxvf fivedirections-e3-official.gz.tar -C fivedirections-e3-official && mv fivedirections-e3-official/fivedirections-e3-attack.txt.0 ../edgelists_attack
	cd ../../../data/fivedirections-e3/attack && rm -f fivedirections-e3-official.gz.tar && rm -rf fivedirections-e3-official
	number=0 ; while [ $$number -le 6 ] ; do \
		cd ../../../data/fivedirections-e3/attack && mkdir fivedirections-e3-official-2-$$number && tar zxvf fivedirections-e3-official-2-$$number.gz.tar -C fivedirections-e3-official-2-$$number && mv fivedirections-e3-official-2-$$number/fivedirections-e3-attack.txt.* ../edgelists_attack ; \
		cd ../../../data/fivedirections-e3/attack && rm -f fivedirections-e3-official-2-$$number.gz.tar && rm -rf fivedirections-e3-official-2-$$number ; \
		number=`expr $$number + 1` ; \
	done
	cd ../../../data/fivedirections-e3/attack && mkdir fivedirections-e3-official-3 && tar zxvf fivedirections-e3-official-3.gz.tar -C fivedirections-e3-official-3 && mv fivedirections-e3-official-3/fivedirections-e3-attack.txt.54 ../edgelists_attack
	cd ../../../data/fivedirections-e3/attack && rm -f fivedirections-e3-official-3.gz.tar && rm -rf fivedirections-e3-official-3
	number=0 ; while [ $$number -le 54 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 10000 -i ../../../data/fivedirections-e3/edgelists_attack/fivedirections-e3-attack.txt.$$number -b ../../../data/fivedirections-e3/test/base/base-fivedirections-e3-attack-$$number.txt -s ../../../data/fivedirections-e3/test/stream/stream-fivedirections-e3-attack-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

clearscope_e3:
	number=0 ; while [ $$number -le 4 ] ; do \
		cd ../../../data/clearscope-e3/benign && mkdir clearscope-benign-$$number && tar zxvf clearscope-benign-$$number.gz.tar -C clearscope-benign-$$number && mv clearscope-benign-$$number/clearscope-benign.txt.* ../edgelists_benign ; \
		cd ../../../data/clearscope-e3/benign && rm -f clearscope-benign-$$number.gz.tar && rm -rf clearscope-benign-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 43 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 5000 -i ../../../data/clearscope-e3/edgelists_benign/clearscope-benign.txt.$$number -b ../../../data/clearscope-e3/train/base/base-clearscope-e3-benign-$$number.txt -s ../../../data/clearscope-e3/train/stream/stream-clearscope-e3-benign-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done
	cd ../../../data/clearscope-e3/attack && mkdir clearscope-e3-official && tar zxvf clearscope-e3-official.gz.tar -C clearscope-e3-official && mv clearscope-e3-official/clearscope-e3-attack.txt.* ../edgelists_attack
	cd ../../../data/clearscope-e3/attack && rm -f clearscope-e3-official.gz.tar && rm -rf clearscope-e3-official
	cd ../../../data/clearscope-e3/attack && mkdir clearscope-e3-official-1-0 && tar zxvf clearscope-e3-official-1-0.gz.tar -C clearscope-e3-official-1-0 && mv clearscope-e3-official-1-0/clearscope-e3-attack.txt.* ../edgelists_attack
	cd ../../../data/clearscope-e3/attack && rm -f clearscope-e3-official-1-0.gz.tar && rm -rf clearscope-e3-official-1-0
	cd ../../../data/clearscope-e3/attack && mkdir clearscope-e3-official-1-1 && tar zxvf clearscope-e3-official-1-1.gz.tar -C clearscope-e3-official-1-1 && mv clearscope-e3-official-1-1/clearscope-e3-attack.txt.* ../edgelists_attack
	cd ../../../data/clearscope-e3/attack && rm -f clearscope-e3-official-1-1.gz.tar && rm -rf clearscope-e3-official-1-1
	cd ../../../data/clearscope-e3/attack && mkdir clearscope-e3-official-2-0 && tar zxvf clearscope-e3-official-2-0.gz.tar -C clearscope-e3-official-2-0 && mv clearscope-e3-official-2-0/clearscope-e3-attack.txt.* ../edgelists_attack
	cd ../../../data/clearscope-e3/attack && rm -f clearscope-e3-official-2-0.gz.tar && rm -rf clearscope-e3-official-2-0
	cd ../../../data/clearscope-e3/attack && mkdir clearscope-e3-official-2-1 && tar zxvf clearscope-e3-official-2-1.gz.tar -C clearscope-e3-official-2-1 && mv clearscope-e3-official-2-1/clearscope-e3-attack.txt.* ../edgelists_attack
	cd ../../../data/clearscope-e3/attack && rm -f clearscope-e3-official-2-1.gz.tar && rm -rf clearscope-e3-official-2-1
	number=0 ; while [ $$number -le 50 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 5000 -i ../../../data/clearscope-e3/edgelists_attack/clearscope-e3-attack.txt.$$number -b ../../../data/clearscope-e3/test/base/base-clearscope-e3-attack-$$number.txt -s ../../../data/clearscope-e3/test/stream/stream-clearscope-e3-attack-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

camflow_apt:
	number=0 ; while [ $$number -le 12 ] ; do \
		cd ../../../data/camflow-apt/benign && mkdir camflow-benign-$$number && tar zxvf camflow-benign-$$number.gz.tar -C camflow-benign-$$number && mv camflow-benign-$$number/camflow-benign.txt.* ../edgelists_benign ; \
		cd ../../../data/camflow-apt/benign && rm -f camflow-benign-$$number.gz.tar && rm -rf camflow-benign-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 124 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 3000 -i ../../../data/camflow-apt/edgelists_benign/camflow-benign.txt.$$number -b ../../../data/camflow-apt/train/base/base-camflow-benign-$$number.txt -s ../../../data/camflow-apt/train/stream/stream-camflow-benign-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 2 ] ; do \
		cd ../../../data/camflow-apt/attack && mkdir camflow-attack-$$number && tar zxvf camflow-attack-$$number.gz.tar -C camflow-attack-$$number && mv camflow-attack-$$number/camflow-attack.txt.* ../edgelists_attack ; \
		cd ../../../data/camflow-apt/attack && rm -f camflow-attack-$$number.gz.tar && rm -rf camflow-attack-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 24 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 3000 -i ../../../data/camflow-apt/edgelists_attack/camflow-attack.txt.$$number -b ../../../data/camflow-apt/test/base/base-camflow-attack-$$number.txt -s ../../../data/camflow-apt/test/stream/stream-camflow-attack-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

camflow_apt_subset:
	number=0 ; while [ $$number -le 12 ] ; do \
		cd ../../../data/camflow-apt/benign && mkdir camflow-benign-$$number && tar zxvf camflow-benign-$$number.gz.tar -C camflow-benign-$$number && mv camflow-benign-$$number/camflow-benign.txt.* ../edgelists_benign ; \
		cd ../../../data/camflow-apt/benign && rm -f camflow-benign-$$number.gz.tar && rm -rf camflow-benign-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 124 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 3000 -i ../../../data/camflow-apt/edgelists_benign/camflow-benign.txt.$$number -b ../../../data/camflow-apt/train/base/base-camflow-benign-$$number.txt -s ../../../data/camflow-apt/train/stream/stream-camflow-benign-$$number.txt ; \
		number=`expr $$number + 4` ; \
	done
	number=0 ; while [ $$number -le 2 ] ; do \
		cd ../../../data/camflow-apt/attack && mkdir camflow-attack-$$number && tar zxvf camflow-attack-$$number.gz.tar -C camflow-attack-$$number && mv camflow-attack-$$number/camflow-attack.txt.* ../edgelists_attack ; \
		cd ../../../data/camflow-apt/attack && rm -f camflow-attack-$$number.gz.tar && rm -rf camflow-attack-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 24 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 3000 -i ../../../data/camflow-apt/edgelists_attack/camflow-attack.txt.$$number -b ../../../data/camflow-apt/test/base/base-camflow-attack-$$number.txt -s ../../../data/camflow-apt/test/stream/stream-camflow-attack-$$number.txt ; \
		number=`expr $$number + 5` ; \
	done

spade_apt:
	number=0 ; while [ $$number -le 12 ] ; do \
		cd ../../../data/spade-wget-apt/benign && mkdir spade-benign-$$number && tar zxvf spade-benign-$$number.gz.tar -C spade-benign-$$number && mv spade-benign-$$number/spade-benign.txt.* ../edgelists_benign ; \
		cd ../../../data/spade-wget-apt/benign && rm -f spade-benign-$$number.gz.tar && rm -rf spade-benign-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 124 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 1000 -i ../../../data/spade-wget-apt/edgelists_benign/spade-benign.txt.$$number -b ../../../data/spade-wget-apt/train/base/base-spade-benign-$$number.txt -s ../../../data/spade-wget-apt/train/stream/stream-spade-benign-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 2 ] ; do \
		cd ../../../data/spade-wget-apt/attack && mkdir spade-attack-$$number && tar zxvf spade-attack-$$number.gz.tar -C spade-attack-$$number && mv spade-attack-$$number/spade-attack.txt.* ../edgelists_attack ; \
		cd ../../../data/spade-wget-apt/attack && rm -f spade-attack-$$number.gz.tar && rm -rf spade-attack-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 24 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 1000 -i ../../../data/spade-wget-apt/edgelists_attack/spade-attack.txt.$$number -b ../../../data/spade-wget-apt/test/base/base-spade-attack-$$number.txt -s ../../../data/spade-wget-apt/test/stream/stream-spade-attack-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

spade_apt_subset:
	number=0 ; while [ $$number -le 12 ] ; do \
		cd ../../../data/spade-wget-apt/benign && mkdir spade-benign-$$number && tar zxvf spade-benign-$$number.gz.tar -C spade-benign-$$number && mv spade-benign-$$number/spade-benign.txt.* ../edgelists_benign ; \
		cd ../../../data/spade-wget-apt/benign && rm -f spade-benign-$$number.gz.tar && rm -rf spade-benign-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 124 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 2500 -i ../../../data/spade-wget-apt/edgelists_benign/spade-benign.txt.$$number -b ../../../data/spade-wget-apt/train/base/base-spade-benign-$$number.txt -s ../../../data/spade-wget-apt/train/stream/stream-spade-benign-$$number.txt ; \
		number=`expr $$number + 4` ; \
	done
	number=0 ; while [ $$number -le 2 ] ; do \
		cd ../../../data/spade-wget-apt/attack && mkdir spade-attack-$$number && tar zxvf spade-attack-$$number.gz.tar -C spade-attack-$$number && mv spade-attack-$$number/spade-attack.txt.* ../edgelists_attack ; \
		cd ../../../data/spade-wget-apt/attack && rm -f spade-attack-$$number.gz.tar && rm -rf spade-attack-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 24 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 2500 -i ../../../data/spade-wget-apt/edgelists_attack/spade-attack.txt.$$number -b ../../../data/spade-wget-apt/test/base/base-spade-attack-$$number.txt -s ../../../data/spade-wget-apt/test/stream/stream-spade-attack-$$number.txt ; \
		number=`expr $$number + 5` ; \
	done

camflow_shellshock:
	number=0 ; while [ $$number -le 12 ] ; do \
		cd ../../../data/shellshock-apt/benign && mkdir camflow-benign-$$number && tar zxvf camflow-benign-$$number.gz.tar -C camflow-benign-$$number && mv camflow-benign-$$number/camflow-normal.txt.* ../edgelists_benign ; \
		cd ../../../data/shellshock-apt/benign && rm -f camflow-benign-$$number.gz.tar && rm -rf camflow-benign-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 124 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 3000 -i ../../../data/shellshock-apt/edgelists_benign/camflow-normal.txt.$$number -b ../../../data/shellshock-apt/train/base/base-shellshock-benign-$$number.txt -s ../../../data/shellshock-apt/train/stream/stream-shellshock-benign-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 2 ] ; do \
		cd ../../../data/shellshock-apt/attack && mkdir camflow-attack-$$number && tar zxvf camflow-attack-$$number.gz.tar -C camflow-attack-$$number && mv camflow-attack-$$number/camflow-attack.txt.* ../edgelists_attack ; \
		cd ../../../data/shellshock-apt/attack && rm -f camflow-attack-$$number.gz.tar && rm -rf camflow-attack-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 24 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 3000 -i ../../../data/shellshock-apt/edgelists_attack/camflow-attack.txt.$$number -b ../../../data/shellshock-apt/test/base/base-shellshock-attack-$$number.txt -s ../../../data/shellshock-apt/test/stream/stream-shellshock-attack-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done

camflow_shellshock_subset:
	number=0 ; while [ $$number -le 12 ] ; do \
		cd ../../../data/shellshock-apt/benign && mkdir camflow-benign-$$number && tar zxvf camflow-benign-$$number.gz.tar -C camflow-benign-$$number && mv camflow-benign-$$number/camflow-normal.txt.* ../edgelists_benign ; \
		cd ../../../data/shellshock-apt/benign && rm -f camflow-benign-$$number.gz.tar && rm -rf camflow-benign-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 124 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 4000 -i ../../../data/shellshock-apt/edgelists_benign/camflow-normal.txt.$$number -b ../../../data/shellshock-apt/train/base/base-shellshock-benign-$$number.txt -s ../../../data/shellshock-apt/train/stream/stream-shellshock-benign-$$number.txt ; \
		number=`expr $$number + 4` ; \
	done
	number=0 ; while [ $$number -le 2 ] ; do \
		cd ../../../data/shellshock-apt/attack && mkdir camflow-attack-$$number && tar zxvf camflow-attack-$$number.gz.tar -C camflow-attack-$$number && mv camflow-attack-$$number/camflow-attack.txt.* ../edgelists_attack ; \
		cd ../../../data/shellshock-apt/attack && rm -f camflow-attack-$$number.gz.tar && rm -rf camflow-attack-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 24 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 4000 -i ../../../data/shellshock-apt/edgelists_attack/camflow-attack.txt.$$number -b ../../../data/shellshock-apt/test/base/base-shellshock-attack-$$number.txt -s ../../../data/shellshock-apt/test/stream/stream-shellshock-attack-$$number.txt ; \
		number=`expr $$number + 5` ; \
	done

tune_camflow_apt:
	number=0 ; while [ $$number -le 1 ] ; do \
		cd ../../../data/camflow-apt/benign && mkdir camflow-benign-$$number && tar zxvf camflow-benign-$$number.gz.tar -C camflow-benign-$$number && mv camflow-benign-$$number/camflow-benign.txt.* ../edgelists_benign ; \
		cd ../../../data/camflow-apt/benign && rm -f camflow-benign-$$number.gz.tar && rm -rf camflow-benign-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 19 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 3000 -i ../../../data/camflow-apt/edgelists_benign/camflow-benign.txt.$$number -b ../../../data/camflow-apt/train/base/base-camflow-benign-$$number.txt -s ../../../data/camflow-apt/train/stream/stream-camflow-benign-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 0 ] ; do \
		cd ../../../data/camflow-apt/attack && mkdir camflow-attack-$$number && tar zxvf camflow-attack-$$number.gz.tar -C camflow-attack-$$number && mv camflow-attack-$$number/camflow-attack.txt.* ../edgelists_attack ; \
		cd ../../../data/camflow-apt/attack && rm -f camflow-attack-$$number.gz.tar && rm -rf camflow-attack-$$number ; \
		number=`expr $$number + 1` ; \
	done
	number=0 ; while [ $$number -le 4 ] ; do \
		python ProvParser/provparser/up.py -v -m -S 3000 -i ../../../data/camflow-apt/edgelists_attack/camflow-attack.txt.$$number -b ../../../data/camflow-apt/test/base/base-camflow-attack-$$number.txt -s ../../../data/camflow-apt/test/stream/stream-camflow-attack-$$number.txt ; \
		number=`expr $$number + 1` ; \
	done