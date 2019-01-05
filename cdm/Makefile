prepare:
	wget http://apache.claz.org/avro/stable/java/avro-tools-1.8.2.jar

tojson:
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-cadets-e3-official-1.bin.1 > json/cadets-e3-1-1.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-cadets-e3-official-1.bin.2 > json/cadets-e3-1-2.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-cadets-e3-official-1.bin.3 > json/cadets-e3-1-3.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-cadets-e3-official-1.bin.4 > json/cadets-e3-1-4.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-cadets-e3-official-2.bin.1 > json/cadets-e3-2-1.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-cadets-e3-official.bin.1 > json/cadets-e3-1.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-cadets-e3-official.bin.2 > json/cadets-e3-2.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-clearscope-e3-official.bin.1 > json/clearscope-e3-1.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-fivedirections-e3-official-3.bin > json/fivedirections-e3-3.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-theia-e3-official-3.bin > json/theia-e3-3.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-theia-e3-official-5m.bin > json/theia-e3-5m.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.1 > json/trace-e3-1-1.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.2 > json/trace-e3-1-2.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.3 > json/trace-e3-1-3.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.4 > json/trace-e3-1-4.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.5 > json/trace-e3-1-5.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.6 > json/trace-e3-1-6.json 2>/dev/null


cadets:
	python convert.py --source json/cadets-e3-1-1.json --save cadets-e3-1-1-intrim.txt
	python convert.py --source json/cadets-e3-1-2.json --save cadets-e3-1-2-intrim.txt
	python convert.py --source json/cadets-e3-1-3.json --save cadets-e3-1-3-intrim.txt
	python convert.py --source json/cadets-e3-1-4.json --save cadets-e3-1-4-intrim.txt
	python convert.py --source json/cadets-e3-2-1.json --save cadets-e3-2-1-intrim.txt
	python convert.py --source json/cadets-e3-1.json --save cadets-e3-1-intrim.txt
	python convert.py --source json/cadets-e3-2.json --save cadets-e3-2-intrim.txt

clearscope:
	python convert.py --source json/clearscope-e3-1.json --save clearscope-e3-1-intrim.txt

fivedirections:
	python convert.py --source json/fivedirections-e3-3.json --save fivedirections-e3-3-intrim.txt

theia:
	python convert.py --source json/theia-e3-3.json --save theia-e3-3-intrim.txt
	python convert.py --source json/theia-e3-5m.json --save theia-e3-5m-intrim.txt

trace:
	python convert.py --source json/trace-e3-1-1.json --save trace-e3-1-1-intrim.txt
	python convert.py --source json/trace-e3-1-2.json --save trace-e3-1-2-intrim.txt
	python convert.py --source json/trace-e3-1-3.json --save trace-e3-1-3-intrim.txt
	python convert.py --source json/trace-e3-1-4.json --save trace-e3-1-4-intrim.txt
	python convert.py --source json/trace-e3-1-5.json --save trace-e3-1-5-intrim.txt
	python convert.py --source json/trace-e3-1-6.json --save trace-e3-1-6-intrim.txt
