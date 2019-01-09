prepare:
	wget http://apache.claz.org/avro/stable/java/avro-tools-1.8.2.jar

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
	
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official.bin > json/clearscope-e3/clearscope-e3-0.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/clearscope-e3/ta1-clearscope-e3-official.bin.1 > json/clearscope-e3/clearscope-e3-1.json 2>/dev/null
	
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-fivedirections-e3-official-3.bin > json/fivedirections-e3-3.json 2>/dev/null
	
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


	
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.1 > json/trace-e3-1-1.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.2 > json/trace-e3-1-2.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.3 > json/trace-e3-1-3.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.4 > json/trace-e3-1-4.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.5 > json/trace-e3-1-5.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-trace-e3-official-1.bin.6 > json/trace-e3-1-6.json 2>/dev/null


cadets:
	python convert.py --system cadets --source json/cadets-e3/ --save cadets-e3.txt

clearscope:
	python convert.py --system clearscope --source json/clearscope-e3/ --save clearscope-e3.txt

fivedirections:
	python convert.py --source json/fivedirections-e3/ --save fivedirections-e3.txt

theia:
	python convert.py --system theia --source json/theia-e3/ --save theia-e3.txt

trace:
	python convert.py --source json/trace-e3/ --save trace-e3.txt