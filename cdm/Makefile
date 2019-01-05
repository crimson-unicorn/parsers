prepare:
	wget http://apache.claz.org/avro/stable/java/avro-tools-1.8.2.jar

tojson:
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-cadets-e3-official-1.bin.1 > json/cadets-e3-1-1.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-cadets-e3-official-1.bin.2 > json/cadets-e3-1-2.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-cadets-e3-official-1.bin.3 > json/cadets-e3-1-3.json 2>/dev/null
	java -jar avro-tools-1.8.2.jar tojson avro/ta1-cadets-e3-official-1.bin.4 > json/cadets-e3-1-4.json 2>/dev/null

cadets:
	python convert.py --source json/cadets-e3-1-1.json --save cadets-e3-1-1-intrim.txt
	python convert.py --source json/cadets-e3-1-2.json --save cadets-e3-1-2-intrim.txt
	python convert.py --source json/cadets-e3-1-3.json --save cadets-e3-1-3-intrim.txt
	python convert.py --source json/cadets-e3-1-4.json --save cadets-e3-1-4-intrim.txt