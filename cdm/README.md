# Convert CDM Data to Unicorn 

## Prepare to Parse CDM Avro Files
We use `avro-tools` provided by `Apache` to obtain schemas in the `.avsc` format so that we can use Avro parsing libraries.
Provide the following commands from the current directory:
```
wget http://apache.claz.org/avro/stable/java/avro-tools-1.8.2.jar
cd schema/
java -jar ../avro-tools-1.8.2.jar idl2schemata CDM19.avdl
```
The above command will create a list of `.avsc` files in the `schema` directory that contain schemas of defined record types.
Be aware:
- Currently, however, parsing CDM Avro format data is *not* supported (as of 01-04-2019). Please refer to next section for CDM JSON files.
- `avro-tools` is constantly being updated to a newer version. Check `http://apache.claz.org/avro/stable/java/` and uses the latest available `jar` if `wget` fails.
- Make sure `avro-tools` can interpret the given `.avdl` file. In our case, `avro-tools-1.8.2` is used to interpret `CDM19.avdl` file.
- Your CDM data should be defined in the corresponding `.avdl` file. In our case, our engagement data is defined by `CDM19.avdl`.

## Prepare to Parse CDM JSON Files
We use `avro-tools` to convert  CDM/Avro files to CDM/JSON files. In the current directory, perform:
```
java -jar avro-tools-1.8.2.jar tojson <AVRO_FILE_DATA_PATH> > <JSON_FILE_LOCATION> 2>/dev/null
``` 
Note:
- You must provide a valid `<AVRO_FILE_DATA_PATH>` that contains the CDM/Avro files to convert.
- The tool will output JSON records to stdout. We use `>` to redirect the output to a provided path `<JSON_FILE_LOCATION>` and dismiss any possible warnings using `2>/dev/null`.

## Prerequisite
We must parse all segmented datasets together so that we (hopefully) will not have any unmatched subject/object `UUID`s.
To do so, we may employ a key-value database (if there is a space constraint). 
We tried to use Python’s `sqlite3` database, but the performance is disappointing.
We are currently trying RocksDB. Installation instructions are in the next section.

To parse CDM JSON in a streaming fashion, use `ijson`.
We use a series of other existing Python packages hoping to improve the performance, including native Python packages such as multiprocessing

### Installation

####macOS
pip install -U sqlitedict

Installing some JSON libraries for parsing:
```
brew install yajl
git clone https://github.com/isagalaev/ijson.git
cd ijson/
pip install .
```
Installing a fast hashing library:
```
pip install xxhash
```
Installing a profiling library for performance evaluation:
```
pip install yappi 
```
Installing RocksDB and its Python bindings:
```
brew install rocksdb
pip install python-rocksd
```
To display progress
```
pip install tqdm
```

####Ubuntu Linux
Installing YAJL is different on Ubuntu:
```
agt-get install yajl2
```
Installing RocksDB is different:
```
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
mkdir build && cd build
cmake ..
make
make install INSTALL_PATH=/usr
```

## Issues
### Cadets
- The following `EVENT` types do not follow CDM semantics correctly:
	* `EVENT_FCNTL`: missing `object`
	* `EVENT_LOGIN`: missing `object`
	* `EVENT_LINK`: missing `object2`
	* `EVENT_MPROTECT`: missing `object`
	* `EVENT_OPEN`: missing `object`
	* `EVENT_OTHER`: missing `object`
	* `EVENT_SIGNAL`: missing `object`