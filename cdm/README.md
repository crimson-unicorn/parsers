# Convert CDM Data to Unicorn 

## Convert `.avdl` to `.avsc`
We use `avro-tools` provided by `Apache` to obtain schemas in the `.avsc` format so that we can use Avro parsing libraries.
Provide the following commands from the current directory:
```
wget http://apache.claz.org/avro/stable/java/avro-tools-1.8.2.jar
cd schema/
java -jar ../avro-tools-1.8.2.jar idl2schemata CDM19.avdl
```
The above command will create a list of `.avsc` files in the `schema` directory that contain schemas of defined records.
Be aware:
- `avro-tools` is constantly being updated to a newer version. Check `http://apache.claz.org/avro/stable/java/` and uses the latest available `jar` if `wget` fails.
- Make sure `avro-tools` can interpret the given `.avdl` file. In our case, `avro-tools-1.8.2` is used to interpret `CDM19.avdl` file.
- Your CDM data should be defined in the corresponding `.avdl` file. In our case, our engagement data is defined by `CDM19.avdl`.
