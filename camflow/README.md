# CamFlow Data Parser
This is a Python data parser for [CamFlow](https://camflow.org).
The parser is designed specifically for CamFlow's JSON output data format and Unicorn's graph processing framework.
This parser optionally outputs some statistics for the graph if certain options are set in the commandline.
However, if you do not need those statistics, you should not turn the options on for better parsing performance.

## Usage
Parsing is implemented in two separate stages, both written in Python 2 (in most cases, if the given data is clean, you should also be able to run the parser using Python 3).
You can either run our Python scripts directly or use our Makefile template that runs them in a virtual environment together (recommended).

The first stage is accomplished by `prepare.py`. To run this script manually, you must install `xxhash` and `tqdm` first:
```
pip install xxhash tqdm
```
You can then run:
```
python prepare.py -h
```
to understand the required and optional arguments.
This stage requires an input CamFlow raw data file in JSON format.
The output will be an edgelist file (see [Graph Format](#graph-format)).
CamFlow is still a research prototype; you can use `-v` flag to log any unexpected anomalies in CamFlow data. 
The log is default to be written to `debug.log`, but you can set your own debug file path.
If you set `-s`, additional statistics of the CamFlow graph and timestamps of graph generation will be recorded.
This is used for Unicorn's performance evaluation and very likely, you do not need to set this flag.
If you set `-n`, node IDs will be in the original CamFlow UUID format, which is most likely *not* what you want. This is for debugging CamFlow, so you do not need to set this flag.
If you set `-t`, the original jiffies from CamFlow will be recorded in the output. `-s` will overwrite `-t`, so do not set `-s` if you plan to set `-t`.

The second and final stage is accomplished by `parse.py`.
To run this script manually, you must install `tqdm` first.
You can run:
```
python parse.py -h
```
to understand the required and optional arguments.
This stage takes the output file from `prepare.py` and outputs a base graph and a stream graph that streams edges onto the base graph.
Both graphs are required for Unicorn's graph analysis pipeline.
You have the flexibility to set the size of the base graph, using option `-b`.
You can set `-s` to parse timestamps of graph generation. If you do so, you must set the same option in the previous stage.
However, this option, and its associated options (`-I`, `-f`) are used for our performance evaluation to see how fast CamFlow generates provenance graph and therefore, are *not* likely what you need.
If you set `-s`, you will see an additional output file `ts.txt`, which records adjusted timestamps (recorded in the previous stage) every N edges where N is determined by `-I`, which you must set if `-s` is set.
Again, it is unlikely that you will want to set those flags.
If you set `-t`, you will parse jiffies of the graph. If you do so, you must set the same option in the previous stage. Note that `-s` can overwrite `-t`.

If you prefer using a virtual environment, make sure you have `virtualenv` installed.
The Makefile template is located in the `example/` folder in which we include a small example to demonstrate how you can run the parser.
Simply run:
```
make example
```
to check out the outputs.
If you really want to see some statistics that we use for performance evaluation, run:
```
make stats
```
To clean up, run:
```
make clean
```

## [Graph Format](#graph-format)
The output of `prepare.py` is an edgelist, with each line recording an edge of the format:
```
<srcID>	<dstID>	<srcType>:<dstType>:<edgeType>:<logicalTimestamp>[:<Timestamp>]
```
* If you set `-n`, `srcID` and `dstID` will be in CamFlow original UUID format. This is usually not you want.
* If you set `-s`, you will see the last `Timestamp` value. This is usually not necessary to have.
* If you set `-t`, you will also see the last `Timestamp` value, but it is the original jiffies from CamFlow. Note that `-s` overwrites `-t`.

The output of `parse.py` consists of a base graph and a stream graph.
In the base graph, each line is an edge of the format:
```
<srcID> <dstID> <srcType>:<dstType>:<edgeType>:<logicalTimestamp>
```
This is the same as in the `prepare.py` except that edges are sorted by the logical timestamp.
However, if you set `-t` in `parse.py`, you have an addition field at the end:
```
<srcID> <dstID> <srcType>:<dstType>:<edgeType>:<logicalTimestamp>:<jiffies>
```

In the stream graph, each line is also an edge, but of the format:
```
<srcID> <dstID> <srcType>:<dstType>:<edgeType>:<srcUnseen>:<dstUnseen>:<logicalTimestamp>[:<Timestamp>]
```
* `srcUnseen` and `dstUnseen` are either 0 (we have seen this node in a previous edge) or 1 (we have not). This is needed for the graph processing framework we use.
* If you set `-s`/`-t`, you will see the last `Timestamp` value. This is usually not necessary to have.
