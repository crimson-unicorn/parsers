## StreamSpot Data Parser
This is a Python data parser for [StreamSpot data](https://github.com/sbustreamspot/sbustreamspot-data).
You can also directly download the parsed data used in our experiments [here](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/83KYJY).
The parser is designed specifically for Unicorn's graph processing framework,
but it can be easily modified and extended.

The output of this parser creates a base graph and a stream graph that streams edges onto the base graph.
Both graphs are required for Unicorn's graph analysis pipeline.
You have the flexibility to set the size of the base graph, however (see [Usage](#usage)).

If you want to use this parser for your own graph, make sure the format of the graph aligns with StreamSpot's format
(see [Graph Format](#graph-format)).

For Unicorn's [analyzer](https://github.com/crimson-unicorn/analyzer) to work, make sure for each graph,
the node IDs start from 0 and do not skip numbers. If you use `parse_fast.py`, you can provide `-a` flag
and the parser will rearrange the graph's node ID to be Unicorn-compliant (`parse.py` does not have this option yet).

### [Usage](#usage)
> :rocket: Use `parse_fast.py` instead for lightening fast (compared to `parse.py`) parsing. Highly recommended for a big dataset!

You can either run our Python script directly or use our Makefile template that runs the parser in a virtual environment (recommended).
Should you choose to run the script directly, you must install `tqdm` first:
```
pip install tqdm
```
You can then run:
```
python parse.py -h
```
to understand the required arguments.

If you prefer using a virtual environment, make sure you have `virtualenv` installed.
The Makefile template is located in the `example/` folder in which we include a small example to demonstrate
how you can run the parser. Simply run:
```
make example
```
to check out the outputs. To clean up, run:
```
make clean
```

:new:
You can now use `parse_fast.py` for a much faster parsing performance!
However, you must install `pandas`:
```
pip install pandas
```
Using `parse_fast.py` is highly recommended. Run:
```
python parse_fast.py -h
```
to understand the required arguments.


### [Graph Format](#graph-format)
The input graph should have one line per edge, and each edge should look like this:
```
4	a	5	c	p	0
```
Where:
* `4`: the node ID of the source node of the edge
* `a`: the type of the source node
* `5`: the node ID of the destination node of the edge
* `c`: the type of the destination node
* `p`: the type of the edge
* `0`: the ID of the graph

With the ID of the graph, you can in fact include multiple graphs in the same input file,
but you must specify the ID when parsing the graph.

The *base* output graph also contains one line per edge, and each edge would look like this:
```
4 5 a:c:p:1
```
The only difference from the format above, besides the format itself, is the logical timestamp (`1` in the example above)
that is attached to each edge.
Note that graph ID is no longer part of the edge, so the output file contains only a single graph as specified during parsing.

The *stream* output graph, written to a different file, again contains one line per edge, and each edge would look like this:
```
4 14 a:c:u:0:1:26
```
`4` and `14` are still the ID of the source and the destination node.
`a`, `c`, and `u` are the type of the source node, the destination node, and the edge, respectively.
`26` is the logical timestamp.
For each edge, `0` means we have observed the same node ID previously while `1` means that this is a new node never before seen.
There two fields are used in the later graph processing framework and may not be useful to you if you use a different framework.


