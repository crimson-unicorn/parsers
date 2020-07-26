import sys
import math
import tqdm
import argparse

def read_single_graph(file_name, graph_id):
    """Read a single graph with ID @graph_id from the file @file_name
    and return the list of its edges."""
    graph = list()                                              # list of parsed graph edges
    node_id_seen = list()                                       # list of the node id that we have seen already
    cnt = 1                                                     # logical timestamps of edges

    desc = "\x1b[6;30;42m[STATUS]\x1b[0m Parsing StreamSpot data (graph id: {}) from {}".format(graph_id, file_name)
    pb = tqdm.tqdm(desc=desc, mininterval=1.0, unit=" edges")
    with open(file_name, "r") as f:
        for line in f:
            edge = line.strip().split("\t")
            if edge[5] == graph_id:                             # we only parse edges that are in the graph @graph_id
                pb.update()                                     # for progress tracking
                if edge[0] in node_id_seen:                     # check if we have seen the source node before
                    edge.append("0")                            # seen node is given 0 in the edge entry
                else:
                    edge.append("1")                            # unseen node is given 1 in the edge entry
                    node_id_seen.append(edge[0])                # the unseen node is now seen
                if edge[2] in node_id_seen:                     # check if we have seen the destination node before
                    edge.append("0")
                else:
                    edge.append("1")
                    node_id_seen.append(edge[2])
                edge.append(cnt)                                # give the edge a logical timestamp
                cnt = cnt + 1
                graph.append(edge)
    f.close()
    pb.close()
    return graph


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--graph', help='the id of the graph to be parsed', required=True)
    # %% is not a typo: https://thomas-cokelaer.info/blog/2014/03/python-argparse-issues-with-the-help-argument-typeerror-o-format-a-number-is-required-not-dict/
    parser.add_argument('-s', '--size', help='the size of the base graph in absolute value (default is 10%% of the entire graph)', type=int)
    parser.add_argument('-i', '--input', help='input StreamSpot data file path', required=True)
    parser.add_argument('-b', '--base', help='output file path of the base graph', required=True)
    parser.add_argument('-S', '--stream', help='output file path of the stream graph', required=True)
    args = parser.parse_args()

    graph = read_single_graph(args.input, args.graph)
    if not args.size:
        base_graph_size = int(math.ceil(len(graph) * 0.1))
    else:
        base_graph_size = args.size
    stream_graph_size = len(graph) - base_graph_size

    base_file = open(args.base, "w")
    stream_file = open(args.stream, "w")

    cnt = 0
    for edge in graph:
        if cnt < base_graph_size:
            cnt = cnt + 1
            base_file.write("{} {} {}:{}:{}:{}\n".format(edge[0], edge[2], edge[1], edge[3], edge[4], edge[8]))
        else:
            stream_file.write("{} {} {}:{}:{}:{}:{}:{}\n".format(edge[0], edge[2], edge[1], edge[3], edge[4], edge[6], edge[7], edge[8]))

    print("\x1b[6;30;42m[SUCCESS]\x1b[0m Graph {} is processed.".format(args.graph))
    print("\x1b[6;30;42m[SUCCESS]\x1b[0m Base graph of size {} is located at {}".format(base_graph_size, args.base))
    print("\x1b[6;30;42m[SUCCESS]\x1b[0m Stream graph of size {} is located at {}".format(stream_graph_size, args.stream))

    base_file.close()
    stream_file.close()

