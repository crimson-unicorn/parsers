import sys
import argparse
import math
import tqdm


# make argparse arguments global
CONSOLE_ARGUMENTS = None


def compare_edges(a, b):
    """Compare two edges @a and @b based on logical timestamps.
    As seen below, edge[5] = edge_logical_timestamp."""
    if long(a[5]) > long(b[5]):
        return 1
    else:
        return -1


def read_single_graph(file_name):
    """Parsing edgelist from the output of prepare.py.
    The format from prepare.py looks like:
    <source_node_id> \t <destination_node_id> \t <hashed_source_type>:<hashed_destination_type>:<hashed_edge_type>:<edge_logical_timestamp>[:<timestamp_stats>]
    The last '<timestamp_stats>' may or may not exist depending on whether the -s/-t option is set when running prepare.py.
    Returned from this funtion is a list of edges, each of which is itself a list containing:
    [source_node_id, destination_node_id, source_node_type, destination_node_type, edge_type, logical_timestamp, [timestamp,] source_node_seen, destination_node_seen]
    The `timestamp` may or may not exist.
    """
    map_id = dict()	# maps original IDs to new IDs, which always start from 0
    new_id = 0
    graph = list()      # list of parsed edges
    
    description = '\x1b[6;30;42m[STATUS]\x1b[0m Sorting edges in CamFlow data from {}'.format(file_name)
    pb = tqdm.tqdm(desc=description, mininterval=1.0, unit=" edges")
    with open(file_name, 'r') as f:
        for line in f:
            pb.update()                                     # for progress tracking
            try:
                edge = line.strip().split("\t")
                attributes = edge[2].strip().split(":")         # [hashed_source_type, hashed_destination_type, hashed_edge_type, edge_logical_timestamp, [timestamp_stats]]
                source_node_type = attributes[0]                # hashed_source_type
                destination_node_type = attributes[1]           # hashed_destination_type
                edge_type = attributes[2]                       # hashed_edge_type
                edge_order = attributes[3]                      # edge_logical_timestamp
                if CONSOLE_ARGUMENTS.stats:
                    ts = attributes[4]                          # timestamp_stats
                elif CONSOLE_ARGUMENTS.jiffies:
                    ts = attributes[4]                          # CamFlow jiffies
                # now we rearrange the edge vector:
                # edge[0] is source_node_id, as orginally split
                # edge[1] is destination_node_id, as originally split
                edge[2] = source_node_type
                edge.append(destination_node_type)              # edge[3] = hashed_destination_type
                edge.append(edge_type)                          # edge[4] = hashed_edge_type
                edge.append(edge_order)                         # edge[5] = edge_logical_timestamp
                if CONSOLE_ARGUMENTS.stats:
                    edge.append(ts)                             # optional: edge[6] = timestamp_stats
                elif CONSOLE_ARGUMENTS.jiffies:
                    edge.append(ts)                             # optional: edge[6] = jiffies

                graph.append(edge)
            except:
                print("{}".format(line))
    f.close()
    pb.close()
    # sort the graph edges based on logical timestamps
    graph.sort(compare_edges)

    description = '\x1b[6;30;42m[STATUS]\x1b[0m Parsing edges in CamFlow data (final stage) from {}'.format(file_name)
    pb = tqdm.tqdm(desc=description, mininterval=1.0, unit=" edge")
    for edge in graph:
        pb.update()
        if edge[0] in map_id:                               # check if source ID has been seen before
            edge[0] = map_id[edge[0]]
            edge.append("0")                                # edge[6/7] = whether source node has been seen before
        else:
            edge.append("1")
            map_id[edge[0]] = str(new_id)
            edge[0] = str(new_id)
            new_id = new_id + 1

        if edge[1] in map_id:                               # check if destination ID has been seen before
            edge[1] = map_id[edge[1]]
            edge.append("0")                                # edge[7/8] = whether destination node has been seen before
        else:
            edge.append("1")
            map_id[edge[1]] = str(new_id)
            edge[1] = str(new_id)
            new_id = new_id + 1

    pb.close()
    return graph


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='input data file path (from the output of prepare.py)', required=True)
    parser.add_argument('-b', '--base-size', help='the size of the base graph (default to 10%% of the total graph size)', type=int)
    parser.add_argument('-B', '--base', help='output destination file path of the base graph', required=True)
    parser.add_argument('-S', '--stream', help='output destination file path of the stream graph', required=True)
    parser.add_argument('-s', '--stats', help='record runtime graph generation speed (you must turn the same option on in the previous prepare.py stage; default is false)', action='store_true')
    parser.add_argument('-I', '--interval', help='the interval (in terms of number of edges) to record time (you must set this value if -s is set)', type=int)
    parser.add_argument('-f', '--stats-file', help='file path to record the statistics (only valid if -s is set; default is ts.txt)', default='ts.txt')
    parser.add_argument('-t', '--jiffies', help='record CamFlow graph jiffies; -s can overwrite this option', action='store_true')
    args = parser.parse_args()

    CONSOLE_ARGUMENTS = args

    graph = read_single_graph(args.input)
    # default to 10% of the total edges in the graph
    base_graph_size = int(math.ceil(len(graph) * 0.1))
    if args.base_size is not None:
        base_graph_size = args.base_size
    stream_graph_size = len(graph) - base_graph_size
    
    base_file = open(args.base, "w")
    stream_file = open(args.stream, "w")

    if args.stats:
        if not args.interval:
            print("You must set -I if you choose to record runtime graph generation performance")
            exit(1)
        # for runtime performance eval.
        ts_file = open(args.stats_file, "w")
        # we use this flag to make sure we record the time it takes to create base graph only once.
        recorded_once = False
        edge_cnt = 0
        
    for num, edge in enumerate(graph):
        if num < base_graph_size:
            if args.jiffies:
                base_file.write("{} {} {}:{}:{}:{}:{}\n".format(edge[0], edge[1], edge[2], edge[3], edge[4], edge[5], edge[6]))
            else:
                base_file.write("{} {} {}:{}:{}:{}\n".format(edge[0], edge[1], edge[2], edge[3], edge[4], edge[5]))
	else:
            if args.stats:
                stream_file.write("{} {} {}:{}:{}:{}:{}:{}:{}\n".format(edge[0], edge[1], edge[2], edge[3], edge[4], edge[7], edge[8], edge[5], edge[6]))
            elif args.jiffies:
                stream_file.write("{} {} {}:{}:{}:{}:{}:{}:{}\n".format(edge[0], edge[1], edge[2], edge[3], edge[4], edge[7], edge[8], edge[5], edge[6]))
            else:
                stream_file.write("{} {} {}:{}:{}:{}:{}:{}\n".format(edge[0], edge[1], edge[2], edge[3], edge[4], edge[6], edge[7], edge[5]))
        if args.stats:
            edge_cnt += 1
            if not recorded_once and edge_cnt == base_graph_size:
                ts_file.write("{}\n".format(edge[6]))
                edge_cnt = 0
                recorded_once = True
            if edge_cnt == args.interval:
                ts_file.write("{}\n".format(edge[6]))
                edge_cnt = 0
            # record time for the last round of edges
            if num == len(graph) - 1:
                ts_file.write("{}\n".format(edge[6]))
    
    print("\x1b[6;30;42m[SUCCESS]\x1b[0m Graph {} is processed.".format(args.input))
    print("\x1b[6;30;42m[SUCCESS]\x1b[0m Base graph of size {} is located at {}".format(base_graph_size, args.base))
    print("\x1b[6;30;42m[SUCCESS]\x1b[0m Stream graph of size {} is located at {}".format(stream_graph_size, args.stream))
    if args.stats:
        print("\x1b[6;30;42m[SUCCESS]\x1b[0m Time information is located at {}".format(args.stats_file))

    base_file.close()
    stream_file.close()
    if args.stats:
        ts_file.close()

