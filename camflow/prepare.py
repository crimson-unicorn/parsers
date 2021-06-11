import os
import sys
import argparse
import json
import logging
import xxhash
import time
import datetime
import tqdm
import sqlite3

# make argparse arguments global
CONSOLE_ARGUMENTS = None
# global database object to store mappings
# from hash values to graph elements
HASHMAP_DB = None


def hashgen(l):
    """Generate a single hash value from a list. @l is a list of
    string values, which can be properties of a node/edge. This
    function returns a single hashed integer value."""
    hasher = xxhash.xxh64()
    for e in l:
        hasher.update(e)
    h = hasher.intdigest()
    # store the hash mapping to the global DB
    if HASHMAP_DB:
        db_add(HASHMAP_DB, h, l)
    return h


def nodegen(node):
    """Generate a single hash value for a CamFlow node.
    We hash type information, SELinux security context, mode, and name.
    @node is the CamFlow node data, parsed as a dictionary. This
    function returns a single hashed integer value for the node."""
    l = list()
    assert(node["prov:type"])               # CamFlow node must contain "prov:type" field
    # NOTE: "link" type appears in both node and edge type
    #       We hack to avoid this for hashmap database
    # TODO: maybe there is a better way?
    if node["prov:type"] == "link":
        l.append("nlink")
    else:
        l.append(node["prov:type"])

    # CamFlow node may or may not have the
    # following fields. If not, we will
    # simply use N/A to represent absense.
    if "cf:secctx" in node:
        l.append(node["cf:secctx"])
    else:
        l.append("N/A")
    if "cf:mode" in node:
        l.append(node["cf:mode"])
    else:
        l.append("N/A")
    if "cf:name" in node:
        l.append(node["cf:name"])
    else:
        l.append("N/A")
    return hashgen(l)


def edgegen(edge):
    """Generate a single hash value for a CamFlow edge. We
    hash type information and flags. @edge is the CamFlow
    edge data, parsed as a dictionary. This function returns
    a single hashed integer value of the edge."""
    l = list()
    assert(edge["prov:type"])               # CamFlow edge must contain "prov:type" field
    l.append(edge["prov:type"])
    if "cf:flags" in edge:
        l.append(edge["cf:flags"])
    else:
        l.append("N/A")
    return hashgen(l)


def db_init(db_file):
    """Create the database we'll use to map hash values to graph structures. """
    sql_table_create = ''' CREATE TABLE IF NOT EXISTS {} (
                        hash text PRIMARY KEY,
                        val text,
                        level integer); '''
    # connect to sqlite3 database
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print("\33[101m[FATAL]\033[0m {}".format(e))
        exit(1)
    # create a cursor to create a table
    try:
        cursor = conn.cursor()
        cursor.execute(sql_table_create.format(CONSOLE_ARGUMENTS.hashmap_name))
    except sqlite3.Error as e:
        print("\33[101m[FATAL]\033[0m {}".format(e))
        exit(1)
    return cursor


def db_add(cursor, h, l):
    """Add a hash value @h and the corresponding list @l being hashed
    to the database pointed by @cursor. """
    # Note: ON ONFLICT is not supported by older version of SQLite (use the next commented code if it is supported)
    # sql_insert = ''' INSERT INTO {} (hash, val, level) VALUES (?,?,0) ON CONFLICT(hash) DO UPDATE SET val=?; '''
    sql_insert = ''' INSERT OR REPLACE INTO {} (hash, val, level) VALUES (?,?,0); '''
    s = str(l)
    # Use the next commented code if use ON CONFLICT
    # cursor.execute(sql_insert.format(CONSOLE_ARGUMENTS.hashmap_name), (str(h), s, s))
    cursor.execute(sql_insert.format(CONSOLE_ARGUMENTS.hashmap_name), (str(h), s))


def db_close(cursor):
    """Try to close the database through the cursor.
    If cursor does not refer to a valid database, we simply do nothing. """
    try:
        conn = cursor.connection
        conn.commit()
        cursor.close()
        conn.close()
    except:
        print("\33[5;30;42m[WARNING]\033[0m Database is not closed properly...maybe you don't even have one")


def parse_nodes(json_string, node_map):
    """Parse a CamFlow JSON string that may contain nodes ("activity" or "entity").
    Parsed nodes populate @node_map, which is a dictionary that maps the node's UID,
    which is assigned by CamFlow to uniquely identify a node object, to a hashed
    value (in str) which represents the 'type' of the node. """
    try:
        # use "ignore" if non-decodeable exists in the @json_string
        json_object = json.loads(json_string.decode("utf-8","ignore"))
    except Exception as e:
        print("Exception ({}) occurred when parsing a node in JSON:".format(e))
        print(json_string)
        exit(1)
    if "activity" in json_object:
        activity = json_object["activity"]
        for uid in activity:
            if not uid in node_map:     # only parse unseen nodes
                if "prov:type" not in activity[uid]:
                    # a node must have a type.
                    # record this issue if logging is turned on
                    if CONSOLE_ARGUMENTS.verbose:
                        logging.debug("skipping a problematic activity node with no 'prov:type': {}".format(uid))
                else:
                    node_map[uid] = str(nodegen(activity[uid]))

    if "entity" in json_object:
        entity = json_object["entity"]
        for uid in entity:
            if not uid in node_map:
                if "prov:type" not in entity[uid]:
                    if CONSOLE_ARGUMENTS.verbose:
                        logging.debug("skipping a problematic entity node with no 'prov:type': {}".format(uid))
                else:
                    node_map[uid] = str(nodegen(entity[uid]))


def parse_all_nodes(filename, node_map):
    """Parse all nodes in CamFlow data. @filename is the file path of
    the CamFlow data to parse. @node_map contains the mappings of all
    CamFlow nodes to their hashed attributes. """
    description = '\x1b[6;30;42m[STATUS]\x1b[0m Parsing nodes in CamFlow data from {}'.format(filename)
    pb = tqdm.tqdm(desc=description, mininterval=1.0, unit=" recs")
    with open(filename, 'r') as f:
        # each line in CamFlow data could contain multiple
        # provenance nodes, we call @parse_nodes routine.
        for line in f:
            pb.update()                 # for progress tracking
            parse_nodes(line, node_map)
    f.close()
    pb.close()


def parse_all_edges(inputfile, outputfile, node_map, noencode):
    """Parse all edges (including their timestamp) from CamFlow data file @inputfile
    to an @outputfile. Before this function is called, parse_all_nodes should be called
    to populate the @node_map for all nodes in the CamFlow file. If @noencode is set,
    we do not hash the nodes' original UUIDs generated by CamFlow to integers. This
    function returns the total number of valid edge parsed from CamFlow dataset.

    The output edgelist has the following format for each line, if -s is not set:
        <source_node_id> \t <destination_node_id> \t <hashed_source_type>:<hashed_destination_type>:<hashed_edge_type>:<edge_logical_timestamp>
    If -s is set, each line would look like:
        <source_node_id> \t <destination_node_id> \t <hashed_source_type>:<hashed_destination_type>:<hashed_edge_type>:<edge_logical_timestamp>:<timestamp_stats>"""
    total_edges = 0
    smallest_timestamp = None
    # scan through the entire file to find the smallest timestamp from all the edges.
    # this step is only needed if we need to add some statistical information.
    if CONSOLE_ARGUMENTS.stats:
        description = '\x1b[6;30;42m[STATUS]\x1b[0m Scanning edges in CamFlow data from {}'.format(inputfile)
        pb = tqdm.tqdm(desc=description, mininterval=1.0, unit=" recs")
        with open(inputfile, 'r') as f:
            for line in f:
                pb.update()
                json_object = json.loads(line.decode("utf-8","ignore"))
 
                if "used" in json_object:
                    used = json_object["used"]
                    for uid in used:
                        if "prov:type" not in used[uid]:
                            continue
                        if "cf:date" not in used[uid]:
                            continue
                        if "prov:entity" not in used[uid]:
                            continue
                        if "prov:activity" not in used[uid]:
                            continue
                        srcUUID = used[uid]["prov:entity"]
                        dstUUID = used[uid]["prov:activity"]
                        if srcUUID not in node_map:
                            continue
                        if dstUUID not in node_map:
                            continue
                        timestamp_str = used[uid]["cf:date"]
                        ts = time.mktime(datetime.datetime.strptime(timestamp_str, "%Y:%m:%dT%H:%M:%S").timetuple())
                        if smallest_timestamp == None or ts < smallest_timestamp:
                            smallest_timestamp = ts

                if "wasGeneratedBy" in json_object:
                    wasGeneratedBy = json_object["wasGeneratedBy"]
                    for uid in wasGeneratedBy:
                        if "prov:type" not in wasGeneratedBy[uid]:
                            continue
                        if "cf:date" not in wasGeneratedBy[uid]:
                            continue
                        if "prov:entity" not in wasGeneratedBy[uid]:
                            continue
                        if "prov:activity" not in wasGeneratedBy[uid]:
                            continue
                        srcUUID = wasGeneratedBy[uid]["prov:activity"]
                        dstUUID = wasGeneratedBy[uid]["prov:entity"]
                        if srcUUID not in node_map:
                            continue
                        if dstUUID not in node_map:
                            continue
                        timestamp_str = wasGeneratedBy[uid]["cf:date"]
                        ts = time.mktime(datetime.datetime.strptime(timestamp_str, "%Y:%m:%dT%H:%M:%S").timetuple())
                        if smallest_timestamp == None or ts < smallest_timestamp:
                            smallest_timestamp = ts

                if "wasInformedBy" in json_object:
                    wasInformedBy = json_object["wasInformedBy"]
                    for uid in wasInformedBy:
                        if "prov:type" not in wasInformedBy[uid]:
                            continue
                        if "cf:date" not in wasInformedBy[uid]:
                            continue
                        if "prov:informant" not in wasInformedBy[uid]:
                            continue
                        if "prov:informed" not in wasInformedBy[uid]:
                            continue
                        srcUUID = wasInformedBy[uid]["prov:informant"]
                        dstUUID = wasInformedBy[uid]["prov:informed"]
                        if srcUUID not in node_map:
                            continue
                        if dstUUID not in node_map:
                            continue
                        timestamp_str = wasInformedBy[uid]["cf:date"]
                        ts = time.mktime(datetime.datetime.strptime(timestamp_str, "%Y:%m:%dT%H:%M:%S").timetuple())
                        if smallest_timestamp == None or ts < smallest_timestamp:
                            smallest_timestamp = ts

                if "wasDerivedFrom" in json_object:
                    wasDerivedFrom = json_object["wasDerivedFrom"]
                    for uid in wasDerivedFrom:
                        if "prov:type" not in wasDerivedFrom[uid]:
                            continue
                        if "cf:date" not in wasDerivedFrom[uid]:
                            continue
                        if "prov:usedEntity" not in wasDerivedFrom[uid]:
                            continue
                        if "prov:generatedEntity" not in wasDerivedFrom[uid]:
                            continue
                        srcUUID = wasDerivedFrom[uid]["prov:usedEntity"]
                        dstUUID = wasDerivedFrom[uid]["prov:generatedEntity"]
                        if srcUUID not in node_map:
                            continue
                        if dstUUID not in node_map:
                            continue
                        timestamp_str = wasDerivedFrom[uid]["cf:date"]
                        ts = time.mktime(datetime.datetime.strptime(timestamp_str, "%Y:%m:%dT%H:%M:%S").timetuple())
                        if smallest_timestamp == None or ts < smallest_timestamp:
                            smallest_timestamp = ts

                if "wasAssociatedWith" in json_object:
                    wasAssociatedWith = json_object["wasAssociatedWith"]
                    for uid in wasAssociatedWith:
                        if "prov:type" not in wasAssociatedWith[uid]:
                            continue
                        if "cf:date" not in wasAssociatedWith[uid]:
                            continue
                        if "prov:agent" not in wasAssociatedWith[uid]:
                            continue
                        if "prov:activity" not in wasAssociatedWith[uid]:
                            continue
                        srcUUID = wasAssociatedWith[uid]["prov:agent"]
                        dstUUID = wasAssociatedWith[uid]["prov:activity"]
                        if srcUUID not in node_map:
                            continue
                        if dstUUID not in node_map:
                            continue
                        timestamp_str = wasAssociatedWith[uid]["cf:date"]
                        ts = time.mktime(datetime.datetime.strptime(timestamp_str, "%Y:%m:%dT%H:%M:%S").timetuple())
                        if smallest_timestamp == None or ts < smallest_timestamp:
                            smallest_timestamp = ts
        f.close()
        pb.close()
    
    # we will go through the CamFlow data (again) and output edgelist to a file
    output = open(outputfile, "w+")
    description = '\x1b[6;30;42m[STATUS]\x1b[0m Parsing edges in CamFlow data from {}'.format(inputfile)
    pb = tqdm.tqdm(desc=description, mininterval=1.0, unit=" recs")
    with open(inputfile, 'r') as f:
        for line in f:
            pb.update()
            json_object = json.loads(line.decode("utf-8","ignore"))

            if "used" in json_object:
                used = json_object["used"]
                for uid in used:
                    if "prov:type" not in used[uid]:
                        # an edge must have a type; if not,
                        # we will have to skip the edge. Log
                        # this issue if verbose is set.
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (used) record without type: {}".format(uid))
                        continue
                    else:
                        edgetype = edgegen(used[uid])
                    # cf:id is used as logical timestamp to order edges
                    if "cf:id" not in used[uid]:
                        # an edge must have a logical timestamp;
                        # if not we will have to skip the edge.
                        # Log this issue if verbose is set.
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (used) record without logical timestamp: {}".format(uid))
                        continue
                    else:
                        timestamp = used[uid]["cf:id"]
		    if "prov:entity" not in used[uid]:
                        # an edge's source node must exist;
                        # if not, we will have to skip the
                        # edge. Log this issue if verbose is set.
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (used/{}) record without source UUID: {}".format(used[uid]["prov:type"], uid))
                        continue
                    if "prov:activity" not in used[uid]:
                        # an edge's destination node must exist;
                        # if not, we will have to skip the edge.
                        # Log this issue if verbose is set.
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (used/{}) record without destination UUID: {}".format(used[uid]["prov:type"], uid))
                        continue
                    srcUUID = used[uid]["prov:entity"]
                    dstUUID = used[uid]["prov:activity"]
                    # both source and destination node must
                    # exist in @node_map; if not, we will
                    # have to skip the edge. Log this issue
                    # if verbose is set.
                    if srcUUID not in node_map:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (used/{}) record with an unseen srcUUID: {}".format(used[uid]["prov:type"], uid))
                        continue
                    else:
                        srcVal = node_map[srcUUID]
                    if dstUUID not in node_map:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (used/{}) record with an unseen dstUUID: {}".format(used[uid]["prov:type"], uid))
                        continue
                    else:
                        dstVal = node_map[dstUUID]
                    if "cf:date" not in used[uid]:
                        # an edge must have a timestamp; if
                        # not, we will have to skip the edge.
                        # Log this issue if verbose is set.
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (used) record without timestamp: {}".format(uid))
                        continue
                    else:
                        # we record original timestamp
                        # for visicorn analysis
                        ts_str = used[uid]["cf:date"]
                        # we only record @adjusted_ts if we need
                        # to record stats of CamFlow dataset.
                        if CONSOLE_ARGUMENTS.stats:
                            ts = time.mktime(datetime.datetime.strptime(ts_str, "%Y:%m:%dT%H:%M:%S").timetuple())
                            adjusted_ts = ts - smallest_timestamp
                    total_edges += 1
                    if noencode:
                        if CONSOLE_ARGUMENTS.stats:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(srcUUID, dstUUID, srcVal, dstVal, edgetype, timestamp, adjusted_ts))
                        else:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(srcUUID, dstUUID, srcVal, dstVal, edgetype, timestamp, ts_str))
		    else:
                        if CONSOLE_ARGUMENTS.stats:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(hashgen([srcUUID]), hashgen([dstUUID]), srcVal, dstVal, edgetype, timestamp, adjusted_ts))
                        else:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(hashgen([srcUUID]), hashgen([dstUUID]), srcVal, dstVal, edgetype, timestamp, ts_str))

            if "wasGeneratedBy" in json_object:
                wasGeneratedBy = json_object["wasGeneratedBy"]
                for uid in wasGeneratedBy:
                    if "prov:type" not in wasGeneratedBy[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasGeneratedBy) record without type: {}".format(uid))
                        continue
                    else:
                        edgetype = edgegen(wasGeneratedBy[uid])
                    if "cf:id" not in wasGeneratedBy[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasGeneratedBy) record without logical timestamp: {}".format(uid))
                        continue
                    else:
                        timestamp = wasGeneratedBy[uid]["cf:id"]
                    if "prov:entity" not in wasGeneratedBy[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasGeneratedBy/{}) record without source UUID: {}".format(wasGeneratedBy[uid]["prov:type"], uid))
                        continue
                    if "prov:activity" not in wasGeneratedBy[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasGeneratedBy/{}) record without destination UUID: {}".format(wasGeneratedBy[uid]["prov:type"], uid))
                        continue
                    srcUUID = wasGeneratedBy[uid]["prov:activity"]
                    dstUUID = wasGeneratedBy[uid]["prov:entity"]
                    if srcUUID not in node_map:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasGeneratedBy/{}) record with an unseen srcUUID: {}".format(wasGeneratedBy[uid]["prov:type"], uid))
                        continue
                    else:
                        srcVal = node_map[srcUUID]
                    if dstUUID not in node_map:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasGeneratedBy/{}) record with an unsen dstUUID: {}".format(wasGeneratedBy[uid]["prov:type"], uid))
                        continue
                    else:
                        dstVal = node_map[dstUUID]
                    if "cf:date" not in wasGeneratedBy[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasGeneratedBy) record without timestamp: {}".format(uid))
                        continue
                    else:
                        ts_str = wasGeneratedBy[uid]["cf:date"]
                        if CONSOLE_ARGUMENTS.stats:
                            ts = time.mktime(datetime.datetime.strptime(ts_str, "%Y:%m:%dT%H:%M:%S").timetuple())
                            adjusted_ts = ts - smallest_timestamp
                    total_edges += 1
                    if noencode:
                        if CONSOLE_ARGUMENTS.stats:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(srcUUID, dstUUID, srcVal, dstVal, edgetype, timestamp, adjusted_ts))
                        else:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(srcUUID, dstUUID, srcVal, dstVal, edgetype, timestamp, ts_str))
                    else:
                        if CONSOLE_ARGUMENTS.stats:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(hashgen([srcUUID]), hashgen([dstUUID]), srcVal, dstVal, edgetype, timestamp, adjusted_ts))
                        else:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(hashgen([srcUUID]), hashgen([dstUUID]), srcVal, dstVal, edgetype, timestamp, ts_str))
	
            if "wasInformedBy" in json_object:
                wasInformedBy = json_object["wasInformedBy"]
                for uid in wasInformedBy:
                    if "prov:type" not in wasInformedBy[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasInformedBy) record without type: {}".format(uid))
                        continue
                    else:
                        edgetype = edgegen(wasInformedBy[uid])
                    if "cf:id" not in wasInformedBy[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasInformedBy) record without logical timestamp: {}".format(uid))
                        continue
                    else:
                        timestamp = wasInformedBy[uid]["cf:id"]
                    if "prov:informant" not in wasInformedBy[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasInformedBy/{}) record without source UUID: {}".format(wasInformedBy[uid]["prov:type"], uid))
                        continue
                    if "prov:informed" not in wasInformedBy[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasInformedBy/{}) record without destination UUID: {}".format(wasInformedBy[uid]["prov:type"], uid))
                        continue
                    srcUUID = wasInformedBy[uid]["prov:informant"]
                    dstUUID = wasInformedBy[uid]["prov:informed"]
                    if srcUUID not in node_map:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasInformedBy/{}) record with an unseen srcUUID: {}".format(wasInformedBy[uid]["prov:type"], uid))
                        continue
                    else:
                        srcVal = node_map[srcUUID]
                    if dstUUID not in node_map:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasInformedBy/{}) record with an unseen dstUUID: {}".format(wasInformedBy[uid]["prov:type"], uid))
                        continue
                    else:
                        dstVal = node_map[dstUUID]
                    if "cf:date" not in wasInformedBy[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasInformedBy) record without timestamp: {}".format(uid))
                        continue
                    else:
                        ts_str = wasInformedBy[uid]["cf:date"]
                        if CONSOLE_ARGUMENTS.stats:
                            ts = time.mktime(datetime.datetime.strptime(ts_str, "%Y:%m:%dT%H:%M:%S").timetuple())
                            adjusted_ts = ts - smallest_timestamp
                    total_edges += 1
                    if noencode:
                        if CONSOLE_ARGUMENTS.stats:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(srcUUID, dstUUID, srcVal, dstVal, edgetype, timestamp, adjusted_ts))
                        else:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(srcUUID, dstUUID, srcVal, dstVal, edgetype, timestamp, ts_str))
                    else:
                        if CONSOLE_ARGUMENTS.stats:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(hashgen([srcUUID]), hashgen([dstUUID]), srcVal, dstVal, edgetype, timestamp, adjusted_ts))
                        else:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(hashgen([srcUUID]), hashgen([dstUUID]), srcVal, dstVal, edgetype, timestamp, ts_str))

            if "wasDerivedFrom" in json_object:
                wasDerivedFrom = json_object["wasDerivedFrom"]
                for uid in wasDerivedFrom:
                    if "prov:type" not in wasDerivedFrom[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasDerivedFrom) record without type: {}".format(uid))
                        continue
                    else:
                        edgetype = edgegen(wasDerivedFrom[uid])
                    if "cf:id" not in wasDerivedFrom[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasDerivedFrom) record without logical timestamp: {}".format(uid))
                            continue
                    else:
                        timestamp = wasDerivedFrom[uid]["cf:id"]
                    if "prov:usedEntity" not in wasDerivedFrom[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasDerivedFrom/{}) record without source UUID: {}".format(wasDerivedFrom[uid]["prov:type"], uid))
                        continue
                    if "prov:generatedEntity" not in wasDerivedFrom[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasDerivedFrom/{}) record without destination UUID: {}".format(wasDerivedFrom[uid]["prov:type"], uid))
                        continue
                    srcUUID = wasDerivedFrom[uid]["prov:usedEntity"]
                    dstUUID = wasDerivedFrom[uid]["prov:generatedEntity"]
                    if srcUUID not in node_map:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasDerivedFrom/{}) record with an unseen srcUUID: {}".format(wasDerivedFrom[uid]["prov:type"], uid))
                        continue
                    else:
                        srcVal = node_map[srcUUID]
                    if dstUUID not in node_map:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasDerivedFrom/{}) record with an unseen dstUUID: {}".format(wasDerivedFrom[uid]["prov:type"], uid))
                        continue
                    else:
                        dstVal = node_map[dstUUID]
                    if "cf:date" not in wasDerivedFrom[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasDerivedFrom) record without timestamp: {}".format(uid))
                        continue
                    else:
                        ts_str = wasDerivedFrom[uid]["cf:date"]
                        if CONSOLE_ARGUMENTS.stats:
                            ts = time.mktime(datetime.datetime.strptime(ts_str, "%Y:%m:%dT%H:%M:%S").timetuple())
                            adjusted_ts = ts - smallest_timestamp
                    total_edges += 1
                    if noencode:
                        if CONSOLE_ARGUMENTS.stats:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(srcUUID, dstUUID, srcVal, dstVal, edgetype, timestamp, adjusted_ts))
                        else:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(srcUUID, dstUUID, srcVal, dstVal, edgetype, timestamp, ts_str))
                    else:
                        if CONSOLE_ARGUMENTS.stats:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(hashgen([srcUUID]), hashgen([dstUUID]), srcVal, dstVal, edgetype, timestamp, adjusted_ts))
                        else:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(hashgen([srcUUID]), hashgen([dstUUID]), srcVal, dstVal, edgetype, timestamp, ts_str))

            if "wasAssociatedWith" in json_object:
                wasAssociatedWith = json_object["wasAssociatedWith"]
                for uid in wasAssociatedWith:
                    if "prov:type" not in wasAssociatedWith[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasAssociatedWith) record without type: {}".format(uid))
                        continue
                    else:
                        edgetype = edgegen(wasAssociatedWith[uid])
                    if "cf:id" not in wasAssociatedWith[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasAssociatedWith) record without logical timestamp: {}".format(uid))
                        continue
                    else:
                        timestamp = wasAssociatedWith[uid]["cf:id"]
                    if "prov:agent" not in wasAssociatedWith[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasAssociatedWith/{}) record without source UUID: {}".format(wasAssociatedWith[uid]["prov:type"], uid))
                        continue
                    if "prov:activity" not in wasAssociatedWith[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasAssociatedWith/{}) record without destination UUID: {}".format(wasAssociatedWith[uid]["prov:type"], uid))
                        continue
                    srcUUID = wasAssociatedWith[uid]["prov:agent"]
                    dstUUID = wasAssociatedWith[uid]["prov:activity"]
                    if srcUUID not in node_map:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasAssociatedWith/{}) record with an unseen srcUUID: {}".format(wasAssociatedWith[uid]["prov:type"], uid))
                        continue
                    else:
                        srcVal = node_map[srcUUID]
                    if dstUUID not in node_map:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasAssociatedWith/{}) record with an unseen dstUUID: {}".format(wasAssociatedWith[uid]["prov:type"], uid))
                        continue
                    else:
                        dstVal = node_map[dstUUID]
                    if "cf:date" not in wasAssociatedWith[uid]:
                        if CONSOLE_ARGUMENTS.verbose:
                            logging.debug("edge (wasAssociatedWith) record without timestamp: {}".format(uid))
                        continue
                    else:
                        ts_str = wasAssociatedWith[uid]["cf:date"]
                        if CONSOLE_ARGUMENTS.stats:
                            ts = time.mktime(datetime.datetime.strptime(ts_str, "%Y:%m:%dT%H:%M:%S").timetuple())
                            adjusted_ts = ts - smallest_timestamp
                    total_edges += 1
                    if noencode:
                        if CONSOLE_ARGUMENTS.stats:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(srcUUID, dstUUID, srcVal, dstVal, edgetype, timestamp, adjusted_ts))
                        else:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(srcUUID, dstUUID, srcVal, dstVal, edgetype, timestamp, ts_str))
                    else:
                        if CONSOLE_ARGUMENTS.stats:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(hashgen([srcUUID]), hashgen([dstUUID]), srcVal, dstVal, edgetype, timestamp, adjusted_ts))
                        else:
                            output.write("{}\t{}\t{}:{}:{}:{}:{}\n".format(hashgen([srcUUID]), hashgen([dstUUID]), srcVal, dstVal, edgetype, timestamp, ts_str))
    f.close()
    output.close()
    pb.close()
    return total_edges


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert CamFlow JSON to Unicorn edgelist')
    parser.add_argument('-i', '--input', help='input CamFlow data file path', required=True)
    parser.add_argument('-o', '--output', help='output edgelist file path', required=True)
    parser.add_argument('-d', '--dir', help='hashmap directory (use this option to create a hashmap database)')
    parser.add_argument('-m', '--hashmap-name', help='the name of the hashmap in the database. Each dataset should have a different name')
    parser.add_argument('-n', '--noencode', help='do not encode UUID in output (default is to encode)', action='store_true')
    parser.add_argument('-v', '--verbose', help='verbose logging (default is false)', action='store_true')
    parser.add_argument('-l', '--log', help='log file path (only valid is -v is set; default is debug.log)', default='debug.log')
    parser.add_argument('-s', '--stats', help='record some statistics of the CamFlow graph data and runtime graph generation speed (default is false)', action='store_true')
    parser.add_argument('-f', '--stats-file', help='file path to record the statistics (only valid if -s is set; default is stats.csv)', default='stats.csv')
    args = parser.parse_args()

    CONSOLE_ARGUMENTS = args

    if args.verbose:
        logging.basicConfig(filename=args.log, level=logging.DEBUG)

    # if --dir is provided, we will create a hashmap database
    if args.dir:
        HASHMAP_DB = db_init("{}/label.db".format(args.dir))
        if not args.hashmap_name:
            print("\33[101m[ERROR]\033[0m You must provide -m <HASHMAP_NAME> if you use -d")
            exit(1)

    node_map = dict()
    parse_all_nodes(args.input, node_map)
    total_edges = parse_all_edges(args.input, args.output, node_map, args.noencode)

    if args.stats:
        total_nodes = len(node_map)
        stats = open(args.stats_file, "a+")
        stats.write("{},{},{}\n".format(args.input, total_nodes, total_edges))

    if HASHMAP_DB:
        db_close(HASHMAP_DB)

