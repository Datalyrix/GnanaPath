####################################################################
# GnanaDWDB Fetch Ops Module on Neo4j
#    - Fetch nodes and its properties from neo4j
#    - Fetch edges(relationships) from Neo4j
###################################################################
import os, sys

curentDir = os.getcwd()
listDir = curentDir.rsplit('/', 1)[0]
# gndwdbDir=listDir+'/gndwdb'
if listDir not in sys.path:
    sys.path.append(listDir)

from gndwdb.gndwdb_neo4j_conn import gndwdb_neo4j_conn_metarepo, gndwdb_neo4j_conn_datarepo
from gnutils.replace_spl_chars import gnutils_filter_json_escval
import csv
import json
import numpy as np
import neo4j
import warnings
import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable



class gndwdbFetchApp:

    def __init__(self, driverconnp):
        ###self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.driver = driverconnp

    def close(self):
        # Don't forget to close the driver connection when you are finished
        # with it
        self.driver.close()

    def find_node_byname(self, node_name, verbose):
        with self.driver.session() as session:
            result = session.read_transaction(
                self._find_and_return_node, node_name)

            njson = '{'

            for record in result:
                if (verbose > 3):
                    print(
                        "gndwdbFetchApp: Found node: {record}".format(
                            record=record))
                if (verbose > 4):
                    print(record)
                njson += ' "node": { ' + "\n"

    @staticmethod
    def _find_and_return_node_byname(tx, node_name, verbose):
        query = (
            "MATCH (p) "
            "WHERE p.name = $name "
            "RETURN p"
        )
        result = tx.run(query, name=node_name)
        njson = ''

        for record in result:
            node = record['p']
            njson += convert_node_rec_json(node, verbose)

        return njson

    def find_node_count_by_metanode(self, metanode, verbose):

        with self.driver.session() as session:
            query = (
                    "MATCH (n {metanode:\"" + metanode +
                    "\"}) "
                    " RETURN COUNT(n)"
                )

            result = session.run(query)

            nodecount = 0
            for record in result:
                nodecount = record['COUNT(n)']
                if (verbose > 3):
                    print(
                        "gndwdbFetchApp: Found node: {record}".format(
                            record=record))

                if (verbose > 4):
                    print(record)
                    
            return nodecount    

    
    def find_node_by_id(self, node_id, verbose):

        with self.driver.session() as session:

            query = (
                "MATCH (p) "
                "WHERE ID(p) = $id "
                "RETURN p"
            )

            ###result = session.read_transaction(self._find_and_return_node_byid, node_id);
            result = session.run(query, id=node_id)

            for record in result:
                node = record['p']
                if (verbose > 3):
                    print(
                        "gndwdbFetchApp: Found node: {record}".format(
                            record=record))

                if (verbose > 4):
                    print(record)
                #njson += ' "node": { '+ "\n";
                ndict = self.convert_node_rec_dict(node, verbose)

                if (verbose > 5):
                    print('gndwdbFetchApp: node id:' + str(node_id))
                    print(ndict)

            return(ndict)

    def _find_and_return_node_byid(tx, id, verbose):
        query = (
            "MATCH (p) "
            "WHERE ID(p) = $id "
            "RETURN p"
        )

        result = tx.run(query, id=id)
        return result

    def convert_node_rec_dict(self, node, verbose):

        ndict = {}
        njson = '{' + "\n"
        nodeid = node.id

        if (verbose > 3):
            print('GNdwFetchApp:  convert node rec dict ' + str(nodeid))

            ###nl_iter = iter(node.labels);
        nodelabel = ''
        for l in iter(node.labels):
            nodelabel = l

        if (verbose > 3):
            print('GNdwFetchApp: convert node label ' + str(nodelabel))

        njson += '  "id": "' + str(nodeid) + '" ,' + "\n"
        njson += '  "nodetype": "' + gnutils_filter_json_escval(nodelabel) + '" '

        ndict['id'] = nodeid
        ndict['nodename'] = nodelabel

        i = 0
        for nkey in node.keys():
            if (verbose > 5):
                print(
                    "GNdwFetchApp: getting node keys Record  Key k:" +
                    str(nkey))

            nkval = node.get(nkey)
            if (verbose > 5):
                print(
                    "GNDwFetchApp: getting node keys Record Key val " +
                    gnutils_filter_json_escval(nkval))

            if (i >= 0):
                njson += ',' + "\n"

            if (nkey == "id"):
                nkey = "node_id"

            njson += '   "' + str(nkey) + '":  "' + \
                gnutils_filter_json_escval(nkval) + '" '
            ndict[nkey] = str(nkval)
            i += 1

        njson += "\n"
        njson += "} "
        # These attributes set at client
        #njson += '  "group": "nodes" ,'+"\n";
        #njson += '   "selectable": true,'+"\n";
        #njson += '  "grabbable":  true '+"\n";
        #njson += '  },'+"\n";
        # if (verbose > 4):
        #   print(njson);
        ndict['jsonstr'] = njson
        return(ndict)

    def find_nodes_return_rec(self, verbose):

        with self.driver.session() as session:
            njson = "{ " + "\n"
            query = (
                "MATCH (n) "
                " RETURN n LIMIT 5"
            )

            result = session.run(query)
            nnum = 0
            njson += ' "nodes": [ ' + "\n"
            for record in result:

                if (verbose > 5):

                    print(record)
                node = record["n"]

                if (nnum > 0):
                    njson += ", " + "\n"

                ndict = self.convert_node_rec_json(node, verbose)
                nj = ndict['jsonstr']
                ###nj = json.dumps(ndict);
                njson += nj
                if (verbose > 4):
                    print(
                        'gndwdbFetchOps: convet node to json ele ' +
                        str(nnum))
                    print(nj)

                nnum += 1

            njson += "\n"
            njson += "]" + "\n"

            njson += "}" + "\n"

            if (verbose > 3):
                print('gndbdwFetchOps: Find Nodes res ')
                print(njson)
            return(njson)

    def convert_rel_rec_dict(self, relnode, verbose):

        if (verbose > 4):
            print(relnode)

        rdict = {}
        reltype = relnode.type
        relid = relnode.id

        if (verbose > 3):
            print("GNDwFetchApp: converting relation  id:" +
                  str(relid) + "  type: " + str(reltype))

        rjson = '  {' + "\n"
        rjson += '  "type": "' + str(reltype) + '" ,' + "\n"
        rjson += '   "id": "' + str(relid) + '"  ,' + "\n"

        rdict['id'] = relid
        rdict['type'] = reltype

        nodes = relnode.nodes

        if (verbose > 6):
            print("GNDwFetchApp: converting relation  relationship Nodes: ")
            print(nodes)

        snode = nodes[0]
        tnode = nodes[1]
        ###print("Source Node id:"+str(snode.id));
        ##print("Target Node id:"+str(tnode.id));
        rjson += '   "source": "' + str(snode.id) + '"  ,' + "\n"
        rjson += '   "target": "' + str(tnode.id) + '"  ,' + "\n"
        rjson += '   "directed": true, ' + "\n"

        if (verbose > 3):
            print('GndwDBFetchApp:   source node ' + str(snode.id))

        sdict = self.convert_node_rec_dict(snode, verbose)
        tdict = self.convert_node_rec_dict(tnode, verbose)

        rdict['source'] = sdict
        rdict['target'] = tdict

        rk = 0

        for rkey in relnode.keys():
            rkeyval = relnode.get(rkey)
            if (rk > 0):
                rjson += ',' + "\n"

            if (verbose > 4):
                print(
                    "GNDwFetchApp: relation key:" +
                    str(rkey) +
                    " val:" +
                    str(rkeyval))

            rdict[rkey] = str(rkeyval)
            rjson += '   "' + str(rkey) + '": "' + \
                gnutils_filter_json_escval(rkeyval) + '"'
            rk += 1

        rjson += "\n"
        rjson += '}'
        rdict['jsonstr'] = rjson

        return (rdict)

    def find_edges_nodes_return_rec(self, nlist, rel_type, nlimit, verbose):

        with self.driver.session() as session:
            rel_list = {}
            nodelist = {}
            nnum = 0
            nedges = 0
            nnodes = 0
            
            if (nlist):
                query = (
                    "MATCH (n {name:\"" + nlist +
                    "\"})-[r:" + rel_type + "]->(t) "
                    " RETURN n,r,t LIMIT " + str(nlimit) + " "
                )
                if (verbose > 3):
                    print('find_edges_nodes_return_rec: processing cql '+str(query))
            else:
                query = (
                    "MATCH ()-[r:" + rel_type + "]->() "
                    " RETURN r LIMIT " + str(nlimit) + " "
                )

            result = session.run(query)
            rnum = 0
            rjson = ''
            rjson += '   "edges": [ ' + "\n"

            for record in result:
                if (verbose > 5):
                    print(record)

                relnode = record["r"]

                if (rnum > 0):
                    rjson += ", " + "\n"

                rdict = self.convert_rel_rec_dict(relnode, verbose)
                relid = rdict['id']
                rel_list[relid] = rdict
                nedges += 1

                snode = rdict['source']
                tnode = rdict['target']
                sid = snode["id"]
                tid = tnode["id"]

                if sid in nodelist.keys():
                    if (verbose > 4):
                        print(
                            " GndwDBFetchOp: source node is present " + str(sid))
                        sdict = nodelist[sid]
                        print(sdict)

                else:
                    sdict = self.find_node_by_id(sid, verbose)
                    nodelist[sid] = sdict
                    if (verbose > 4):
                        print('GndwDBFetchApp: new snode id:' + str(sid))
                        print(sdict)
                    nnum += 1
                    nnodes += 1

                if tid in nodelist.keys():
                    if (verbose > 4):
                        print(
                            " GndwDBFetchOp: target node is present " + str(tid))
                        tdict = nodelist[tid]
                        print(tdict)
                else:
                    tdict = self.find_node_by_id(tid, verbose)
                    nodelist[tid] = tdict
                    if (verbose > 4):
                        print('GndwDBFetchApp: new tnode id ' + str(tid))
                        print(tdict)
                    nnum += 1
                    nnodes += 1

                rj = rdict['jsonstr']
                rjson += rj
                rnum += 1

            rjson += "\n"
            rjson += "]"

            if (verbose > 3):
                print('gndwdbFetchOps: Fetch edges #edges: ' + str(rnum))
                print(rel_list)
                print('gndwDBFetchOps: Fetch nodes #node: ' + str(nnum))
                print(nodelist)
            nj = '   "nodes": [ ' + "\n"
            nnum = 0
            for nk in nodelist.keys():
                n = nodelist[nk]
                if (nnum > 0):
                    nj += ', ' + "\n"
                nj += n['jsonstr']
                ####nj += json.dumps(n, indent=4);
                nnum += 1

            nj += "\n"
            nj += "]" + "\n"

            retjson = '{ ' + "\n"
            retjson += ' "status": "SUCCESS" ,' + "\n"
            retjson += '  "nedges": ' + str(nedges) + ' ,' + "\n"
            retjson += '  "nnodes": ' + str(nnodes) + ' ,' + "\n"
            retjson += rjson
            retjson += ", " + "\n"
            retjson += nj
            retjson += '}' + "\n"

            #rlist_json = json.dumps(rel_list, indent=4);
            # print(rlist_json);

            return(retjson)


def gndwdb_metarepo_nodes_fetch_api(verbose):

    graph_connp = gndwdb_neo4j_conn_metarepo(verbose)

    if (graph_connp == ''):
        if (verbose > 3):
            print('gndwdb_metarepo_edges_fetch_api: Unable to connect to db server')
        rjson = '{' + "\n"
        rjson += '"status": "ERROR",' + "\n"
        rjson += '"statusmsg": "Unable to connect db server",' + "\n"
        rjson += '"edges":[],' + "\n"
        rjson += '"nodes":[] ' + "\n"
        rjson += '}' + "\n"
        return rjson

    fetchApp = gndwdbFetchApp(graph_connp)
    njson = fetchApp.find_nodes_return_rec(verbose)
    #reljson = fetchApp.find_rels_return_rec(verbose);
    # rjson = '{ '."\n";
    #rjson += njson;
    # rjson += ','."\n";

    fetchApp.close()
    return njson


def gndwdb_metarepo_edges_fetch_api(srchqry, verbose):

    graph_connp = gndwdb_neo4j_conn_metarepo(verbose)

    if (graph_connp == ''):
        if (verbose > 3):
            print('gndwdb_metarepo_edges_fetch_api: Unable to connect to db server')
        rjson = '{' + "\n"
        rjson += '"status": "ERROR", ' + "\n"
        rjson += '"statusmsg": "Unable to connect db server", '
        rjson += '"edges":[], ' + "\n"
        rjson += '"nodes":[] ' + "\n"
        rjson += '}' + "\n"
        return rjson

    fetchApp = gndwdbFetchApp(graph_connp)
    rel_type = "HAS_ATTR"

    if (srchqry):
        nodename = srchqry
    else:
        nodename = ''

    nlimit = 500
    rjson = fetchApp.find_edges_nodes_return_rec(
        nodename, rel_type, nlimit, verbose)
    #reljson = fetchApp.find_rels_return_rec(verbose);
    # rjson = '{ '."\n";
    #rjson += njson;
    # rjson += ','."\n";

    fetchApp.close()
    return rjson


def gndwdb_datarepo_edges_fetch_api(verbose):

    graph_connp = gndwdb_neo4j_conn_datarepo(verbose)

    if (graph_connp == ''):
        if (verbose > 3):
            print('gndwdb_metarepo_edges_fetch_api: Unable to connect to db server')
        rjson = '{' + "\n"
        rjson += '"status": "ERROR",' + "\n"
        rjson += '"statusmsg": "Unable to connect db server",'
        rjson += '"edges":[] ,' + "\n"
        rjson += '"nodes":[] ' + "\n"
        rjson += '}' + "\n"
        return rjson

    fetchApp = gndwdbFetchApp(graph_connp)
    rel_type = "IS"
    nodename = ''
    nlimit = 2000
    rjson = fetchApp.find_edges_nodes_return_rec(
        nodename, rel_type, nlimit, verbose)
    #reljson = fetchApp.find_rels_return_rec(verbose);
    # rjson = '{ '."\n";
    #rjson += njson;
    # rjson += ','."\n";

    fetchApp.close()
    return rjson

def gndwdb_datarepo_edges_fetch_bynodelist_api(nodelist, verbose):

    graph_connp = gndwdb_neo4j_conn_datarepo(verbose)

    if (graph_connp == ''):
        if (verbose > 3):
            print('gndwdb_metarepo_edges_fetch_api: Unable to connect to db server')
        rjson = '{' + "\n"
        rjson += '"status": "ERROR",' + "\n"
        rjson += '"statusmsg": "Unable to connect db server",'
        rjson += '"edges":[] ,' + "\n"
        rjson += '"nodes":[] ' + "\n"
        rjson += '}' + "\n"
        return rjson

    fetchApp = gndwdbFetchApp(graph_connp)
    rel_type = "IS"
    nodename = ''
    nlimit = 2000

    if (verbose > 4):
        print('gndwdb_datarepo_fetch_bynodelist: processing nodelist ');
        print(nodelist);
        
    rjson = fetchApp.find_edges_nodes_return_rec(
        nodelist, rel_type, nlimit, verbose)


    fetchApp.close()
    return rjson



if __name__ == "__main__":
    verbose = 0
    rjson = gndwdb_metarepo_edges_fetch_api(verbose)
    print(rjson)
    djson = gndwdb_datarepo_edges_fetch_api(verbose)
    print(djson)
