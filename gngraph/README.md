GNGraph Implementation with postgres DB and Static files.

The new implementation will following directories

gngraph/ : Main GNGraph related functions and classes

gngraph/ingest:
This directory contains ingestion functions and classes.

gngraph_ingest_pd.py: This file is Pandas-dataframe based ingestion to both PostgresDB and StaticFiles
gngraph/ingest/bizrules: This directory contains functions and classes to add and search business rule driven logic.
Currently the business rule format is JSON based and the rule will applied to all graph nodes and appropriate BizRule edges will be created.

gngraph/search: This directory contains search related functions and classes.

gngraph_search_main.py: This file is Spark-based functions to map existing nodes and then execute sql search to get nodes and edges.
gngraph_sqlparser.py: parses sql statement and used to augment user-search queries.
gngraph/gngraph_dbops:
This directory contains file and db operations for Postgres and Static files.

The files:

gngraph_pgresdbops.py
gngraph_staticfileops.py
are used for ingestion and Pandas based.

The files:

gngraph_pgresdbops_srch.py
gngraph_staticfiles_srch.py
are used for user search queries and spark-based


gndata/: The directory where gngraph data is stored as static files and also contains uploads directory.