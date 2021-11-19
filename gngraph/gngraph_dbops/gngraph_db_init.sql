-- Create main DATABASE
CREATE DATABASE :d;
\connect :d
--- GNMeta Schema and Tables
CREATE SCHEMA gnmeta;


CREATE TABLE gnmeta.gnnodes (  gnnodeid bigint NOT NULL PRIMARY KEY,
gnnodename text, gnnodetype text, gnnodeprop json, uptmstmp timestamp);

CREATE TABLE gnmeta.gnedges ( gnedgeid bigint NOT NULL PRIMARY KEY, gnedgename text, gnedgetype text, gnsrcnodeid bigint, gntgtnodeid bigint, gnedgeprop json, uptmstmp timestamp);


CREATE TABLE gnmeta.gnbizrules ( gnrelid bigint NOT NULL PRIMARY KEY, gnrelname text, gnreltype text, gnsrcnodeid bigint,  gntgtnodeid bigint, gnmatchnodeid bigint, gnrelprop json, gnrelobj json,  state text, freq text, uptmstmp timestamp);
   

--- Default Business Domains

CREATE SCHEMA CUSTOMER_DOMAIN;
CREATE SCHEMA PRODUCT_DOMAIN;
CREATE SCHEMA SALES_DOMAIN;

