# mysql_span_exporter

uses the event tables from the mysql `performance_schema` (primarily the one for [statements](https://dev.mysql.com/doc/mysql-perfschema-excerpt/5.7/en/performance-schema-statement-tables.html)) to create spans corresponding to important SQL events

appends a `traceparent` comment to issued queries to propagate context from client code to the SQL server

very basic example
![example](./example.png)

TODOs:

- split out querier to separate service to better illustrate propagation
- isolate sql driver instrumentation (the thing that appends the comment) from the above querier service
- teach the exporter to incorporate transaction/stage/wait data, add more attributes and stuff
  - debug timer stuff; should we just give up and add the span start time as another piece of propagation data?
- teach the exporter to check if [perf monitoring](https://dev.mysql.com/doc/refman/8.0/en/performance-schema-statement-tables.html#performance-schema-statement-tables-configuration) is on; optionally turn it on for them
- make the exporter run on a schedule, dockerize, helmify, etc
