dirty-js-etl
============

a dump of my nodejs etl tool

written in about 1 day, and expanded on over 3-4 days;

the tool produces T-SQL to copy data from 1 database to another.

there are probably better tools available

---

the goal was to eliminate the duplication required by sql for updates & inserts

usage:

node etl.js > test.sql

