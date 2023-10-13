# ETL
PGCOPY module

This module copies a set of tables from one DB to another (both are PG SQL).

Copy is linear (no transformations).

Module uses additional file with parameters to: 
 * get a set of tables to be copied
 * set names for new table
 * SQL script to check DDL statement when recreating a table

It uses logging to local file and alerts to telegram.
