
-- Export all statistics collections after all load tests

-- Investigation is ongoing to figure out if it is possible to export to current client via an SQL command
-- If not:
-- * Select all tables and right click on the tables in the navigation bar in HANA Studio
-- * Export
-- * Next
-- * Select
--   - BINARY
--   - Including data
--   - Export catalog objects to current client
--   - Browse or type a new directory, e.g. C:\Users\D059259\WORK\HANACollector\Out2 
--   - Finish
--   - Zip (here --> Out2.zip)
--   - Share
-- * Maybe cleanup? DELETE SCHEMA ... CASCADE... ? 
--
-- This can be imported to target system (e.g. XXX@XXX) with
-- * Download from share
-- * Unzip (here --> Out2) 
-- * HANA Studio --> Navigation bar --> XXX@XXX --> Right click on Catalog --> Import 
-- * Import catalog objects from current client 
-- * Browse or type the directory, e.g.  C:\<path>\Out2
-- * Select all tables, Add, Next
-- * Including data, Replace existing catalog objects (maybe?)
-- * Finish
--   --> There is now a schema STAT_COLL in your target system with the tables
--	     LOAD_TESTS, STAT_COLL_EXPLAIN_PLANS, STAT_COLL_LOAD_HISTORY_HOST, STAT_COLL_LOAD_HISTORY_SERVICE,
--       STAT_COLL_SERVICE_THREAD_SAMPLES, and STAT_COLL_SQL_PLAN_STATISTICS

 
-- Under Investigation:
--
-- EXPORT "STAT_COLL"."*" INTO '/tmp' WITH REPLACE;
-- EXPORT "STAT_COLL"."TAB1" AS BINARY INTO #CLIENT_EXPORT_1598436297640 WITH NO DEPENDENCIES
