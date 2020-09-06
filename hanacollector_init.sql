--drop schema "STAT_COLL" cascade;
-- Create a new schema for all the collector tables
create schema "STAT_COLL";

-- Before all load tests start, create tables that will collect the statistics
-- These tables must have an additional column with the load test ID
-- Since we use "select *" we must build something that checks that if new columns been added
-- to the M_ views, and if so, automatically add the new columns to these collection tables --> TODO
--drop table "STAT_COLL"."STAT_COLL_LOAD_HISTORY_SERVICE"; 
create column table "STAT_COLL"."STAT_COLL_LOAD_HISTORY_SERVICE" as (select top 1 * from "SYS"."M_LOAD_HISTORY_SERVICE"); 
alter table "STAT_COLL"."STAT_COLL_LOAD_HISTORY_SERVICE" add (LOAD_ID varchar(100) default '-1');
delete from "STAT_COLL"."STAT_COLL_LOAD_HISTORY_SERVICE" where LOAD_ID = '-1';      --select * from "STAT_COLL"."STAT_COLL_LOAD_HISTORY_SERVICE";

--drop table "STAT_COLL"."STAT_COLL_LOAD_HISTORY_HOST"; 
create column table "STAT_COLL"."STAT_COLL_LOAD_HISTORY_HOST" as (select top 1 * from "SYS"."M_LOAD_HISTORY_HOST"); 
alter table "STAT_COLL"."STAT_COLL_LOAD_HISTORY_HOST" add (LOAD_ID varchar(100) default '-1');
delete from "STAT_COLL"."STAT_COLL_LOAD_HISTORY_HOST" where LOAD_ID = '-1';         --select * from "STAT_COLL"."STAT_COLL_LOAD_HISTORY_HOST";

--drop table "STAT_COLL"."STAT_COLL_SQL_PLAN_STATISTICS"; 
create column table "STAT_COLL"."STAT_COLL_SQL_PLAN_STATISTICS" as (select top 1 * from "SYS"."M_SQL_PLAN_STATISTICS_RESET"); 
alter table "STAT_COLL"."STAT_COLL_SQL_PLAN_STATISTICS" add (LOAD_ID varchar(100) default '-1');
delete from "STAT_COLL"."STAT_COLL_SQL_PLAN_STATISTICS" where LOAD_ID = '-1';       --select * from "STAT_COLL"."STAT_COLL_SQL_PLAN_STATISTICS"; 

--drop table "STAT_COLL"."STAT_COLL_SERVICE_THREAD_SAMPLES"; 
create column table "STAT_COLL"."STAT_COLL_SERVICE_THREAD_SAMPLES" as (select top 1 * from "SYS"."M_SERVICE_THREAD_SAMPLES"); 
alter table "STAT_COLL"."STAT_COLL_SERVICE_THREAD_SAMPLES" add (LOAD_ID varchar(100) default '-1');
delete from "STAT_COLL"."STAT_COLL_SERVICE_THREAD_SAMPLES" where LOAD_ID = '-1';    --select * from "STAT_COLL"."STAT_COLL_SERVICE_THREAD_SAMPLES";

--drop table "STAT_COLL"."STAT_COLL_EXPLAIN_PLANS"; 
create column table "STAT_COLL"."STAT_COLL_EXPLAIN_PLANS" as (select top 1 * from "SYS"."EXPLAIN_PLAN_TABLE"); 
alter table "STAT_COLL"."STAT_COLL_EXPLAIN_PLANS" add (LOAD_ID varchar(100) default '-1');
alter table "STAT_COLL"."STAT_COLL_EXPLAIN_PLANS" add (STATEMENT_HASH varchar(32) default '-1');
delete from "STAT_COLL"."STAT_COLL_EXPLAIN_PLANS" where LOAD_ID = '-1';              --select * from "STAT_COLL"."STAT_COLL_EXPLAIN_PLANS";

--drop table "STAT_COLL"."LOAD_TESTS";                               - time zone server  or  UTC ?
create column table "STAT_COLL"."LOAD_TESTS"(TEST_ID VARCHAR(100), START_TIME DATETIME, STOP_TIME DATETIME, TEST_COMMENT VARCHAR(500));


-- Before all load tests start, create a procedure that will insert the statistics into the collect tables
-- This procedure has the load test ID as an input variable
-- The test ID will be saved as an additional column in every collect table
-- For the view M_SQL_PLAN_STATISTICS we will reset the RESET view of this M_ view before every load test
-- Then we can easily collect all data in that RESET view after the load test
-- To get the explain plans we will use the view M_SQL_PLAN_CACHE
-- We will also reset the RESET view of M_SQL_PLAN_CACHE
-- Explain Plans from the most "expensive" statements from that RESET view will be collected 
-- For the other M_ views there are no _RESET versions, so we must have a start and stop time of the collection period
-- We have implemented two ways to do this
-- 1. Before every load test we run a statement that saves the current timestamp in a table. This assumes we always want 
--    to collect the whole test time period.
-- 2. The collect statistics procedure gets a start and stop time as input variables (then application team must provide 
--    test time ranges manually). It is also possible to run the procedure with only the start time or only the stop time.
DROP PROCEDURE "STAT_COLL"."COLLECT_STATISTICS_FROM_LOAD_TEST";
CREATE PROCEDURE "STAT_COLL"."COLLECT_STATISTICS_FROM_LOAD_TEST"(IN ID VARCHAR(100), IN START_TIME_IN DATETIME DEFAULT NULL, IN STOP_TIME_IN DATETIME DEFAULT NULL, IN SAPSCHEMA VARCHAR(100))
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
AS
BEGIN
    -- Collect the Execution Plans of the 1000 most "expensive" w.r.t. average execution time and 1000 most "expensive" w.r.t. total execution time from the Reset of SQL Plan Cache Statistics
	-- Note: they must be of such SQL statements that support explain plan (see the where clause below)
	DECLARE all_supported_ids TABLE(PLAN_ID BIGINT, AVG_EXECUTION_TIME BIGINT, TOTAL_EXECUTION_TIME BIGINT) = select PLAN_ID, AVG_EXECUTION_TIME, TOTAL_EXECUTION_TIME from "SYS"."M_SQL_PLAN_STATISTICS_RESET"  
             where STATEMENT_STRING like 'INSERT%' 
             or STATEMENT_STRING like 'UPDATE%' 
             or STATEMENT_STRING like 'DELETE%' 
             or STATEMENT_STRING like 'REPLACE%' 
             or STATEMENT_STRING like 'UPSERT%' 
             or STATEMENT_STRING like 'MERGE INTO%' 
             or STATEMENT_STRING like 'SELECT%';
	DECLARE avgdesc      TABLE(PLAN_ID BIGINT, exec_time BIGINT) = select top 1000 PLAN_ID, avg_execution_time as exec_time from :all_supported_ids order by avg_execution_time desc;
	DECLARE totdesc      TABLE(PLAN_ID BIGINT, exec_time BIGINT) = select top 1000 PLAN_ID, total_execution_time as exec_time from :all_supported_ids order by total_execution_time desc;
	DECLARE unionsel     TABLE(PLAN_ID BIGINT, exec_time BIGINT) = select * from :avgdesc union select * from :totdesc;
	-- Some plan ids of M_SLQ_PLAN_STATISTICS_RESET are invalid, so the Execution Plans cannot be created from them, therefore we pick out the 20 most "expensive" (wrt both average execution time and 
	-- total exeuction time) valid ones by joining with M_SQL_PLAN_CACHE_RESET on plan_id because all plan ids in M_SQL_PLAN_CACHE_RESET are valid (in the same time we also collect the statement hash) 
	DECLARE	new_union    TABLE(PLAN_ID BIGINT, STATEMENT_HASH VARCHAR(32), exec_time BIGINT) = select b.plan_id, b.statement_hash, a.exec_time from :unionsel a join "SYS"."M_SQL_PLAN_CACHE_RESET" b on a.plan_id = b.plan_id;
    DECLARE new_avgdesc  TABLE(PLAN_ID BIGINT, STATEMENT_HASH VARCHAR(32), exec_time BIGINT) = select top 20 a.* from :new_union a join :avgdesc b on a.plan_id = b.plan_id order by b.exec_time desc;
    DECLARE new_totdesc  TABLE(PLAN_ID BIGINT, STATEMENT_HASH VARCHAR(32), exec_time BIGINT) = select top 20 a.* from :new_union a join :totdesc b on a.plan_id = b.plan_id order by b.exec_time desc;
    DECLARE new_unionsel TABLE(PLAN_ID BIGINT, STATEMENT_HASH VARCHAR(32), exec_time BIGINT) = select * from :new_avgdesc union select * from :new_totdesc; 
	-- Declare a cursor to be used to loop over the expensive statements to collect their Execution Plans, and declare variables and a continue handler just in case EXPLAIN PLAN SET anyway gets an "invalid" plan-id 	
	DECLARE CURSOR cur FOR select * from :new_unionsel;
    DECLARE plan_id_str VARCHAR(100);
    DECLARE sql_hash VARCHAR(32);
    DECLARE CONTINUE HANDLER FOR SQL_ERROR_CODE 428 SELECT ::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE FROM DUMMY;
	-- Loop over the 20 + 20 "expensive" statements (that all have valid plan ids) and collect their Execution Plans
    FOR cur_row AS cur DO
       plan_id_str := CAST(:cur_row.PLAN_ID AS VARCHAR);     
       sql_hash := cur_row.STATEMENT_HASH;                     -- select :plan_id_str, :sql_hash from dummy;
       delete from sys.explain_plan_table where statement_name = :plan_id_str;
       EXEC 'EXPLAIN PLAN SET STATEMENT_NAME = '''||plan_id_str||''' FOR SQL PLAN CACHE ENTRY '||plan_id_str||' ';  -- if it would happen that plan-id is "invalid", then the CONTINUE HANDLER
       INSERT INTO "STAT_COLL"."STAT_COLL_EXPLAIN_PLANS" SELECT *, :ID as "LOAD_ID", :sql_hash as "STATEMENT_HASH"  -- will give a warning and the EXPLAIN PLAN SET will not be done, and this 
             FROM sys.explain_plan_table WHERE statement_name = :plan_id_str;                                       -- INSERT INTO will not insert anything since that plan id is not there
    END FOR;
    -- Collect statistics of the 1000 + 1000 "expensive" statements from the SQL Plan Statistics since the reset (some of them might have invalide plan ids) 
    INSERT INTO "STAT_COLL"."STAT_COLL_SQL_PLAN_STATISTICS" SELECT  a.*, :ID AS "LOAD_ID"
       FROM "SYS"."M_SQL_PLAN_STATISTICS_RESET" a join :unionsel b on a.plan_id = b.plan_id;
       --WHERE SCHEMA_NAME = :SAPSCHEMA AND IS_INTERNAL = 'FALSE' ORDER BY TOTAL_EXECUTION_TIME;  --- add      AND RESET_TIME IS NOT NULL       TODO:input parameter	
    -- If start time is not defined as an input parameter, get the start time from the LOAD_TESTS table for this test
    IF (:START_TIME_IN is NULL) THEN
       select distinct START_TIME into START_TIME_IN from "STAT_COLL"."LOAD_TESTS" where TEST_ID = :ID;
    END IF;
    -- If stop time is not defined as an input parameter, set current time as the stop time (since this procedure will be called right after the test)
    IF (:STOP_TIME_IN is NULL) THEN
       select CURRENT_TIMESTAMP into STOP_TIME_IN from dummy;
    END IF;
    -- Update the LOAD_TESTS table with start and stop time
    update "STAT_COLL"."LOAD_TESTS" SET START_TIME = :START_TIME_IN where TEST_ID = :ID;
    update "STAT_COLL"."LOAD_TESTS" SET STOP_TIME = :STOP_TIME_IN where TEST_ID = :ID;        --select * from "STAT_COLL"."LOAD_TESTS";
    -- Collect all statistics from M_LOAD_HISTORY_SERVICE between the start and stop time
    INSERT INTO "STAT_COLL"."STAT_COLL_LOAD_HISTORY_SERVICE" SELECT *, :ID AS "LOAD_ID"
       FROM "SYS"."M_LOAD_HISTORY_SERVICE" 
       WHERE TIME BETWEEN START_TIME_IN AND STOP_TIME_IN ORDER BY TIME;
    -- Collect all statistics from M_LOAD_HISTORY_HOST between the start and stop time
    INSERT INTO "STAT_COLL"."STAT_COLL_LOAD_HISTORY_HOST" SELECT *, :ID AS "LOAD_ID"
       FROM "SYS"."M_LOAD_HISTORY_HOST" 
       WHERE TIME BETWEEN START_TIME_IN AND STOP_TIME_IN ORDER BY TIME;
    -- Collect all statistics from M_SERVICE_THREAD_SAMPLES between the start and stop time
    INSERT INTO "STAT_COLL"."STAT_COLL_SERVICE_THREAD_SAMPLES" SELECT *, :ID AS "LOAD_ID"
       FROM "SYS"."M_SERVICE_THREAD_SAMPLES" 
       WHERE TIMESTAMP BETWEEN START_TIME_IN AND STOP_TIME_IN ORDER BY TIMESTAMP;
END;
