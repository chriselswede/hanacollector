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
CREATE PROCEDURE "STAT_COLL"."COLLECT_STATISTICS_FROM_LOAD_TEST"(IN ID VARCHAR(100), IN START_TIME_IN DATETIME DEFAULT NULL, IN STOP_TIME_IN DATETIME DEFAULT NULL)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
AS
BEGIN
    -- Collect the Execution Plans of the 100 (currently 17) most "expensive" w.r.t. average execution time and 100 (currently 17) most "expensive" w.r.t. total execution time from the Reset SQL Plan Cache
	-- Note1: they must be of such SQL statements that support explain plan
	-- Note2: here we cannot user M_SLQ_PLAN_STATISTICS, some plan ids are invalid
	DECLARE all_statements TABLE(PLAN_ID BIGINT, STATEMENT_STRING NCLOB, STATEMENT_HASH VARCHAR(32), AVG_EXECUTION_TIME BIGINT, TOTAL_EXECUTION_TIME BIGINT) = select PLAN_ID, STATEMENT_STRING, STATEMENT_HASH, AVG_EXECUTION_TIME, TOTAL_EXECUTION_TIME from "SYS"."M_SQL_PLAN_STATISTICS_RESET" 
             where STATEMENT_STRING like 'INSERT%' 
             or STATEMENT_STRING like 'UPDATE%' 
             or STATEMENT_STRING like 'DELETE%' 
             or STATEMENT_STRING like 'REPLACE%' 
             or STATEMENT_STRING like 'UPSERT%' 
             or STATEMENT_STRING like 'MERGE INTO%' 
             or STATEMENT_STRING like 'SELECT%';
	DECLARE avgdesc  TABLE(PLAN_ID BIGINT, STATEMENT_HASH VARCHAR(32)) = select top 17 PLAN_ID, STATEMENT_HASH from :all_statements order by avg_execution_time desc;
	DECLARE totdesc  TABLE(PLAN_ID BIGINT, STATEMENT_HASH VARCHAR(32)) = select top 17 PLAN_ID, STATEMENT_HASH from :all_statements order by total_execution_time desc;
	DECLARE unionsel TABLE(PLAN_ID BIGINT, STATEMENT_HASH VARCHAR(32)) = select * from :avgdesc union select * from :totdesc;        
	DECLARE CURSOR cur FOR select * from :unionsel;
    DECLARE plan_id_str VARCHAR(100);
    DECLARE sql_hash VARCHAR(32);
    --DECLARE CONTINUE HANDLER FOR SQL_ERROR_CODE 428 SELECT ::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE FROM DUMMY;        -- to catch when EXPLAIN PLAN SET gets an "invalid" plan-id
	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SELECT ::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE FROM DUMMY;
    FOR cur_row AS cur DO
       plan_id_str := CAST(:cur_row.PLAN_ID AS VARCHAR);     
       sql_hash := cur_row.STATEMENT_HASH;                     -- select :plan_id_str, :sql_hash from dummy;
       delete from sys.explain_plan_table where statement_name = :plan_id_str;
       EXEC 'EXPLAIN PLAN SET STATEMENT_NAME = '''||plan_id_str||''' FOR SQL PLAN CACHE ENTRY '||plan_id_str||' ';  -- it happens that the plan-id is "invalid", then the CONTINUE HANDLER
       INSERT INTO "STAT_COLL"."STAT_COLL_EXPLAIN_PLANS" SELECT *, :ID as "LOAD_ID", :sql_hash as "STATEMENT_HASH"  -- will give a warning and the EXPLAIN PLAN SET will not be done, and  
             FROM sys.explain_plan_table WHERE statement_name = :plan_id_str;                                       -- this INSERT INTO will not insert anything since that plan id is not there
    END FOR;
    -- Collect all statistics from the SQL Plan Cache since the reset 
    INSERT INTO "STAT_COLL"."STAT_COLL_SQL_PLAN_STATISTICS" SELECT TOP 1000 *, :ID AS "LOAD_ID"
       FROM "SYS"."M_SQL_PLAN_STATISTICS_RESET"  
       WHERE SCHEMA_NAME = 'SAPQH1' AND IS_INTERNAL = 'FALSE'  -- Change SAPQH1 --> SAP<SID> or similar and add   AND RESET_TIME IS NOT NULL  TODO:input parameter
       ORDER BY TOTAL_EXECUTION_TIME;
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
