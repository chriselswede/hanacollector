
-- After the load test with ID LOAD_TEST_2020_11_11_XX run the statistic collection procedure, either
--     1. by providing the start and stop times, or
call "STAT_COLL"."COLLECT_STATISTICS_FROM_LOAD_TEST"('LOAD_TEST_2020_11_11_XX', '29.08.2020 08:00:00.000000', '29.08.2020 08:30:00.000000', 'SAPQH1');
--     2. by letting the procedure get the start time from the LOAD_TESTS table and stop time is provided, or
--call "STAT_COLL"."COLLECT_STATISTICS_FROM_LOAD_TEST"('LOAD_TEST_2020_11_11_XX', NULL, '22.08.2020 08:30:00.000000', 'SAPQH1');
--     3. by providing the start time and letting the procedure get the stop time from current time, or
--call "STAT_COLL"."COLLECT_STATISTICS_FROM_LOAD_TEST"('LOAD_TEST_2020_11_11_XX', '22.08.2020 08:00:00.000000', 'SAPQH1');
--     4. by letting the procedure get the start time from the LOAD_TESTS table and stop time from current time
--call "STAT_COLL"."COLLECT_STATISTICS_FROM_LOAD_TEST"('LOAD_TEST_2020_11_11_XX');


-- Some ways to check that data was collected in the collect tables:
--select LOAD_ID, STATEMENT_HASH, STATEMENT_NAME as PLAN_ID, OPERATOR_NAME, OPERATOR_DETAILS, OPERATOR_PROPERTIES, EXECUTION_ENGINE, DATABASE_NAME from "STAT_COLL"."STAT_COLL_EXPLAIN_PLANS" where LOAD_ID = 'LOAD_TEST_2020_11_11_XX';
--select LOAD_ID, HOST, PORT, TIME, MEMORY_USED from "STAT_COLL"."STAT_COLL_LOAD_HISTORY_SERVICE" where LOAD_ID = 'LOAD_TEST_2020_11_11_XX';
--select LOAD_ID, HOST, TIME, DISK_USED from "STAT_COLL"."STAT_COLL_LOAD_HISTORY_HOST" where LOAD_ID = 'LOAD_TEST_2020_11_11_XX';
--select LOAD_ID, HOST, PORT, STATEMENT_HASH, AVG_EXECUTION_TIME from "STAT_COLL"."STAT_COLL_SQL_PLAN_STATISTICS" where LOAD_ID = 'LOAD_TEST_2020_11_11_XX';
--select LOAD_ID, HOST, PORT, TIMESTAMP, THREAD_ID, THREAD_TYPE, THREAD_STATE from "STAT_COLL"."STAT_COLL_SERVICE_THREAD_SAMPLES" where LOAD_ID = 'LOAD_TEST_2020_11_11_XX';

