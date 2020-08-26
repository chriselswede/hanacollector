
-- Before the load test with ID LOAD_TEST_2020_11_11_XX reset the SQL plan cache monitoring view and fill info about this test
alter system reset monitoring view "SYS"."M_SQL_PLAN_STATISTICS_RESET";
alter system reset monitoring view "SYS"."M_SQL_PLAN_CACHE_RESET";
insert into "STAT_COLL"."LOAD_TESTS" values ('LOAD_TEST_2020_11_11_XX', CURRENT_TIMESTAMP, Null, 'This is a cool test'); 
--select distinct * from "STAT_COLL"."LOAD_TESTS";