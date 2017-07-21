-- +======================================================================================================+
-- |                                           .                                        |
-- |                                                                                           |
-- +======================================================================================================+

-- | ==================================================================================================== |
-- | Version     Date           Author           Remarks                                                  |
-- | ==========  ============= ================= =========================================================|
-- | 1.0         02/13/2015     Diego Resendi    
-- +======================================================================================================+
set pagesize 0 feedback off ver off heading off echo off serverout on
set linesize 2000
set termout off
set arraysize 5000

col time new_v v_time
select to_char(sysdate, 'YYYYMMDD_HH24MISS') time  from dual;
spool Report_XXXX_&v_time..csv
prompt system_date,system_timestamp,user_logged,
select sysdate||','||systimestamp||','||user||','
from dual;
spool off;

show errors;
EXIT;
