select *
from dba_source
where loweR(text) like '%menu_options%';

select *
from thot.service_branches;

select abs(round(SYS.DBMS_RANDOM.value*10)) from dual;
select round(SYS.DBMS_RANDOM.value*1000)  from dual;
select round(SYS.DBMS_RANDOM.value(296,555))  from dual;


select (rownum-rownum)+0
from dictionary
where rownum <=10;

select level,rownum
from dictionary

where rownum <= 10;




select idx,1+(select
from (
select rownum as idx
from dictionary
where rownum <= 10)
--connect by prior idx < nidx
;


with s1 as (select 0 s from dual),
		 s2 as  (select 1 s from dual),
		 seq as (select rownum s,rownum-1 sm from dictionary where rownum <= 10)
select s
from s1
union
select s
from s2
union all
select --(select s from s1)+(select s from s2)
 
			decode(s
							,1
							,(select s from s1)+(select s from s2)
							,(LAG(s,val-1,0) OVER (order by val))+ (LAG(s,val,0) OVER (order by val))
							) as val
from seq
where s = 1;

SELECT 
--case rownum
--when 1 then 0
--when 2 then 1
--else 
lag(sm-2,1) OVER ( ORDER BY Sm) as r1
,lag(sm-1,1) OVER ( ORDER BY Sm) as r2
,lag(sm,1) OVER ( ORDER BY Sm) as r3
,lag(s,1) OVER ( ORDER BY Sm) as r35
,lag(sm,1) OVER ( ORDER BY Sm)+lag(s,1) OVER ( ORDER BY Sm)+sm as r4
,sm
--s,sm,
--lag(Sm,1) OVER ( ORDER BY Sm) as r1
--,lag(Sm-1,1) OVER ( ORDER BY Sm) AS r2

FROM (
select 0 s,rownum-1 as sm 
from dictionary where rownum <= 10);


with s1 as (select 0 val from dual),
		 s2	as (select 1 val from dual)
select lag(s1.val,1) over (order by s1.val)+ lag(s2.val,1) OVER ( ORDER BY s2.val)
from dictionary d;


WITH Fibonacci (PrevN, N) AS
(
     SELECT 0, 1
		 FROM DUAL
     UNION ALL
     SELECT N, PrevN + N
     FROM Fibonacci
     WHERE N < 1000000000
)
SELECT PrevN as Fibo
     FROM Fibonacci
     OPTION (MAXRECURSION 0);
		 
		 
		 SELECT ROUND(POWER(1.6180339,LEVEL) / POWER(5,0.5)) FROM DUAL CONNECT BY LEVEL < =10;
		 
		 select 1*level
		 from dual
		 connect by level <=20
		 order by 1 desc;
		 
		 select rpad('a',2,'b') from dual;
		 --------------------------------------- P(n) Pattern
		 select replace(rpad(ch,n,'*'),'*','* ') as p20
		 --substr(rpad(n,n,'*'),length(n),length(rpad(n,n,'*')))
		 --,n
		 from (
		 select '*' as ch,level as n
		 from dual
		 connect by level <=20
		 order by 1 desc)
		 order by n desc
		 ;
		 
		 select rpad('*',level,'*') as ch,level as n
		 from dual
		 connect by level <=20 
		 order by level desc;


WITH FibonacciNumbers (RecursionLevel, FibonacciNumber, NextNumber) 
AS (
   -- Anchor member definition
   SELECT  1  AS RecursionLevel,
           0  AS FibonacciNumber,
           1  AS NextNumber
   FROM Dual
   UNION ALL
   -- Recursive member definition
   SELECT  a.RecursionLevel + 1             AS RecursionLevel,
           a.NextNumber                     AS FibonacciNumber,
           a.FibonacciNumber + a.NextNumber AS NextNumber
   FROM FibonacciNumbers a
   WHERE a.RecursionLevel <= 10
)
-- Statement that executes the CTE
SELECT  'F' || TO_CHAR( fn.RecursionLevel - 1 ) AS FibonacciOrdinal, 
        fn.FibonacciNumber,
        fn.NextNumber
FROM FibonacciNumbers fn; 
