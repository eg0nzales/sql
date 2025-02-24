-- Declare variables
declare @AsOf datetime, 
        @Through datetime, 
        @zips varchar(100)

-- Set variable values
set @AsOf = getdate()  -- Set @AsOf to today's date
set @Through = '12/31/2099'
set @zips = ''

-- Set @Through to @AsOf if it is the default value
if @Through='12/31/2099' set @Through=@AsOf

-- Drop the temporary table if it already exists
if object_id('tempdb..#results') is not null drop table #results

-- Create a temporary table #results to store intermediate results
select convert(varchar(64),null) as AcctDir, c.client_id, c.client_nm, m.market_id, m.market_dsc, q.question_id, q.field_nm, q.question_dsc, q.question_eng,
	left(datename(month,FieldingBegin),3)+' '+convert(varchar,year(FieldingBegin)) as FieldingBegin, 
	left(datename(month,FieldingEnd),3)+' '+convert(varchar,year(FieldingEnd)) as FieldingEnd
into #results
from pq.marketquestion mq, pq.question q, market m, client c
where mq.question_id=q.question_id
and mq.client_id=c.client_id
and mq.market_id=m.market_id
and mq.recordstate=1
and 1=2

-- Execute the sub-procedure to populate #results
exec pq.WhatsFielding_sub '', @asof, @through, @zips

-- Select distinct records from #results and order them
select AcctDir, client_id, client_nm, market_id, market_dsc, field_nm, question_dsc, question_eng, FieldingBegin, FieldingEnd
into #distinct_results
from #results 
order by AcctDir, client_nm, market_dsc, field_nm, fieldingbegin

-- Add count of client and market combinations
select client_id, client_nm, market_id, market_dsc, 
	count(field_nm) as question_count
from (
    select distinct client_id, client_nm, market_id, market_dsc, field_nm
    from #distinct_results
) as distinct_fields
where client_id not in (2152, 2076, 1, 1861, 2146, 2088, 282, 2023, 2101, 2187)  -- Exclude client_id 2152, 2076, 1, and 1861
group by client_id, client_nm, market_id, market_dsc
having count(field_nm) > 5  -- Only show clients with more than 5 question counts
order by client_nm, market_dsc

-- Drop the temporary tables
drop table #results
drop table #distinct_results
