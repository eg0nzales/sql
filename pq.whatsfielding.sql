CREATE procedure PQ.WhatsFielding
@searchterm varchar(50), @AsOf datetime = '12/31/2099', @Through datetime = '12/31/2099', @zips varchar(100)=''
as

if @AsOf='12/31/2099' set @asof=dateadd(month,1,dbo.lastpublishedmonth())
if @Through='12/31/2099' set @Through=@AsOf

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

exec pq.WhatsFielding_sub @searchterm, @asof, @through, @zips

select distinct AcctDir, client_id, client_nm, market_id, market_dsc, field_nm, question_dsc, question_eng, FieldingBegin, FieldingEnd
from #results 
order by AcctDir, client_nm, market_dsc, field_nm, fieldingbegin

drop table #results
