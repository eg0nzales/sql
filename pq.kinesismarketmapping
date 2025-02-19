
CREATE procedure [PQ].[KinesisMarketMapping]
@ExportMonth datetime, @VersionIt bit=0
as


if left(system_user,4) <> 'NRC\'
begin
	select 'You are currently connected to the database using the ' + system_user + ' username. Please log out and reconnect using your NRC windows login.' as [ERROR]	
	return 100
end

--	declare @ExportMonth datetime, @VersionIt bit
--	set @exportmonth='4/1/11'
--	set @versionit=0

set @ExportMonth = dbo.yearmonth(@exportmonth)

declare @LastExport datetime, @lastVersion datetime
select @lastExport=max(ExportMonth) from PQ.KinesisExportMarketVersions
select @lastVersion=max(versiondate) from PQ.KinesisExportMarketVersions where ExportMonth=@lastExport

if @VersionIt=1 and @ExportMonth<=@LastExport
begin
	set @VersionIt=0
	select 'Cannot create a new version for '+left(datename(month,@ExportMonth),3)+' '+convert(varchar,year(@ExportMonth))+'. An export has already been created for '+left(datename(month,@LastExport),3)+' '+convert(varchar,year(@LastExport)) as [ERROR]
	return 101
end

select q.question_id, q.question_typ, q.scale_id, q.kinesisfield_nm, m.market_id, m.market_dsc, m.markettype_def, convert(char(5),scm.statecounty_cd) as FIPS
into #MQ
from pq.marketquestion mq, pq.question q, market m, statecounty_market scm
where mq.question_id=q.question_id
and mq.market_id=m.market_id
and m.market_id=scm.market_id
and mq.recordstate=1
and q.recordstate=1
and @ExportMonth between mq.fieldingbegin and mq.fieldingend
and mq.CreatedBy not like 'Backpopulation%'

insert into #MQ
select q.question_id, q.question_typ, q.scale_id, q.kinesisfield_nm, m.market_id, m.market_dsc, 3 as markettype_def, sc.statecounty_cd as FIPS
from pq.marketquestion mq, pq.question q, market m, state_market sm, statecounty sc
where mq.question_id=q.question_id
and mq.market_id=m.market_id
and m.market_id=sm.market_id
and sm.state_cd = sc.statecounty_cd/1000
and mq.recordstate=1
and q.recordstate=1
and @ExportMonth between mq.fieldingbegin and mq.fieldingend
and mq.CreatedBy not like 'Backpopulation%'

insert into #MQ
select q.question_id, q.question_typ, q.scale_id, q.kinesisfield_nm, m.market_id, m.market_dsc, m.markettype_def, zm.zip5_cd as ZIP
from pq.marketquestion mq, pq.question q, market m, zip_market zm
where mq.question_id=q.question_id
and mq.market_id=m.market_id
and m.market_id=zm.market_id
and mq.recordstate=1
and q.recordstate=1
and @ExportMonth between mq.fieldingbegin and mq.fieldingend
and mq.CreatedBy not like 'Backpopulation%'

-- add the "other, please specify fields
insert into #MQ
select q.question_id, q.question_typ, q.scale_id, q.kinesisfield_nm+'o'+convert(varchar,r.recodeval), m.market_id, m.market_dsc, m.markettype_def, scm.statecounty_cd as FIPS
from pq.marketquestion mq, pq.question q, pq.response r, market m, statecounty_market scm
where mq.question_id=q.question_id
and q.scale_id=r.scale_id
and mq.market_id=m.market_id
and m.market_id=scm.market_id
and r.isPleaseSpecify=1
and mq.recordstate=1
and q.recordstate=1
and r.recordstate=1
and @ExportMonth between mq.fieldingbegin and mq.fieldingend
and mq.CreatedBy not like 'Backpopulation%'

insert into #MQ
select q.question_id, q.question_typ, q.scale_id, q.kinesisfield_nm+'o'+convert(varchar,r.recodeval), m.market_id, m.market_dsc, 3 as markettype_def, sc.statecounty_cd as FIPS
from pq.marketquestion mq, pq.question q, pq.response r, market m, state_market sm, statecounty sc
where mq.question_id=q.question_id
and q.scale_id=r.scale_id
and mq.market_id=m.market_id
and m.market_id=sm.market_id
and sm.state_cd = sc.statecounty_cd/1000
and r.isPleaseSpecify=1
and mq.recordstate=1
and q.recordstate=1
and r.recordstate=1
and @ExportMonth between mq.fieldingbegin and mq.fieldingend
and mq.CreatedBy not like 'Backpopulation%'

insert into #MQ
select q.question_id, q.question_typ, q.scale_id, q.kinesisfield_nm+'o'+convert(varchar,r.recodeval), m.market_id, m.market_dsc, m.
