
CREATE procedure [PQ].[KinesisHospitalList]

@ExportMonth datetime, @VersionIt bit=0

as

--declare @ExportMonth datetime, @VersionIt bit
--select @ExportMonth ='2/1/12', @VersionIt =0

set nocount on


if left(system_user,4) <> 'NRC\'
begin
	select 'You are currently connected to the database using the ' + system_user + ' username. Please log out and reconnect using your NRC windows login.' as [ERROR]	
	return 100
end

set @ExportMonth = dbo.yearmonth(@exportmonth)

declare @LastExport datetime, @lastVersion datetime
select @lastExport=max(datyear) from hcmg_staging..weblookuphospital
select @lastVersion=max(AsOf) from hcmg_staging..weblookuphospital where datyear=@lastExport

if @ExportMonth<=@LastExport
begin
	set @VersionIt=0
	select 'Cannot export a list for '+left(datename(month,@ExportMonth),3)+' '+convert(varchar,year(@ExportMonth))+'. An export has already been created for '+left(datename(month,@LastExport),3)+' '+convert(varchar,year(@LastExport)) as [ERROR]
	return 101
end

create table #list ([hospital_cd] int, [Parent_cd] varchar(10), [Hospital_nm] varchar(100), City varchar(50), state char(2), HospFIPS int)
insert into #list
select hn.intHospital_cd, null, hn.strhospital_nm, hn.strCity, s.strStateAbbrev, hn.intStateCounty_cd --replace(hn.strHospital_nm,' (NS)','') + isnull(' - ' + isnull(hn.strCity+', ','')+s.strStateAbbrev,'')
from hcmg_dev..hospital_name hn, hcmg_dev..state s
where hn.intState_cd=s.state_cd
and hn.showpanel=1
and isnull(hn.intInactive,0)=0

insert into #list
select aka.hospitalaka_id+100000,aka.intHospital_cd, aka.strAlias_nm, hn.strCity, s.strStateAbbrev, hn.intStateCounty_cd --replace(aka.strAlias_nm,' (NS)','') + isnull(' - ' + isnull(hn.strCity+', ','')+s.strStateAbbrev,'')
from hcmg_dev..hospitalaka aka, hcmg_dev..hospital_name hn, hcmg_dev..state s
where aka.showpanel=1
and aka.inthospital_cd=hn.inthospital_cd
and hn.intState_cd=s.state_cd
and hn.showpanel=1
and aka.intHospital_cd in (select hospital_cd from #list)

while @@rowcount>0
	update #list SET hospital_nm = replace(hospital_nm,'  ',' ') where hospital_nm like '%  %'
update #list set hospital_nm = ltrim(rtrim(hospital_nm))
update #list set hospital_nm = replace(hospital_nm, 'hildrens', 'hildren''s') where hospital_nm like '%childrens%'

create table #fipsfips (hospfips int, respfips char(5))

insert into #fipsfips
select distinct hospfips,Respfips
from hcmg_staging..weblookuphospital
where AsOf=@LastVersion

while @@rowcount>0
	update #fipsfips set respfips = '0' + respfips where len(respfips)<5

set nocount on

if exists (	select l.*
			from #list l
				left outer join #fipsfips ff on isnull(l.hospfips,-1)=ff.hospfips
			where ff.hospfips is null)
begin
	select 'There are one or more showpaneled hospitals that aren''t mapped to any respondent counties. Un-ShowPanel these facilities or run CopyCountyMappings to resolve.' as [ERROR]	
	select l.Hospital_cd, l.Hospital_nm, l.City, l.State, l.hospFIPS
		from #list l
			left outer join #fipsfips ff on isnull(l.hospfips,-1)=ff.hospfips
		where ff.hospfips is null
	return 102
end

update #list 
set hospital_nm=replace(Hospital_nm,' (NS)','') + isnull(' - ' + isnull(City+', ','')+State,'')

if @VersionIt=1
begin
	insert into hcmg_staging..weblookuphospital (AsOf, RespFIPS, HospFIPS, Hospital_cd, Parent_cd, Hospital_nm, City, State, datYear)
	select getdate() as AsOf, respfips, l.hospfips, hospital_cd, Parent_cd, hospital_nm, city, state, @ExportMonth
	from #list l, #fipsfips ff
	where l.hospfips=ff.hospfips
end

select distinct '"'+convert(varchar,hospital_cd)+'"' as [Unique Hospital ID], isnull(' "'+parent_cd+'"','') as [Parent Hospital ID], ' "'+replace(hospital_nm,'"','""')+'"' as [Hospital Name]
from #list
union select '"99998"', '', ' "I Don''t Know"'
union select '"99997"', ' "99998"', ' "No hospital comes to mind"'
union select '"99996"', ' "99998"', ' "No preference"'
order by 2

sele
