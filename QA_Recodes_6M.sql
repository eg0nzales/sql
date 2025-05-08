with deduplicated_results as (
    select distinct q.field_nm, mq.fieldingbegin,
        case 
            when mq.fieldingend > getdate() then 'Active'
            else 'Not Active'
        end as status,
        'Custom' as month,  -- Placeholder column to match the second part
        'Yes' as Recode  -- Fixed unclosed quotation mark
    from hcmg_dev.pq.marketquestion mq
    Inner join hcmg_dev.pq.question q on mq.question_id=q.question_id
    inner join hcmg_dev.pq.response r on q.scale_id=r.scale_id
    where q.recordstate=1 and mq.fieldingend >= getdate()
    and mq.fieldingbegin >= dateadd(month, -6, getdate())
    and (r.serialval!=r.recodeval or r.recodeval in (0,100))

    union all

    select distinct q.field_nm, null as fieldingbegin,
        case 
            when q.QuestionTags like '% RemovedCore %' or q.QuestionTags like 'RemovedCore%' or q.QuestionTags like '%RemovedCore' then 'Not Active'
            when q.QuestionTags like '% Core %' or q.QuestionTags like 'Core%' or q.QuestionTags like '%Core' then 'Active'
            else 'Unknown'
        end as status,
        case 
            when q.field_nm in ('Q0024482', 'Q0050022', 'Q0050114', 'Q0016959', 'Q0050005', 'Q0050016', 'Q0050049', 'Q0050018', 'Q0050019', 'Q0050473', 'Q0050027', 'Q0050028', 'Q0050029', 'Q0050030', 'Q0050031', 'Q0050032', 'Q0050033', 'Q0050035', 'Q0050036', 'Q0050037') then 'Month 1'
            when q.field_nm in ('Q0050115A', 'Q0050115B', 'Q0050115C', 'Q0050115D', 'Q0050115E', 'Q0050115F', 'Q0050115G', 'Q0012242', 'Q0012236', 'Q0028170', 'Q0028171', 'Q0012239', 'Q0012238', 'Q0050100', 'Q0050101', 'Q0050068', 'Q0050069', 'Q0050070', 'Q0050074', 'Q0050075', 'Q0050076', 'Q0050493', 'Q0050494', 'Q0050495', 'Q0050496A', 'Q0050496B', 'Q0050496C', 'Q0050496D') then 'Month 2'
            when q.field_nm in ('Q0050257B', 'Q0050257C', 'Q0050257F', 'Q0050257H', 'Q0050257I', 'Q0050259B', 'Q0050259C', 'Q0050259F', 'Q0050259H', 'Q0050259I', 'Q0050351', 'Q0050363', 'Q0050364', 'Q0050365', 'Q0050367', 'Q0050397', 'Q0050398', 'Q0050399', 'Q0050400', 'Q0050401', 'Q0050481', 'Q0050482', 'Q0050483', 'Q0050484', 'Q0050489A', 'Q0050489B', 'Q0050489C', 'Q0050489D', 'Q0050489E', 'Q0050489F', 'Q0050489G', 'Q0050489H', 'Q0050489I', 'Q0050489J', 'Q0050489K', 'Q0050489L', 'Q0050490', 'Q0050491', 'Q0050492') then 'Month 3'
            else 'Unknown'
        end as month,
        case 
            when exists (
                select 1
                from hcmg_dev.pq.response r
                where r.scale_id = q.scale_id
                and (r.serialval != r.recodeval or r.recodeval in (0, 100))
            ) then 'Yes'
            else 'No'
        end as Recode
    from hcmg_dev.pq.question q
    where (q.QuestionTags like '% RemovedCore %' or q.QuestionTags like 'RemovedCore%' or q.QuestionTags like '%RemovedCore'
           or q.QuestionTags like '% Core %' or q.QuestionTags like 'Core%' or q.QuestionTags like '%Core')
    and q.QuestionTags like '%Rotating%'
),
ranked_results as (
    select field_nm, fieldingbegin, status, month, Recode,
           row_number() over (partition by field_nm order by field_nm) as rn
    from deduplicated_results
)
select field_nm, fieldingbegin, status, month, Recode
from ranked_results
where rn = 1
order by month, field_nm
