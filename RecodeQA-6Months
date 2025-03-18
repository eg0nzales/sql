select distinct q.field_nm, mq.fieldingbegin
from hcmg_dev.pq.marketquestion mq
Inner join hcmg_dev.pq.question q on mq.question_id=q.question_id
inner join hcmg_dev.pq.response r on q.scale_id=r.scale_id
where q.recordstate=1 and mq.fieldingend >= getdate()
and mq.fieldingbegin >= dateadd(month, -6, getdate())
and (r.serialval!=r.recodeval or r.recodeval in (0,100))
order by field_nm
