WITH Deduped AS (
    SELECT 
        q.question_dsc,
        q.question_eng,
        q.field_nm,
        q.scale_id,
        CASE 
            WHEN q.question_typ = 'simple' AND q.scale_id = 1001 THEN 'Type Assist'
            WHEN q.question_typ = 'grid' THEN 'Grid'
            WHEN q.question_typ = 'multi' THEN 'Multi-Select'
            WHEN q.question_typ = 'text' THEN 'Open End - Short'
            WHEN q.question_typ = 'rank' THEN 'Rank'
            ELSE 'Single Select'
        END AS [Question Type],
        q.Programming AS [Skip Pattern],
        STUFF((
            SELECT ';' + CONVERT(NVARCHAR(MAX), r2.Response_eng)
            FROM PQ.Response AS r2
            WHERE r2.Scale_id = q.scale_id
              AND r2.Response_typ = 'simple'
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''
        ) AS ConcatenatedResponses,
        ROW_NUMBER() OVER (PARTITION BY q.field_nm ORDER BY q.question_dsc) AS rn
    FROM hcmg_dev.dbo.client c
    INNER JOIN hcmg_dev.pq.marketquestion mq ON c.client_id = mq.client_id
    INNER JOIN hcmg_dev.pq.question q ON mq.question_id = q.question_id
    INNER JOIN hcmg_dev.dbo.market m ON mq.market_id = m.market_id
    WHERE q.scale_id IS NOT NULL AND q.scale_id <> 1001
)
SELECT
    question_dsc,
    question_eng,
    field_nm,
    scale_id,
    [Question Type],
    [Skip Pattern],
    ConcatenatedResponses
FROM Deduped
WHERE rn = 1 AND ConcatenatedResponses IS NOT NULL
ORDER BY field_nm;
