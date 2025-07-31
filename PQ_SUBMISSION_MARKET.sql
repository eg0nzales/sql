WITH AllMarkets AS (
    SELECT 
        NULL AS Client_id,
        NULL AS client_nm,
        market_id,
        market_dsc
    FROM [HCMG_Dev_FromProd].[dbo].Market

    UNION

    SELECT 
        NULL AS Client_id,
        NULL AS client_nm,
        market_id,
        market_dsc
    FROM dbadata.dbo.vulcan_market
),

DedupedMarkets AS (
    SELECT 
        market_id, 
        market_dsc,
        ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY market_dsc) AS rn
    FROM AllMarkets
),

ClientData AS (
    -- From Brand-to-Market relationships
    SELECT 
        c.MI10ClientId AS Client_id,
        c.customername AS client_nm,
        bm.marketid AS market_id,
        vm.market_dsc AS market_dsc
    FROM dbadata.dbo.pimisql02_customers c
    INNER JOIN dbadata.dbo.pimisql02_brands b 
        ON c.CustomerId = b.CustomerId
    INNER JOIN dbadata.dbo.pimisql02_brandmarkets bm 
        ON b.BrandId = bm.BrandId
    INNER JOIN dbadata.dbo.vulcan_market vm 
        ON bm.marketid = vm.market_id

    UNION ALL

    -- From MarketQuestion_view (PQ assignments)
    SELECT 
        mq.Client_id,
        mq.client_nm,
        mq.Market_id,
        mq.market_dsc
    FROM [HCMG_Dev_FromProd].[PQ].MarketQuestion_view mq

    UNION ALL

    -- Unassigned deduplicated markets
    SELECT 
        NULL AS Client_id,
        NULL AS client_nm,
        market_id,
        market_dsc
    FROM DedupedMarkets
    WHERE rn = 1
)

SELECT 
    ISNULL(cd.client_nm, 'Unassigned') AS client_nm,
    ISNULL(CAST(cd.Client_id AS VARCHAR), '-') AS Client_id,
    cd.market_id,
    cd.market_dsc
FROM ClientData cd
LEFT JOIN [HCMG_Dev_FromProd].[dbo].Client cl 
    ON cd.Client_id = cl.Client_id
WHERE 
    (cl.datRemoved IS NULL OR cl.datRemoved >= CAST(GETDATE() AS DATETIME) OR cd.Client_id IS NULL)
    AND cd.market_dsc NOT LIKE 'xx%' 
    AND cd.market_dsc NOT LIKE 'zz%' 
    AND NOT (
        cd.client_nm IS NULL 
        AND cd.market_dsc NOT LIKE '%-%'
    )
ORDER BY 
    CASE WHEN cd.client_nm IS NULL THEN 1 ELSE 0 END, 
    cd.client_nm,
    cd.market_dsc;
