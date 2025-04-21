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
    SELECT 
        c.MI10ClientId AS Client_id,
        c.customername AS client_nm,
        m.market_id AS Market_id,
        m.market_dsc AS market_dsc
    FROM dbadata.dbo.pimisql02_customers c
    JOIN dbadata.dbo.pimisql02_brands b ON c.MI10ClientId = b.customerid
    JOIN dbadata.dbo.pimisql02_brandmarkets bm ON b.brandid = bm.brandid
    JOIN dbadata.dbo.vulcan_market m ON bm.marketid = m.market_id

    UNION

    SELECT 
        mq.Client_id,
        mq.client_nm,
        mq.Market_id,
        mq.market_dsc
    FROM [HCMG_Dev_FromProd].[PQ].MarketQuestion_view mq

    UNION

    SELECT 
        NULL AS Client_id,
        NULL AS client_nm,
        market_id,
        market_dsc
    FROM DedupedMarkets
    WHERE rn = 1
)
-- Final Selection with Correct Order
SELECT 
    cd.client_nm,
    cd.Client_id,
    cd.market_dsc,
    cd.Market_id
FROM ClientData cd
LEFT JOIN [HCMG_Dev_FromProd].[dbo].Client cl ON cd.Client_id = cl.Client_id
WHERE (cl.datRemoved IS NULL OR cl.datRemoved >= CAST(GETDATE() AS DATETIME) OR cd.Client_id IS NULL)
ORDER BY cd.client_nm, cd.market_dsc;
