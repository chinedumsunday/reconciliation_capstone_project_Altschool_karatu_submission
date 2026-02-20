CREATE DATABASE IF NOT EXISTS quickcart_reconcilitation;

\i './schema.sql'
\i './quickcart_data/seed_orders.sql'
\i './quickcart_data/seed_payments.sql'   
\i './quickcart_data/seed_bank_settlements.sql'

WITH successful_payments AS (
    SELECT 
        o.order_id,
        p.payment_id,
        p.attempt_no,
        p.provider,
        p.provider_ref,
        p.status,
        p.amount_cents,
        p.attempted_at
    FROM orders o
    INNER JOIN payments p 
        ON p.order_id = o.order_id
    WHERE p.status = 'SUCCESS'
      AND o.is_test = 0
      AND p.amount_cents > 0
),

deduplicated_payments AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY attempt_no DESC, attempted_at DESC
        ) AS rn
    FROM successful_payments
),

internal_clean AS (
    SELECT
        order_id,
        payment_id,
        provider_ref,
        CAST(amount_cents AS DECIMAL(18,2)) / 100 AS amount_usd
    FROM deduplicated_payments
    WHERE rn = 1
),

internal_summary AS (
    SELECT
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(amount_usd) AS total_internal_sales_usd
    FROM internal_clean
),

bank_clean AS (
    SELECT
        settlement_id,
        payment_id,
        provider_ref,
        CAST(settled_amount_cents AS DECIMAL(18,2)) / 100 AS settled_amount_usd,
        settled_at
    FROM bank_settlements
    WHERE status = 'SETTLED'
      AND settled_amount_cents > 0
),

bank_deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY settlement_id 
            ORDER BY settled_at DESC
        ) AS rn
    FROM bank_clean
),

bank_final AS (
    SELECT
        settlement_id,
        payment_id,
        provider_ref,
        settled_amount_usd
    FROM bank_deduplicated
    WHERE rn = 1
),

bank_summary AS (
    SELECT
        COUNT(*) AS total_settlements,
        SUM(settled_amount_usd) AS total_bank_settled_usd
    FROM bank_final
),

/* ---------------- MATCHING USING COALESCE ---------------- */

reconciliation AS (
    SELECT
        i.order_id,
        i.payment_id AS internal_payment_id,
        i.provider_ref AS internal_provider_ref,
        i.amount_usd AS internal_amount_usd,
        b.payment_id AS bank_payment_id,
        b.provider_ref AS bank_provider_ref,
        b.settled_amount_usd
    FROM internal_clean i
    LEFT JOIN bank_final b 
        ON COALESCE(i.payment_id, i.provider_ref) = COALESCE(b.payment_id, b.provider_ref)
),

/* ---------------- ORPHANS ---------------- */

orphan_bank_payments AS (
    SELECT b.* FROM bank_final b
    WHERE NOT EXISTS (
        SELECT 1 FROM internal_clean i
        WHERE COALESCE(i.payment_id, i.provider_ref) = COALESCE(b.payment_id, b.provider_ref)
    )
),

orphan_summary AS (
    SELECT
        COUNT(*) AS orphan_count,
        SUM(settled_amount_usd) AS orphan_total_usd
    FROM orphan_bank_payments
)

SELECT
    i.total_orders,
    i.total_internal_sales_usd,
    b.total_bank_settled_usd,
    o.orphan_count,
    o.orphan_total_usd,
    (i.total_internal_sales_usd - b.total_bank_settled_usd) AS discrepancy_gap_usd
FROM internal_summary i
CROSS JOIN bank_summary b
CROSS JOIN orphan_summary o;