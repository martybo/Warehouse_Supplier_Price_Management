-- Margin calculator (global clawback) v1
INSERT INTO price_rule(name, scope, formula_json, valid_from)
SELECT 'Global clawback', 'global', '{"type":"clawback","rate":0.085}', CURRENT_DATE
WHERE NOT EXISTS (SELECT 1 FROM price_rule WHERE name='Global clawback');

CREATE OR REPLACE VIEW v_global_clawback AS
SELECT (formula_json->>'rate')::numeric AS rate
FROM price_rule WHERE name='Global clawback'
ORDER BY valid_from DESC LIMIT 1;

WITH latest_dmd AS (
  SELECT di.*, ROW_NUMBER() OVER (PARTITION BY di.vmpp_id, di.ampp_id ORDER BY di.effective_date DESC) rn
  FROM dmd_item di
)
CREATE OR REPLACE VIEW v_product_dt AS
SELECT pm.product_id, di.dt_price, di.dt_cat, di.effective_date
FROM product_mapping pm
JOIN latest_dmd di ON di.rn=1
WHERE pm.dmd_item_id = di.id;

CREATE OR REPLACE VIEW v_best_cost_current_month AS
WITH ranked AS (
  SELECT pq.product_id, pq.supplier_id, pq.price_ex_vat,
         ROW_NUMBER() OVER (PARTITION BY pq.product_id ORDER BY pq.price_ex_vat ASC) rn
  FROM price_quote pq
  WHERE date_trunc('month', pq.valid_from) = date_trunc('month', CURRENT_DATE)
)
SELECT product_id, supplier_id, price_ex_vat AS best_cost
FROM ranked WHERE rn=1;

CREATE OR REPLACE VIEW v_margin_current_month AS
SELECT p.medicare_pip, p.name, bc.product_id, s.name AS best_supplier,
       bc.best_cost, dt.dt_price AS dt_reimb_price,
       (SELECT rate FROM v_global_clawback) AS clawback_rate,
       (dt.dt_price * (1 - (SELECT rate FROM v_global_clawback))) - bc.best_cost AS margin_value,
       CASE WHEN dt.dt_price > 0 THEN ((dt.dt_price * (1 - (SELECT rate FROM v_global_clawback))) - bc.best_cost) / dt.dt_price END AS margin_pct
FROM v_best_cost_current_month bc
JOIN product p ON p.id = bc.product_id
LEFT JOIN v_product_dt dt ON dt.product_id = bc.product_id
LEFT JOIN supplier s ON s.id = bc.supplier_id
ORDER BY p.medicare_pip;
