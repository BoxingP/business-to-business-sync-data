DROP VIEW IF EXISTS sku;
DROP VIEW IF EXISTS quote;
DROP VIEW IF EXISTS group_st_mapping;
CREATE VIEW sku AS SELECT sl.sku_id, sl.sku, sl.is_active, lp.unit_price AS sku_price, lp.effective_date AS sku_price_effective_date, lp.expiration_date AS sku_price_expiration_date FROM list_price AS lp LEFT JOIN sku_list AS sl ON lp.sku_id = sl.sku_id WHERE 1 = 1;
CREATE VIEW quote AS SELECT qp.quote_price, qp.quote_type, qp.quote_number, qp.min_order_quantity, qp.sku_id, qp.st_id, qp.effective_date AS quote_price_effective_date, qp.expiration_date AS quote_price_expiration_date FROM quote_price AS qp WHERE 1 = 1;
CREATE VIEW group_st_mapping AS SELECT crg.group_name, st.st_id, st.st FROM casmart_research_group AS crg LEFT JOIN ship_to_casmart_research_group AS stcrg ON crg.group_id = stcrg.group_id LEFT JOIN ship_to AS st ON stcrg.st_id = st.st_id WHERE 1 = 1