DROP VIEW IF EXISTS product;
DROP VIEW IF EXISTS quote;
DROP VIEW IF EXISTS group_st_mapping;
DROP VIEW IF EXISTS product_action;
CREATE VIEW product AS SELECT pl.sku, lp.list_price, pl.discontinued, pl.business_unit FROM product_list AS pl LEFT JOIN list_price AS lp ON lp.sku = pl.sku AND lp.effective_date <= CURRENT_TIMESTAMP AND lp.expiration_date >= CURRENT_TIMESTAMP;
CREATE VIEW quote AS SELECT qp.sku, qp.quote_type, qp.quote_number, qp.discount, qp.fixed_price, qp.min_order_quantity, qp.st, qp.effective_date AS quote_price_effective_date, qp.expiration_date AS quote_price_expiration_date FROM quote_price AS qp WHERE 1 = 1;
CREATE VIEW group_st_mapping AS SELECT crg.research_group_name, crg.research_group_contact_name, crg.research_group_contact_phone, crg.sourcing, st.st, st.is_default AS is_st_default FROM casmart_research_group AS crg INNER JOIN ship_to AS st ON crg.st = st.st WHERE 1 = 1;
CREATE VIEW product_action AS SELECT pbl.sku, pbl.action FROM product_action_list AS pbl WHERE 1 = 1