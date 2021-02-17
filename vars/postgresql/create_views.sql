DROP VIEW IF EXISTS product;
DROP VIEW IF EXISTS quote;
DROP VIEW IF EXISTS group_st_mapping;
DROP VIEW IF EXISTS product_action;
CREATE VIEW product AS SELECT pl.sku, lp.list_price, pl.discontinued, pl.business_unit FROM list_price AS lp INNER JOIN product_list AS pl ON lp.product_id = pl.product_id WHERE 1 = 1 AND lp.effective_date <= current_timestamp AND lp.expiration_date >= current_timestamp;
CREATE VIEW quote AS SELECT pl.sku, qp.quote_type, qp.quote_number, qp.discount, qp.fixed_price, qp.min_order_quantity, st.st, qp.effective_date AS quote_price_effective_date, qp.expiration_date AS quote_price_expiration_date FROM quote_price AS qp LEFT JOIN ship_to AS st ON qp.st_id = st.st_id LEFT JOIN product_list AS pl ON qp.product_id = pl.product_id WHERE 1 = 1;
CREATE VIEW group_st_mapping AS SELECT crg.research_group_name, crg.research_group_contact_name, crg.research_group_contact_phone, crg.sourcing, st.st, st.is_default AS is_st_default FROM casmart_research_group AS crg LEFT JOIN ship_to_casmart_research_group AS stcrg ON crg.research_group_id = stcrg.research_group_id LEFT JOIN ship_to AS st ON stcrg.st_id = st.st_id WHERE 1 = 1;
CREATE VIEW product_action AS SELECT pbl.sku, pbl.action FROM product_action_list AS pbl WHERE 1 = 1