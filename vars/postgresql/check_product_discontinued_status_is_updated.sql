SELECT 1 FROM product_list WHERE ( updated_date IS NULL OR updated_date < NOW() - INTERVAL '{0} HOURS' ) AND discontinued = '0' FETCH FIRST 1 ROWS ONLY