SELECT sku FROM product_list WHERE updated_by <> '{0}' OR updated_date IS NULL FETCH FIRST {1} ROWS ONLY