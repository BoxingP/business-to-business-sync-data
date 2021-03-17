SELECT ARRAY(SELECT pg_get_indexdef(idx.indexrelid, k + 1, TRUE) FROM generate_subscripts(idx.indkey, 1) AS k ORDER BY k) AS indkey_names FROM pg_index AS idx JOIN pg_class AS i ON i.oid = idx.indexrelid JOIN pg_am AS am ON i.relam = am.oid WHERE idx.indrelid::regclass = %s::regclass AND idx.indisprimary = FALSE