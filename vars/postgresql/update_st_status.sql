UPDATE ship_to SET status = data.status, updated_date = CURRENT_TIMESTAMP FROM (VALUES %s) AS data (st, status) WHERE ship_to.st = data.st