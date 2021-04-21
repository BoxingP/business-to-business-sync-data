SELECT t1.Q556QTTYPE AS QUOTE_TYPE, t1.Q556QTNO AS QUOTE_NUMBER, t2.Q456QTSTAT AS QUOTE_STATUS, t1.{sku} AS SKU, t1.{sku} AS PPL, t4.ABAN8 AS ST, t1.Q5QTY / 100 AS MIN_ORDER_QUANTITY, t1.Q5ADPE / 100000 AS DISCOUNT, t1.Q556NUM1 / 10000 AS FIXED_PRICE, t1.Q5EFTJ AS EFFECTIVE_DATE, t1.Q5EFDJ AS EXPIRATION_DATE, t1.Q5UPMJ AS DATE_UPDATED, t1.Q5UPMT AS TIME_UPDATED FROM (SELECT Q556QTTYPE, Q556QTNO, {sku}, Q5QTY, Q5ADPE, Q556NUM1, Q5EFTJ, Q5EFDJ, Q5UPMJ, Q5UPMT FROM PRODDTA.F5651A WHERE Q556QTTYPE = '{quote_type}' AND ({updated_time_condition}) AND Q5CTR = 'CN' AND {sku} IN ({skus})) t1 INNER JOIN (SELECT Q456QTTYPE, Q456QTNO, Q456QTSTAT FROM PRODDTA.F5650A WHERE Q456QTTYPE = '{quote_type}' AND Q4CTR='CN') t2 ON t2.Q456QTTYPE = t1.Q556QTTYPE AND t2.Q456QTNO = t1.Q556QTNO INNER JOIN (SELECT KC56QTTYPE, KC56QTNO, KCSHAN FROM PRODDTA.F5653A WHERE KC56QTTYPE = '{quote_type}') t3 ON t3.KC56QTTYPE = t2.Q456QTTYPE AND t3.KC56QTNO = t2.Q456QTNO INNER JOIN (SELECT ABAN81, ABAN8 FROM PRODDTA.F0101_ADT WHERE ABAN8 IN ({sts}) AND ABAT1 = 'S') t4 ON t4.{st} = t3.KCSHAN