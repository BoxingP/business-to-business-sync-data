SELECT t1.Q556QTTYPE AS QUOTE_TYPE, t1.Q556QTNO AS QUOTE_NUMBER, t1.{1} AS SKU, t3.KCSHAN AS ST, t1.Q5QTY / 100 AS MIN_ORDER_QUANTITY, t1.Q5ADPE / 100000 AS DISCOUNT, t1.Q556NUM1 / 10000 AS FIXED_PRICE, t1.Q5EFTJ AS EFFECTIVE_DATE, t1.Q5EFDJ AS EXPIRATION_DATE, t1.Q5UPMJ AS DATE_UPDATED, t1.Q5UPMT AS TIME_UPDATED FROM (SELECT Q556QTNO, Q556QTTYPE, {1}, Q5QTY, Q5ADPE, Q556NUM1, Q5EFTJ, Q5EFDJ, Q5UPMJ, Q5UPMT FROM PRODDTA.F5651A WHERE Q556QTTYPE = 'E' AND Q5EFDJ >= {3} AND Q5CTR = 'CN' AND {1} IN ({0})) t1 INNER JOIN (SELECT Q456QTNO, Q456QTTYPE FROM PRODDTA.F5650A WHERE Q456QTTYPE = 'E' AND Q4CTR='CN' AND Q456QTSTAT = 'A') t2 ON t1.Q556QTNO = t2.Q456QTNO AND t1.Q556QTTYPE = t2.Q456QTTYPE INNER JOIN (SELECT KC56QTNO, KC56QTTYPE, KCSHAN FROM PRODDTA.F5653A WHERE KC56QTTYPE = 'E' AND KCSHAN IN ({2})) t3 ON t3.KC56QTNO = t2.Q456QTNO AND t3.KC56QTTYPE = t2.Q456QTTYPE