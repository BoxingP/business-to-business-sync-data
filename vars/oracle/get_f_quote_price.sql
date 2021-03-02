SELECT Q556QTTYPE AS QUOTE_TYPE, Q556QTNO AS QUOTE_NUMBER, {1} AS SKU, {2} AS ST, Q5QTY / 100 AS MIN_ORDER_QUANTITY, Q5ADPE / 100000 AS DISCOUNT, Q556NUM1 / 10000 AS FIXED_PRICE, Q5EFTJ AS EFFECTIVE_DATE, Q5EFDJ AS EXPIRATION_DATE, Q5UPMJ AS DATE_UPDATED, q5upmt AS TIME_UPDATED FROM PRODDTA.F5651A WHERE Q556QTTYPE = 'F' AND Q556QTNO IN (SELECT DISTINCT t2.Q456QTNO FROM (SELECT KC56QTNO, KC56QTTYPE, KCSHAN FROM PRODDTA.F5653A WHERE KC56QTTYPE = 'F') t1 INNER JOIN (SELECT Q456QTNO, Q456QTTYPE FROM PRODDTA.F5650A WHERE Q456QTTYPE = 'F' AND Q4URC1 = 'CNY' AND Q456QTSTAT = 'A') t2 ON t1.KC56QTNO = t2.Q456QTNO AND t1.KC56QTTYPE = t2.Q456QTTYPE INNER JOIN (SELECT ABAN81 FROM proddta.F0101_ADT WHERE ABAN8 = {2} AND ABAT1='S') t3 ON t3.ABAN81 = t1.KCSHAN) AND Q5EFDJ >= {3} AND Q5CTR = 'CN' AND {1} IN ({0})