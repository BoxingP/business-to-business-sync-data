SELECT Q556QTTYPE AS QUOTE_TYPE, Q556QTNO AS QUOTE_NUMBER, '{0}' AS SKU, {1} AS ST, Q5QTY / 100 AS MIN_ORDER_QUANTITY, Q5ADPE / 100000 AS DISCOUNT, Q556NUM1 / 10000 AS FIXED_PRICE, Q5EFTJ AS EFFECTIVE_DATE, Q5EFDJ AS EXPIRATION_DATE, Q5UPMJ AS DATE_UPDATED, q5upmt AS TIME_UPDATED FROM PRODDTA.F5651A WHERE Q556QTTYPE = 'D' AND Q556QTNO IN ( SELECT DISTINCT Q456QTNO FROM PRODDTA.F5650A WHERE Q4SHAN IN ( {1}, 90714) AND Q456QTTYPE = 'D' AND Q4CTR='CN' AND Q456QTSTAT = 'A' ) AND Q5CTR = 'CN' AND ( Q5LITM IN ('{0}') OR Q5ITTP IN ( SELECT DISTINCT IBSRP3 FROM PRODDTA.F4102 WHERE IBLITM = '{0}' )) AND Q5EFDJ >= {2}