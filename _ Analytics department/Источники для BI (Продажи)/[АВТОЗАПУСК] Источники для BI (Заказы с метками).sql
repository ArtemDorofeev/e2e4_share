/*
 * ИЗМЕНЕННЫЙ фон для BI 
 */
 
with params as (
    select
--         ('01.10.2022' || ' 00:00:00')::timestamp frm,
--         ('31.10.2022' || ' 23:59:59')::timestamp til
   ( 'DATE_START_replce' || ' 00:00:00')::timestamp    frm 
   ,( 'DATE_END_replce' || ' 23:59:59')::timestamp    til 
)
-- select * from params

, wares as (
    select
        a.id AS id_order,
        null::INT AS id_order_item,
        a.firm AS order_type,
        s.date_sold AS date_sold,
        s.sold AS state, 
        s.price AS price
    FROM params _
    CROSS JOIN opentech.store s
    JOIN opentech.accounts a ON a.store_id = s.id
    WHERE a.state != orders.item_state('BUNDLED') -- WHEN 'BUNDLED' THEN RETURN 27;                  --Комплект
        AND s.sold != store.item_sold('NONE')
        AND s.date_sold BETWEEN _.frm AND _.til
        AND s.price > 0
)

, bundles AS (
    SELECT
        a2.id AS id_order,          /* id заказа комплекта */
        a1.id AS id_order_item,     /* id заказа комплектующей */
        a2.firm AS order_type,
        s2.date_sold AS date_sold,
        s1.sold AS state, 
        s1.price AS price
    FROM params _
    CROSS JOIN opentech.store s1                            /* Складская позиция комплектующей */
    JOIN opentech.accounts a1 ON a1.store_id = s1.id        /* Заказ комплектующей */
    JOIN store.bundled_items bu ON bu.id_store_item = s1.id
    JOIN opentech.store s2 ON s2.id = bu.id_bundle          /* Складская позиция комплекта */
    JOIN opentech.accounts a2 ON a2.store_id = s2.id        /* Заказ комплекта */
    WHERE a1.state = orders.item_state('BUNDLED')
        AND s2.sold != store.item_sold('NONE')
        AND s2.date_sold BETWEEN _.frm AND _.til
        AND s2.price > 0
)

, services AS (
    SELECT
        a.id AS id_order,
        null::INT AS id_order_item,
        a.firm AS order_type,
        os.done AS date_sold,
        store.item_sold('SHIPPED') AS state,
        a.price AS price
    FROM params _
    CROSS JOIN opentech.orderservices os
    JOIN opentech.accounts a ON a.i = os.orderitemid
    WHERE os.state = service.state('SOLD')
        AND os.done BETWEEN _.frm AND _.til
        AND a.price > 0
)

, deliveries AS (
    SELECT
        a.id AS id_order,
        null::INT AS id_order_item,
        a.firm AS order_type,
        d.date AS date_sold,
        store.item_sold('SHIPPED') AS state,
        a.price AS price
    FROM params _
    CROSS JOIN logistics.deliveries d
    JOIN logistics.delivery_items di ON di.id_delivery = d.id
    JOIN opentech.accounts a ON a.i = di.id_order_item AND a.correct_name ILIKE 'Доставка%'
    WHERE d.state = logistics.delivery_state('CLOSED')
        AND d.date BETWEEN _.frm AND _.til
        AND a.price > 0
)

, giftcards AS (
    SELECT
        a.id AS id_order,
        null::INT AS id_order_item,
        a.firm AS order_type,
        g.sold_on AS date_sold,
        store.item_sold('SHIPPED') AS state,
        a.price AS price
    FROM params _
    CROSS JOIN payments.giftcards g
    JOIN opentech.accounts a ON a.i = g.id_order_item
    WHERE g.status = ANY(payments.giftcard_status(ARRAY['ISSUED', 'EXPENDED']))
        AND g.sold_on BETWEEN _.frm AND _.til
        AND a.price > 0
)

, items AS (
    SELECT * FROM wares
    UNION ALL SELECT * FROM bundles
    UNION ALL SELECT * FROM services
    UNION ALL SELECT * FROM deliveries
    UNION ALL SELECT * FROM giftcards
)

--, orders AS (
--    SELECT
--        o.id_order,
--        o.order_type,
--        o.date_sold,
--        o.is_full_sold,
--        p.pay_date,
--        o.price = p.pay_sum AS is_full_paid,
--        CASE WHEN m_tender.id_order IS NOT NULL THEN 'Т' END AS mark_tender,
--        CASE WHEN m_tender.id_order IS NOT NULL THEN m_tender.author END AS m_autor,
--        CASE WHEN m_request.id_order IS NOT NULL THEN 'З' END AS mark_request,
--        CASE WHEN m_request.id_order IS NOT NULL THEN m_request.timestamp::date END AS mark_request_date,
--        CASE WHEN m_kp.id_order IS NOT NULL THEN 'КП' END AS mark_kp,
--        CASE WHEN m_A.id_order IS NOT NULL THEN 'А' END AS m_A,
--        CASE WHEN m_P.id_order IS NOT NULL THEN 'П' END AS m_P,
--        CASE WHEN m_VI.id_order IS NOT NULL THEN 'ВИ' END AS m_VI
--    FROM (
--        SELECT
--            i.id_order,
----            COALESCE(i.id_order_item, ai.account_id) id_order, 
--            MIN(i.order_type) AS order_type,
--            MAX(i.date_sold)::date AS date_sold,
--            SUM(i.price) AS price,
--            ARRAY_AGG(DISTINCT i.state) = store.item_sold(ARRAY['SHIPPED']) AS is_full_sold
--        FROM items i
--        JOIN opentech.account_item ai ON ai.account_id = i.id_order
--
--        GROUP BY i.id_order
----        GROUP BY i.id_order_item, ai.account_id --i.id_order
--    ) o
--    LEFT JOIN LATERAL (
--        SELECT
--            MAX(p.pay_date)::date AS pay_date,
--            SUM(
--                CASE
--                    WHEN p.type = payments.type('CASHLESS') AND p.date_out IS NOT NULL THEN -pa.sum
--                    ELSE pa.sum
--                END
--            ) AS pay_sum
--        FROM opentech.payments_account pa
--        JOIN opentech.payments p ON p.id = pa.payment_id
--        WHERE pa.account_id = o.id_order
--    ) p ON TRUE
--    LEFT JOIN orders.marks m_tender ON m_tender.id_order = o.id_order AND m_tender.id_mark = 1 /*Тендер*/
--    LEFT JOIN orders.marks m_request ON m_request.id_order = o.id_order AND m_request.id_mark = 5 /*Запрос цен*/
--    LEFT JOIN orders.marks m_kp ON m_kp.id_order = o.id_order AND m_kp.id_mark = 6 /*Коммерческое предложение*/
--    LEFT JOIN orders.marks m_A ON m_kp.id_order = o.id_order AND m_kp.id_mark = 12 /*Метка А*/
--    LEFT JOIN orders.marks m_P ON m_kp.id_order = o.id_order AND m_kp.id_mark = 11 /*Метка П*/
--    LEFT JOIN orders.marks m_VI ON m_VI.id_order = o.id_order AND m_VI.id_mark = 9 /*Метка ВИ	Продажа товара с витрины*/
--)

--select distinct
--
--    o.id_order "ID заказа",
--    o.mark_tender "Тендер",
--    o.m_autor "Автор метки Т",
--    o.mark_request "Запрос цен",
--    o.mark_request_date "Дата запроса цен",
--    o.mark_kp "Коммерческое предложение",
--    o.m_A "Активные продажи", --Активные продажи метка А
--    o.m_P "Проект", --Проект метка П
--    o.m_VI "ВИ" --Метка ВИ	Продажа товара с витрины
--
--from orders o
--LEFT JOIN items item ON o.id_order = item.id_order

--where
--	o.id_order in (6811949)


select distinct

    i.id_order "ID заказа",
    CASE WHEN m_tender.id_order IS NOT NULL THEN 'Т' END AS "Тендер",
    CASE WHEN m_tender.id_order IS NOT NULL THEN m_tender.author END AS "Автор метки Т",
    CASE WHEN m_request.id_order IS NOT NULL THEN 'З' END AS "Запрос цен",
    CASE WHEN m_request.id_order IS NOT NULL THEN m_request.timestamp::date END AS "Дата запроса цен",
    CASE WHEN m_kp.id_order IS NOT NULL THEN 'КП' END AS "Коммерческое предложение",
    CASE WHEN m_A.id_order IS NOT NULL THEN 'А' END AS "Активные продажи",
    CASE WHEN m_P.id_order IS NOT NULL THEN 'П' END AS "Проект",
    CASE WHEN m_VI.id_order IS NOT NULL THEN 'ВИ' END AS "ВИ"
    
--    o.mark_tender "Тендер",
--    o.m_autor "Автор метки Т",
--    o.mark_request "Запрос цен",
--    o.mark_request_date "Дата запроса цен",
--    o.mark_kp "Коммерческое предложение",
--    o.m_A "Активные продажи", --Активные продажи метка А
--    o.m_P "Проект", --Проект метка П
--    o.m_VI "ВИ" --Метка ВИ	Продажа товара с витрины

--    id_mark,	author,	"timestamp"

from items i
--LEFT JOIN orders.marks om ON om.id_order = i.id_order 
LEFT JOIN orders.marks m_tender ON m_tender.id_order = i.id_order AND m_tender.id_mark = 1 /*Тендер*/
LEFT JOIN orders.marks m_request ON m_request.id_order = i.id_order AND m_request.id_mark = 5 /*Запрос цен*/
LEFT JOIN orders.marks m_kp ON m_kp.id_order = i.id_order AND m_kp.id_mark = 6 /*Коммерческое предложение*/
LEFT JOIN orders.marks m_A ON m_A.id_order = i.id_order AND m_A.id_mark = 12 /*Метка А*/
LEFT JOIN orders.marks m_P ON m_P.id_order = i.id_order AND m_P.id_mark = 11 /*Метка П*/
LEFT JOIN orders.marks m_VI ON m_VI.id_order = i.id_order AND m_VI.id_mark = 9 /*Метка ВИ	Продажа товара с витрины*/

--where
--	i.id_order in (6811949)











