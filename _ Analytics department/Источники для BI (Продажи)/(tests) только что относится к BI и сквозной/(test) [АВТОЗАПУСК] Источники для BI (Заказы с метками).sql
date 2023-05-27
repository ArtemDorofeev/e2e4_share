
/*
 * ИЗМЕНЕННЫЙ фон для BI 
 * 238 222 (так же как и фон) 01.11.2021 01.12.2021 (но там по позициям а метки к заказам привязаны)
 * */
 
with params as (
    select
--        ('01.04.2022' || ' 00:00:00')::timestamp frm,
--        ('30.04.2022' || ' 23:59:59')::timestamp til
    
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
   	JOIN opentech.account_item ai ON ai.account_id = a.id

    WHERE 
    	a.state != orders.item_state('BUNDLED') -- WHEN 'BUNDLED' THEN RETURN 27;                  --Комплект
--        AND a.in_account_date BETWEEN _.frm AND _.til --Время добавления позиции в заказ
        AND ai.tm_created BETWEEN _.frm AND _.til --Дата создания заказа

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
  	JOIN opentech.account_item ai ON ai.account_id = a1.id

    WHERE 
    	a1.state = orders.item_state('BUNDLED')
--        AND a1.in_account_date BETWEEN _.frm AND _.til --Время добавления позиции в заказ
        AND ai.tm_created BETWEEN _.frm AND _.til --Дата создания заказа

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
	JOIN opentech.account_item ai ON ai.account_id = a.id
    WHERE 
--        a.in_account_date BETWEEN _.frm AND _.til
        ai.tm_created BETWEEN _.frm AND _.til --Дата создания заказа
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
	JOIN opentech.account_item ai ON ai.account_id = a.id

    WHERE 
--        d.date BETWEEN _.frm AND _.til
        ai.tm_created BETWEEN _.frm AND _.til --Дата создания заказа

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
	JOIN opentech.account_item ai ON ai.account_id = a.id

    WHERE 
--        g.sold_on BETWEEN _.frm AND _.til
        ai.tm_created BETWEEN _.frm AND _.til --Дата создания заказа
)

, items AS (
    SELECT * FROM wares
    UNION ALL SELECT * FROM bundles
    UNION ALL SELECT * FROM services
    UNION ALL SELECT * FROM deliveries
    UNION ALL SELECT * FROM giftcards
)

, ORDERS AS (
    SELECT
        o.id_order,
        o.order_type,
        o.date_sold,
        o.is_full_sold,
        p.pay_date,
        o.price = p.pay_sum AS is_full_paid,      
        CASE WHEN m_tender.id_order IS NOT NULL THEN 'Т' END AS mark_tender,
        CASE WHEN m_tender.id_order IS NOT NULL THEN m_tender.author END AS m_autor,
        CASE WHEN m_request.id_order IS NOT NULL THEN 'З' END AS mark_request,
        CASE WHEN m_request.id_order IS NOT NULL THEN m_request.timestamp::date END AS mark_request_date,
        CASE WHEN m_kp.id_order IS NOT NULL THEN 'КП' END AS mark_kp,
        CASE WHEN m_A.id_order IS NOT NULL THEN 'А' END AS m_A,
        CASE WHEN m_P.id_order IS NOT NULL THEN 'П' END AS m_P
        
    FROM (
        SELECT
            i.id_order,
--            COALESCE(i.id_order_item, ai.account_id) id_order, 
            MIN(i.order_type) AS order_type,
            MAX(i.date_sold)::date AS date_sold,
            SUM(i.price) AS price,
            ARRAY_AGG(DISTINCT i.state) = store.item_sold(ARRAY['SHIPPED']) AS is_full_sold
        FROM items i
        JOIN opentech.account_item ai ON ai.account_id = i.id_order
        GROUP BY i.id_order
--        GROUP BY i.id_order_item, ai.account_id --i.id_order
    ) o
    LEFT JOIN LATERAL (
        SELECT
            MAX(p.pay_date)::date AS pay_date,
            SUM(
                CASE
                    WHEN p.type = payments.type('CASHLESS') AND p.date_out IS NOT NULL THEN -pa.sum
                    ELSE pa.sum
                END
            ) AS pay_sum
        FROM opentech.payments_account pa
        JOIN opentech.payments p ON p.id = pa.payment_id
        WHERE pa.account_id = o.id_order
    ) p ON TRUE
    LEFT JOIN orders.marks m_tender ON m_tender.id_order = o.id_order AND m_tender.id_mark = 1 /*Тендер*/
    LEFT JOIN orders.marks m_request ON m_request.id_order = o.id_order AND m_request.id_mark = 5 /*Запрос цен*/
    LEFT JOIN orders.marks m_kp ON m_kp.id_order = o.id_order AND m_kp.id_mark = 6 /*Коммерческое предложение*/
    LEFT JOIN orders.marks m_A ON m_kp.id_order = o.id_order AND m_kp.id_mark = 12 /*Метка А*/
    LEFT JOIN orders.marks m_P ON m_kp.id_order = o.id_order AND m_kp.id_mark = 11 /*Метка П*/
)

--select count(distinct OS.id_order) from ORDERS OS;

, PAYMENTS_DATES_ORDERS as  (
	select
		ai.account_id "Заказ"
		,ai.tm_created "Дата создания заказа"
	    ,sum(pa.sum) "Сумма оплаты заказа"
	    ,max(pa.time)::timestamp "Дата оплаты заказа"
	from params _
	cross join opentech.account_item ai  
	left join opentech.payments_account pa on pa.account_id = ai.account_id
	where 
		ai.tm_created between _.frm and _.til --дата создания заказа
--		and ai.account_id in (6667763)
	group by ai.account_id, ai.tm_created
)


select distinct
    OS.id_order "ID заказа",
    OS.mark_tender "Тендер",
    OS.m_autor "Автор метки Т",
    OS.mark_request "Запрос цен",
    OS.mark_request_date "Дата запроса цен",
    OS.mark_kp "Коммерческое предложение",
    OS.m_A "Активные продажи", --Активные продажи метка А
    OS.m_P "Проект" --Проект метка П
    ,PDO."Сумма оплаты заказа"
    ,PDO."Дата оплаты заказа"
from ORDERS OS
LEFT JOIN items item ON OS.id_order = item.id_order
LEFT join PAYMENTS_DATES_ORDERS PDO ON PDO."Заказ" = OS.id_order





    
    
    
    
    
    
    
