/*ИЗМЕНЕННЫЙ фон для BI  (Продажи)*/
 
 /*
--https://jira.e2e4.ru/browse/BA-464
--DROP FUNCTION opentech.get_flat_categories_from_children_to_roots(integer) ;
--CREATE OR REPLACE FUNCTION opentech.get_flat_categories_from_children_to_roots(id_section_sootv int) 
--/*возвращает плоский каталог от детей (section_sootv) до рутовой категории начиная от рута */
--/*tests*/
--/*--select section_sootv, * from opentech.reference_price where id in (587005); /*section_sootv in (4017)*/ */
--RETURNS text AS $$
--DECLARE
--   result text;
--BEGIN
--   WITH RECURSIVE categories(section_sootv, id, name, name_translit, parent, view, issection, level, goodId) AS (
--			select 
--				sn.id as section_sootv, sn.id, sn.name, sn.name_translit, sn.parent, sn.view, sn.issection, 0, sn.goodId
--			FROM  opentech.section_new sn
--			left JOIN opentech.section_new tg ON tg.id = sn.goodId
--			WHERE sn.id = id_section_sootv /*--4017 --3606*/
--			UNION ALL
--			SELECT
--				null, sn.id, sn.name, sn.name_translit, sn.parent, sn.view, sn.issection, level - 1, sn.goodId
--			FROM categories c
--			JOIN opentech.section_new sn ON sn.id = c.parent
--		)
--		, flat_categories as (
--		select 
--		--	array_to_string( array_agg(section_sootv), '' ) section_sootv 
--		--	,array_to_string( array_agg(id ORDER BY level), ' / ' ) 
--			array_to_string( array_agg(name ORDER BY level), ' / ' )::text flat_categories
--		--	,array_to_string( array_agg(name_translit ORDER BY level), ' / ' ) 
--		--	,array_to_string( array_agg(level), ' / ' ) 
--		FROM categories
--		)
--	select fc.flat_categories
--	INTO
--      	result 
--	from flat_categories fc;
--	
--	RETURN result;
--END;
--$$ LANGUAGE plpgsql STABLE STRICT;
--select opentech.get_flat_categories_from_children_to_roots(4017)
--union
--select opentech.get_flat_categories_from_children_to_roots(3606);
*/

with params as (
    select
--         ('01.11.2021' || ' 00:00:00')::timestamp frm,
--         ('01.12.2021' || ' 23:59:59')::timestamp til
   ( 'DATE_START_replce' || ' 00:00:00')::timestamp    frm 
   ,( 'DATE_END_replce' || ' 23:59:59')::timestamp    til 
)
-- select * from params

, wares as  (
    select
        -- a.i AS i_log,
        a.i,
        a.id AS id_order,
        null::INT AS id_order_item,
        a.id_customer AS id_customer,
        a.firm AS order_type,
        a.storage_id AS id_storage,
        a.user_login AS order_source,
        'Товар' AS TYPE,
        rp.id AS id,
        rp.name AS name,
        g.name AS category,
--        rp.section_sootv,
        opentech.get_flat_categories_from_children_to_roots(rp.section_sootv) category2,
        b.name AS brand,
        s.price AS price,
        round(s.cost) AS cost,
        round(s.sprice) AS sprice,
        s.date_sold AS date_sold,
        s.sold AS state, 
        COALESCE(r.date, checks.created_at) AS realization_date,
        ROUND(s.price * (COALESCE(rnds.value, cnds.value, 20) / (100 + COALESCE(rnds.value, cnds.value, 20))), 2) AS nds,
        c.name AS supplier,
        u.id AS manager
        --Состояние сборки: (oc.collect) 0 - сборка не заказана, 1 - собрать, 2 - собрана, 3 - разобрать, 4 - разобрана, 5 - продана
        ,case when (oc.collect = 0) or (oc.collect is null) then null else 'c' end "сборка/комплект" 
        ,case when (oc.collect = 0) or (oc.collect is null) then null else oc.collectcount::text end "Кол-во сборок" --oc.collectcount - Количество собранных компьютеров в составе заказа
    FROM params _
    CROSS JOIN opentech.store s
    JOIN opentech.accounts a ON a.store_id = s.id
    LEFT join opentech.order_client oc ON a.id = oc.account_id
    LEFT JOIN opentech.users u ON lower(u.login) = lower(a.user_login)
    LEFT JOIN orders.realization_items_order_items_bindings bi ON bi.id_order_item = a.i
    LEFT JOIN orders.realization_items i ON i.id = bi.id_realization_item
    LEFT JOIN orders.realizations r ON r.id = i.id_realization
    LEFT JOIN payments.order_items_locked_by_check_items il ON il.id_order_item = a.i
    LEFT JOIN payments.check_items ci ON ci.id = il.id_check_item_fullpaid
    LEFT JOIN payments.checks checks ON checks.id = ci.id_check
    LEFT JOIN public.nds rnds ON rnds.id = i.id_nds
    LEFT JOIN public.nds cnds ON cnds.code = ci.vat
    JOIN opentech.reference_price rp ON rp.id = s.reference_id
    LEFT JOIN opentech.section_new sn ON sn.id = rp.section_sootv
    LEFT JOIN catalog.plain_section_list g ON g.id = sn.goodid
    LEFT JOIN catalog.brands b ON b.id = rp.brand_id
    LEFT JOIN opentech.supplyitems si ON si.id = s.supplyitemid
    LEFT JOIN opentech.supplies su ON su.id = si.supplyid
    LEFT JOIN customers.suppliers sup ON sup.id_customer = su.supplierid
    LEFT JOIN opentech.customers c ON c.id = COALESCE(sup.id_customer_alpha, sup.id_customer)
    WHERE a.state != orders.item_state('BUNDLED') -- WHEN 'BUNDLED' THEN RETURN 27;                  --Комплект
        AND s.sold != store.item_sold('NONE')
        AND s.date_sold BETWEEN _.frm AND _.til
        AND s.price > 0
)
--select * from wares

, bundles AS  (
    SELECT
        -- a.i AS i_log,
        a2.i,
        a2.id AS id_order,          /* id заказа комплекта */
        a1.id AS id_order_item,     /* id заказа комплектующей */
        a2.id_customer AS id_customer,
        a2.firm AS order_type,
        a2.storage_id AS id_storage,
        a2.user_login AS order_source,
        'Комплектующая' AS TYPE,
        rp.id AS id,
        rp.name AS name,
        g.name AS category,
--        rp.section_sootv,
        opentech.get_flat_categories_from_children_to_roots(rp.section_sootv) category2,
        b.name AS brand,
        s1.price AS price,
        round(s1.cost) AS cost,
        round(s1.sprice) AS sprice,
        s2.date_sold AS date_sold,
        s1.sold AS state, 
        COALESCE(r.date, checks.created_at) AS realization_date,
        ROUND(s1.price * (COALESCE(rnds.value, cnds.value, 20) / (100 + COALESCE(rnds.value, cnds.value, 20))), 2) AS nds,
        c.name AS supplier,
        u.id AS manager
        ,'к' "сборка/комплект" 
        ,null
    FROM params _
    CROSS JOIN opentech.store s1                            /* Складская позиция комплектующей */
    JOIN opentech.accounts a1 ON a1.store_id = s1.id        /* Заказ комплектующей */
    JOIN store.bundled_items bu ON bu.id_store_item = s1.id
    JOIN opentech.store s2 ON s2.id = bu.id_bundle          /* Складская позиция комплекта */
    JOIN opentech.accounts a2 ON a2.store_id = s2.id        /* Заказ комплекта */
    /* НДС комплектующей */
    LEFT JOIN orders.realization_items_order_items_bindings bi1 ON bi1.id_order_item = a1.i
    LEFT JOIN orders.realization_items i1 ON i1.id = bi1.id_realization_item
    LEFT JOIN payments.order_items_locked_by_check_items il1 ON il1.id_order_item = a1.i
    LEFT JOIN payments.check_items ci1 ON ci1.id = il1.id_check_item_fullpaid
    LEFT JOIN public.nds rnds ON rnds.id = i1.id_nds
    LEFT JOIN public.nds cnds ON cnds.code = ci1.vat
    /* Комплектующая */
    JOIN opentech.reference_price rp ON rp.id = s1.reference_id
    LEFT JOIN opentech.section_new sn ON sn.id = rp.section_sootv
    LEFT JOIN catalog.plain_section_list g ON g.id = sn.goodid
    LEFT JOIN catalog.brands b ON b.id = rp.brand_id
    /* Комплект */    
    LEFT JOIN opentech.users u ON lower(u.login) = lower(a2.user_login)
    LEFT JOIN orders.realization_items_order_items_bindings bi ON bi.id_order_item = a2.i
    LEFT JOIN orders.realization_items i ON i.id = bi.id_realization_item
    LEFT JOIN orders.realizations r ON r.id = i.id_realization
    LEFT JOIN payments.order_items_locked_by_check_items il ON il.id_order_item = a2.i
    LEFT JOIN payments.check_items ci ON ci.id = il.id_check_item_fullpaid
    LEFT JOIN payments.checks checks ON checks.id = ci.id_check
    LEFT JOIN opentech.supplyitems si ON si.id = s2.supplyitemid
    LEFT JOIN opentech.supplies su ON su.id = si.supplyid
    LEFT JOIN customers.suppliers sup ON sup.id_customer = su.supplierid
    LEFT JOIN opentech.customers c ON c.id = COALESCE(sup.id_customer_alpha, sup.id_customer)
    WHERE a1.state = orders.item_state('BUNDLED')
        AND s2.sold != store.item_sold('NONE')
        AND s2.date_sold BETWEEN _.frm AND _.til
        AND s2.price > 0
)
--SELECT * FROM wares
--union all 
--select * from bundles

, services AS  (
    SELECT
        -- a.i AS i_log,
        a.i,
        a.id AS id_order,
        null::INT AS id_order_item,
        a.id_customer AS id_customer,
        a.firm AS order_type,
        a.storage_id AS id_storage,
        a.user_login AS order_source,
        'Услуга' AS TYPE,
        t.id AS id,
        t.name AS name,
        c.name AS category,
        c.name AS category2,
        NULL::TEXT AS brand,
        a.price AS price,
        NULL::int AS cost,
        NULL::int AS sprice,
        os.done AS date_sold,
        store.item_sold('SHIPPED') AS state,
        COALESCE(r.date, checks.created_at) AS realization_date,
        ROUND(a.price * (COALESCE(rnds.value, cnds.value, 20) / (100 + COALESCE(rnds.value, cnds.value, 20))), 2) AS nds,
        NULL::TEXT AS supplier,
        u.id AS manager
        ,null
        ,null
    FROM params _
    CROSS JOIN opentech.orderservices os
    JOIN opentech.accounts a ON a.i = os.orderitemid
    LEFT JOIN opentech.users u ON lower(u.login) = lower(a.user_login)
    LEFT JOIN orders.realization_items_order_items_bindings bi ON bi.id_order_item = a.i
    LEFT JOIN orders.realization_items i ON i.id = bi.id_realization_item
    LEFT JOIN orders.realizations r ON r.id = i.id_realization
    LEFT JOIN payments.order_items_locked_by_check_items il ON il.id_order_item = a.i
    LEFT JOIN payments.check_items ci ON ci.id = il.id_check_item_fullpaid
    LEFT JOIN payments.checks checks ON checks.id = ci.id_check
    LEFT JOIN public.nds rnds ON rnds.id = i.id_nds
    LEFT JOIN public.nds cnds ON cnds.code = ci.vat
    JOIN service.types t ON t.id = os.type
    JOIN service.categories c ON c.id = t.id_category
    WHERE os.state = service.state('SOLD')
        AND os.done BETWEEN _.frm AND _.til
        AND a.price > 0
)

, deliveries AS  (
    SELECT
        -- a.i AS i_log,
        a.i,
        a.id AS id_order,
        null::INT AS id_order_item,
        a.id_customer AS id_customer,
        a.firm AS order_type,
        a.storage_id AS id_storage,
        a.user_login AS order_source,
        'Доставка' AS TYPE,
        d.id AS id,
        a.correct_name AS name,
        t.name AS category,
        t.name AS category2,
        NULL::text AS brand,
        a.price AS price,
        NULL::int AS cost,
        NULL::int AS sprice,
        d.date AS date_sold,
        store.item_sold('SHIPPED') AS state,
        COALESCE(r.date, checks.created_at) AS realization_date,
        ROUND(a.price * (COALESCE(rnds.value, cnds.value, 20) / (100 + COALESCE(rnds.value, cnds.value, 20))), 2) AS nds,
        NULL::text AS supplier,
        u.id AS manager
        ,null
        ,null
    FROM params _
    CROSS JOIN logistics.deliveries d
    JOIN logistics.delivery_types t ON t.id = d.type
    JOIN logistics.delivery_items di ON di.id_delivery = d.id
    JOIN opentech.accounts a ON a.i = di.id_order_item AND a.correct_name ILIKE 'Доставка%'
    LEFT JOIN opentech.users u ON lower(u.login) = lower(a.user_login)
    LEFT JOIN orders.realization_items_order_items_bindings bi ON bi.id_order_item = a.i
    LEFT JOIN orders.realization_items i ON i.id = bi.id_realization_item
    LEFT JOIN orders.realizations r ON r.id = i.id_realization
    LEFT JOIN payments.order_items_locked_by_check_items il ON il.id_order_item = a.i
    LEFT JOIN payments.check_items ci ON ci.id = il.id_check_item_fullpaid
    LEFT JOIN payments.checks checks ON checks.id = ci.id_check
    LEFT JOIN public.nds rnds ON rnds.id = i.id_nds
    LEFT JOIN public.nds cnds ON cnds.code = ci.vat
    WHERE d.state = logistics.delivery_state('CLOSED')
        AND d.date BETWEEN _.frm AND _.til
        AND a.price > 0
)

, giftcards AS  (
    SELECT
        -- a.i AS i_log,
        a.i,
        a.id AS id_order,
        null::INT AS id_order_item,
        a.id_customer AS id_customer,
        a.firm AS order_type,
        a.storage_id AS id_storage,
        a.user_login AS order_source,
        'Подарочная карта' AS TYPE,
        1 AS id,
        a.correct_name AS name,
        'Подарочная карта' AS category,
        'Подарочная карта' AS category2,
        NULL::text AS brand,
        a.price AS price,
        NULL::int AS cost,
        NULL::int AS sprice,
        g.sold_on AS date_sold,
        store.item_sold('SHIPPED') AS state,
        COALESCE(r.date, checks.created_at) AS realization_date,
        ROUND(a.price * (COALESCE(rnds.value, cnds.value, 20) / (100 + COALESCE(rnds.value, cnds.value, 20))), 2) AS nds,
        NULL::text AS supplier,
        u.id AS manager
        ,null
        ,null
    FROM params _
    CROSS JOIN payments.giftcards g
    JOIN opentech.accounts a ON a.i = g.id_order_item
    LEFT JOIN opentech.users u ON lower(u.login) = lower(a.user_login)
    LEFT JOIN orders.realization_items_order_items_bindings bi ON bi.id_order_item = a.i
    LEFT JOIN orders.realization_items i ON i.id = bi.id_realization_item
    LEFT JOIN orders.realizations r ON r.id = i.id_realization
    LEFT JOIN payments.order_items_locked_by_check_items il ON il.id_order_item = a.i
    LEFT JOIN payments.check_items ci ON ci.id = il.id_check_item_fullpaid
    LEFT JOIN payments.checks checks ON checks.id = ci.id_check
    LEFT JOIN public.nds rnds ON rnds.id = i.id_nds
    LEFT JOIN public.nds cnds ON cnds.code = ci.vat
    WHERE g.status = ANY(payments.giftcard_status(ARRAY['ISSUED', 'EXPENDED']))
        AND g.sold_on BETWEEN _.frm AND _.til
        AND a.price > 0
)

, items AS  (
    SELECT * FROM wares
    UNION ALL SELECT * FROM bundles
    UNION ALL SELECT * FROM services
    UNION ALL SELECT * FROM deliveries
    UNION ALL SELECT * FROM giftcards
)

, logistics AS  (
    SELECT DISTINCT ON (items.id_order)
        items.id_order,
        d.type,
        c.name AS freighter,
        ga.region,
        ga.city,
        ga.address
    FROM items
    JOIN logistics.delivery_items di ON di.id_order_item = items.i
    JOIN logistics.deliveries d ON d.id = di.id_delivery
    LEFT JOIN opentech.customers c ON c.id = d.id_freighter
    LEFT JOIN geo.addresses ga ON ga.id = d.id_address
    WHERE d.state = logistics.delivery_state('CLOSED')
    ORDER BY items.id_order, d.date DESC
)
/*
, customer_managers_log AS  (
    /* Дата последней отвязки ответственного менеджера */
    SELECT c.id, max(l._tm)::date unbind_date
    FROM opentech.customers c
    JOIN _logs.opentech__customers l ON l.id = c.id
        AND l."_op_type" = 2    /* это запись об изменении карточки */
        AND exist(l._values, 'manager') /* изменялся ответственный менеджер */
        AND NOT defined(l._values, 'manager') /* ответственный менеджер был снят */
    WHERE c.manager IS NULL
    GROUP BY c.id
)
*/
--, orders AS  (
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
--        CASE WHEN m_P.id_order IS NOT NULL THEN 'П' END AS m_P
--    FROM (
--        SELECT
--            i.id_order,
--            MIN(i.order_type) AS order_type,
--            MAX(i.date_sold)::date AS date_sold,
--            SUM(i.price) AS price,
--            ARRAY_AGG(DISTINCT i.state) = store.item_sold(ARRAY['SHIPPED']) AS is_full_sold
--        FROM items i
--        GROUP BY i.id_order
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
--)
/*
, profitability AS  (
    /* расчет маржинальности клиентов */
    SELECT
        id_customer,
        CASE WHEN sum(cost) <> 0 THEN round(100 * (sum(price) - sum(cost))/ sum(cost)) END AS value,
        sum(price) - sum(cost) AS sum
    FROM items
    WHERE NOT customers.is_special_customer(id_customer)
    GROUP BY id_customer
)
*/
--, logs_date_state as ( 
--	/*максимум по дате la._tm (тоесть последнене изменение) для заказа товара если хстор 5 (На складе выдачи)
--	 * отимизировать может дату езе огр... или попр сортир  по уб и взять первую (деск и димит 1)
--	 * */
--    select 
--        oai.account_id "ID заказа"
--        ,oa.reference_id "Артикул (id товара)"
--        ,max(la._tm)::timestamp             "Дата готовности к выдаче товара"
--        ,to_char(max(la._tm), 'HH24:MI')     "Время готовности к выдаче товара" -- 'HH24:MI' "09:14"
--        ,to_char(max(la._tm), 'DD')         "День готовности к выдаче товара" 
--        ,to_char(max(la._tm), 'MM')         "Месяц готовности к выдаче товара" -- 'TMMonth' "Января"
--        ,to_char(max(la._tm), 'YYYY')         "Год готовности к выдаче товара"
----     ,ois.name "Состояние name"
----     ,oa.store_id "Складская позиция, выбранная для продажи"
----     ,oa.state "Состояние позиции заказа"
----     ,ois.code "Состояние code"
----     ,max(in_account_date) "Время добавления позиции в заказ"
----     ,max(acc_date) "Дата создания/оформления заказа"
----     ,max(fn_date) "Дата реализации"
--    from opentech.accounts oa 
--    left join opentech.account_item oai on oai.account_id = oa.id
--    left join orders.item_states ois on ois.id = oa.state
--    left join _logs.opentech__accounts la on la.i = oa.i
--    where 
--        la._values::hstore -> 'state' = '5' --5	RESERVED_ON_STORAGE	На складе выдачи
----        and account_id in( 3752476 ) --"ID заказа"
----     and account_id not in( 2290882 ) 652503
----     and reference_id in (652503)
----     and oa.store_id is null --не нужен !!
----     and oa.state = 5 --тоже не нужен! (не относится к истории)
--    group by 1,2--,3,4,5,6--,7,8,9
--    order by 1,2--,3,4,5,6--,7,8,9
--)

/*
, under_the_order as (
select 
    oa.id "ID заказа"
    ,oa.reference_id "ID позиции"
    ,oa.i "i позиции"
    ,la."_tm"
	,case when ois.id in (9,10,11) then 'под заказ'
	else la."_tm" || ' ' || ois.name end "state" 
FROM params _
CROSS JOIN opentech.store s
JOIN opentech.accounts oa ON oa.store_id = s.id
join opentech.account_item ai on ai.account_id = oa.id 
--join orders.item_states ois on ois.id = oa.state
/*_logs*/
left join _logs.opentech__accounts la on la.i = oa.i
left join orders.item_states ois on ois.id::text = la._values::hstore -> 'state' 

where 
	s.sold != store.item_sold('NONE')
--	and oa.state = 23 --которые купили товары или услуги в статусе списания "Продано"
	and s.date_sold between _.frm and _.til
	and s.price > 0
	and la._values::hstore -> 'state' is not null 
--	and oa.id in ( 4404831 ) -- зак 6103702, 5985528 ,5829470, 
--	and oa.reference_id in (10678, 140345) -- позиц
--	and oa.i in (126520825) --"i позиции" 
	
--group by oa.id, oa.reference_id, oa.i, la."_tm", ois.id, ois.name
--order by la."_tm"
)
, under_the_order2 as (
select --*  
	uo."ID заказа"
	,uo."ID позиции"
	,uo."i позиции"
	,case when array_to_string(array_agg(uo.state order by uo.state),'; ') ilike '%под заказ%' then '0' -- 'Под заказ'
	when array_to_string(array_agg(uo.state order by uo.state),'; ') ilike '%е оформлен%а другом склад%тгружаетс%' then '6' -- 'На другом складе'
	when array_to_string(array_agg(uo.state order by uo.state),'; ') ilike '%е оформлен%а складе выдач%' then '5' -- 'На складе выдачи'
	else array_to_string(array_agg(uo.state order by uo.state),'; ') end "Под заказ-0;склады выдачи-5;другой-6"
from under_the_order uo 
group by uo."ID заказа", uo."ID позиции", uo."i позиции" 
order by uo."ID заказа", uo."ID позиции", uo."i позиции" 
)
*/

, all_end as (
SELECT
    customer.id::text "ID клиента",
/*
    item.manager "ФИО оформившего заказ",
*/
    ai.siteuserid "e2e4 ID", --Учетная запись сайта, с которой был оформлен заказ
    item.manager "ID оформившего заказ",
    
/*
    COALESCE(customer.name, customer.phone) "Наименование клиента",
    CASE WHEN customer.inn IS NULL THEN 'ФЛ' ELSE 'ЮЛ' END "Тип клиента",
    CASE
        WHEN budget.id_customer IS NOT NULL THEN 'бюджетник'
        WHEN secondhand.id_customer IS NOT NULL THEN 'перекуп'
        WHEN miner.id_customer IS NOT NULL THEN 'майнер'
        WHEN transport.id_customer IS NOT NULL THEN 'ТК'
        WHEN customer.asc = 1 THEN 'АСЦ'
        WHEN customer.concurent = 1 THEN 'конкурент'
    END "Классификация",
    customer.inn "ИНН",
*/
    
--    NULLIF(customer.respite_period, 0) "Отсрочка, дни",
/*
    CASE
        WHEN o.order_type = orders.type('ORGANIZATION') AND o.is_full_paid AND o.is_full_sold THEN
            NULLIF(GREATEST(0, o.pay_date - o.date_sold), 0)
    END AS "Отсрочка факт, дни",
*/
/*
    ok.id_okved "ОКВЭД",
    okved_section.name "Отрасль",
    okved.name "Вид деятельности",
    customer_city.name "Город клиента",
    1 + EXTRACT(DAY FROM _.til - customer.registrationdate)::int "Срок жизни клиента",
*/
    
/*
    manager.name "Ответственный менеджер", --заменить на логи потом может быть но пока оставть id 
*/
    manager.id "Ответственный менеджер", --заменить на логи потом может быть но пока оставть id 
    
/*
    cml.unbind_date "Дата отвязки менеджера",
    profitability.value "Маржинальность клиента",
    profitability.sum "Прибыль c клиента",
*/
    
    COALESCE(item.id_order_item, ai.account_id) "ID заказа",
    
/*
    CASE 
        WHEN ai.siteuserid IS NULL THEN 'Менеджер' 
        WHEN item.order_source ILIKE 'SHOP' THEN 'Новый сайт' ELSE 'Старый сайт'
    END AS "Кто оформил заказ", --Исправить на "ID площадки" 
*/
    ai.id_platform "ID площадки", 
    
    ai.tm_placed::date "Дата оформления заказа",
    to_char(ai.tm_placed, 'HH24:MI') "Время оформления заказа",
/*
    to_char(ai.tm_placed, 'TMMonth') "Месяц оформления заказа",
    to_char(ai.tm_placed, 'YYYY') "Год оформления заказа",
*/
/*
    ai.tm_closing::date "Дата закрытия заказа",
    to_char(ai.tm_closing, 'HH24:MI') "Время закрытия заказа",
*/
/*
    to_char(ai.tm_closing, 'TMMonth') "Месяц закрытия заказа",
    to_char(ai.tm_closing, 'YYYY') "Год закрытия заказа",
*/
    item.date_sold::date "Дата отгрузки товара",
    to_char(item.date_sold, 'HH24:MI') "Время отгрузки",
/*
    to_char(item.date_sold, 'TMMonth') "Месяц отгрузки",
    to_char(item.date_sold, 'YYYY') "Год отгрузки",
*/
--    CASE WHEN o.is_full_paid THEN o.pay_date END "Дата полной оплаты заказа",
--    CASE WHEN o.is_full_sold THEN o.date_sold END "Дата полной отгрузки заказа",
    item.TYPE AS "Тип позиции",
    item.id "Артикул",

    /*Товары (справочник)*/
    item.name "Наименование позиции",
    item.brand "Производитель",
    item.category "Категория",
    item.category2 "Категория полная",
    item.supplier "Поставщик",

    item.cost "Расчетная себестоимость",
    item.sprice "Себестоимость чистая",
    item.price "Продажная цена",
    sold_state.name AS "Статус списания",
/*
    CASE WHEN item.price <> 0 THEN round(100 * (item.price - item.cost) / item.price) END "Маржа",
    CASE WHEN item.cost <> 0 THEN round(100 * (item.price - item.cost) / item.cost) END "Наценка",
*/
/*
    storage.name "Склад отгрузки", --Переделать на ID склада
*/
    storage.id "ID склада отгрузки", --Переделать на ID склада
/*
    city.name "Город отгрузки",
*/
    item.realization_date::date "Дата реализации",
/*    
    to_char(item.realization_date, 'HH24:MI') "Время реализации",
    to_char(item.realization_date, 'TMMonth') "Месяц реализации",
    to_char(item.realization_date, 'YYYY') "Год реализации",
*/
/*
    storage.fulladdress "Адрес склада",
    CASE item.order_type WHEN orders.type('INDIVIDUAL') THEN 'Нал' ELSE 'Безнал' END "Тип оплаты",
*/
    CASE l.TYPE
        WHEN logistics.delivery_type('LOCAL') THEN 'по городу'
        WHEN logistics.delivery_type('TRANSPORT') THEN 'ТК'
        ELSE 'самовывовоз'
    END "Способ доставки",
    l.freighter "Транспортная компания",
    l.region "Регион доставки",
    l.city "Город доставки",
    l.address "Адрес доставки",
    
    item.nds "Сумма НДС",
/*
    supplier.name "Фирма-поставщик", --Переделать на ID
    shipper.name "Фирма-грузоотправитель", --Переделать на ID
*/
    ai.supplier_cid "Фирма-поставщик", --Переделать на ID
    ai.shipperid "Фирма-грузоотправитель", --Переделать на ID

--    o.mark_tender "Тендер",
--    --автор метки Т добавть
--    o.m_autor "Автор метки Т",
--
--    o.mark_request "Запрос цен",
--    --Дата запроса цен добавть
--	o.mark_request_date "Дата запроса цен",
--	
--    o.mark_kp "Коммерческое предложение",
--    --Активные продажи метка А добавть
--	--Проект метка П добавть
--    o.m_A "Активные продажи",
--    o.m_P "Проект"
    
--	,max(case when la._values::hstore -> 'state' = '5' then la._tm end) over (partition by COALESCE(item.id_order_item, ai.account_id), item.id) --01.09.2018 22 минуты
--	,lds."ID заказа"
--	,lds."Артикул (id товара)"
    
--    ,lds."Дата готовности к выдаче товара"
--    ,lds."Время готовности к выдаче товара" -- 'HH24:MI' "09:14"
/*
    ,lds."День готовности к выдаче товара" 
    ,lds."Месяц готовности к выдаче товара" -- 'TMMonth' "Января"
    ,lds."Год готовности к выдаче товара"
*/
    item."сборка/комплект",
    item."Кол-во сборок"
/*
    ,uo2."Под заказ-0;склады выдачи-5;другой-6"
*/

FROM params _
CROSS JOIN items item
JOIN store.shipping_states sold_state ON sold_state.id = item.state
JOIN opentech.account_item ai ON ai.account_id = item.id_order
--LEFT JOIN orders o ON o.id_order = item.id_order
/* клиент */
JOIN opentech.customers customer ON customer.id = item.id_customer
/*
LEFT JOIN opentech.customers supplier ON supplier.id = ai.supplier_cid
LEFT JOIN opentech.customers shipper ON shipper.id = ai.shipperid
LEFT JOIN customer_managers_log cml ON cml.id = customer.id
LEFT JOIN public.cities customer_city ON customer_city.id = customer.cityid
*/
LEFT JOIN opentech.users manager ON manager.id = customer.manager

/*
LEFT JOIN customers.customer_okveds ok ON ok.id_customer = customer.id
LEFT JOIN customers.okveds okved ON okved.id = ok.id_okved
LEFT JOIN customers.okved_sections okved_section ON okved_section.id = okved.id_okved_section
LEFT JOIN profitability ON profitability.id_customer = customer.id
LEFT JOIN customers.budgetaries budget ON budget.id_customer = customer.id
LEFT JOIN customers.secondhand_dealers secondhand ON secondhand.id_customer = customer.id
LEFT JOIN customers.miners miner ON miner.id_customer = customer.id
LEFT JOIN customers.transports transport ON transport.id_customer = customer.id
*/
/* место продажи */
JOIN opentech.storage storage ON storage.id = item.id_storage
JOIN public.cities city ON city.id = storage.city_id
/* информация о доставке */
LEFT JOIN logistics l ON l.id_order = item.id_order
/* ЛОГИ (макс дата при - На складе выдачи)*/
--left join _logs.opentech__accounts la on la.i = item.i_log -- если max(case) over()...
--left join logs_date_state lds on lds."ID заказа" = all_end."ID заказа" -- если сте logs_date_state
--    and lds."Артикул (id товара)" = all_end."Артикул (id товара)"     
--left join logs_date_state lds on lds."ID заказа" = COALESCE(item.id_order_item, ai.account_id) 
--    and lds."Артикул (id товара)" = item.id -- если join по основному селекту !быстрее всего так
/*
left join under_the_order2 uo2 on uo2."ID заказа" = COALESCE(item.id_order_item, ai.account_id)
	and uo2."ID позиции" = item.id 
	and uo2."i позиции" = item.i
*/
)

--/*where --с подзапросом 16 минут вместо case 22х мин
--    logs_date_state."ID заказа"  in (select all_end."ID заказа" from all_end)
--    and logs_date_state."Артикул (id товара)"  in (select all_end."Артикул (id товара)" from all_end)
--left join logs_date_state on 
--    logs_date_state."ID заказа" = all_end."ID заказа"
--    and logs_date_state."Артикул (id товара)" = all_end."Артикул (id товара)"*/

select 
--     distinct
    * from all_end

UNION ALL

select  

--    NULL, NULL,
--    'Возвраты' AS "Наименование клиента",
--    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
--    NULL, NULL, NULL, NULL, NULL, NULL, 
--    _.til::date AS "Дата отгрузки товара", 
--    NULL, NULL, NULL, NULL, NULL,
--    'Возвраты' AS "Тип позиции",
--    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
--    SUM(p.sum) AS "Продажная цена",
--    'Возвраты' AS "Статус списания",
--    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
--    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
--FROM params _
--CROSS JOIN opentech.payments p
--WHERE p.TYPE = payments.type('WARRANTY') AND p.date_po BETWEEN _.frm AND _.til 
--group by 28 

'Возвраты' "ID клиента", --"ID клиента", 
null, --"e2e4 ID", 
null, --"ID оформившего заказ", 
null, --"Ответственный менеджер", 
null, --"ID заказа", 
null, --"ID площадки", 
null, --"Дата оформления заказа", 
null, --"Время оформления заказа", 

case when EXTRACT(year FROM _.til::date ) = EXTRACT(year FROM now()::date)
	and EXTRACT(month FROM _.til::date ) = EXTRACT(month FROM now()::date)
	then now()::date-1
	else _.til::date
	end "Дата отгрузки товара", --"Дата отгрузки товара", 
null, --"Время отгрузки", 
--null, --"Дата полной оплаты заказа", 
--null, --"Дата полной отгрузки заказа", 
'Возвраты' "Тип позиции", --"Тип позиции", 
null, --"Артикул", 
null, --"Наименование позиции", 
null, --"Производитель", 
null, --"Категория", 
null, --"Категория полная", 
null, --"Поставщик", 
null, --"Расчетная себестоимость", 
null, --"Себестоимость чистая", 
SUM(p.sum) "Продажная цена", --"Продажная цена", 
'Возвраты' "Статус списания", --"Статус списания", 
null, --"ID склада отгрузки", 
null, --"Дата реализации", 
null, --"Способ доставки", 
null, --"Транспортная компания", 
null, --"Регион доставки", 
null, --"Город доставки", 
null, --"Адрес доставки", 
null, --"Сумма НДС", 
null, --"Фирма-поставщик", 
null, --"Фирма-грузоотправитель", 
--null, --"Тендер", 
--null, --"Автор метки Т", 
--null, --"Запрос цен", 
--null, --"Дата запроса цен", 
--null, --"Коммерческое предложение", 
--null, --"Активные продажи", 
--null, --"Проект", 
--null, --"Дата готовности к выдаче товара", 
--null, --"Время готовности к выдаче товара", 
null, --"сборка/комплект", 
null --"Кол-во сборок"

FROM params _
CROSS JOIN opentech.payments p
WHERE p.TYPE = payments.type('WARRANTY') AND p.date_po BETWEEN _.frm AND _.til 
group by _.til::date







/* старый вариант
with params as (
    select
--         ('01.11.2021' || ' 00:00:00')::timestamp frm,
--         ('01.12.2021' || ' 23:59:59')::timestamp til
   ( 'DATE_START_replce' || ' 00:00:00')::timestamp    frm 
   ,( 'DATE_END_replce' || ' 23:59:59')::timestamp    til 
)
-- select * from params

, wares as  (
    select
        -- a.i AS i_log,
        a.i,
        a.id AS id_order,
        null::INT AS id_order_item,
        a.id_customer AS id_customer,
        a.firm AS order_type,
        a.storage_id AS id_storage,
        a.user_login AS order_source,
        'Товар' AS TYPE,
        rp.id AS id,
        rp.name AS name,
        g.name AS category,
        b.name AS brand,
        s.price AS price,
        round(s.cost) AS cost,
        round(s.sprice) AS sprice,
        s.date_sold AS date_sold,
        s.sold AS state, 
        COALESCE(r.date, checks.created_at) AS realization_date,
        ROUND(s.price * (COALESCE(rnds.value, cnds.value, 20) / (100 + COALESCE(rnds.value, cnds.value, 20))), 2) AS nds,
        c.name AS supplier,
        u.id AS manager
        --Состояние сборки: (oc.collect) 0 - сборка не заказана, 1 - собрать, 2 - собрана, 3 - разобрать, 4 - разобрана, 5 - продана
        ,case when (oc.collect = 0) or (oc.collect is null) then null else 'c' end "сборка/комплект" 
        ,case when (oc.collect = 0) or (oc.collect is null) then null else oc.collectcount::text end "Кол-во сборок" --oc.collectcount - Количество собранных компьютеров в составе заказа
    FROM params _
    CROSS JOIN opentech.store s
    JOIN opentech.accounts a ON a.store_id = s.id
    LEFT join opentech.order_client oc ON a.id = oc.account_id
    LEFT JOIN opentech.users u ON lower(u.login) = lower(a.user_login)
    LEFT JOIN orders.realization_items_order_items_bindings bi ON bi.id_order_item = a.i
    LEFT JOIN orders.realization_items i ON i.id = bi.id_realization_item
    LEFT JOIN orders.realizations r ON r.id = i.id_realization
    LEFT JOIN payments.order_items_locked_by_check_items il ON il.id_order_item = a.i
    LEFT JOIN payments.check_items ci ON ci.id = il.id_check_item_fullpaid
    LEFT JOIN payments.checks checks ON checks.id = ci.id_check
    LEFT JOIN public.nds rnds ON rnds.id = i.id_nds
    LEFT JOIN public.nds cnds ON cnds.code = ci.vat
    JOIN opentech.reference_price rp ON rp.id = s.reference_id
    LEFT JOIN opentech.section_new sn ON sn.id = rp.section_sootv
    LEFT JOIN catalog.plain_section_list g ON g.id = sn.goodid
    LEFT JOIN catalog.brands b ON b.id = rp.brand_id
    LEFT JOIN opentech.supplyitems si ON si.id = s.supplyitemid
    LEFT JOIN opentech.supplies su ON su.id = si.supplyid
    LEFT JOIN customers.suppliers sup ON sup.id_customer = su.supplierid
    LEFT JOIN opentech.customers c ON c.id = COALESCE(sup.id_customer_alpha, sup.id_customer)
    WHERE a.state != orders.item_state('BUNDLED') -- WHEN 'BUNDLED' THEN RETURN 27;                  --Комплект
        AND s.sold != store.item_sold('NONE')
        AND s.date_sold BETWEEN _.frm AND _.til
        AND s.price > 0
)

, bundles AS  (
    SELECT
        -- a.i AS i_log,
        a2.i,
        a2.id AS id_order,          /* id заказа комплекта */
        a1.id AS id_order_item,     /* id заказа комплектующей */
        a2.id_customer AS id_customer,
        a2.firm AS order_type,
        a2.storage_id AS id_storage,
        a2.user_login AS order_source,
        'Комплектующая' AS TYPE,
        rp.id AS id,
        rp.name AS name,
        g.name AS category,
        b.name AS brand,
        s1.price AS price,
        round(s1.cost) AS cost,
        round(s1.sprice) AS sprice,
        s2.date_sold AS date_sold,
        s1.sold AS state, 
        COALESCE(r.date, checks.created_at) AS realization_date,
        ROUND(s1.price * (COALESCE(rnds.value, cnds.value, 20) / (100 + COALESCE(rnds.value, cnds.value, 20))), 2) AS nds,
        c.name AS supplier,
        u.id AS manager
        ,'к' "сборка/комплект" 
        ,null
    FROM params _
    CROSS JOIN opentech.store s1                            /* Складская позиция комплектующей */
    JOIN opentech.accounts a1 ON a1.store_id = s1.id        /* Заказ комплектующей */
    JOIN store.bundled_items bu ON bu.id_store_item = s1.id
    JOIN opentech.store s2 ON s2.id = bu.id_bundle          /* Складская позиция комплекта */
    JOIN opentech.accounts a2 ON a2.store_id = s2.id        /* Заказ комплекта */
    /* НДС комплектующей */
    LEFT JOIN orders.realization_items_order_items_bindings bi1 ON bi1.id_order_item = a1.i
    LEFT JOIN orders.realization_items i1 ON i1.id = bi1.id_realization_item
    LEFT JOIN payments.order_items_locked_by_check_items il1 ON il1.id_order_item = a1.i
    LEFT JOIN payments.check_items ci1 ON ci1.id = il1.id_check_item_fullpaid
    LEFT JOIN public.nds rnds ON rnds.id = i1.id_nds
    LEFT JOIN public.nds cnds ON cnds.code = ci1.vat
    /* Комплектующая */
    JOIN opentech.reference_price rp ON rp.id = s1.reference_id
    LEFT JOIN opentech.section_new sn ON sn.id = rp.section_sootv
    LEFT JOIN catalog.plain_section_list g ON g.id = sn.goodid
    LEFT JOIN catalog.brands b ON b.id = rp.brand_id
    /* Комплект */    
    LEFT JOIN opentech.users u ON lower(u.login) = lower(a2.user_login)
    LEFT JOIN orders.realization_items_order_items_bindings bi ON bi.id_order_item = a2.i
    LEFT JOIN orders.realization_items i ON i.id = bi.id_realization_item
    LEFT JOIN orders.realizations r ON r.id = i.id_realization
    LEFT JOIN payments.order_items_locked_by_check_items il ON il.id_order_item = a2.i
    LEFT JOIN payments.check_items ci ON ci.id = il.id_check_item_fullpaid
    LEFT JOIN payments.checks checks ON checks.id = ci.id_check
    LEFT JOIN opentech.supplyitems si ON si.id = s2.supplyitemid
    LEFT JOIN opentech.supplies su ON su.id = si.supplyid
    LEFT JOIN customers.suppliers sup ON sup.id_customer = su.supplierid
    LEFT JOIN opentech.customers c ON c.id = COALESCE(sup.id_customer_alpha, sup.id_customer)
    WHERE a1.state = orders.item_state('BUNDLED')
        AND s2.sold != store.item_sold('NONE')
        AND s2.date_sold BETWEEN _.frm AND _.til
        AND s2.price > 0
)

, services AS  (
    SELECT
        -- a.i AS i_log,
        a.i,
        a.id AS id_order,
        null::INT AS id_order_item,
        a.id_customer AS id_customer,
        a.firm AS order_type,
        a.storage_id AS id_storage,
        a.user_login AS order_source,
        'Услуга' AS TYPE,
        t.id AS id,
        t.name AS name,
        c.name AS category,
        NULL::TEXT AS brand,
        a.price AS price,
        NULL::int AS cost,
        NULL::int AS sprice,
        os.done AS date_sold,
        store.item_sold('SHIPPED') AS state,
        COALESCE(r.date, checks.created_at) AS realization_date,
        ROUND(a.price * (COALESCE(rnds.value, cnds.value, 20) / (100 + COALESCE(rnds.value, cnds.value, 20))), 2) AS nds,
        NULL::TEXT AS supplier,
        u.id AS manager
        ,null
        ,null
    FROM params _
    CROSS JOIN opentech.orderservices os
    JOIN opentech.accounts a ON a.i = os.orderitemid
    LEFT JOIN opentech.users u ON lower(u.login) = lower(a.user_login)
    LEFT JOIN orders.realization_items_order_items_bindings bi ON bi.id_order_item = a.i
    LEFT JOIN orders.realization_items i ON i.id = bi.id_realization_item
    LEFT JOIN orders.realizations r ON r.id = i.id_realization
    LEFT JOIN payments.order_items_locked_by_check_items il ON il.id_order_item = a.i
    LEFT JOIN payments.check_items ci ON ci.id = il.id_check_item_fullpaid
    LEFT JOIN payments.checks checks ON checks.id = ci.id_check
    LEFT JOIN public.nds rnds ON rnds.id = i.id_nds
    LEFT JOIN public.nds cnds ON cnds.code = ci.vat
    JOIN service.types t ON t.id = os.type
    JOIN service.categories c ON c.id = t.id_category
    WHERE os.state = service.state('SOLD')
        AND os.done BETWEEN _.frm AND _.til
        AND a.price > 0
)

, deliveries AS  (
    SELECT
        -- a.i AS i_log,
        a.i,
        a.id AS id_order,
        null::INT AS id_order_item,
        a.id_customer AS id_customer,
        a.firm AS order_type,
        a.storage_id AS id_storage,
        a.user_login AS order_source,
        'Доставка' AS TYPE,
        d.id AS id,
        a.correct_name AS name,
        t.name AS category,
        NULL::text AS brand,
        a.price AS price,
        NULL::int AS cost,
        NULL::int AS sprice,
        d.date AS date_sold,
        store.item_sold('SHIPPED') AS state,
        COALESCE(r.date, checks.created_at) AS realization_date,
        ROUND(a.price * (COALESCE(rnds.value, cnds.value, 20) / (100 + COALESCE(rnds.value, cnds.value, 20))), 2) AS nds,
        NULL::text AS supplier,
        u.id AS manager
        ,null
        ,null
    FROM params _
    CROSS JOIN logistics.deliveries d
    JOIN logistics.delivery_types t ON t.id = d.type
    JOIN logistics.delivery_items di ON di.id_delivery = d.id
    JOIN opentech.accounts a ON a.i = di.id_order_item AND a.correct_name ILIKE 'Доставка%'
    LEFT JOIN opentech.users u ON lower(u.login) = lower(a.user_login)
    LEFT JOIN orders.realization_items_order_items_bindings bi ON bi.id_order_item = a.i
    LEFT JOIN orders.realization_items i ON i.id = bi.id_realization_item
    LEFT JOIN orders.realizations r ON r.id = i.id_realization
    LEFT JOIN payments.order_items_locked_by_check_items il ON il.id_order_item = a.i
    LEFT JOIN payments.check_items ci ON ci.id = il.id_check_item_fullpaid
    LEFT JOIN payments.checks checks ON checks.id = ci.id_check
    LEFT JOIN public.nds rnds ON rnds.id = i.id_nds
    LEFT JOIN public.nds cnds ON cnds.code = ci.vat
    WHERE d.state = logistics.delivery_state('CLOSED')
        AND d.date BETWEEN _.frm AND _.til
        AND a.price > 0
)

, giftcards AS  (
    SELECT
        -- a.i AS i_log,
        a.i,
        a.id AS id_order,
        null::INT AS id_order_item,
        a.id_customer AS id_customer,
        a.firm AS order_type,
        a.storage_id AS id_storage,
        a.user_login AS order_source,
        'Подарочная карта' AS TYPE,
        1 AS id,
        a.correct_name AS name,
        'Подарочная карта' AS category,
        NULL::text AS brand,
        a.price AS price,
        NULL::int AS cost,
        NULL::int AS sprice,
        g.sold_on AS date_sold,
        store.item_sold('SHIPPED') AS state,
        COALESCE(r.date, checks.created_at) AS realization_date,
        ROUND(a.price * (COALESCE(rnds.value, cnds.value, 20) / (100 + COALESCE(rnds.value, cnds.value, 20))), 2) AS nds,
        NULL::text AS supplier,
        u.id AS manager
        ,null
        ,null
    FROM params _
    CROSS JOIN payments.giftcards g
    JOIN opentech.accounts a ON a.i = g.id_order_item
    LEFT JOIN opentech.users u ON lower(u.login) = lower(a.user_login)
    LEFT JOIN orders.realization_items_order_items_bindings bi ON bi.id_order_item = a.i
    LEFT JOIN orders.realization_items i ON i.id = bi.id_realization_item
    LEFT JOIN orders.realizations r ON r.id = i.id_realization
    LEFT JOIN payments.order_items_locked_by_check_items il ON il.id_order_item = a.i
    LEFT JOIN payments.check_items ci ON ci.id = il.id_check_item_fullpaid
    LEFT JOIN payments.checks checks ON checks.id = ci.id_check
    LEFT JOIN public.nds rnds ON rnds.id = i.id_nds
    LEFT JOIN public.nds cnds ON cnds.code = ci.vat
    WHERE g.status = ANY(payments.giftcard_status(ARRAY['ISSUED', 'EXPENDED']))
        AND g.sold_on BETWEEN _.frm AND _.til
        AND a.price > 0
)

, items AS  (
    SELECT * FROM wares
    UNION ALL SELECT * FROM bundles
    UNION ALL SELECT * FROM services
    UNION ALL SELECT * FROM deliveries
    UNION ALL SELECT * FROM giftcards
)

, logistics AS  (
    SELECT DISTINCT ON (items.id_order)
        items.id_order,
        d.type,
        c.name AS freighter,
        ga.region,
        ga.city,
        ga.address
    FROM items
    JOIN logistics.delivery_items di ON di.id_order_item = items.i
    JOIN logistics.deliveries d ON d.id = di.id_delivery
    LEFT JOIN opentech.customers c ON c.id = d.id_freighter
    LEFT JOIN geo.addresses ga ON ga.id = d.id_address
    WHERE d.state = logistics.delivery_state('CLOSED')
    ORDER BY items.id_order, d.date DESC
)
/*
, customer_managers_log AS  (
    /* Дата последней отвязки ответственного менеджера */
    SELECT c.id, max(l._tm)::date unbind_date
    FROM opentech.customers c
    JOIN _logs.opentech__customers l ON l.id = c.id
        AND l."_op_type" = 2    /* это запись об изменении карточки */
        AND exist(l._values, 'manager') /* изменялся ответственный менеджер */
        AND NOT defined(l._values, 'manager') /* ответственный менеджер был снят */
    WHERE c.manager IS NULL
    GROUP BY c.id
)
*/
--, orders AS  (
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
--        CASE WHEN m_P.id_order IS NOT NULL THEN 'П' END AS m_P
--    FROM (
--        SELECT
--            i.id_order,
--            MIN(i.order_type) AS order_type,
--            MAX(i.date_sold)::date AS date_sold,
--            SUM(i.price) AS price,
--            ARRAY_AGG(DISTINCT i.state) = store.item_sold(ARRAY['SHIPPED']) AS is_full_sold
--        FROM items i
--        GROUP BY i.id_order
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
--)
/*
, profitability AS  (
    /* расчет маржинальности клиентов */
    SELECT
        id_customer,
        CASE WHEN sum(cost) <> 0 THEN round(100 * (sum(price) - sum(cost))/ sum(cost)) END AS value,
        sum(price) - sum(cost) AS sum
    FROM items
    WHERE NOT customers.is_special_customer(id_customer)
    GROUP BY id_customer
)
*/
--, logs_date_state as ( 
--	/*максимум по дате la._tm (тоесть последнене изменение) для заказа товара если хстор 5 (На складе выдачи)
--	 * отимизировать может дату езе огр... или попр сортир  по уб и взять первую (деск и димит 1)
--	 * */
--    select 
--        oai.account_id "ID заказа"
--        ,oa.reference_id "Артикул (id товара)"
--        ,max(la._tm)::timestamp             "Дата готовности к выдаче товара"
--        ,to_char(max(la._tm), 'HH24:MI')     "Время готовности к выдаче товара" -- 'HH24:MI' "09:14"
--        ,to_char(max(la._tm), 'DD')         "День готовности к выдаче товара" 
--        ,to_char(max(la._tm), 'MM')         "Месяц готовности к выдаче товара" -- 'TMMonth' "Января"
--        ,to_char(max(la._tm), 'YYYY')         "Год готовности к выдаче товара"
----     ,ois.name "Состояние name"
----     ,oa.store_id "Складская позиция, выбранная для продажи"
----     ,oa.state "Состояние позиции заказа"
----     ,ois.code "Состояние code"
----     ,max(in_account_date) "Время добавления позиции в заказ"
----     ,max(acc_date) "Дата создания/оформления заказа"
----     ,max(fn_date) "Дата реализации"
--    from opentech.accounts oa 
--    left join opentech.account_item oai on oai.account_id = oa.id
--    left join orders.item_states ois on ois.id = oa.state
--    left join _logs.opentech__accounts la on la.i = oa.i
--    where 
--        la._values::hstore -> 'state' = '5' --5	RESERVED_ON_STORAGE	На складе выдачи
----        and account_id in( 3752476 ) --"ID заказа"
----     and account_id not in( 2290882 ) 652503
----     and reference_id in (652503)
----     and oa.store_id is null --не нужен !!
----     and oa.state = 5 --тоже не нужен! (не относится к истории)
--    group by 1,2--,3,4,5,6--,7,8,9
--    order by 1,2--,3,4,5,6--,7,8,9
--)

/*
, under_the_order as (
select 
    oa.id "ID заказа"
    ,oa.reference_id "ID позиции"
    ,oa.i "i позиции"
    ,la."_tm"
	,case when ois.id in (9,10,11) then 'под заказ'
	else la."_tm" || ' ' || ois.name end "state" 
FROM params _
CROSS JOIN opentech.store s
JOIN opentech.accounts oa ON oa.store_id = s.id
join opentech.account_item ai on ai.account_id = oa.id 
--join orders.item_states ois on ois.id = oa.state
/*_logs*/
left join _logs.opentech__accounts la on la.i = oa.i
left join orders.item_states ois on ois.id::text = la._values::hstore -> 'state' 

where 
	s.sold != store.item_sold('NONE')
--	and oa.state = 23 --которые купили товары или услуги в статусе списания "Продано"
	and s.date_sold between _.frm and _.til
	and s.price > 0
	and la._values::hstore -> 'state' is not null 
--	and oa.id in ( 4404831 ) -- зак 6103702, 5985528 ,5829470, 
--	and oa.reference_id in (10678, 140345) -- позиц
--	and oa.i in (126520825) --"i позиции" 
	
--group by oa.id, oa.reference_id, oa.i, la."_tm", ois.id, ois.name
--order by la."_tm"
)
, under_the_order2 as (
select --*  
	uo."ID заказа"
	,uo."ID позиции"
	,uo."i позиции"
	,case when array_to_string(array_agg(uo.state order by uo.state),'; ') ilike '%под заказ%' then '0' -- 'Под заказ'
	when array_to_string(array_agg(uo.state order by uo.state),'; ') ilike '%е оформлен%а другом склад%тгружаетс%' then '6' -- 'На другом складе'
	when array_to_string(array_agg(uo.state order by uo.state),'; ') ilike '%е оформлен%а складе выдач%' then '5' -- 'На складе выдачи'
	else array_to_string(array_agg(uo.state order by uo.state),'; ') end "Под заказ-0;склады выдачи-5;другой-6"
from under_the_order uo 
group by uo."ID заказа", uo."ID позиции", uo."i позиции" 
order by uo."ID заказа", uo."ID позиции", uo."i позиции" 
)
*/

, all_end as (
SELECT
    customer.id::text "ID клиента",
/*
    item.manager "ФИО оформившего заказ",
*/
    ai.siteuserid "e2e4 ID", --Учетная запись сайта, с которой был оформлен заказ
    item.manager "ID оформившего заказ",
    
/*
    COALESCE(customer.name, customer.phone) "Наименование клиента",
    CASE WHEN customer.inn IS NULL THEN 'ФЛ' ELSE 'ЮЛ' END "Тип клиента",
    CASE
        WHEN budget.id_customer IS NOT NULL THEN 'бюджетник'
        WHEN secondhand.id_customer IS NOT NULL THEN 'перекуп'
        WHEN miner.id_customer IS NOT NULL THEN 'майнер'
        WHEN transport.id_customer IS NOT NULL THEN 'ТК'
        WHEN customer.asc = 1 THEN 'АСЦ'
        WHEN customer.concurent = 1 THEN 'конкурент'
    END "Классификация",
    customer.inn "ИНН",
*/
    
--    NULLIF(customer.respite_period, 0) "Отсрочка, дни",
/*
    CASE
        WHEN o.order_type = orders.type('ORGANIZATION') AND o.is_full_paid AND o.is_full_sold THEN
            NULLIF(GREATEST(0, o.pay_date - o.date_sold), 0)
    END AS "Отсрочка факт, дни",
*/
/*
    ok.id_okved "ОКВЭД",
    okved_section.name "Отрасль",
    okved.name "Вид деятельности",
    customer_city.name "Город клиента",
    1 + EXTRACT(DAY FROM _.til - customer.registrationdate)::int "Срок жизни клиента",
*/
    
/*
    manager.name "Ответственный менеджер", --заменить на логи потом может быть но пока оставть id 
*/
    manager.id "Ответственный менеджер", --заменить на логи потом может быть но пока оставть id 
    
/*
    cml.unbind_date "Дата отвязки менеджера",
    profitability.value "Маржинальность клиента",
    profitability.sum "Прибыль c клиента",
*/
    
    COALESCE(item.id_order_item, ai.account_id) "ID заказа",
    
/*
    CASE 
        WHEN ai.siteuserid IS NULL THEN 'Менеджер' 
        WHEN item.order_source ILIKE 'SHOP' THEN 'Новый сайт' ELSE 'Старый сайт'
    END AS "Кто оформил заказ", --Исправить на "ID площадки" 
*/
    ai.id_platform "ID площадки", 
    
    ai.tm_placed::date "Дата оформления заказа",
    to_char(ai.tm_placed, 'HH24:MI') "Время оформления заказа",
/*
    to_char(ai.tm_placed, 'TMMonth') "Месяц оформления заказа",
    to_char(ai.tm_placed, 'YYYY') "Год оформления заказа",
*/
/*
    ai.tm_closing::date "Дата закрытия заказа",
    to_char(ai.tm_closing, 'HH24:MI') "Время закрытия заказа",
*/
/*
    to_char(ai.tm_closing, 'TMMonth') "Месяц закрытия заказа",
    to_char(ai.tm_closing, 'YYYY') "Год закрытия заказа",
*/
    item.date_sold::date "Дата отгрузки товара",
    to_char(item.date_sold, 'HH24:MI') "Время отгрузки",
/*
    to_char(item.date_sold, 'TMMonth') "Месяц отгрузки",
    to_char(item.date_sold, 'YYYY') "Год отгрузки",
*/
--    CASE WHEN o.is_full_paid THEN o.pay_date END "Дата полной оплаты заказа",
--    CASE WHEN o.is_full_sold THEN o.date_sold END "Дата полной отгрузки заказа",
    item.TYPE AS "Тип позиции",
    item.id "Артикул",

    /*Товары (справочник)*/
    item.name "Наименование позиции",
    item.brand "Производитель",
    item.category "Категория",
    item.supplier "Поставщик",

    item.cost "Расчетная себестоимость",
    item.sprice "Себестоимость чистая",
    item.price "Продажная цена",
    sold_state.name AS "Статус списания",
/*
    CASE WHEN item.price <> 0 THEN round(100 * (item.price - item.cost) / item.price) END "Маржа",
    CASE WHEN item.cost <> 0 THEN round(100 * (item.price - item.cost) / item.cost) END "Наценка",
*/
/*
    storage.name "Склад отгрузки", --Переделать на ID склада
*/
    storage.id "ID склада отгрузки", --Переделать на ID склада
/*
    city.name "Город отгрузки",
*/
    item.realization_date::date "Дата реализации",
/*    
    to_char(item.realization_date, 'HH24:MI') "Время реализации",
    to_char(item.realization_date, 'TMMonth') "Месяц реализации",
    to_char(item.realization_date, 'YYYY') "Год реализации",
*/
/*
    storage.fulladdress "Адрес склада",
    CASE item.order_type WHEN orders.type('INDIVIDUAL') THEN 'Нал' ELSE 'Безнал' END "Тип оплаты",
*/
    CASE l.TYPE
        WHEN logistics.delivery_type('LOCAL') THEN 'по городу'
        WHEN logistics.delivery_type('TRANSPORT') THEN 'ТК'
        ELSE 'самовывовоз'
    END "Способ доставки",
    l.freighter "Транспортная компания",
    l.region "Регион доставки",
    l.city "Город доставки",
    l.address "Адрес доставки",
    
    item.nds "Сумма НДС",
/*
    supplier.name "Фирма-поставщик", --Переделать на ID
    shipper.name "Фирма-грузоотправитель", --Переделать на ID
*/
    ai.supplier_cid "Фирма-поставщик", --Переделать на ID
    ai.shipperid "Фирма-грузоотправитель", --Переделать на ID

--    o.mark_tender "Тендер",
--    --автор метки Т добавть
--    o.m_autor "Автор метки Т",
--
--    o.mark_request "Запрос цен",
--    --Дата запроса цен добавть
--	o.mark_request_date "Дата запроса цен",
--	
--    o.mark_kp "Коммерческое предложение",
--    --Активные продажи метка А добавть
--	--Проект метка П добавть
--    o.m_A "Активные продажи",
--    o.m_P "Проект"
    
--	,max(case when la._values::hstore -> 'state' = '5' then la._tm end) over (partition by COALESCE(item.id_order_item, ai.account_id), item.id) --01.09.2018 22 минуты
--	,lds."ID заказа"
--	,lds."Артикул (id товара)"
    
--    ,lds."Дата готовности к выдаче товара"
--    ,lds."Время готовности к выдаче товара" -- 'HH24:MI' "09:14"
/*
    ,lds."День готовности к выдаче товара" 
    ,lds."Месяц готовности к выдаче товара" -- 'TMMonth' "Января"
    ,lds."Год готовности к выдаче товара"
*/
    item."сборка/комплект",
    item."Кол-во сборок"
/*
    ,uo2."Под заказ-0;склады выдачи-5;другой-6"
*/

FROM params _
CROSS JOIN items item
JOIN store.shipping_states sold_state ON sold_state.id = item.state
JOIN opentech.account_item ai ON ai.account_id = item.id_order
--LEFT JOIN orders o ON o.id_order = item.id_order
/* клиент */
JOIN opentech.customers customer ON customer.id = item.id_customer
/*
LEFT JOIN opentech.customers supplier ON supplier.id = ai.supplier_cid
LEFT JOIN opentech.customers shipper ON shipper.id = ai.shipperid
LEFT JOIN customer_managers_log cml ON cml.id = customer.id
LEFT JOIN public.cities customer_city ON customer_city.id = customer.cityid
*/
LEFT JOIN opentech.users manager ON manager.id = customer.manager

/*
LEFT JOIN customers.customer_okveds ok ON ok.id_customer = customer.id
LEFT JOIN customers.okveds okved ON okved.id = ok.id_okved
LEFT JOIN customers.okved_sections okved_section ON okved_section.id = okved.id_okved_section
LEFT JOIN profitability ON profitability.id_customer = customer.id
LEFT JOIN customers.budgetaries budget ON budget.id_customer = customer.id
LEFT JOIN customers.secondhand_dealers secondhand ON secondhand.id_customer = customer.id
LEFT JOIN customers.miners miner ON miner.id_customer = customer.id
LEFT JOIN customers.transports transport ON transport.id_customer = customer.id
*/
/* место продажи */
JOIN opentech.storage storage ON storage.id = item.id_storage
JOIN public.cities city ON city.id = storage.city_id
/* информация о доставке */
LEFT JOIN logistics l ON l.id_order = item.id_order
/* ЛОГИ (макс дата при - На складе выдачи)*/
--left join _logs.opentech__accounts la on la.i = item.i_log -- если max(case) over()...
--left join logs_date_state lds on lds."ID заказа" = all_end."ID заказа" -- если сте logs_date_state
--    and lds."Артикул (id товара)" = all_end."Артикул (id товара)"     
--left join logs_date_state lds on lds."ID заказа" = COALESCE(item.id_order_item, ai.account_id) 
--    and lds."Артикул (id товара)" = item.id -- если join по основному селекту !быстрее всего так
/*
left join under_the_order2 uo2 on uo2."ID заказа" = COALESCE(item.id_order_item, ai.account_id)
	and uo2."ID позиции" = item.id 
	and uo2."i позиции" = item.i
*/
)

--/*where --с подзапросом 16 минут вместо case 22х мин
--    logs_date_state."ID заказа"  in (select all_end."ID заказа" from all_end)
--    and logs_date_state."Артикул (id товара)"  in (select all_end."Артикул (id товара)" from all_end)
--left join logs_date_state on 
--    logs_date_state."ID заказа" = all_end."ID заказа"
--    and logs_date_state."Артикул (id товара)" = all_end."Артикул (id товара)"*/

select 
--     distinct
    * from all_end

UNION ALL

select  

--    NULL, NULL,
--    'Возвраты' AS "Наименование клиента",
--    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
--    NULL, NULL, NULL, NULL, NULL, NULL, 
--    _.til::date AS "Дата отгрузки товара", 
--    NULL, NULL, NULL, NULL, NULL,
--    'Возвраты' AS "Тип позиции",
--    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
--    SUM(p.sum) AS "Продажная цена",
--    'Возвраты' AS "Статус списания",
--    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
--    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
--FROM params _
--CROSS JOIN opentech.payments p
--WHERE p.TYPE = payments.type('WARRANTY') AND p.date_po BETWEEN _.frm AND _.til 
--group by 28 

'Возвраты' "ID клиента", --"ID клиента", 
null, --"e2e4 ID", 
null, --"ID оформившего заказ", 
null, --"Ответственный менеджер", 
null, --"ID заказа", 
null, --"ID площадки", 
null, --"Дата оформления заказа", 
null, --"Время оформления заказа", 

case when EXTRACT(year FROM _.til::date ) = EXTRACT(year FROM now()::date)
	and EXTRACT(month FROM _.til::date ) = EXTRACT(month FROM now()::date)
	then now()::date-1
	else _.til::date
	end "Дата отгрузки товара", --"Дата отгрузки товара", 
null, --"Время отгрузки", 
--null, --"Дата полной оплаты заказа", 
--null, --"Дата полной отгрузки заказа", 
'Возвраты' "Тип позиции", --"Тип позиции", 
null, --"Артикул", 
null, --"Наименование позиции", 
null, --"Производитель", 
null, --"Категория", 
null, --"Поставщик", 
null, --"Расчетная себестоимость", 
null, --"Себестоимость чистая", 
SUM(p.sum) "Продажная цена", --"Продажная цена", 
'Возвраты' "Статус списания", --"Статус списания", 
null, --"ID склада отгрузки", 
null, --"Дата реализации", 
null, --"Способ доставки", 
null, --"Транспортная компания", 
null, --"Регион доставки", 
null, --"Город доставки", 
null, --"Адрес доставки", 
null, --"Сумма НДС", 
null, --"Фирма-поставщик", 
null, --"Фирма-грузоотправитель", 
--null, --"Тендер", 
--null, --"Автор метки Т", 
--null, --"Запрос цен", 
--null, --"Дата запроса цен", 
--null, --"Коммерческое предложение", 
--null, --"Активные продажи", 
--null, --"Проект", 
--null, --"Дата готовности к выдаче товара", 
--null, --"Время готовности к выдаче товара", 
null, --"сборка/комплект", 
null --"Кол-во сборок"

FROM params _
CROSS JOIN opentech.payments p
WHERE p.TYPE = payments.type('WARRANTY') AND p.date_po BETWEEN _.frm AND _.til 
group by _.til::date
*/





