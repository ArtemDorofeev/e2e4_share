--/*10.06.2021 ФОН ju (+даты по статусу На складе выдачи)  + тествозвраты*/ 2019-01-13  2021-02-27 

/* В ВОЗВРАТАХ ЧИСТО ТОВАРЫ НЕТ СЕРВИСА И ТД
 * 08.06  ФОН ju + возвраты    08.06.2021 
 * чист себес, дата с ju, + возвраты
 * 
 * */

WITH params AS MATERIALIZED (
select

    ( 'DATE_START_replce' || ' 00:00:00')::timestamp    frm 
    ,( 'DATE_END_replce' || ' 23:59:59')::timestamp    til
--     ( '01.01.2017' || ' 00:00:00')::timestamp    frm 
--     ,( '01.01.2022' || ' 23:59:59')::timestamp    til
    
)

, 
warranty_opentech as (
select distinct 
-- 751 705 029.15
-- 180464  2000-01-01 2021-12-31   751 705 029 совпадает (вместе  pa и ww если p.id и distinct)
-- 751 705 030 (округление) ексель сходится (возвраты динамические суммы меняются но сходимость ДОЛЖНА БЫТЬ!)
    ww.id_store_item "id принятой по ЗН железяки"   
    ,p.sum "Сумма возврата"                            
    ,p.id "id платежа"    

--    ,p.customer_id "p.customer_id (кто платил?)"
--    ,p.type
    ,p.date_po "Дата поступления денег (возврат)"
--    ,p.purpose "Назначение платежа"
--    ,ww.number "Номер ЗН"      
       
--    ,ww.date_pass "Дата сдачи в сервис"
--    ,ww.id_payment "ww Платёж, на основании ЗН"
--    ,p.id as "p.id Платёж, на основании ЗН"
--    ,ww.date_sold "Дата продажи товара (возвратов)"
--    ,ww.price_sold
--    ,ww.id_account_sold "ID Заказа, по которому был продан товар"
--    ,ww.id_reference_price "Артикул оборудования"
--    ,ww.fault "Описание неисправности"
    
--    ,t.name as "Тип платежа" 
    /*пришлось вырезать "," из "неустойка, штраф", а то таблица ползет*/
    --    case when position(',' in t.name) > 0 then overlay(t.name placing ' ' from position(',' in t.name)) else t.name end as "тип платежа" 
--        ,c.id "c.id"
--        ,c.name "фирма получатель"
--        ,a.id "Позиции клиентских заказов"
--        ,a.reference_id "Товарная позиция"
--        ,a.store_id "Складская позиция, выбранная для продажи"
--        ,a.id_customer "Плательщик"
--        ,c.id ",c.id"
--        ,c.name         as "фирма"
--        ,ww.
--        ,*
from params _
cross join opentech.payments p
--left join payments.types t on t.id = p.type 
left join opentech.payments_account pa on p.id = pa.payment_id 
left join warranty.warranty ww on p.id = ww.id_payment
--    left join opentech.accounts a on a.id = pa.account_id
--    left join opentech.customers c on c.id = p.firmid
where 
    p.date_po::date between _.frm and _.til
    and p.type = payments.type('WARRANTY') -- 6 
--        and a.state = orders.item_state('BUNDLED')
--order by 1 --ww.id_store_item
)

--,params2 as (
--select
--    min(wo."Дата продажи товара (возвратов)") frm  
--    ,max(wo."Дата продажи товара (возвратов)") til
--    from warranty_opentech wo
--)

, wares as materialized (
    select
        -- a.i as i_log,
        a.i,
        a.id as id_order,
        null::int as id_order_item,
        a.id_customer as id_customer,
        a.firm as order_type,
        a.storage_id as id_storage,
        a.user_login as order_source,
        'Товар' as type,
        rp.id as id,
        rp.name as name,
        g.name as category,
        b.name as brand,
        s.price as price,
        round(s.cost) as cost,
        round(s.sprice) as sprice,
        s.date_sold as date_sold,
        s.sold as state, 
        coalesce(r.date, checks.created_at) as realization_date,
        round(s.price * (coalesce(rnds.value, cnds.value, 20) / (100 + coalesce(rnds.value, cnds.value, 20))), 2) as nds,
        c.name as supplier,
        u.name as manager
        ,s.id s_id
        
    -- from params _
    from  opentech.store s
    join opentech.accounts a on a.store_id = s.id
    left join opentech.users u on lower(u.login) = lower(a.user_login)
    left join orders.realization_items_order_items_bindings bi on bi.id_order_item = a.i
    left join orders.realization_items i on i.id = bi.id_realization_item
    left join orders.realizations r on r.id = i.id_realization
    left join payments.order_items_locked_by_check_items il on il.id_order_item = a.i
    left join payments.check_items ci on ci.id = il.id_check_item_fullpaid
    left join payments.checks checks on checks.id = ci.id_check
    left join public.nds rnds on rnds.id = i.id_nds
    left join public.nds cnds on cnds.code = ci.vat
    join opentech.reference_price rp on rp.id = s.reference_id
    left join opentech.section_new sn on sn.id = rp.section_sootv
    left join catalog.plain_section_list g on g.id = sn.goodid
    left join catalog.brands b on b.id = rp.brand_id
    left join opentech.supplyitems si on si.id = s.supplyitemid
    left join opentech.supplies su on su.id = si.supplyid
    left join customers.suppliers sup on sup.id_customer = su.supplierid
    left join opentech.customers c on c.id = coalesce(sup.id_customer_alpha, sup.id_customer)
    
    right join warranty_opentech wo on wo."id принятой по ЗН железяки" = s.id
    
    where a.state != orders.item_state('BUNDLED')
        and s.sold != store.item_sold('NONE')
--        and s.date_sold between _.frm and _.til
        and s.price > 0
)
, bundles as materialized (
    select
        -- a.i as i_log,
        a2.i,
        a2.id as id_order,          /* id заказа комплекта */
        a1.id as id_order_item,     /* id заказа комплектующей */
        a2.id_customer as id_customer,
        a2.firm as order_type,
        a2.storage_id as id_storage,
        a2.user_login as order_source,
        'Комплектующая' as type,
        rp.id as id,
        rp.name as name,
        g.name as category,
        b.name as brand,
        s1.price as price,
        round(s1.cost) as cost,
        round(s1.sprice) as sprice,
        s2.date_sold as date_sold,
        s1.sold as state, 
        coalesce(r.date, checks.created_at) as realization_date,
        round(s1.price * (coalesce(rnds.value, cnds.value, 20) / (100 + coalesce(rnds.value, cnds.value, 20))), 2) as nds,
        c.name as supplier,
        u.name as manager
        ,s2.id s_id

    -- from params _
    from  opentech.store s1                            /* складская позиция комплектующей */
    join opentech.accounts a1 on a1.store_id = s1.id        /* заказ комплектующей */
    join store.bundled_items bu on bu.id_store_item = s1.id
    join opentech.store s2 on s2.id = bu.id_bundle          /* складская позиция комплекта */
    join opentech.accounts a2 on a2.store_id = s2.id        /* заказ комплекта */
    /* ндс комплектующей */
    left join orders.realization_items_order_items_bindings bi1 on bi1.id_order_item = a1.i
    left join orders.realization_items i1 on i1.id = bi1.id_realization_item
    left join payments.order_items_locked_by_check_items il1 on il1.id_order_item = a1.i
    left join payments.check_items ci1 on ci1.id = il1.id_check_item_fullpaid
    left join public.nds rnds on rnds.id = i1.id_nds
    left join public.nds cnds on cnds.code = ci1.vat
    /* комплектующая */
    join opentech.reference_price rp on rp.id = s1.reference_id
    left join opentech.section_new sn on sn.id = rp.section_sootv
    left join catalog.plain_section_list g on g.id = sn.goodid
    left join catalog.brands b on b.id = rp.brand_id
    /* комплект */    
    left join opentech.users u on lower(u.login) = lower(a2.user_login)
    left join orders.realization_items_order_items_bindings bi on bi.id_order_item = a2.i
    left join orders.realization_items i on i.id = bi.id_realization_item
    left join orders.realizations r on r.id = i.id_realization
    left join payments.order_items_locked_by_check_items il on il.id_order_item = a2.i
    left join payments.check_items ci on ci.id = il.id_check_item_fullpaid
    left join payments.checks checks on checks.id = ci.id_check
    left join opentech.supplyitems si on si.id = s2.supplyitemid
    left join opentech.supplies su on su.id = si.supplyid
    left join customers.suppliers sup on sup.id_customer = su.supplierid
    left join opentech.customers c on c.id = coalesce(sup.id_customer_alpha, sup.id_customer)
    
    right join warranty_opentech wo on wo."id принятой по ЗН железяки" = s2.id
    
    where a1.state = orders.item_state('BUNDLED')
        and s2.sold != store.item_sold('NONE')
--        and s2.date_sold between _.frm and _.til
        and s2.price > 0
)
--, services AS MATERIALIZED (
--    SELECT
--        -- a.i AS i_log,
--        a.i,
--        a.id AS id_order,
--        null::INT AS id_order_item,
--        a.id_customer AS id_customer,
--        a.firm AS order_type,
--        a.storage_id AS id_storage,
--        a.user_login AS order_source,
--        'Услуга' AS TYPE,
--        t.id AS id,
--        t.name AS name,
--        c.name AS category,
--        NULL::TEXT AS brand,
--        a.price AS price,
--        NULL::int AS cost,
--        NULL::int AS sprice,
--        os.done AS date_sold,
--        store.item_sold('SHIPPED') AS state,
--        COALESCE(r.date, checks.created_at) AS realization_date,
--        ROUND(a.price * (COALESCE(rnds.value, cnds.value, 20) / (100 + COALESCE(rnds.value, cnds.value, 20))), 2) AS nds,
--        NULL::TEXT AS supplier,
--        u.name AS manager
--    -- FROM params _
--    from  opentech.orderservices os
--    JOIN opentech.accounts a ON a.i = os.orderitemid
--    LEFT JOIN opentech.users u ON lower(u.login) = lower(a.user_login)
--    LEFT JOIN orders.realization_items_order_items_bindings bi ON bi.id_order_item = a.i
--    LEFT JOIN orders.realization_items i ON i.id = bi.id_realization_item
--    LEFT JOIN orders.realizations r ON r.id = i.id_realization
--    LEFT JOIN payments.order_items_locked_by_check_items il ON il.id_order_item = a.i
--    LEFT JOIN payments.check_items ci ON ci.id = il.id_check_item_fullpaid
--    LEFT JOIN payments.checks checks ON checks.id = ci.id_check
--    LEFT JOIN public.nds rnds ON rnds.id = i.id_nds
--    LEFT JOIN public.nds cnds ON cnds.code = ci.vat
--    JOIN service.types t ON t.id = os.type
--    JOIN service.categories c ON c.id = t.id_category
--    WHERE os.state = service.state('SOLD')
--        AND os.done BETWEEN _.frm AND _.til
--        AND a.price > 0
--), deliveries AS MATERIALIZED (
--    SELECT
--        -- a.i AS i_log,
--        a.i,
--        a.id AS id_order,
--        null::INT AS id_order_item,
--        a.id_customer AS id_customer,
--        a.firm AS order_type,
--        a.storage_id AS id_storage,
--        a.user_login AS order_source,
--        'Доставка' AS TYPE,
--        d.id AS id,
--        a.correct_name AS name,
--        t.name AS category,
--        NULL::text AS brand,
--        a.price AS price,
--        NULL::int AS cost,
--        NULL::int AS sprice,
--        d.date AS date_sold,
--        store.item_sold('SHIPPED') AS state,
--        COALESCE(r.date, checks.created_at) AS realization_date,
--        ROUND(a.price * (COALESCE(rnds.value, cnds.value, 20) / (100 + COALESCE(rnds.value, cnds.value, 20))), 2) AS nds,
--        NULL::text AS supplier,
--        u.name AS manager
--    -- FROM params _
--    from  logistics.deliveries d
--    JOIN logistics.delivery_types t ON t.id = d.type
--    JOIN logistics.delivery_items di ON di.id_delivery = d.id
--    JOIN opentech.accounts a ON a.i = di.id_order_item AND a.correct_name ILIKE 'Доставка%'
--    LEFT JOIN opentech.users u ON lower(u.login) = lower(a.user_login)
--    LEFT JOIN orders.realization_items_order_items_bindings bi ON bi.id_order_item = a.i
--    LEFT JOIN orders.realization_items i ON i.id = bi.id_realization_item
--    LEFT JOIN orders.realizations r ON r.id = i.id_realization
--    LEFT JOIN payments.order_items_locked_by_check_items il ON il.id_order_item = a.i
--    LEFT JOIN payments.check_items ci ON ci.id = il.id_check_item_fullpaid
--    LEFT JOIN payments.checks checks ON checks.id = ci.id_check
--    LEFT JOIN public.nds rnds ON rnds.id = i.id_nds
--    LEFT JOIN public.nds cnds ON cnds.code = ci.vat
--    WHERE d.state = logistics.delivery_state('CLOSED')
--        AND d.date BETWEEN _.frm AND _.til
--        AND a.price > 0
--), giftcards AS MATERIALIZED (
--    SELECT
--        -- a.i AS i_log,
--        a.i,
--        a.id AS id_order,
--        null::INT AS id_order_item,
--        a.id_customer AS id_customer,
--        a.firm AS order_type,
--        a.storage_id AS id_storage,
--        a.user_login AS order_source,
--        'Подарочная карта' AS TYPE,
--        1 AS id,
--        a.correct_name AS name,
--        'Подарочная карта' AS category,
--        NULL::text AS brand,
--        a.price AS price,
--        NULL::int AS cost,
--        NULL::int AS sprice,
--        g.sold_on AS date_sold,
--        store.item_sold('SHIPPED') AS state,
--        COALESCE(r.date, checks.created_at) AS realization_date,
--        ROUND(a.price * (COALESCE(rnds.value, cnds.value, 20) / (100 + COALESCE(rnds.value, cnds.value, 20))), 2) AS nds,
--        NULL::text AS supplier,
--        u.name AS manager
--    -- FROM params _
--    from  payments.giftcards g
--    JOIN opentech.accounts a ON a.i = g.id_order_item
--    LEFT JOIN opentech.users u ON lower(u.login) = lower(a.user_login)
--    LEFT JOIN orders.realization_items_order_items_bindings bi ON bi.id_order_item = a.i
--    LEFT JOIN orders.realization_items i ON i.id = bi.id_realization_item
--    LEFT JOIN orders.realizations r ON r.id = i.id_realization
--    LEFT JOIN payments.order_items_locked_by_check_items il ON il.id_order_item = a.i
--    LEFT JOIN payments.check_items ci ON ci.id = il.id_check_item_fullpaid
--    LEFT JOIN payments.checks checks ON checks.id = ci.id_check
--    LEFT JOIN public.nds rnds ON rnds.id = i.id_nds
--    LEFT JOIN public.nds cnds ON cnds.code = ci.vat
--    WHERE g.status = ANY(payments.giftcard_status(ARRAY['ISSUED', 'EXPENDED']))
--        AND g.sold_on BETWEEN _.frm AND _.til
--        AND a.price > 0
--)
, items AS MATERIALIZED (
    SELECT * FROM wares
--    UNION ALL SELECT * FROM bundles
--    UNION ALL SELECT * FROM services
--    UNION ALL SELECT * FROM deliveries
--    UNION ALL SELECT * FROM giftcards
)
, logistics as materialized (
    select distinct on (items.id_order)
        items.id_order,
        d.type,
        c.name as freighter,
        ga.region,
        ga.city,
        ga.address
    from items
    join logistics.delivery_items di on di.id_order_item = items.i
    join logistics.deliveries d on d.id = di.id_delivery
    left join opentech.customers c on c.id = d.id_freighter
    left join geo.addresses ga on ga.id = d.id_address
    where d.state = logistics.delivery_state('CLOSED')
    order by items.id_order, d.date desc
)
, customer_managers_log as materialized (
    /* дата последней отвязки ответственного менеджера */
    select c.id, max(l._tm)::date unbind_date
    from opentech.customers c
    join _logs.opentech__customers l on l.id = c.id
        and l."_op_type" = 2    /* это запись об изменении карточки */
        and exist(l._values, 'manager') /* изменялся ответственный менеджер */
        and not defined(l._values, 'manager') /* ответственный менеджер был снят */
    where c.manager is null
    group by c.id
)
, orders as materialized (
    select
        o.id_order,
        o.order_type,
        o.date_sold,
        o.is_full_sold,
        p.pay_date,
        o.price = p.pay_sum as is_full_paid,
        case when m_tender.id_order is not null then 'Т' end as mark_tender,
        case when m_request.id_order is not null then 'З' end as mark_request,
        case when m_kp.id_order is not null then 'КП' end as mark_kp
    from (
        select
            i.id_order,
            min(i.order_type) as order_type,
            max(i.date_sold)::date as date_sold,
            sum(i.price) as price,
            array_agg(distinct i.state) = store.item_sold(array['SHIPPED']) as is_full_sold
        from items i
        group by i.id_order
    ) o
    left join lateral (
        select
            max(p.pay_date)::date as pay_date,
            sum(
                case
                    when p.type = payments.type('CASHLESS') and p.date_out is not null then -pa.sum
                    else pa.sum
                end
            ) as pay_sum
        from opentech.payments_account pa
        join opentech.payments p on p.id = pa.payment_id
        where pa.account_id = o.id_order
    ) p on true
    left join orders.marks m_tender on m_tender.id_order = o.id_order and m_tender.id_mark = 1 /*тендер*/
    left join orders.marks m_request on m_request.id_order = o.id_order and m_request.id_mark = 5 /*запрос цен*/
    left join orders.marks m_kp on m_kp.id_order = o.id_order and m_kp.id_mark = 6 /*коммерческое предложение*/
)
----, profitability as materialized (
----    /* расчет маржинальности клиентов */
----    select
----        id_customer,
----        case when sum(cost) <> 0 then round(100 * (sum(price) - sum(cost))/ sum(cost)) end as value,
----        sum(price) - sum(cost) as sum
----    from items
----    where not customers.is_special_customer(id_customer)
----    group by id_customer
----)
, logs_date_state as ( 
    /*
     * максимум по дате la._tm (тоесть последнене изменение) для заказа товара если хстор 5 (на складе выдачи)
     */
    select 
        oai.account_id "ID заказа"
        ,oa.reference_id "Артикул (id товара)"
        ,max(la._tm)::timestamp             "Дата готовности к выдаче товара"
        ,to_char(max(la._tm), 'HH24:MI')     "Время готовности к выдаче товара" -- 'HH24:MI' "09:14"
        ,to_char(max(la._tm), 'DD')         "День готовности к выдаче товара" 
        ,to_char(max(la._tm), 'MM')         "Месяц готовности к выдаче товара" -- 'TMMonth' "Января"
        ,to_char(max(la._tm), 'YYYY')         "Год готовности к выдаче товара"
--     ,ois.name "Состояние name"
--     ,oa.store_id "Складская позиция, выбранная для продажи"
--     ,oa.state "Состояние позиции заказа"
--     ,ois.code "Состояние code"
--     ,max(in_account_date) "Время добавления позиции в заказ"
--     ,max(acc_date) "Дата создания/оформления заказа"
--     ,max(fn_date) "Дата реализации"
    from opentech.accounts oa 
    left join opentech.account_item oai on oai.account_id = oa.id
    left join orders.item_states ois on ois.id = oa.state
    left join _logs.opentech__accounts la on la.i = oa.i
    where 
        la._values::hstore -> 'state' = '5' --5    RESERVED_ON_STORAGE    На складе выдачи
--     and account_id in( 3752476 ) --"ID заказа"
--     and account_id not in( 2290882 ) 652503
--     and reference_id in (652503)
--     and oa.store_id is null --не нужен !!
--     and oa.state = 5 --тоже не нужен! (не относится к истории)
    group by 1,2--,3,4,5,6--,7,8,9
    order by 1,2--,3,4,5,6--,7,8,9
)
, all_end as (
select distinct
    
    customer.id "ID клиента",
    item.manager "ФИО оформившего заказ",
    COALESCE(customer.name, customer.phone) "Наименование клиента",
    case when customer.inn is null then 'ФЛ' else 'ЮЛ' end "Тип клиента",
    case
        when budget.id_customer is not null then 'бюджетник'
        when secondhand.id_customer is not null then 'перекуп'
        when miner.id_customer is not null then 'майнер'
        when transport.id_customer is not null then 'ТК'
        when customer.asc = 1 then 'АСЦ'
        when customer.concurent = 1 then 'конкурент'
    end "Классификация",
    customer.inn "инн",
    nullif(customer.respite_period, 0) "Отсрочка, дни",
    
    case
        when o.order_type = orders.type('ORGANIZATION') and o.is_full_paid and o.is_full_sold then
            nullif(greatest(0, o.pay_date - o.date_sold), 0)
    end as "Отсрочка факт, дни",
    
    ok.id_okved "ОКВЭД",
    okved_section.name "Отрасль",
    okved.name "Вид деятельности",
    customer_city.name "Город клиента",
    
--    /*1 + EXTRACT(DAY FROM _.til - customer.registrationdate)::int "Срок жизни клиента",*/
    NULL "Срок жизни клиента",
    
    manager.name "Ответственный менеджер",
    
    cml.unbind_date "Дата отвязки менеджера",
    
--    /*profitability.value "Маржинальность клиента",
--    profitability.sum "Прибыль c клиента",*/
    NULL "Маржинальность клиента",
    NULL "Прибыль c клиента",
    
    COALESCE(item.id_order_item, ai.account_id) "ID заказа",
    case 
        when ai.siteuserid is null then 'Менеджер' 
        when item.order_source ilike 'SHOP' then 'Новый сайт' else 'Старый сайт'
    end as "Кто оформил заказ",
    ai.tm_placed::date "Дата оформления заказа",
    to_char(ai.tm_placed, 'HH24:MI') "Время оформления заказа",
    to_char(ai.tm_placed, 'TMMonth') "Месяц оформления заказа",
    to_char(ai.tm_placed, 'YYYY') "Год оформления заказа",
    ai.tm_closing::date "Дата закрытия заказа",
    to_char(ai.tm_closing, 'HH24:MI') "Время закрытия заказа",
    to_char(ai.tm_closing, 'TMMonth') "Месяц закрытия заказа",
    to_char(ai.tm_closing, 'YYYY') "Год закрытия заказа",
    
    wo."Дата поступления денег (возврат)"::date "Дата отгрузки товара",
    to_char(wo."Дата поступления денег (возврат)", 'HH24:MI') "Время отгрузки",
    to_char(wo."Дата поступления денег (возврат)", 'TMMonth') "Месяц отгрузки",
    to_char(wo."Дата поступления денег (возврат)", 'YYYY') "Год отгрузки",
--     NULL "Дата отгрузки товара",
--     NULL "Время отгрузки",
--     NULL "Месяц отгрузки",
--     NULL "Год отгрузки",
    
    
    case when o.is_full_paid then o.pay_date end "Дата полной оплаты заказа",
    case when o.is_full_sold then o.date_sold end "Дата полной отгрузки заказа",
    
    item.type as "Тип позиции",
    item.id "Артикул",
    item.name "Наименование позиции",
    item.brand "Производитель",
    item.category "Категория",
    item.supplier "Поставщик",
    item.cost "Расчетная себестоимость",
    item.sprice "Себестоимость чистая"
    
--    /*item.price "Продажная цена",
--    sold_state.name AS "Статус списания",*/

    ,wo."Сумма возврата" "Продажная цена", --Сумма платежа с типом "Гарантия" по тз BA-287
    'Возвраты' "Статус списания", --Возвраты по тз BA-287
    
    case when item.price <> 0 then round(100 * (item.price - item.cost) / item.price) end "Маржа",
    case when item.cost <> 0 then round(100 * (item.price - item.cost) / item.cost) end "Наценка",
    storage.name "Склад отгрузки",
    city.name "Город отгрузки",
    
--    /*item.realization_date::date "Дата реализации",
--    to_char(item.realization_date, 'HH24:MI') "Время реализации",
--    to_char(item.realization_date, 'TMMonth') "Месяц реализации",
--    to_char(item.realization_date, 'YYYY') "Год реализации",*/
    NULL "Дата реализации",
    NULL "Время реализации",
    NULL "Месяц реализации",
    NULL "Год реализации",
    
    storage.fulladdress "Адрес склада",
    case item.order_type when orders.type('INDIVIDUAL') then 'Нал' else 'Безнал' end "Тип оплаты",
    
    case l.type
        when logistics.delivery_type('LOCAL') then 'по городу'
        WHEN logistics.delivery_type('TRANSPORT') then 'ТК'
        else 'самовывовоз'
    end "Способ доставки",
    
    l.freighter "Транспортная компания",
    l.region "Регион доставки",
    l.city "Город доставки",
    l.address "Адрес доставки",
    
    item.nds "Сумма НДС",
    supplier.name "Фирма-поставщик",
    shipper.name "Фирма-грузоотправитель",
    
    o.mark_tender "Тендер",
    o.mark_request "Запрос цен",
    o.mark_kp "Коммерческое предложение"
    /*
    --,max(case when la._values::hstore -> 'state' = '5' then la._tm end) over (partition by COALESCE(item.id_order_item, ai.account_id), item.id) --01.09.2018 22 минуты
    --,lds."ID заказа"
    --,lds."Артикул (id товара)"
    */
    
    ,lds."Дата готовности к выдаче товара"
    ,lds."Время готовности к выдаче товара" -- 'HH24:MI' "09:14"
    ,lds."День готовности к выдаче товара" 
    ,lds."Месяц готовности к выдаче товара" -- 'TMMonth' "Января"
    ,lds."Год готовности к выдаче товара"
    
     ,item.s_id "id товара находящийся на складе"
     ,wo."id принятой по ЗН железяки" 
     ,case when item.s_id = wo."id принятой по ЗН железяки" then '=' else 'not' end
     ,wo."id платежа"
     ,wo."Сумма возврата"
     
    
-- FROM params _
from  items item
join store.shipping_states sold_state on sold_state.id = item.state
join opentech.account_item ai on ai.account_id = item.id_order
left join orders o on o.id_order = item.id_order
/* клиент */
join opentech.customers customer on customer.id = item.id_customer
left join opentech.customers supplier on supplier.id = ai.supplier_cid
left join opentech.customers shipper on shipper.id = ai.shipperid
left join customer_managers_log cml on cml.id = customer.id
left join public.cities customer_city on customer_city.id = customer.cityid
left join opentech.users manager on manager.id = customer.manager
left join customers.customer_okveds ok on ok.id_customer = customer.id
left join customers.okveds okved on okved.id = ok.id_okved
left join customers.okved_sections okved_section on okved_section.id = okved.id_okved_section
--left join profitability on profitability.id_customer = customer.id
left join customers.budgetaries budget on budget.id_customer = customer.id
left join customers.secondhand_dealers secondhand on secondhand.id_customer = customer.id
left join customers.miners miner on miner.id_customer = customer.id
left join customers.transports transport on transport.id_customer = customer.id
/* место продажи */
join opentech.storage storage on storage.id = item.id_storage
join public.cities city on city.id = storage.city_id
/* информация о доставке */
left join logistics l on l.id_order = item.id_order
/* ЛОГИ (макс дата - На складе выдачи)*/
left join logs_date_state lds on lds."ID заказа" = COALESCE(item.id_order_item, ai.account_id) --  !быстрее всего так
    and lds."Артикул (id товара)" = item.id    
    
/*--left join _logs.opentech__accounts la on la.i = item.i_log -- если max(case) over()...
--left join logs_date_state lds on lds."ID заказа" = all_end."ID заказа" -- если сте logs_date_state
--    and lds."Артикул (id товара)" = all_end."Артикул (id товара)" */
/*--where --с подзапросом 16 минут вместо case 22х мин
--    logs_date_state."ID заказа"  in (select all_end."ID заказа" from all_end)
--    and logs_date_state."Артикул (id товара)"  in (select all_end."Артикул (id товара)" from all_end)
--left join logs_date_state on 
--    logs_date_state."ID заказа" = all_end."ID заказа"
--    and logs_date_state."Артикул (id товара)" = all_end."Артикул (id товара)"*/
  
/*ВОЗВРАТЫ*/
right join warranty_opentech wo on wo."id принятой по ЗН железяки" = item.s_id
)

/****************************************************************************************************************************/

select * from all_end

union all
select    
    sum( p.sum )::int,'sum( p.sum )'
    ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null
    ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null
    ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null
    ,null,null,null,null,null,null,null,null,null,null,null,null,null--,null        
from params _
cross join opentech.payments p
where p.type = payments.type('WARRANTY') and p.date_po between _.frm and _.til 

union all
select
    sum( wo."Сумма возврата" )::int,'sum( wo."Сумма возврата" )'
    ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null
    ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null
    ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null
    ,null,null,null,null,null,null,null,null,null,null,null,null,null--,null 
from warranty_opentech wo

union all
select
    sum( ea."Сумма возврата" )::int,'sum( ea."Продажная цена Сумма возврата" )'
    ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null
    ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null
    ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null
    ,null,null,null,null,null,null,null,null,null,null,null,null,null--,null
from all_end ea

union all
select
    sum( ea."Продажная цена" )::int,'sum( ea."Продажная цена Сумма возврата" )'
    ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null
    ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null
    ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null
    ,null,null,null,null,null,null,null,null,null,null,null,null,null--,null
from all_end ea





