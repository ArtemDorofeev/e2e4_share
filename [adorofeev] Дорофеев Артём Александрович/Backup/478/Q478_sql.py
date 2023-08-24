#Импортируем библы
import pandas as pd

#База данных
import psycopg2 as pc2
from psycopg2 import Error
from psycopg2 import sql

############################## End ##############################

# Соединение с сервером PostgreSQL

def db_connect():
    """ Connect to the PostgreSQL database server """
    param_connect = {
        "host"      : "pg_replica.opentech.local",
        "database"  : "opentech",
        "user"      : "bi_admin",
        "password"  : "89736b83700432cdf6311671a0013eb4"
    }
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = pc2.connect(**param_connect)
        print("1. Соединение с PostgreSQL установлено. Загружаем данные...")
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    return conn

# Отправка запроса на выгрузку данных

def query(sql_query, values=None, conn=None):
    """ Execute a QUERY request """
    try:
        result=None
        conn=db_connect()
        result=pd.read_sql_query(sql_query, conn)
        print('2. Заказы успешно выгружены')
        return result
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:        
        conn.close()
        print("3. Соединение с PostgreSQL закрыто")
        

# SQL запрос

def sql_query(date_start, date_end):
    
    sql_query = f'''
    
    with params AS materialized (select
                                    ('{date_start}' || ' 23:59:59')::timestamp frm
                                    ,('{date_end}' || ' 23:59:59')::timestamp til
                                )
    --select * from params

        , orders_logs as (select distinct 
                    ai.account_id "id" --id - номер заказа,				
                    ,(max(lai._values::hstore -> 'tm_placed') over (partition by ai.account_id ))::timestamptz max_tm_placed				

                from params _
                cross join opentech.account_item ai
                left join orders.platforms oplatforms on oplatforms.id = ai.id_platform			
                left join _logs.opentech__account_item lai on lai.id = ai.id

                where 
                    oplatforms.id in ( 7832, 1246, 3252)
                    and 
                    ai.id_customer not in (531058)
                    and 
                    lai._id_user in (1017, 112)
                    and 
                    lai._values::hstore -> 'tm_placed' is not NULL
                    AND 
                    lai."_tm" between frm and til
                    /*test*/
                    --and	account_id in (7376837, 7363369, 7377343)			
            )
        --SELECT * FROM orders_logs

        ,orders_total AS (select DISTINCT
                                ai.account_id "id" --id - номер заказа,
                                ,case 
                                    when ai.state in (9) 
                                        and orders.get_order_paid_sum( account_id ) >= orders.get_order_summa_brutto( account_id ) 
                                        then 'PAID'			
                                    when ai.state in (1, 2, 4, 5, 6, 7)
                            --			and orders.get_order_paid_sum( account_id ) < orders.get_order_summa_brutto( account_id ) 
                                        then 'IN_PROGRESS'
                                    when ai.state in (8, 0, 3) 
                                        then 'CANCELLED'	
                                    when ai.state in (10) 
                                        and orders.get_order_paid_sum( account_id ) < orders.get_order_summa_brutto( account_id ) 
                                        then 'IN_PROGRESS'			
                                    when ai.state in (11) 
                                        and orders.get_order_paid_sum( account_id ) = orders.get_order_summa_brutto( account_id ) 
                                        then 'IN_PROGRESS'									
                                    else 'IN_PROGRESS'			

                                end "order_status"
                                ,a.id_customer "db_client_ids"
                                ,yandex_client_id "y_client_ids" --client_ids - идентификатор клиента в системе Яндекс Метрики, который передаем по задаче - сбор и передача данных систем аналитики,
                                ,ct."name" "client_sity"
                                ,date_trunc('minute', max_tm_placed ) + round(cast(date_part('second', max_tm_placed ) as float4)) * INTERVAL '1 second' "order_date"
                                ,orders.get_order_summa_brutto_without_logistics(account_id)  "revenue" -- сумма оформленного заказа без стоимости доставки
                                ,orders.get_order_summa_netto ( account_id ) "custom" 
                                ,count(a.i) FILTER (WHERE reference_id <> 0) OVER (PARTITION BY ai.account_id) AS "item_count" -- количество товарных позиций в заказе без услуг и доставки 						 	

                            from params _
                            cross join opentech.account_item ai
                            left join opentech.accounts a on a.id = ai.account_id
                            left join site.order_to_analytics_ids so on so.order_id = ai.account_id
                            left join orders.states state on state.id = ai.state
                            LEFT JOIN opentech.customers c ON c.id = a.id_customer
                            LEFT JOIN public.cities ct ON c.cityid = ct.id 
                            join orders_logs lai on lai.id = ai.account_id

                            WHERE ai.id_platform in ( 7832, 1246, 3252)							
                        )

        ,orders_shipped AS (select 
                                    ai.account_id "id" --id - номер заказа,    
                                    ,orders.get_order_summa_shipped(account_id)  "sum_shipped" -- сумма оформленного заказа без стоимости доставки	
                                    ,count(a.i) FILTER (WHERE reference_id <> 0) "ship_item_count" -- количество товарных позиций в заказе без услуг и доставки
                                    ,orders.get_order_paid_sum( account_id ) "sum_payed"
                                    ,max(case 
                                        when a.correct_name like 'Доставка%' then a.price_net 
                                        else 0 
                                    END) delivery 
                                    --,round((orders.get_order_summa_brutto_without_logistics(account_id) - orders.get_order_summa_netto ( account_id )) / orders.get_order_summa_brutto_without_logistics(account_id) * 100, 2) "margin"
                                    ,(SELECT sum(cost)
                                        FROM orders.get_order_item_costs(array_agg(a.i))) cost
                                from params _
                                cross join opentech.account_item ai
                                left join opentech.accounts a on a.id = ai.account_id
                                left join orders.states state on state.id = ai.state
                                join orders_logs lai on lai.id = ai.account_id

                                WHERE ai.id_platform in ( 7832, 1246, 3252)
                                    AND a.state = ANY(orders.item_state(ARRAY['SOLD', 'FOR_REPLACEMENT']))						

                                GROUP BY ai.account_id
                            )

    SELECT ot.*
            ,round(("revenue" - "custom" ) / "revenue" * 100, 2) "marginality"
            ,sum_shipped - delivery sum_shipped
            ,ship_item_count
            ,sum_payed - delivery sum_payed
            ,sum_shipped - delivery - cost "margin"		

    FROM orders_total ot
    LEFT JOIN orders_shipped os USING (id)

    '''
    return sql_query