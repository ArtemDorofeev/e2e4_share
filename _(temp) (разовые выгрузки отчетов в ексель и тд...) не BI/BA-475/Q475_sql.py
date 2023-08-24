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
        

# SQL запросы

def sql_query():
    
    sql_query = f'''
    
WITH intrance AS (SELECT * --186
					FROM customers.branches
					WHERE id_branch IN (SELECT DISTINCT id_customer --4179
										FROM customers.branches					
										)					
				)
				
	,outsource AS (select id_outsource id
							, array_agg(id_customer) "Аутсорсеры"
					from customers.outsources
					group by id_outsource
					having count(*) > 1
				)
	
	,branches AS (SELECT intrance.id_customer "Головная организация"
							,cb.id_customer id
							,array_agg(cb.id_branch) "Филиалы" --340
					FROM customers.branches cb
					LEFT JOIN intrance ON cb.id_customer = intrance.id_branch
					WHERE cb.id_customer IN (SELECT id_branch --186
											FROM intrance)
					GROUP BY cb.id_customer, intrance.id_customer
					)
					
	,closures AS (SELECT DISTINCT ON (c.id)
					 c.id id,
					 c.name "Наименование компании",	
						cs."name" "Статус компании"
					FROM opentech.customers c
					JOIN customers.corporate_statuses_history ccs ON ccs.id_customer = c.id AND id_corporate_status = 5
					JOIN customers.corporate_statuses cs ON cs.id = ccs.id_corporate_status					
					ORDER BY c.id, ccs.date DESC
				)
					
					
SELECT id "id компании"
		,cm."name" "Название компании"
		,"Головная организация"
		,"Филиалы"
		,"Аутсорсеры"
		,case when "Статус компании" = 'Данное ЮЛ более не актуально' then 'ЮЛ закрыто/ликвидировано' else 'ЮЛ активно' END "Статус компании"
FROM branches br
FULL JOIN outsource os USING(id)
FULL JOIN closures cl USING(id)
LEFT JOIN opentech.customers cm USING(id)
ORDER BY id

    '''
    return sql_query

def sql_query1():
    
    sql_query = f'''

    SELECT id, name, inn
    FROM opentech.customers   
    ORDER BY id

    '''
    return sql_query
