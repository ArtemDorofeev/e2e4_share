# 0 0 * * * /opt/anaconda3/envs/jupyter_env/bin/ipython /home/slebedev/jhub-analytics/'_ Analytics department'/'[АВТОЗАПУСК]'/'[АВТОЗАПУСК] BA-470 Оплаченные заказы с сайта для метрики.py'

# magic    
get_ipython().run_line_magic('matplotlib', 'inline')
# run files
get_ipython().run_line_magic('run', '/srv/jhub/share/._Additions/import_libs.py')
get_ipython().run_line_magic('run', '/srv/jhub/share/._Additions/Connecting_.py')
get_ipython().run_line_magic('run', '/srv/jhub/share/._Additions/my_script.py')
get_ipython().run_line_magic('run', '/srv/jhub/share/._Additions/Date_gen.py')
get_ipython().run_line_magic('run', '/srv/jhub/share/._Additions/xx.py')

############################## End ##############################


AmazonS3_folder_Public = r'/Public/Отчеты (выгрузка jupyter notebooks)'
AmazonS3_path_to_file = r'/Оплаченные заказы с сайта для метрики/'
# print('AmazonS3_file_name - ', AmazonS3_file_name, '/nAmazonS3_full_path - ', AmazonS3_full_path)
Amazon_owncloud = owncloud.Client('https://nextcloud.e2e4.ru')
Amazon_owncloud.login(loginAtlassianjhubAdmin, passwordAtlassianjhubAdmin)


sql_customer = '''

/*
https://jira.e2e4.ru/browse/BA-470
Оплаченные заказы с сайта для метрики
функция которая статус оплаты определяет public_api_v01._order_get_status_payment
*/

/*
--get_order_summa_brutto_without_logistics --Вычисление общей суммы заказа
create or replace function orders.get_order_summa_brutto_without_logistics(id_order integer)
 returns numeric
 language sql
 stable strict security definer
as $function$
-- вычисление суммы товара по заказу без стоимости доставки
    select
        coalesce(sum(a.price_net), 0)
    from opentech.accounts a
    where a.id=id_order
    	and a.del=0
    	and a.correct_name not like 'Доставка%' ;
$function$
;
*/

/* Так правильнее, чтобы оплачен+выдан. А те, которые "в пути", "на складе" и т.п. все в in_progress
1	Проблема с заказом   
2	В обработке
4	В пути
5	На складе сборки
6	На складе доставки
7	На складе выдачи
8	Отменен
9	Выдан
10	Не оплачен
0	Удален
11	Доставляется
3	Не оформлен
*/

with params as (
select
	now() - interval '21 day'	frm -- - interval '3 months' 21 day 7 hour 13 199
	,now()  					til  
--	('01.05.2023' || ' 00:00:00')::timestamp frm,
--	,('31.05.2023' || ' 23:59:59')::timestamp til
--	( 'DATE_START_replce' || ' 00:00:00')::timestamp    frm 
--	,( 'DATE_END_replce' || ' 23:59:59')::timestamp    til 
)
-- select * from params

, orders_logs as (
select distinct 
	ai.account_id "id" --id - номер заказа,
	,max(lai._tm) over (partition by ai.account_id ) max_tm
	,max(lai._values::hstore -> 'tm_placed') over (partition by ai.account_id ) max_tm_placed
	,last_value(_id_user) over (partition by ai.account_id order by lai._tm) last_id_user
	
from params _
cross join opentech.account_item ai
left join orders.platforms oplatforms on oplatforms.id = ai.id_platform
--left join opentech.customers oc on oc.id = ai.id_customer
/*logs*/
left join _logs.opentech__account_item lai on lai.id = ai.id

where 
	oplatforms.id in ( 7832, 1246, 3252)
	and 
	ai.id_customer not in (531058)
	and 
	lai._id_user in (1017, 112)
	and 
	lai._values::hstore -> 'tm_placed' is not null
	/*test*/
--	and 
--	account_id in (7107049)
--	and yandex_client_id is not null
)
--select * from orders_logs os

, orders as (
select 

	max_tm::text "z create_date_time" 
	,to_char(max_tm, 'YYYY-MM-DD HH24:MI:SS') "create_date_time" 
	,to_char(max_tm, 'YYYY') "YYYY create_date_time" 
	,to_char(max_tm, 'MM') "MM create_date_time" 
	,to_char(max_tm, 'DD') "DD create_date_time" 
	,max_tm_placed::text "max_tm_placed"
	,yandex_client_id "client_ids" --client_ids - идентификатор клиента в системе Яндекс Метрики, который передаем по задаче - сбор и передача данных систем аналитики,
	,ai.account_id "id" --id - номер заказа,

	,case 
		when ai.state in (9) 
			and orders.get_order_paid_sum( account_id ) >= orders.get_order_summa_brutto( account_id ) 
			then 'PAID'			
		when ai.state in (1, 2, 3, 4, 5, 6, 7)
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
	
	,orders.get_order_summa_brutto_without_logistics(account_id)  "revenue без logistics" -- сумма заказа без стоимости доставки
	,orders.get_order_summa_brutto(account_id) "revenue c logistics" -- сумма заказа 
	
	,last_id_user "id Оформил 1017, 112"
	,u.name "Оформил 1017, 112"
	
	,ai.state "id статусы"
	,state.name "name статусы"
	,oplatforms.id "id oplatforms"
	,oplatforms.name "name oplatforms"
	 
	,orders.get_order_paid_sum( account_id ) = orders.get_order_summa_brutto( account_id ) "суммы оплат = общей суммы"
	,orders.get_order_paid_sum( account_id ) < orders.get_order_summa_brutto( account_id ) "суммы оплат < общей суммы "
	
from params _
cross join opentech.account_item ai
left join site.order_to_analytics_ids so on so.order_id = ai.account_id
left join orders.platforms oplatforms on oplatforms.id = ai.id_platform
left join orders.states state on state.id = ai.state
/*logs*/
join orders_logs lai on lai.id = ai.account_id
left join opentech.users u on u.id = lai.last_id_user

where 
	max_tm between _.frm and _.til

)

select * from orders

'''


AmazonS3_folder_Public = r'/Public/Отчеты (выгрузка jupyter notebooks)'
AmazonS3_path_to_file = r'/Оплаченные заказы с сайта для метрики/'

AmazonS3_full_path_customer = AmazonS3_folder_Public + AmazonS3_path_to_file + r'Оплаченные заказы с сайта для метрики' 

for file_zip in zip( [AmazonS3_full_path_customer]
                     ,[sql_customer]):
    print( 'connect...' )
    with psycopg2.connect(dbname=database, user=user, password=password, host=host) as cnxn: # коннектимся 
        df_fon = pd.read_sql_query( file_zip[1], cnxn) 
    print( f"✔ .read_sql_query {file_zip[0].split('/')[-1]}" )    
    
    if df_fon.shape[0] != 0:
        df_fon.to_excel(temp_tmp + file_zip[0].split('/')[-1] + '.xlsx',  index=False, encoding='cp-1251') #   to_excel
        Amazon_owncloud.put_file( file_zip[0] + '.xlsx', temp_tmp + file_zip[0].split('/')[-1] + '.xlsx' )
        print( f"✔ Файл записан в облако\n{file_zip[0].split('/')[-1]}.xlsx" )  
        os.remove(temp_tmp + file_zip[0].split('/')[-1] + '.xlsx' )
        print( '✔ os.remove' )
        #     print(df_fon) 
        print(f"{datasize.DataSize(sys.getsizeof(df_fon)):MiB}", '(MiB)\n\n' )
        # df_fon
    else:
        print( file_zip[0].split('/')[-1], '-- df.shape =', df_fon.shape)
    



