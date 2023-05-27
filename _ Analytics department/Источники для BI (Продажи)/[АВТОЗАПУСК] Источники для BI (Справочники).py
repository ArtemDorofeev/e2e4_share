# magic    
get_ipython().run_line_magic('matplotlib', 'inline')
# run files
get_ipython().run_line_magic('run', '/srv/jhub/share/._Additions/import_libs.py')
get_ipython().run_line_magic('run', '/srv/jhub/share/._Additions/Connecting_.py')
get_ipython().run_line_magic('run', '/srv/jhub/share/._Additions/my_script.py')
get_ipython().run_line_magic('run', '/srv/jhub/share/._Additions/Date_gen.py')
get_ipython().run_line_magic('run', '/srv/jhub/share/._Additions/xx.py')

# print
###########################
# print(
# df_test_xx.shape, '\n'
# ,abs_path, '\n' # abs_path - путь до шары (видна всем езерам - '/srv/jhub/share/')
# ,path_OUT_dbDATA, '\n'
# ,date_start, date_end, '\n'
# )
############################## End ##############################

# --СПРАВОЧНИКИ
# sql_customer # --_Клиенты (справочник)
# sql_storage # --_Склады (справочник)
# sql_users # -- _Сотрудники (справочник)
# sql_platforms # --_Площадки (справочник)
# sql_e2e4 # --_e2e4 ID (справочник)

# В НАЧАЛЕ СКРИПТА АВТОЗАПУСКА
# logs crontab АВТОЗАПУСК
start_datetime = datetime.datetime.today()
name_file = r' [АВТОЗАПУСК] Источники для BI (Справочники).py'
path_ipy = r"/opt/anaconda3/envs/jupyter_env/bin/ipython" 
path_file = fr"/home/slebedev/jhub-analytics/'_ Analytics department'/'{name_file}'" 
log_info = []

###########################

AmazonS3_folder_Public = r'/Public/Отчеты (выгрузка jupyter notebooks)'
AmazonS3_path_to_file = r'/Источники для BI/Продажи/'
# print('AmazonS3_file_name - ', AmazonS3_file_name, '/nAmazonS3_full_path - ', AmazonS3_full_path)
Amazon_owncloud = owncloud.Client('https://nextcloud.e2e4.ru')
Amazon_owncloud.login(loginAtlassianjhubAdmin, passwordAtlassianjhubAdmin)


sql_customer = '''
	/*
    Клиенты (справочник) 874 657 // 921 691 // 921 872 // 921 725 // 921 889
    */
    with CUSTOMER_MANAGERS_LOG AS (
        /* Дата последней отвязки ответственного менеджера */
        SELECT 
            c.id
            ,max(l._tm)::date unbind_date

        FROM opentech.customers c
        JOIN _logs.opentech__customers l ON l.id = c.id
            AND l."_op_type" = 2    /* это запись об изменении карточки */
            AND exist(l._values, 'manager') /* изменялся ответственный менеджер */
            AND NOT defined(l._values, 'manager') /* ответственный менеджер был снят */
        WHERE c.manager IS NULL
        GROUP BY c.id
    )

    , CUSTOMERS_REFERENCE as(
    select distinct 
        customer.id "ID клиента"
        ,customer.name "Наименование клиента"
        ,customer.phone "Телефоны клиента"
        ,customer.cityid "Город"
        ,customer.address "Почтовый адрес"
        ,customer.email "Электронная почта"
        ,customer.first_order_date "Дата первого заказа"
        ,CASE WHEN customer.inn IS NULL THEN 'ФЛ' ELSE 'ЮЛ' END "Тип клиента"

        ,CASE
            WHEN budget.id_customer IS NOT NULL THEN 'бюджетник'
            WHEN secondhand.id_customer IS NOT NULL THEN 'перекуп'
            WHEN miner.id_customer IS NOT NULL THEN 'майнер'
            WHEN transport.id_customer IS NOT NULL THEN 'ТК'
            WHEN customer.asc = 1 THEN 'АСЦ'
            WHEN customer.concurent = 1 THEN 'конкурент'
        END "Классификация"

        ,customer.inn "ИНН"
        ,customer.kpp "КПП"
        
        ,NULLIF(customer.respite_period, 0) "Отсрочка, дни"
    /*
        ,CASE
            WHEN o.order_type = orders.type('ORGANIZATION') AND o.is_full_paid AND o.is_full_sold THEN
                NULLIF(GREATEST(0, o.pay_date - o.date_sold), 0)
        END AS "Отсрочка факт, дни"
    */
        ,cco.id_okved "ОКВЭД"
        ,okved_section.name "Отрасль"
        ,okved.name "Вид деятельности"
        ,customer_city.name "Город клиента"
        ,customer.registrationdate::timestamp "Дата регистрации клиента"
        ,manager.name "Ответственный менеджер" --заменить на логи потом может быть но пока оставть id 
        ,manager.id "Ответственный менеджер" --заменить на логи потом может быть но пока оставть id 
        ,cml.unbind_date::timestamp "Дата отвязки менеджера"
        
        ,case when co.id_outsource is not null then 'Аутсорсер' else null 
        	end "Признак аутсорсера"	
        ,co1.id_customer "Обслуживающая компания"
        ,cb.id_customer "Головная организация"

    from  opentech.customers customer --ON customer.id = item.id_customer
    --LEFT JOIN opentech.customers supplier ON supplier.id = ai.supplier_cid
    --LEFT JOIN opentech.customers shipper ON shipper.id = ai.shipperid
    left join CUSTOMER_MANAGERS_LOG cml ON cml.id = customer.id
    left join opentech.users manager on manager.id = customer.manager

    left join customers.customer_okveds cco on cco.id_customer = customer.id
    left join customers.okveds okved on okved.id = cco.id_okved
    left join customers.okved_sections okved_section on okved_section.id = okved.id_okved_section

    left join customers.budgetaries budget on budget.id_customer = customer.id
    left join customers.secondhand_dealers secondhand on secondhand.id_customer = customer.id
    left join customers.miners miner on miner.id_customer = customer.id
    left join customers.transports transport on transport.id_customer = customer.id
    left join public.cities customer_city on customer_city.id = customer.cityid
    /*Аутсорсер + id Обслуживающая компания*/
    left join customers.outsources co on  customer.id = co.id_customer --Аутсорсер
    left join customers.outsources co1 on customer.id = co1.id_outsource -- Отношение: Обслуживающая компания
    left join customers.branches cb on customer.id = cb.id_branch  -- Отношение: Головная организация

    )
    /*ВСЕ*/
    select distinct on ("ID клиента")
        *
    from CUSTOMERS_REFERENCE cr

    
    
 	/*проверка на повторение в ID клиента (было изза нескк Отношение: Обслуживающая компания ИЛИ Отношение: Головная организация)*/
    
--    select
--    	*
--    from CUSTOMERS_REFERENCE cr
--    where 
--		"ID клиента" in (
--		    select
----		    	*
--		    	"ID клиента"
----		    	,count("Головная организация")
--		    from CUSTOMERS_REFERENCE cr
--		    group by 
--		    	"ID клиента"
--		    having 
--		    	count("Головная организация") > 1
--		    order by "ID клиента"	
--    	)


    '''


sql_storage = '''
    /*
    Склады (справочник) 145
    */
    select 
        storage.id "ID склада отгрузки", --Переделать на ID склада
        storage.name "Склад отгрузки", --Переделать на ID склада
        city.name "Город отгрузки",
        storage.fulladdress "Адрес склада"
    from opentech.storage storage --ON storage.id = item.id_storage
    JOIN public.cities city ON city.id = storage.city_id
    '''


sql_users = '''
    /*
    Сотрудники (справочник)
    */
	select --*
        u.id, 
        u.login, u."name", u.customer_id "Карточка клиента", u.fired "Признак, что сотрудник уволен",
        u.id_1c "Инедтификатор сотрудника в 1С", ud.id "Отдел id", ud.name "Отдел", u.email, u.phone, u.workphone, u.workphone_login, 
        u.employmentdate "Дата приёма на работу", u.terminationdate "Дата увольнения (последний рабочий день)", 
        u.siteuser_id "Учетная запись на сайте"
        ,u.timezone "Часовой пояс"
        ,u.appointment "Должность ID"
        ,ap.name "Должность"
        ,ud.name "Справочник отделов компании"
        ,u.first_name "Имя", u.last_name "Фамилия", u.middle_name "Отчество"
        
    from opentech.users u
    left join users.departments ud on ud.id = u.department
    left join users.appointments ap on u.appointment = ap.id 
    '''


sql_platforms = '''
    /*
    Площадки (справочник)
    */
    select * from orders.platforms op
    '''


sql_e2e4 = '''
    /*
    e2e4 ID (справочник)
    */
    select 
        id, login, "name", surname, id_author, tm_create::timestamp, entrance_datetime::timestamp "Момент времени последнего входа",  
        id_city "Выбранный город (не отгрузки, а доставки)",  payment_type "Тип платежа по умолчанию"
    from site.users -- 577 692 
    
    '''

directory_book_of_reference = r'/Public/Отчеты (выгрузка jupyter notebooks)/Источники для BI'

AmazonS3_full_path_customer = directory_book_of_reference +  r'/_Клиенты (справочник)/_Клиенты (справочник)' 
AmazonS3_full_path_storage = directory_book_of_reference +   r'/_Склады (справочник)/_Склады (справочник)' 
AmazonS3_full_path_users = directory_book_of_reference +     r'/_Сотрудники (справочник)/_Сотрудники (справочник)' 
AmazonS3_full_path_platforms = directory_book_of_reference + r'/_Площадки (справочник)/_Площадки (справочник)' 
AmazonS3_full_path_e2e4ID = directory_book_of_reference +    r'/_e2e4 ID (справочник)/_e2e4 ID (справочник)' 

for file_zip in zip( [AmazonS3_full_path_customer, AmazonS3_full_path_storage, AmazonS3_full_path_users, AmazonS3_full_path_platforms, AmazonS3_full_path_e2e4ID]
                     ,[sql_customer, sql_storage, sql_users, sql_platforms, sql_e2e4]):
    print( 'connect...' )
    with psycopg2.connect(dbname=database, user=user, password=password, host=host) as cnxn: # коннектимся 
        df_fon = pd.read_sql_query( file_zip[1], cnxn) 
    print( f"✔ .read_sql_query {file_zip[0].split('/')[-1]}" )    
    log_info.append( (file_zip[0].split('/')[-1], df_fon.shape) )
    
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
    
# В КОНЦЕ СКРИПТА АВТОЗАПУСКА!
log_info = str(log_info[0][0]) + ''':''' + str(log_info[0][1])  
cron_logs( start_datetime=start_datetime, name_file=name_file, path_ipy=path_ipy, path_file=path_file, log_info=log_info )
print('✔ cron_logs\n\n')



