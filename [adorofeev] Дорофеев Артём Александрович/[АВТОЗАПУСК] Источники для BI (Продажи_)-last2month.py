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

# В НАЧАЛЕ СКРИПТА АВТОЗАПУСКА
# logs crontab АВТОЗАПУСК
start_datetime = datetime.datetime.today()
name_file = r'[АВТОЗАПУСК] Источники для BI (Продажи_)-last2month.py'
path_ipy = r"/opt/anaconda3/envs/jupyter_env/bin/ipython" 
path_file = fr"/home/adorofeev/jhub-analytics/'_ Analytics department'/'Источники для BI (Продажи)'/'{name_file}'" 
log_info = []

###########################

path_SQL_sales = glob(abs_path + r'**/' + r"*АВТОЗАПУСК] Источники для BI (Продажи_).sql", recursive=True)[0]
print(f'path_SQL_sales - {path_SQL_sales}')

date_df_start = df_date_m['start'][0]
date_df_end = df_date_m.iloc[-1, -1]
# date_df_start, date_df_end, df_date_m.iloc[-2:, :]

# public_link = '''https://nextcloud.e2e4.ru/apps/files/?dir=/Public/Отчеты%20(выгрузка%20jupyter%20notebooks)&fileid=2018892'''
AmazonS3_folder_Public = r'/Public/Отчеты (выгрузка jupyter notebooks)'
AmazonS3_path_to_file = r'/Источники для BI/Продажи_/'
# print('AmazonS3_file_name - ', AmazonS3_file_name, '/nAmazonS3_full_path - ', AmazonS3_full_path)
Amazon_owncloud = owncloud.Client('https://nextcloud.e2e4.ru')
Amazon_owncloud.login(loginAtlassianjhubAdmin, passwordAtlassianjhubAdmin)
### Amazon_owncloud.mkdir('testdir')

## идем по датам, коннектимся, читаем sql file, подставляем даты
df_fon = pd.DataFrame()
for i in df_date_m.iloc[-2:,:].values:
    AmazonS3_file_name = fr'Источники для BI (Продажи) {i[0]} - {i[1]}' # ({getpass.getuser()})
    AmazonS3_full_path = AmazonS3_folder_Public + AmazonS3_path_to_file + AmazonS3_file_name

    print(f'{i[0]} || {i[1]}')
#   читаем sql file
    with open( path_SQL_sales, "r", encoding = "utf-8" ) as sql_file_sales:
#       коннектимся 
        with psycopg2.connect(dbname=database, user=user, password=password, host=host) as cnxn:
#           подставляем даты
            print('подставляем даты, выгружаем')
            df_fon = pd.read_sql_query(sql_file_sales.read().replace('DATE_START_replce', f'{i[0]}').replace('DATE_END_replce', f'{i[1]}'), cnxn) 
            print('✔ .read_sql_query')
    
#   !! отсекаем товары на сегодня потому что грузится ночью до указанной даты а надо до всчерашнего конца дня ИСКЛЮЧАЯ СЕГОНЯ НОЧЬ!!  
    df_fon = df_fon[df_fon['Дата отгрузки товара'].astype(str) != pd.Timestamp.now().strftime( '%Y-%m-%d' )]
            
    log_info.append( (i, df_fon.shape) )
    if df_fon.shape[0] != 0:
    #   to_excel
        df_fon.to_excel(temp_tmp + AmazonS3_file_name + '.xlsx',  index=False, encoding='cp-1251') 
        print('✔ .to_excel')
    #   to_csv 
    #     df_fon.to_csv(path_OUT_dbDATA + r'Фоновый отчет по выручке (по дате продажи) с возвратами/' \
    #         + fr'Источники для BI (Продажи) { i[0] } { i[1] } .csv', sep='☺',  index=False) # cp-1251 encoding='UTF-8', engine='xlsxwriter'
    #   .to_pickle
        df_fon.to_pickle(temp_tmp + AmazonS3_file_name + '.pkl', compression='zip') 
        print('✔ .to_pickle')

        Amazon_owncloud.put_file( # полный путь и там и там!!! 
                    AmazonS3_full_path + '.xlsx', temp_tmp + AmazonS3_file_name + '.xlsx')
        print( f'✔ Файл записан в облако\n {AmazonS3_full_path}.xlsx')  

        Amazon_owncloud.put_file( # полный путь и там и там!!! 
                    AmazonS3_full_path + '.pkl', temp_tmp + AmazonS3_file_name + '.pkl'
                    ) 
        print( f'✔ Файл записан в облако\n {AmazonS3_full_path}.pkl') 

        os.remove(temp_tmp + AmazonS3_file_name + '.xlsx')
        os.remove(temp_tmp + AmazonS3_file_name + '.pkl')
        print('✔ os.remove')
    #     print(df_fon) 
        print(f"{datasize.DataSize(sys.getsizeof(df_fon)):MiB}", '(MiB)\n\n' )

    else:
        print( df_fon, '-- df.shape =', df_fon.shape)

# В КОНЦЕ СКРИПТА АВТОЗАПУСКА!
# print('log_info')

# print( log_info[0] )
# print( log_info[0][0] )
# print( log_info[0][1] )
# print( log_info[0][2] )

log_info = str(log_info[0][0]) + ''' ''' + str(log_info[0][1]) + ''' ''' + str(log_info[0][2]) 
cron_logs( start_datetime=start_datetime, name_file=name_file, path_ipy=path_ipy, path_file=path_file, log_info=log_info )
print('✔ cron_logs\n\n')
















