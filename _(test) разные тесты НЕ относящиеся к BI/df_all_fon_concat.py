# import 
import os 
import glob
with open( glob.glob('/'.join(os.getcwd().split('/')[:3])+'/**/' + r'Imports_Formats.py', recursive=True)[0],'r') as exec_f:
    exec(exec_f.read())

with open( glob.glob('/'.join(os.getcwd().split('/')[:3])+'/**/' + r'xx.py', recursive=True)[0],'r') as exec_f:
    exec(exec_f.read())
############################################################

abs_path = glob.glob('/'.join(os.getcwd().split('/')[:3])+'/**/'r"_ Analytics department", recursive=True) [0].replace('/_ Analytics department','') + r"/"

path_OUT_dbDATA = '/'.join(glob.glob(abs_path + r'**/' + '_ Папка с данными для выгрузки с БД.txt', recursive=True )[0].split('/')[:-1]) + r"/"


df_all_fon = pd.DataFrame()

print(abs_path)
print( path_OUT_dbDATA )
# print( glob.glob( abs_path + r'**/df_fon*pkl', recursive=True ) )

for i in glob.glob( abs_path + r'**/df_fon*pkl', recursive=True ) [:] : # 5 сек
#     print( i.split('/')[-1] )
    df_all_fon = pd.concat([ df_all_fon, pd.read_pickle(i, compression='zip').iloc[:,:] ])
#     print(df_all_fon.shape)
    gc.collect() # https://docs.python.org/3/library/gc.html

    df_all_fon .to_pickle( path_OUT_dbDATA + r'Фоновый отчет по выручке (по дате продажи) с возвратами/df_all_fon/' \
        + fr'df_all_fon { df_all_fon.shape } .pkl', compression='zip') 


# print( df_all_fon )




############################## End ##############################

# print('xx')















