# import 
import os 
import glob
with open( glob.glob('/'.join(os.getcwd().split('/')[:3])+'/**/' + r'Imports_Formats.py', recursive=True)[0],'r') as exec_f:
        exec(exec_f.read())

############################################################


# xx

# путь до файла (пробелы если то в кавычки "")
abs_path = glob.glob('/'.join(os.getcwd().split('/')[:3])+'/**/'r"_ Analytics department", recursive=True) [0].replace('/_ Analytics department','') + r"/"

# path_sql_files = '/'.join(glob.glob(abs_path + r'/**/' + '_ Папка с файлами sql запросов.txt', recursive=True )[0].split('/')[:-2]) + r"/"
# path_sql_files = '/'.join(glob.glob(abs_path + r'/**/' + '_ Папка с файлами sql запросов.txt', recursive=True )[0].split('/')[:-2]) + r"/"

# path_OUT_dbDATA = '/'.join(glob.glob(abs_path + r'**/' + '_ Папка с данными для выгрузки с БД.txt', recursive=True )[0].split('/')[:-1]) + r"/"
path_OUT_dbDATA = '/'.join(glob.glob(abs_path + r'**/' + '_ Папка с данными для выгрузки с БД.txt', recursive=True )[0].split('/')[:-1]) + r"/"









############################## End ##############################

# print('xx')















