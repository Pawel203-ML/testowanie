#program ma za zadanie pobrac dane z plikow i stworzyc tablice z danymi
'''
pierwszym zadaniem ma byc pobieranie danych z plikow i tworzenie z nich slownika ktory bedzie przechowywala na raz tylko jeden plik
nastepnie ten slownik ma byc dodany do stworzonej tabeli
pozniej ma byc pobrany drugi plik i stworzony z niego slownik
'''

from sqlalchemy import create_engine, inspect
from sqlalchemy import Table, Column, String, Float, Date, Integer, MetaData
from sqlalchemy.exc import SQLAlchemyError


engine = create_engine('sqlite:///database2.db')
file_data = ['clean_stations.csv','clean_measure.csv']
names_tables = ['stations','measures']

def Create_connection(engine):
    '''
    Create connection with database SQLAlchemy
    :param engine: Name of database
    '''
    def decorator(func):
        '''
        Decorating selected function
        Catch error with connection
        :param *args, **kwargs: parrams of functions
        :param conn: Connection with database
        '''
        def wrapper(*args, **kwargs):
            try:
                with engine.connect() as conn:
                    result = func(conn, *args, **kwargs)
                return result
            except SQLAlchemyError as e:
                print(e)
        return wrapper
    return decorator

def InsertingDataFile():
    '''
    Function reading files 
    Importing lines of files to specific function
    Creating_columns() or Inserting_data()
    '''

    for index, i in enumerate(file_data):
        with open(i, 'r') as read_file:
            for breaker, line in enumerate(read_file.read().splitlines()):
                if breaker == 0:
                    Creating_columns(line,names_tables[index])
                else:
                    table = names_tables[1]
                    gettingColumns(line, table)
            

@Create_connection(engine)
def Creating_columns(conn, columns_name, name_table):
    '''
    Function creating names of columns and checking if tables doesn't exist already
    Names of columns in names_tables determinate the correct order of declarating columns
    :param lines: line of rcurrent reading file
    out: Create_tables()
    '''
    data = [line.strip() for line in columns_name.split(',')]
    for i in names_tables:
        if engine.has_table(i) == False:
            Create_tables(data, name_table)
            break
        
@Create_connection(engine)
def gettingColumns(conn, line, c_table):
    '''
    Function getting names of columns in current table
    :param line: line of current reading file for importing to Isolating_data()
    :param c_table: current table in database
    out: Isolating_data()
    '''
    meta = MetaData()
    tables = engine.table_names()
    for table in tables:
        if table == c_table:
            tab = Table(table, meta, autoload_with=engine)
            column = [column.name for column in  tab.columns]
            for col in column:
                Isolating_data(line, c_table, *column)
            break

@Create_connection(engine)
def Isolating_data(conn, lines, c_table, *columns):
    '''
    Function isolating values for columns
    :param lines: line of current reading file
    :param c_table: current table in database
    :param *columns: list of columns in current table
    out: Inserting_data()
    '''

    '''insert_statement = insert(users_table).values(name="Alice", age=30)'''
    values_dict = dict()
    data_file = [data.strip() for data in lines.split(',')]
    for i, column in enumerate(columns):
        values_dict[column] = data_file[i]
    Inserting_data( c_table, *values_dict)

@Create_connection(engine)
def Inserting_data(conn, c_table, *args):
    '''
    Inserting data into tables
    :param conn: Create connection with database
    '''
    meta = MetaData()
    table_name = c_table
    table = Table(table_name, meta, autoload_with=engine)  
    ins = table.insert()  
    conn.execute(ins, args)


@Create_connection(engine)
def Create_tables(conn, columns_name, name_table):
    '''
    Creating tables in database
    :param conn: Create connection with database
    '''
    type_data_columns1 = [String, Float, Float, Float, String, String, String]
    type_data_columns2 = [String, Date, Float, Integer]
    
        
    meta = MetaData()
    columns = []
    if len(columns_name) > 5:
        for i, column_name in enumerate(columns_name):
            columns.append(Column(column_name, type_data_columns1[i]))
        columns[0] = Column(columns_name[0], String,primary_key=True)
    else:
        for i, column_name in enumerate(columns_name):
            columns.append(Column(column_name, type_data_columns2[i]))
        columns[0] = Column(columns_name[0], String, primary_key=True)
        columns[-1] = columns[-1]
    table = Table(name_table, meta, *columns)
    meta.create_all(engine) #tworzy tabele

if __name__ == '__main__':
    InsertingDataFile()