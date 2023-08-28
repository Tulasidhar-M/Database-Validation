import snowflake.connector
import pandas as pd
from datetime import datetime


def connection_to_snowflake(user_email):
    """
    Definition:
        Function to establish connection to snowflake via external browser authentication
    Input Args:
        user_email(string): Email address of the user
    Returns:
        conn(object) : Snowflake connection object
    """
    
    print('Connecting to Snowflake')
    conn = snowflake.connector.connect(user = user_email,
                                        account='<snowflake_account>',
                                        region = '<snowflake_region>',
                                        warehouse ='<warehouse>',
                                        authenticator = 'externalbrowser',
                                        autocommit = True)
    return conn

def list_of_objects_in_db(db_name, user_email):
    """
    Definition:
        To list all the objects in the database
    Input args:
        db_name(string): Database name
        user_email (string): E-Mail address of the user
    Returns:
        object_list(list): list of all objects in the database
    """

    schema_names_to_ignore = ['INFORMATION_SCHEMA', 'PUBLIC']
    conn = connection_to_snowflake(user_email)
    print(f'Fetching objects list from {db_name}')

    table_query = f'show tables in database {db_name}'
    table_df = pd.read_sql(table_query, conn)
    
    # Filtering out the objects in schemas present in schema_names_to_ignore list
    table_df = table_df[~table_df.schema_name.isin(schema_names_to_ignore)]
    
    # Creating a new column i.e., a three part name of the object in snowflake
    table_df['table_list'] = table_df['database_name'].map(str) + '.' + table_df['schema_name'].map(str) + '.'  + table_df['name'].map(str)
    
    # Converting the table_list column into a list
    table_list = list(table_df['table_list'])

    view_query = f'show views in database {db_name}'
    view_df = pd.read_sql(view_query, conn)
    view_df = view_df[~view_df.schema_name.isin(schema_names_to_ignore)]
    view_df['view_list'] = view_df['database_name'].map(str) + '.' + view_df['schema_name'].map(str) + '.'  + view_df['name'].map(str)
    view_list = list(view_df['view_list'])

    # Concatenating tables and view list
    object_list = table_list + view_list
    return object_list

def compare_objects(common_in_src_and_dest, src_db, tgt_db, result_df, user_email, exception_df):
    """
    Defintion:
        To compare objects in two databases
    Input args:
        common_in_src_and_dest(list): Objects in both databases
        src_db(string): source database name
        tgt_db(string): target database name
        result_df(dataframe): Empty dataframe
        user_email (string): E-Mail address of the user
    Returns:
        result_df(dataframe): returns mismatches count in a dataframe
    """
    print(f'Comparing objects in {src_db} and {tgt_db}')
    conn = connection_to_snowflake(user_email)
    
    # Number of records after execution of except clause b/w src and tgt model
    itr = 1
    for i in common_in_src_and_dest:
        query1 = f"""select count(*) as CNT from 
                    (select * from {src_db}.{i} except select * from {tgt_db}.{i})"""
        query2 = f"""select count(*) as CNT from 
                    (select * from {tgt_db}.{i} except select * from {src_db}.{i})"""
      
        print(f'{itr}: Validating {i}')
        try:
            df = pd.read_sql(query1, conn)
            val = df['CNT'].values[0]
            
            # Count 0 indicates a perfect match
            if val!=0:
                new_row = {'object_name':i, 'mismatch_rows_count':val,'query':query1}
                new_df = pd.DataFrame([new_row])
                result_df = pd.concat([result_df,new_df], ignore_index= True)

            df = pd.read_sql(query2, conn)
            val = df['CNT'].values[0]
            
            # Count 0 indicateS a perfect match
            if val!=0:
                new_row = {'object_name':i, 'mismatch_rows_count':val,'query':query2}
                new_df = pd.DataFrame([new_row])
                result_df = pd.concat([result_df,new_df], ignore_index= True)
        except Exception as e:
            #handle the exception
            new_row = {'object_name':i,'query': query1, 'Exception':str(e)}
            new_df = pd.DataFrame([new_row])
            exception_df = pd.concat([exception_df,new_df],ignore_index= True)
            write_df_to_csv(exception_df,'exceptions.csv')
            print(e)
        itr = itr + 1
      
    return result_df


def write_df_to_csv(df,file_name):
    """
    Definiton:
        To convert dataframe to csv file
    Input args:
        df(dataframe): Dataframe
        file_name(string): name of csv file
    Returns:
        None
    """
    df.to_csv(file_name, index = False)


def validation(src_db, tgt_db, user_email):
    """
    Definiton:
        To validate Objects in different databases
    Input args:
        src_db(string): source database name
        tgt_db(string): target database name
        user_email (string): E-Mail address of the user
    Returns:
        None
    """
    src_objects = list_of_objects_in_db(src_db, user_email)
    tgt_objects = list_of_objects_in_db(tgt_db, user_email)
    
    src_objects_without_dbname = set([x.split('.',1)[1] for x in src_objects])
    tgt_objects_without_dbname = set([x.split('.',1)[1] for x in tgt_objects])
    
    present_only_in_src = list(src_objects_without_dbname.difference(tgt_objects_without_dbname))
    present_only_in_tgt = list(tgt_objects_without_dbname.difference(src_objects_without_dbname))
    common_in_src_and_tgt = list(src_objects_without_dbname.intersection(tgt_objects_without_dbname))
    
    result_df = pd.DataFrame([],columns=['object_name', 'mismatch_rows_count', 'query'])
    src_df = pd.DataFrame([],columns=['object_name', 'Description'])
    tgt_df = pd.DataFrame([],columns=['object_name', 'Description'])
    exception_df = pd.DataFrame([],columns=['object_name', 'query', 'Exception'])
    
    for i in present_only_in_src:
        new_row = {'object_name':i, 'Description':f'present only in {src_db}'}
        new_df = pd.DataFrame([new_row])
        obj_in_src = pd.concat([src_df,new_df],ignore_index= True)
        write_df_to_csv(obj_in_src,'obj_in_src_not_in_tgt.csv')

    for i in present_only_in_tgt:
        new_row = {'object_name':i, 'Description':f'present only in {tgt_db}'}
        new_df = pd.DataFrame([new_row])
        obj_in_tgt = pd.concat([tgt_df,new_df], ignore_index= True)
        write_df_to_csv(obj_in_tgt,'obj_in_tgt_not_in_src.csv')

    print(f'Common objects between {src_db} and {tgt_db} : {len(common_in_src_and_tgt)}')
    result_df = compare_objects(common_in_src_and_tgt, src_db, tgt_db, result_df, user_email,exception_df)
    write_df_to_csv(result_df,'obj_in_src_&_tgt.csv')
    print("End time:",datetime.now())
    

# Handler Function : validation(src_db, tgt_db, user_email)
print("start time:",datetime.now())
validation('<SRC_DB>', '<TGT_DB>', '<user_email>')
