#!/usr/bin/python
from cryptography.fernet import Fernet
from subprocess import Popen, PIPE
import os
import logging
import sys
import getopt
import traceback
import cx_Oracle
from datetime import datetime
import smtplib
from email.message import EmailMessage
import time
import config
import oci
import pandas as pd
import numpy as np
from numpy.random import randint

pd.set_option('display.max_rows', 100)
pd.options.display.max_columns = None
pd.options.display.width = 2000
pd.options.display.max_colwidth = 300


#Ociconfig = oci.config.from_file(os.environ['OCI_PATH_EXAMPLE'],"DEFAULT" )
Ociconfig = oci.config.from_file("<<path_to_private_key_of_oci_user>>","DEFAULT")
database_client = oci.database.DatabaseClient(Ociconfig) 

global conn,username_dba,password_dba,v_connection_string_pdb_ages_dba


username_dba = config.username_dba
#password_dba = os.environ['DBA_USERPASS']
password_dba = config.password_dba
v_extra = '\n'

def LoopOnThewholeCDB():

        conn = OpenAGES_DBAConnection() 
        curs = conn.cursor()
        
        CdbsqlCommand = "select name from v$pdbs where name not like \'AFORMS%\' order by name"
        curs.execute(CdbsqlCommand)
        for row in curs:
              #print (row)
              str_row = str(row) 
              str_row = str_row.replace('(','')
              str_row = str_row.replace(')','')
              str_row = str_row.replace(',','') 
              str_row = str_row.replace('\'','')
              v_pdb = str_row
              str_row = str_row + "_S"
              print (str_row)

              tns_alias = str_row
              v_user_string=config.username_dba + "/" + config.password_dba + "@" + tns_alias
              print (config.username_dba + "@" + tns_alias )

             
          

              try:
                  queryResult, errorMessage = runSqlQuery(config.sql, v_user_string )
              except Exception as e:
                  logging.info('In Exception')
              
              print (queryResult)

        curs.close
        conn.close      

def QueryTheCDB():
        conn = OpenAGES_DBAConnection() 
        curs = conn.cursor()

        v_user_string=config.username_dba + "/" + config.password_dba + "@" + config.tns_alias
        
        queryResult, errorMessage = runSqlQuery(config.sql, v_user_string )
        
        print (queryResult)
        curs.close
        conn.close 

def runSqlQuery(sqlCommand, v_user_string):
   session =  Popen(["<<path to sqlc>>", "-S", v_user_string], stdout=PIPE, stdin=PIPE,universal_newlines=True ,encoding='latin1' )   
   session.stdin.write('SET PAGESIZE 500; \n')
   print (sqlCommand)     
   session.stdin.write(sqlCommand)
   print ('Running Now')
   return session.communicate()

def SysConnExecuteCurs(sql):

        #print( "***********************Connection as sys ******************************" )
        username_sys = 'sys'
        #password_sys = 'InsertPWD'
        password_sys = os.environ['SYS_PWD'] 
        #print (config.tns_alias)
        conn = cx_Oracle.connect(username_sys, password_sys,config.tns_alias,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()
        print (sql)
        curs.execute(sql)
        curs.close
        conn.close

def OpenSysConnection():
   
   username_sys = 'sys' 
   #password_sys = "InsertPWD"
   password_sys = os.environ['SYS_PWD'] 
   connSys = cx_Oracle.connect(username_sys, password_sys,config.tns_alias,mode=cx_Oracle.SYSDBA)
   return connSys
   
def CloseSysConnection(var_connection):     
   var_connection.close()        




def OpenCMDBConnection():
   tns_alias_cdb = 'CMDB_S'
   username_ages_clone = 'central_conf_user'
   password_ages_clone = os.environ['central_conf_user_pwd']
   
   connCMDB = cx_Oracle.connect(username_ages_clone, password_ages_clone,tns_alias_cdb)
   return connCMDB
   
def CloseCMDBConnection(var_connection):     
   var_connection.close()        

def OpenAGES_DBAConnection():
    connAges_DBA = cx_Oracle.connect(username_dba, password_dba,config.tns_alias)
    return connAges_DBA
   
def CloseAGES_DBAConnection(var_connection):     
   var_connection.close()  



def ClosePdb(pdb):
        sql = 'ALTER PLUGGABLE DATABASE ' + pdb + ' CLOSE IMMEDIATE INSTANCES=ALL '
        #print(sql)
        SysConnExecuteCurs(sql)
      

def OpenPdb(pdb):
        sql = 'ALTER PLUGGABLE DATABASE ' + pdb + ' OPEN  INSTANCES=ALL '
        #print(sql)
        SysConnExecuteCurs(sql)

def FindTargetHost():
        
        connection = OpenSysConnection()
        cursSys = connection.cursor()
        sqlFindtheHostofPrimary='select HOST_NAME from V$instance'
        
        for row in cursSys.execute(sqlFindtheHostofPrimary):
                config.destination_host = row[0]


        cursSys.close
        CloseSysConnection(connection)
       


def DeactivatePdbResourcePlan(pdbdest):
        
        connection = OpenSysConnection()
        cursSys = connection.cursor()

        sqlSetContainer = 'ALTER SESSION SET CONTAINER=' + pdbdest +''
        
        cursSys.execute(sqlSetContainer)
        
        sqlFindtheActiveResourcePlan=' SELECT name FROM v$rsrc_plan where is_top_plan=\'TRUE\' '
        
        for row in cursSys.execute(sqlFindtheActiveResourcePlan):
                resource_plan = row[0]

        sqldeactivatethePlan='ALTER SYSTEM SET RESOURCE_MANAGER_PLAN = \'\' ' 
        cursSys.execute(sqldeactivatethePlan)

        cursSys.close
        CloseSysConnection(connection)

def ActivatePdbResourcePlan(pdbdest):
        
        connection = OpenSysConnection()
        cursSys = connection.cursor()

        sqlSetContainer = 'ALTER SESSION SET CONTAINER=' + pdbdest +''
        
        cursSys.execute(sqlSetContainer)
        
        sqlFindtheActiveResourcePlan=' SELECT PLAN FROM DBA_RSRC_PLANS where PLAN like ' + '\'' +  pdbdest + '%' + '\'' 
        print(sqlFindtheActiveResourcePlan)
        
        for row in cursSys.execute(sqlFindtheActiveResourcePlan):
                resource_plan = row[0]

        sqldeactivatethePlan='ALTER SYSTEM SET RESOURCE_MANAGER_PLAN = '  + '\'' + resource_plan + '\''  + ' scope=both sid=\'*\' ' 
        print (sqldeactivatethePlan)
        cursSys.execute(sqldeactivatethePlan)

        cursSys.close
        CloseSysConnection(connection)




def StopAppServices(pdb):
        # Connection to CMDB 
        
        connection = OpenCMDBConnection()
        cursCDBDB = connection.cursor()
        print ( "*****************Stop Application Services for PDB Target*****************"  )
        sqlReadServices = 'select D1_SERVICE_NAME from APP_AGES_SEC.TNS_MATRIX where pdb = :var_pdb_dest' 

        #dbname = cdbdest
        #print (dbname)
        for row in cursCDBDB.execute(sqlReadServices,var_pdb_dest=pdb):
                servicename = row[0] 
                print ("Service: " + servicename)
                StopService(config.cdbtarget,servicename)

        cursCDBDB.close
        CloseCMDBConnection(connection)        






def FindDGInfo():
        
        i=0
        connection = OpenSysConnection()
        cursSys = connection.cursor()
        
        sqlFindDBRole= 'select DB_UNIQUE_NAME,DEST_ROLE from V$DATAGUARD_CONFIG'
        #print (sqlFindDBRole)
        
        for row in cursSys.execute(sqlFindDBRole):
                if i==0:
                        first_db_unique_name = row[0]
                        first_dest_role = row[1]
                
                if i==1:
                        second_db_unique_name = row[0]
                        second_dest_role = row[1]
                
                i=i+1

        
        if first_dest_role == second_dest_role:
                SendEmail('notify') 

        if first_dest_role != second_dest_role:
                         
                if first_dest_role == 'PRIMARY DATABASE':

                                primary = first_db_unique_name 
                                standby = second_db_unique_name 
                                
                if second_dest_role == 'PRIMARY DATABASE':

                                primary = second_db_unique_name 
                                standby = first_db_unique_name 
             
                print ("*****************************************************")
                print ("Primary is: " + primary)
                print ("Standby is: " + standby)

                config.primary=primary
                config.standby=standby
        
        cursSys.close
        CloseSysConnection(connection)

def SetPhase(pdb,phase):
        # Connection to CMDB 
        v_connection_string_pdb_ages_dba = username_dba + "/" + config.password_dba + "@" + config.pdb_tns_alias
       
        if phase=='DEPLOY':
                other_phase='NORMAL'        
        if phase=='NORMAL':
                other_phase='DEPLOY'

        connection = OpenCMDBConnection()
        cursCDBDB = connection.cursor()
        
        sqlsetActivePhase = 'update APP_AGES_SEC.DB_ROLE_PHASE set ACTIVE = 1 where pdb = '  + '\'' +  pdb + '\'' + ' and phase = ' + '\'' + phase  + '\''
     
        print(sqlsetActivePhase)
        cursCDBDB.execute(sqlsetActivePhase)
        
        sqlsetPassivePhase = 'update APP_AGES_SEC.DB_ROLE_PHASE set ACTIVE = 0 where pdb = ' + '\'' + pdb + '\'' + ' and phase = ' + '\'' + other_phase + '\''
        cursCDBDB.execute(sqlsetPassivePhase)

        sqlsetPassivePhase = 'update APP_AGES_SEC.DB_ROLE_PHASE set ACTIVE = 0 where pdb = '  + '\'' + pdb  + '\'' + ' and phase = '  + '\'' + other_phase + '\''
        cursCDBDB.execute(sqlsetPassivePhase)

        cursCDBDB.close
        CloseCMDBConnection(connection)   

        connection = OpenAGES_DBAConnection()
        cursAgesDBA = connection.cursor()
        
        sqlsetGrant = 'exec ages_sec.revoke_role_lock_privuser;'
        queryResult, errorMessage = runSqlQuery(sqlsetGrant, v_connection_string_pdb_ages_dba , v_extra)
       

        sqlsetGrant = 'exec ages_sec.grant_role_to_privuser;'
        queryResult, errorMessage = runSqlQuery(sqlsetGrant, v_connection_string_pdb_ages_dba , v_extra)

        cursAgesDBA.close
        CloseAGES_DBAConnection(connection) 

def SearchSql_ID(v_text):
     
        sqlSearchSql_ID = 'SELECT SQL_ID , dbms_lob.substr(sql_text,50,1) text FROM DBA_HIST_SQLTEXT WHERE dbms_lob.substr(sql_text,3500,1) like ' + '\'%' + v_text + '%\';' 
        print (sqlSearchSql_ID)
       
        v_extra = '\n'
        queryResult, errorMessage = runSqlQuery(sqlSearchSql_ID, config.v_connection_string_pdb_ages_dba , v_extra)
        print (queryResult)
        
def ShowExecutionPlanInfoforSql_ID(v_sql_id):
        
        ShowExecutionPlanInfo = 'SELECT SQL_ID , dbms_lob.substr(sql_text,50,1) text FROM DBA_HIST_SQLTEXT WHERE dbms_lob.substr(sql_text,3500,1) like ' + '\'%' + v_text + '%\';' 
        print (sqlSearchSql_ID)
        #print(config.v_connection_string_pdb_ages_dba)
       
        v_extra = '\n'
        queryResult, errorMessage = runSqlQuery(sqlSearchSql_ID, config.v_connection_string_pdb_ages_dba , v_extra)
        print (queryResult)

def resource_search(oci_object):

    dictAttribute_list_pdb={'availability_domain','display_name','identifier','resource_type'}
    search_client = oci.resource_search.ResourceSearchClient(Ociconfig)
    # Initialize service client with default config file
    resource_search_client = oci.resource_search.ResourceSearchClient(Ociconfig )
    #print(os.environ)
    search_resources_response = resource_search_client.search_resources(
    search_details=oci.resource_search.models.FreeTextSearchDetails(
             type="FreeText",
             text=oci_object)
   
    )
    # # Get the data from response
    #print(search_resources_response.data)
    free_text_search = oci.resource_search.models.FreeTextSearchDetails(text=oci_object,
                                                                        type='FreeText',
                                                                        matching_context_type=oci.resource_search.models.SearchDetails.MATCHING_CONTEXT_TYPE_HIGHLIGHTS)

    for response in oci.pagination.list_call_get_all_results_generator(search_client.search_resources, 'response', free_text_search):
        for resource in response.data.items:
            #print("**************************************************************************")
             
            if resource.resource_type == 'Database':
                get_database(resource.identifier) 
       
        config.df_all_cdb = config.df_all_cdb.sort_values(by=['db_unique_name'])
        print (config.df_all_cdb)
        
        
def get_pluggable_database (pdb_id):
        database_client = oci.database.DatabaseClient(Ociconfig)
        get_pluggable_database_response=database_client.get_pluggable_database(
        pluggable_database_id=pdb_id)
        pd_object= pd.read_json(str(get_pluggable_database_response.data))
        df = pd.DataFrame(pd_object,columns=['container_database_id','pdb_name','open_mode'] )
        df=df.iloc[0]
        v_cdb_id = df['container_database_id']
        config.cdb_open_mode = df['open_mode']
        #print(cdb_open_mode)
        get_database(v_cdb_id)

def get_database (db_id):
        database_client = oci.database.DatabaseClient(Ociconfig)
        get_database_response = database_client.get_database(db_id)
        cdb_object= pd.read_json(str(get_database_response.data))
        #print (cdb_object.columns)
        df_cdb = pd.DataFrame(cdb_object,columns=['db_unique_name','connection_strings'])
        df_cdb = df_cdb.reset_index(drop=True) 
        df_cdb = df_cdb.head(n=2)
        list_data_guard_associations_response = database_client.list_data_guard_associations(
                database_id=db_id 
                )
        cdb_object_dg= pd.read_json(str(list_data_guard_associations_response.data))
        df_cdb_dg = pd.DataFrame(cdb_object_dg,columns=['role'] )

        num = df_cdb.iloc[1]['connection_strings'].rfind(':')
        scan_name = df_cdb.iloc[1]['connection_strings'][0:num]

        if len(df_cdb_dg) > 0 and 'T' not in df_cdb.iloc[1]['db_unique_name'] and  df_cdb_dg.iloc[0]['role'] !='STANDBY':
                config.df_all_cdb.loc[len(config.df_all_cdb.index)] = [df_cdb.iloc[1]['db_unique_name'],scan_name,df_cdb_dg.iloc[0]['role'] ]
       
