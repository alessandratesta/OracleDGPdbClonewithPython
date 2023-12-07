#!/usr/bin/env
## Make Imports
from subprocess import Popen, PIPE
import os
import logging
import sys
import getopt
import traceback
import cx_Oracle
import smtplib
from email.message import EmailMessage
from DBUsefulModuleToRunSQL_TEMP import OpenSysConnection
from DBUsefulModuleToRunSQL_TEMP import CloseSysConnection
from DBUsefulModuleToRunSQL_TEMP import LoopOnThewholeCDB
from DBUsefulModuleToRunSQL_TEMP import QueryTheCDB
from DBUsefulModuleToRunSQL_TEMP import resource_search
 

import config

def setCdbSqlCommand():
       CdbsqlCommand = "select name from v$pdbs order by name"
       return CdbsqlCommand

## Function to Execute Sql command using sqlplus
def runSqlQuery(sqlCommand, v_user_string, v_pdb):
   session =  Popen(["sqlplus", "-S", v_user_string], stdout=PIPE, stdin=PIPE,universal_newlines=True)
   session.stdin.write(sqlCommand)
   print ('Running Now')
   print (sqlCommand)
   return session.communicate()

def SendEmail(var_mail_type):
        msg = EmailMessage()
        me = "<<email address>>"

        you = "<<email address>>"
        if var_mail_type == 'notify':
                MailSub = 'Problem on TEMP Tablespace of PDB '  + config.pdbtarget + ''
                message = "This problem can occure after a switch over"
        
        msg['Subject'] = MailSub
        msg['From'] = me
        msg['To'] = you

        # Send the message via our own SMTP server.
        s = smtplib.SMTP('<<SMTP Server>>')
        msg.set_content(message)
        s.send_message(msg)
        s.quit()  


def CheckTemp():
        check = "EMPTY"
        try:
                connection = OpenSysConnection()
                cursSys = connection.cursor()
                sqlFindTempofUsers ='select local_temp_tablespace from dba_users where username = \'<<USER that exists in every PDB>>\''
                #print (sqlFindTempofUsers)
                for row in cursSys.execute(sqlFindTempofUsers):
                        user_temp_tbs_name = row[0]
                        sqlFindTempTbs ='select tablespace_name from dba_tablespaces where tablespace_name='  + '\'' + user_temp_tbs_name + '\'' + '' 
                        print ("user_temp=" + user_temp_tbs_name) 
                        for row in cursSys.execute(sqlFindTempTbs): 
                                #print (sqlFindTempTbs)
                                temp_tbs_name = row[0]
                                sqlFindTempFile = 'select count(*) num from dba_temp_files where tablespace_name=' + '\'' + temp_tbs_name + '\'' + '' 
                                
                                for row in cursSys.execute(sqlFindTempFile): 
                                        #print(row[0])
                                        #print(sqlFindTempFile)
                                        temp_datafile_check = row[0] 
                                        if temp_datafile_check == 0 :
                                                check == "EMPTY" 

                                        if temp_datafile_check != 0 :        
                                                check = "OK"   
                                                print("************Temp Datafiles OK******")
                                          
                        if   check == "EMPTY" :
                                SendEmail('notify')               
                                
                cursSys.close
                CloseSysConnection(connection)
        except  Exception as e:
                print(e)
                pass  


# Program Execution Starts here
############################################################################################################

def main(argv=None):
        
        username = 'oracle dba user'
        #password = os.environ['<<oracle dba user password>>']
        #password = "<<oracle dba user password>>"
        
        resource_search('CDB')
        df_all_cdb = config.df_all_cdb
  
        for config.counter in range(len(df_all_cdb)):
            #print (config.counter)
            #print(df_all_cdb.loc[config.counter, "db_unique_name"])
            cdbname = df_all_cdb.loc[config.counter, "db_unique_name"]
            role = df_all_cdb.loc[config.counter, "role"]
            print ('***************************')
            print (cdbname)
                 
            if role == 'PRIMARY'  :    
                tns_alias = cdbname + "_S"

                print (tns_alias)
                conn = cx_Oracle.connect(username, password,tns_alias) 
                curs = conn.cursor()
                CdbsqlCommand = setCdbSqlCommand()

                curs.execute(CdbsqlCommand)
                for row in curs:
                        #print (row)
                        str_row = str(row) 
                        str_row = str_row.replace('(','')
                        str_row = str_row.replace(')','')
                        str_row = str_row.replace(',','') 
                        str_row = str_row.replace('\'','')
                        config.pdbtarget = str_row
                        str_row = str_row + "_S"
                        print (str_row)
                
                        config.tns_alias = str_row
                        v_user_string=username + "/" + password + "@" + config.tns_alias
                        #print ("Pdb Iteration= " + config.tns_alias )
                        #sqlCommand = setSqlCommand()
                        #try:
                        CheckTemp()
                        
                conn.close      


if __name__ == "__main__":
    main(sys.argv)
