#!/usr/bin/python
## Make Imports
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
import ansible_runner
import time
 
user_with_dba_role = '<<user_with_dba_role>>'
username_sys = 'sys'

v_extra = '\n'
encoded_password = 'xxxxxxx'

def setCdbSqlCommand():
       CdbsqlCommand = "select name from v$pdbs order by name"
       return CdbsqlCommand

def setSqlCommand(sqlfilename):
        sqlCommand = '@' + sqlfilename
        return sqlCommand

## Function to Execute Sql command using sqlplus
def runSqlQuery(sqlCommand, v_user_string, v_extra):
   session =  Popen(["sqlplus", "-S", v_user_string], stdout=PIPE, stdin=PIPE,universal_newlines=True ,encoding='latin1',env={"nls_lang":"GERMAN_GERMANY.WE8MSWIN1252"})
   session.stdin.write('set serveroutput on; \n')
   session.stdin.write( v_extra )

   session.stdin.write(sqlCommand)
   
   
   return session.communicate()

def OpenCMDBConnection():
   tns_alias_cdb = 'CMDB_S'
   username_of_config_rep02 = '<<username_of_config_rep02>>'
   #password_of_config_rep02 = 'Insertpwd'
   connCMDB = cx_Oracle.connect(username_of_config_rep02, password_of_config_rep02,tns_alias_cdb)
   return connCMDB
   
def CloseCMDBConnection(var_connection):     
   var_connection.close()
   
class Transcript(object):

    def __init__(self, filename):
        self.terminal = sys.stdout
        self.logfile = open(filename, "a")

    def write(self, message):
        self.terminal.write(message)
        self.logfile.write(message)

    def flush(self):
       pass  

    def fileno(self):
        return 1    
       
def start(filename):
    """Start transcript, appending print output to given filename"""
    sys.stdout = Transcript(filename)

def stop():
    """Stop transcript and return print functionality to normal"""
    sys.stdout.logfile.close()
    sys.stdout = sys.stdout.terminal
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'



def SendEmail(var_mail_type):
        msg = EmailMessage()
        me = "<<Sender>>"
        you =  "<<Receiper>>"
        if var_mail_type == 'start':
                MailSub = 'Python Clone(Called by Jenkins) from ' + cdbsource + ':' + pdbsource + ' to ' + cdbdest + ':' + pdbdest + ' started'
                message = "Clone started"

        if var_mail_type == 'Clonefinish':
                MailSub = 'Python Clone(Called by Jenkins) from ' + cdbsource + ':' + pdbsource + ' to ' + cdbdest + ':' + pdbdest + ' finished'
                message = "Clone finished...Backup still running."
        
        if var_mail_type == 'Backupfinish':
                MailSub = 'Python Clone(Called by Jenkins) from ' + cdbsource + ':' + pdbsource + ' to ' + cdbdest + ':' + pdbdest + ' Backup has also finished'
                message = "Backup after Clone finished."
        
        msg['Subject'] = MailSub
        msg['From'] = me
        msg['To'] = you

        # Send the message via our own SMTP server.
        s = smtplib.SMTP('<<smtp server>>')
        
       
        msg.set_content(message)
        s.send_message(msg)
        s.quit()  

def DropDestPdb():   
        #If the destination PDB already exists close and drop it 
        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()
        sqlCheckPDBexist = 'select *  from v$pdbs where name = :var_pdbdest '
        curs.execute(sqlCheckPDBexist,var_pdbdest=pdbdest)
        
        row = curs.fetchall()
        
        for r in row:
               
                 sqlCloseDestPdb ='alter pluggable database '  + pdbdest + ' close immediate instances=all' 
                 print(sqlCloseDestPdb)
                 curs.execute(sqlCloseDestPdb) 
                 sqlDropDestPdb = 'drop pluggable database ' + pdbdest + ' including datafiles'
                 print(sqlDropDestPdb)
                 curs.execute(sqlDropDestPdb)  
                 curs.close

def CreateCloneDbLink():
        #Check if the dblink already exists do NOT create it
        sqlCheckCloneDbLinkexist = 'select count(*)  from dba_db_links where db_link like :var_dblink'
        CloneDbLink = cdbsource + '_S_DBL%' 
        #print (CloneDbLink)
        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()
       
        Existslink = 0
        print (sqlCheckCloneDbLinkexist)
 
        for row in curs.execute(sqlCheckCloneDbLinkexist,var_dblink=CloneDbLink):  
                Existslink = row[0]
        if Existslink == 0:
                sqlCreateDBLink = 'CREATE DATABASE LINK ' + cdbsource + '_S' + '_DBL CONNECT TO C##REMOTE_CLONE_USER IDENTIFIED BY '  + '\"' + password_sys + '\"' + '  USING \'' + cdbsource + '_S'+ '\'' 
                print (sqlCreateDBLink.replace(password_sys,encoded_password))
                curs.execute(sqlCreateDBLink)
        curs.close
        conn.close

def CreteAdmServiceforDestPdb():
        print( "*****************Create ADM Service of target PDB*****************" )
        if servicetpye == 'RACSERVICE':
                var_playbook = '<<Path>>/CreateRacService.yml'
        if servicetpye == 'NORACSERVICE':        
                var_playbook = '<<Path>>/CreateService.yml'
        
        var_option_name = '--extra-vars'
        
        instance_base_name = cdbdest[2:] + 'R'
        print (dbname)
        
        servicename = pdbdest + "_ADM_S"
        var_option_value = 'variable_host=' + destination_host +  ' cdb_unique_name=' + cdbdest + ' pdb_name=' + pdbdest + ' cdb_name=' + instance_base_name  + ' service_name=' + servicename
        #print (var_option_value)
        out, err, rc = ansible_runner.run_command(
        executable_cmd='ansible-playbook',
        cmdline_args = [var_playbook,var_option_name,var_option_value],
        input_fd=sys.stdin,
        output_fd=sys.stdout,
        error_fd=sys.stderr
        )               

def CopyKeystoretoStandby(p_source_host,p_source_path,p_dest_host,p_dest_path):
        
        #ansible-playbook CopytoStandby.yml --extra-vars "source_host=host01-01 source_path=<<path_to_wallet>>/keystore01.out dest_host=host02-01 dest_path=<<path_to_wallet>>"        
        var_playbook = '<<path>>/CopytoStandby.yml'
        var_option_name = '--extra-vars'
    
        var_option_value = 'variable_host=' + p_source_host + ' source_host=' + p_source_host +  ' source_path=' + p_source_path + ' dest_host=' + p_dest_host + ' dest_path=' + p_dest_path
        print (var_option_value)
        out, err, rc = ansible_runner.run_command(
        executable_cmd='ansible-playbook',
        cmdline_args = [var_playbook,var_option_name,var_option_value],
        input_fd=sys.stdin,
        output_fd=sys.stdout,
        error_fd=sys.stderr
        )  

def CreateAppServices():
  
        connection = OpenCMDBConnection()
        cursCDBDB = connection.cursor()
        print ( "*****************Create Service for PDB Target*****************"  )
        sqlReadServices = 'select D1_SERVICE_NAME from <<table_of_the_rac_services>> where pdb = :var_pdb_dest' 
        
        if cdbdest == '<<unique_cdb_name>>'  :
                instance_base_name = cdbdest[2:] + 'R'
                print (dbname)
        

        #dbname = cdbdest
        #print (dbname)
        for row in cursCDBDB.execute(sqlReadServices,var_pdb_dest=v_pdb):
                servicename = row[0] 
                print ("Service: " + servicename)
                #instance_base_name = cdbdest + 'R'
                #print("Create Service of target PDB")
                if servicetpye == 'RACSERVICE':
                        var_playbook = 'CreateRacService.yml'
                        var_playbook = '<<path>>/CreateRacService.yml'
                var_option_name = '--extra-vars'
                var_option_value =  'variable_host=' + destination_host + ' cdb_unique_name=' + cdbdest + ' pdb_name=' + pdbdest + ' cdb_name=' + instance_base_name + ' service_name=' + servicename
                #print (var_option_value)
                out, err, rc = ansible_runner.run_command(
                executable_cmd='ansible-playbook',
                cmdline_args = [var_playbook,var_option_name,var_option_value],
                input_fd=sys.stdin,
                output_fd=sys.stdout,
                error_fd=sys.stderr
                )
                      

        cursCDBDB.close
        CloseCMDBConnection(connection)


def DiskBackup():

         print( "****Disk Backup of target PDB****" )
         var_playbook = '<<path>>/DiskBackup.yml'
         var_option_name = '--extra-vars'
         var_option_value = 'variable_host=' + destination_host + ' cdb_name=' + cdbdest + ' pdb_name=' + pdbdest
         print (var_option_value)

         out, err, rc = ansible_runner.run_command(
                 executable_cmd='ansible-playbook',
                 cmdline_args = [var_playbook,var_option_name,var_option_value],
                 input_fd=sys.stdin,
                 output_fd=sys.stdout,
                 error_fd=sys.stderr
         )
       


def LegatoBackup():
        print( "****Legato Backup of target PDB****" )
        var_playbook = '<<path>>/LegatoBackup.yml'
        var_option_name = '--extra-vars'
        var_option_value = 'variable_host=' + destination_host + ' cdb_name=' + cdbdest + ' pdb_name=' + pdbdest
        

        out, err, rc = ansible_runner.run_command(
                executable_cmd='ansible-playbook',
                cmdline_args = [var_playbook,var_option_name,var_option_value],
                input_fd=sys.stdin,
                output_fd=sys.stdout,
                error_fd=sys.stderr
        )        

def PatchPdb():
        print( "Patch the target Pdb" )
        pdbdesttras = pdbdest + '_transient'
        var_playbook = '<<path>>/PatchPdb.yml'
        var_option_name = '--extra-vars'
        var_option_value = 'variable_host=' + destination_host + ' cdb_name=' + cdbdest + ' pdb_name=' + pdbdesttras
        

        out, err, rc = ansible_runner.run_command(
                executable_cmd='ansible-playbook',
                cmdline_args = [var_playbook,var_option_name,var_option_value],
                input_fd=sys.stdin,
                output_fd=sys.stdout,
                error_fd=sys.stderr
        )

def CloneRemotePdb():
        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()
        print( "***********************Create Pluggable database: Remote Cloning******************************" )
        sqlCloneRemotePdb =' CREATE PLUGGABLE DATABASE ' + pdbdest + ' FROM ' + pdbsource + '@' + cdbsource + '_S_DBL' +' PARALLEL 1 ' +' KEYSTORE IDENTIFIED BY  '  + '\"' + password_sys + '\"'  
        print (sqlCloneRemotePdb.replace(password_sys,encoded_password))

        curs.execute(sqlCloneRemotePdb)
        sqlOpenPdb = ' ALTER PLUGGABLE DATABASE ' + pdbdest + ' OPEN INSTANCES=ALL '
        print (sqlOpenPdb)
        curs.execute(sqlOpenPdb)
        sqlSetContainer = 'ALTER SESSION SET CONTAINER=' + pdbdest +''
        print (sqlSetContainer)
        curs.execute(sqlSetContainer)

        sqlRekey = 'ADMINISTER KEY MANAGEMENT SET KEY FORCE KEYSTORE IDENTIFIED BY '  + '\"' + password_sys + '\"' + ' with backup CONTAINER=CURRENT' ;
        print (sqlRekey.replace(password_sys,encoded_password))
        curs.execute(sqlRekey)
       
        sqlSetJobs = 'alter system set job_queue_processes=0 sid=\'*\' '
        print (sqlSetJobs)
        curs.execute(sqlSetJobs)
        curs.close
        conn.close 

def SetXmlRepository():
                # XML Repository
                # Connection to CMDB 
                connection = OpenCMDBConnection()
                cursCDBDB = connection.cursor()
                        
                sqlReadPDBXmlSettings = 'select * from CLONE_XML_REP where pdb = :var_pdb_dest'
                
                check_exists_Xml = 0
                check_xml = 0
                v_extra = '\n'
                for row in cursCDBDB.execute(sqlReadPDBXmlSettings,var_pdb_dest=v_pdb):
                
                                check_exists_Xml = 1
                                pdb_xml_port_name = row[1]
                                pdb_xml_port_number = str( row[2] )
                                #print (v_extra)
                                v_extra = v_extra +'define ' + pdb_xml_port_name + ' = ' + pdb_xml_port_number + ';\n'
                                check_xml = 1
                cursCDBDB.close
                CloseCMDBConnection(connection)
               

def CommonPostTaskSys():                
                print ( "*****************sys connection at PDB level*****************" )
                print (v_connection_string_pdb_sys.replace(password_sys,encoded_password))
               
                SetXmlRepository()

                
                if check_exists_Xml == 1 :
                        #print (v_extra)
                        sqlfilename = home_dir + 'sql/postexec_xml_repository.sql'
                        sqlCommand = setSqlCommand(sqlfilename)
                        #alter session set "_ORACLE_SCRIPT" = true;
                        #alter session set "_ORACLE_SCRIPT" = false;
                        
                        print ("Set Xml Repository")
                        # Connection as SYS PDB level and run:
                        queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_sys , v_extra)
                        print ("Running: " + sqlfilename )
                        print (queryResult)

                 # Connection to PDB 
                print (tns_alias_pdb)
                conn = cx_Oracle.connect(username_sys, password_sys,tns_alias_pdb,mode=cx_Oracle.SYSDBA)
                curs = conn.cursor()

                dbname = cdbdest[2:] + 'R'
                
                pdbsourceSrvformat = pdbsource[0:len(pdbsource)-1] + '_' + pdbsource[-1]
               
                sqlFindSourceServices = 'select name from dba_services where name like '  + '\'' +  pdbsource + '%' + '\'' +  ' or name like ' + '\'' +  pdbsourceSrvformat + '%'  + '\'' ;
                print (sqlFindSourceServices)              

                print ('Deleting services coming from the source')
                # Changing password
                for row in curs.execute(sqlFindSourceServices):
                     
                        SourceServName = row[0]
                        print ('Deleting service:' + pdbsourceSrvformat)
 
                        # Connection as SYS PDB level and run:  
                        sqlCommand = 'exec DBMS_SERVICE.delete_service(service_name => ' + '\'' + SourceServName + '\'' + ')' + ' ;'
                        print (sqlCommand)
                        queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_sys , v_extra)
                        print (queryResult)
                        
                curs.close
                conn.close        

                sqlfilename = home_dir + 'sql/grants2_ages_dba.sql'
                print (sqlfilename)
                sqlCommand = setSqlCommand(sqlfilename)
                queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_sys , v_extra)
                print ('giving grants to user with dba role')
                print (queryResult)


def CommonPostTaskUser_with_dba_role():


                # Connection as user with dba role at PDB level and run:
                print ( "***************** connection at PDB level with user with dba role*****************" )

                v_extra = '\n'                
        
                v_user_string = user_with_dba_role + "/" + password_of_user_with_dba_role + "@" + tns_alias_pdb 
                print (v_user_string.replace(password_of_user_with_dba_role,encoded_password))
                v_pdb = pdbdest
                print ('setting global names to false' )
                sqlfilename = home_dir + 'sql/set_global_names_false.sql'
                sqlCommand = setSqlCommand(sqlfilename)
                queryResult, errorMessage = runSqlQuery(sqlCommand, v_user_string , v_extra)
                print (sqlfilename)
                print (queryResult)

                print ('deleting db links coming from source' )
                sqlfilename = home_dir + 'sql/postexec_GENERIC_dba-del_dblinks.sql'
                sqlCommand = setSqlCommand(sqlfilename)
                queryResult, errorMessage = runSqlQuery(sqlCommand, v_user_string , v_extra)
                print (sqlfilename)
                print (queryResult)

                print ('creating db links to configuration repositories' )
                conn = cx_Oracle.connect(user_with_dba_role, password_of_user_with_dba_role,tns_alias_pdb)
                curs = conn.cursor()
                sqlCreateCmdbDBLink = 'CREATE DATABASE LINK <<Link to the config repo schema>> CONNECT TO APP_pck_sec IDENTIFIED BY '  + '\"' + password_of_config_rep01 + '\"' + ' USING \'<<tns_alias_of_config_rep>>\''
                print (sqlCreateCmdbDBLink.replace(password_of_config_rep01,encoded_password))
                curs.execute(sqlCreateCmdbDBLink)
                curs.close
                conn.close


                print ('generic PDB tasks' )
                sqlfilename = home_dir + 'sql/ages_dba_generic_PDB.sql'
                sqlCommand = setSqlCommand(sqlfilename)
                queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_user_with_dba_role , v_extra)
                print (sqlfilename)
                print (queryResult)

                # Connection to CMDB 
                connection = OpenCMDBConnection()
                cursCDBDB = connection.cursor()

                sqlReadPDBpwd = 'select *  from CLONE_DB_USERS where pdb = :var_pdb_dest'
                v_pdb = pdbdest

                print ('Changing application schema password' )
                # Changing password
                for row in cursCDBDB.execute(sqlReadPDBpwd,var_pdb_dest=v_pdb):
                     
                        pdb_user_name = row[1]
                        pdb_user_name_enc_pwd = bytes(row[2], 'ascii')
                        
                        pdb_user_name_pwd = fernet.decrypt(pdb_user_name_enc_pwd).decode()
                        
                  
                        sqlCommand = 'alter user ' + pdb_user_name + ' identified by ' + '\"' + pdb_user_name_pwd + '\" ;'
                        print (sqlCommand.replace(pdb_user_name_pwd,encoded_password))
                      
                        queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_user_with_dba_role , v_extra)
                        print (queryResult)
                        
                cursCDBDB.close
                CloseCMDBConnection(connection)

                connection = OpenCMDBConnection()
                cursCDBDB = connection.cursor()
                 # Changing DbLinks
                
                sqlReadPDBpwd = 'select B.password , A.* from CLONE_DB_LINKS A, CLONE_DB_USERS B where B.username=A.owner and A.pdb=B.pdb and B.pdb = :var_pdb_dest'
                print (sqlReadPDBpwd)

                v_pdb = pdbdest
                print ( "Creating application DB links and test it"  )
                for row in cursCDBDB.execute(sqlReadPDBpwd,var_pdb_dest=v_pdb):
                       
                        dblinkownwerpwd =  bytes(row[0], 'ascii')
                        dblinkownwerpwd = fernet.decrypt(dblinkownwerpwd).decode()

                        dblinkownwer = row[2]
                        dblink_name = row[3]
                        dblink_remoteuser  = row[4]
                       
                        dblink_remoteuserpwd =  bytes(row[5], 'ascii')
                        dblink_remoteuserpwd = fernet.decrypt(dblink_remoteuserpwd).decode()

                        dblink_host = row[6]
                        
                        # connection to pdb with schema owner
                        
                        sqlCommand = ' CREATE DATABASE LINK ' +  dblink_name + ' CONNECT TO ' + dblink_remoteuser + ' IDENTIFIED BY ' + '\"' + dblink_remoteuserpwd + '\"' + ' USING ' + '\'' + dblink_host + '\'' + ';'
                        
                        print (sqlCommand.replace(dblink_remoteuserpwd,encoded_password))
                        v_connection_string_pdb_schema_owner = dblinkownwer + "/" + dblinkownwerpwd + "@" + tns_alias_pdb
                        queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_schema_owner , v_extra)
                        print (queryResult)

                        sqlCommand = 'select * from dual@'+ dblink_name  + ';'
                        print (sqlCommand)
                        queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_schema_owner , v_extra)
                        print (queryResult)

                # create  oracle directory needed by application and give the grant on it to application user and role        
                print ( "Create or Replace application Oracle Directory"  )
                sqlReadPDBDir="select * from CLONE_APP_DIRECTORY where pdb = :var_pdb_dest"
                for row in cursCDBDB.execute(sqlReadPDBDir,var_pdb_dest=v_pdb):
                        dir_name = row[1]
                        dir_path = row[2]

                        sqlCommand = 'CREATE OR REPLACE DIRECTORY '  +  dir_name  + ' AS '  + '\'' + dir_path  + '\'' + ' ;'
                        print (sqlCommand)
                        queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_user_with_dba_role , v_extra)
                        print (queryResult)

                print ("Grant on application Oracle Directory")
                sqlReadPDBDirGrant="select B.GRANTED,B.PERMISSION,nvl(B.EXTRA,'N'), A.DIRECTORY_NAME from CLONE_APP_DIRECTORY A,CLONE_APP_DIRECTORY_GRANTS B where A.DIR_NUM=B.CLONE_APP_DIR_NUM and A.pdb = :var_pdb_dest"
                for row in cursCDBDB.execute(sqlReadPDBDirGrant,var_pdb_dest=v_pdb):
                        granted = row[0]
                        permission = row[1]
                        extra = row[2]
                        directoryname= row[3]
                        if extra != 'N':
                                sqlCommand = 'GRANT ' + permission + ' ON DIRECTORY ' + directoryname + ' TO ' + granted +  ' ' + extra +  ' ;'
                        if extra == 'N':
                                sqlCommand = 'GRANT ' + permission + ' ON DIRECTORY ' + directoryname + ' TO ' + granted  +  ' ;'
                        
                                print (sqlCommand)
                        queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_user_with_dba_role , v_extra)
                        print (queryResult)
                
                # Remove prevoius application oracle directory java permission and grant no one
                print ( "*****************Remove application oracle directory java permission coming from source and grant the ones that are appropriate for the target*****************")
                
 
                sqlCommand = 'exec pck_clone.set_pdb_java_permission;'
                print (sqlCommand)
                queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_user_with_dba_role , v_extra)
                print (queryResult)

                cursCDBDB.close
                CloseCMDBConnection(connection)

                print ("*****************Remove acls coming from source and create the ones that are appropriate for the target*****************")

                v_extra = v_extra + 'col host format a30'  + ';\n'
                v_extra = v_extra + 'col PRINCIPAL format a20'  + ';\n'
                
                print ('Acls coming from source' )
                sqlCommand = 'select HOST,LOWER_PORT,UPPER_PORT,PRINCIPAL from DBA_HOST_ACES order by HOST,LOWER_PORT;'
                queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_user_with_dba_role , v_extra)
                print (queryResult)

               
                v_pdb = pdbdest
                sqlCommand = 'exec pck_clone.set_my_acl;'
                queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_user_with_dba_role , v_extra)
                print (queryResult)

                print ('Acls after deleting all acls and creating the acl for target' )
                v_extra = v_extra + 'col host format a30'  + ';\n'
                v_extra = v_extra + 'col PRINCIPAL format a20'  + ';\n'
                v_extra = v_extra + 'col PRIVILEGE format a10'   + ';\n'
                
                sqlCommand = 'select HOST,LOWER_PORT,UPPER_PORT,PRINCIPAL from DBA_HOST_ACES order by HOST,LOWER_PORT,PRIVILEGE;'
                queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_user_with_dba_role , v_extra)
                print (queryResult)        

                print ( "*****************Revoke source grant and give destination grant to the personalized user*****************" )
                # 6)  revoke source grant and give destination grant to the personalized user
                sqlCommand = 'exec pck_sec.revoke_role_lock_privuser;'
                print (sqlCommand)
                queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_user_with_dba_role , v_extra)
                print (queryResult)

                sqlCommand = 'exec pck_sec.grant_role_to_privuser;' 
                print (sqlCommand)
                queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_user_with_dba_role , v_extra)
                print (queryResult)

        

                curs.close()
                conn.close

def SpecificPostTaskSchemaOwner():
                # Connection to the PDB where the configuration tables resides 
                connection = OpenCMDBConnection()
                cursCDBDB = connection.cursor()
                cursCDBDBExtra = connection.cursor()
                v_extra = '\n' 

                sqlReadPostCloneScript = 'select a.username,b.password,a.file_name,A.step from CLONE_POST_PDB_STEP A,  CLONE_DB_USERS B where B.username=A.username and A.pdb=B.pdb and active=1 and A.pdb = :var_pdb_dest order by step' 

                for row in cursCDBDB.execute(sqlReadPostCloneScript,var_pdb_dest=v_pdb):
                        print ( "*****************Application user connection at PDB level*****************")
                        poststepuser = row[0] 
               
                        poststepuserpwd =  bytes(row[1], 'ascii')
                        poststepuserpwd = fernet.decrypt(poststepuserpwd).decode()

                        poststepfilename = row[2]
                        poststep = row[3]

                        sqlfilename = home_dir + 'sql/' + v_pdb + '/' + poststepfilename
                        sqlCommand = setSqlCommand(sqlfilename)
                        print ('Running '+ sqlCommand)

                        v_connection_string_pdb_schema_owner = poststepuser + "/" + poststepuserpwd + "@" + tns_alias_pdb
                        
                       
                        print ('Connection: ' + v_connection_string_pdb_schema_owner.replace(poststepuserpwd,encoded_password))
                        v_extra = v_extra +'define pdb = ' + pdbdest + ';\n'
                        sqlReadExtraVariable= 'select var_name,var_value from CLONE_POST_PDB_STEP_VAR where pdb= :var_pdb_dest and step= :var_poststep'
                        for row in cursCDBDBExtra.execute(sqlReadExtraVariable,var_pdb_dest=v_pdb,var_poststep=poststep):
                                var_name = row[0]
                                var_value  = row[1]
                                v_extra = v_extra +'define ' + var_name + ' = ' + var_value + ';\n'       
                        
                        #print (v_extra)
                        queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_schema_owner , v_extra)
                        print (queryResult)
                
                cursCDBDBExtra.close
                cursCDBDB.close
                CloseCMDBConnection(connection)

def ClonePdbwithDataGuard():
        global primary_host,standby_host,wallet_path,standby_scanname,standby_db_unique_name,standby_db_name,tns_alias_standby

        print( "***********************Gathering Info from Primary******************************" )

        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()
        
        
        #Check if the dblink already exists.If NOT create it

        sqlCheckCloneDbLinkexist = 'select *  from dba_db_links where lower(db_link) like :var_dblink'
        
         
        Existslink = 0
        for row in curs.execute(sqlCheckCloneDbLinkexist,var_dblink='my_copy_link%'):  
                Existslink = row[0]
        if Existslink == 0 and localclone == 0:

                sqlCreateSelfDbLink = ' create  database link my_copy_link connect to system identified by '  + '\"' + password_sys + '\"'  + ' using \'' + cdbdest + '\''
                print (sqlCreateSelfDbLink.replace(password_sys,encoded_password))
                curs.execute(sqlCreateSelfDbLink)
         

        #Find the standby dbname
        sqlFindStandbyDBName= 'select db_unique_name from V$DATAGUARD_CONFIG where DEST_ROLE=\'PHYSICAL STANDBY\''
        print (sqlFindStandbyDBName)
        for row in curs.execute(sqlFindStandbyDBName):
                standby_name = row[0]

    
        #FindtheHostofPrimary
        sqlFindtheHostofPrimary='select HOST_NAME from V$instance'
        print (sqlFindtheHostofPrimary)
        for row in curs.execute(sqlFindtheHostofPrimary):
                primary_host = row[0]
                print ('primary_host is '+ primary_host)
        #FindtheWalletPath
        sqlFindWalletPath = 'select wrl_parameter from v$encryption_wallet where wrl_parameter is not null'
        print (sqlFindWalletPath)
        for row in curs.execute(sqlFindWalletPath):
                wallet_path = row[0]
                #print (wallet_path)

        curs.close
        conn.close

        if primary_host=='host01-1' or primary_host=='host01-2':
                 standby_scanname = "host01-scan" 
        if primary_host=='host02-1' or primary_host=='host02-2' :
                 standby_scanname = "host02-scan" 

        standby_serv_dg_ro = dbname + '_DG_RO'
       
        tns_alias_standby = standby_scanname + "/" + standby_serv_dg_ro
        print ('tns_alias_standby:' + tns_alias_standby)


        print( "***********************Prepare Standby Database******************************" )
        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias_standby,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()

        #Set parameter
        sqlSetParam = 'alter system set standby_pdb_source_file_dblink=\'my_copy_link\' scope=both sid=\'*\''
        print (sqlSetParam)
        curs.execute(sqlSetParam)

        #Find the standby host_name, unique_name, db_name
        sqlFindStandbyHost = 'SELECT SYS_CONTEXT(\'USERENV\',\'SERVER_HOST\'),SYS_CONTEXT(\'USERENV\',\'DB_UNIQUE_NAME\'), SYS_CONTEXT(\'USERENV\',\'DB_NAME\') FROM dual '
        print (sqlFindStandbyHost)
        for row in curs.execute(sqlFindStandbyHost):
                standby_host = row[0]
                standby_db_unique_name = row[1]
                standby_db_name = row[2]        

        curs.close
        conn.close
        
        wallet_file01= 'ewallet.p12'
        wallet_file02 = 'cwallet.sso'
        wallet_full_path01 = wallet_path + wallet_file01
        wallet_full_path02 = wallet_path + wallet_file02

        
        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias_source,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()

        sqlcheckVersionofSource = 'select version_full from v$instance'        
        for row in curs.execute(sqlcheckVersionofSource):
                SourceDBVersion = row[0]
       
        curs.close
        conn.close
        
        
        print( "***********************Connection on Primary******************************" )
        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()
        sqlcheckVersionofSource = 'select version_full from v$instance'
        for row in curs.execute(sqlcheckVersionofSource):
                TargetDBVersion = row[0]


        print( "***********************Create transient Pluggable database on Primary******************************" )
        transientpdb = pdbdest + '_transient'
        if localclone == 0:
                sqlCloneRemotePdb ='CREATE PLUGGABLE DATABASE ' + transientpdb + ' FROM ' + pdbsource + '@' + cdbsource + '_S_DBL' +' PARALLEL 1 ' +' KEYSTORE IDENTIFIED BY  '  + '\"' + password_sys + '\"'  + ' STANDBYS=NONE ' 
        if localclone == 1:
                sqlCloneRemotePdb ='CREATE PLUGGABLE DATABASE ' + transientpdb + ' FROM ' + pdbsource +' PARALLEL 1 '  +' KEYSTORE IDENTIFIED BY  '  + '\"' + password_sys + '\"'  + ' STANDBYS=NONE '
        print (sqlCloneRemotePdb.replace(password_sys,encoded_password))
        curs.execute(sqlCloneRemotePdb)
        
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y_%H_%M_%S")
        print (dt_string)

        sqlOpenPdb = 'ALTER PLUGGABLE DATABASE ' + transientpdb + ' OPEN INSTANCES=ALL '
        print (sqlOpenPdb)
        curs.execute(sqlOpenPdb)
        
        print ('SourceDBVersion is ' + SourceDBVersion )
        print ('TargetDBVersion is ' + TargetDBVersion)
        if SourceDBVersion != TargetDBVersion :

                sqlSetContainer = 'ALTER SESSION SET CONTAINER=' + transientpdb +''
                print (sqlSetContainer)
                curs.execute(sqlSetContainer)

                # sqlNoAudit1 = 'noaudit policy ORA_SECURECONFIG'
                # curs.execute(sqlNoAudit1)
                # sqlNoAudit2 = 'noaudit policy ORA_LOGON_FAILURES'
                # curs.execute(sqlNoAudit2)

                PatchPdb()

                sqlSetContainertoRoot = 'ALTER SESSION SET CONTAINER=CDB$ROOT'
                print (sqlSetContainertoRoot)
                curs.execute(sqlSetContainertoRoot)

                sqlOpenPdb = 'ALTER PLUGGABLE DATABASE ' + transientpdb + ' CLOSE IMMEDIATE INSTANCES=ALL '
                print (sqlOpenPdb)
                curs.execute(sqlOpenPdb)    

                sqlOpenPdb = 'ALTER PLUGGABLE DATABASE ' + transientpdb + ' OPEN INSTANCES=ALL '
                print (sqlOpenPdb)
                curs.execute(sqlOpenPdb)




        print( "***********************Re-Key on Primary******************************" )
        sqlSetContainer = 'ALTER SESSION SET CONTAINER=' + transientpdb +''
        print (sqlSetContainer)
        curs.execute(sqlSetContainer)

        sqlRekey = 'ADMINISTER KEY MANAGEMENT SET KEY FORCE KEYSTORE IDENTIFIED BY '  + '\"' + password_sys + '\"' + ' with backup CONTAINER=CURRENT' ;
        print (sqlRekey.replace(password_sys,encoded_password))
        curs.execute(sqlRekey)

        sqlSetContainertoRoot = 'ALTER SESSION SET CONTAINER=CDB$ROOT'
        print (sqlSetContainertoRoot)
        curs.execute(sqlSetContainertoRoot)


        # #COPY the Keystore from primary to standby and activate redolog shipping
       
        
   
        Backup_string = 'PythonBackup' 
        SqlBackupKeystore= 'ADMINISTER KEY MANAGEMENT BACKUP KEYSTORE USING' + '\'' + Backup_string + '\'' + ' FORCE KEYSTORE IDENTIFIED BY  '  + '\"' + password_sys + '\"'
        print (SqlBackupKeystore.replace(password_sys,encoded_password))
        curs.execute(SqlBackupKeystore)
        curs.close
        conn.close 

        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y_%H_%M_%S")
        print (dt_string)
 
        print( "***********************Copy Keystore ******************************" )
        CopyKeystoretoStandby(primary_host,wallet_full_path01,standby_host,wallet_path)
        CopyKeystoretoStandby(primary_host,wallet_full_path02,standby_host,wallet_path)

        print( "***********************Standby: Activate Log Transport ******************************" )
        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias_standby,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()

        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y_%H_%M_%S")
        print (dt_string)
        print ('Sleeping 10 minutes')
        time.sleep(600)
  
        try:
                sqlActivateRedoShipping = 'ALTER DATABASE RECOVER MANAGED STANDBY DATABASE DISCONNECT'
                print (sqlActivateRedoShipping)
                curs.execute(sqlActivateRedoShipping)
                
        except  Exception as e:
                print(e)
                pass    

        curs.close
        conn.close  
        print ('Sleeping 15 minutes')
        time.sleep(900)

  

    
        print( "***********************Primary: Connection ******************************" )

        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y_%H_%M_%S")
        print (dt_string)
        
        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()

        sqlClosePdb = 'ALTER PLUGGABLE DATABASE ' + transientpdb + ' CLOSE IMMEDIATE INSTANCES=ALL '
        print (sqlClosePdb)
        curs.execute(sqlClosePdb)

        sqlOpenPdb = 'ALTER PLUGGABLE DATABASE  ' + transientpdb + ' OPEN READ ONLY INSTANCES=ALL '
        print (sqlOpenPdb)
        curs.execute(sqlOpenPdb)

        print( "***********************Cold Clone******************************" )
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y_%H_%M_%S")
        print (dt_string)

        sqlCloneLocalPdb =' CREATE PLUGGABLE DATABASE ' + pdbdest + ' FROM ' + transientpdb +' PARALLEL 1 ' + ' KEYSTORE IDENTIFIED BY  '  + '\"' + password_sys + '\"' 
        print (sqlCloneLocalPdb.replace(password_sys,encoded_password))
        curs.execute(sqlCloneLocalPdb)

        CopyKeystoretoStandby(primary_host,wallet_full_path01,standby_host,wallet_path)
        CopyKeystoretoStandby(primary_host,wallet_full_path02,standby_host,wallet_path)

        time.sleep(600)

        ActivateRedoLogShipping()

        print( "***********************Primary: Connection ******************************" )

        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()

        sqlOpenPdb = 'ALTER PLUGGABLE DATABASE ' + pdbdest + ' OPEN INSTANCES=ALL '
        print (sqlOpenPdb)
        curs.execute(sqlOpenPdb)

        sqlSetContainer = 'ALTER SESSION SET CONTAINER=' + pdbdest +''
        print (sqlSetContainer)
        curs.execute(sqlSetContainer)

        sqlSetJobs = 'alter system set job_queue_processes=0 sid=\'*\' '
        print (sqlSetJobs)
        curs.execute(sqlSetJobs)


        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y_%H_%M_%S")
        print (dt_string)


        curs.close
        conn.close 
  
        curs.close
        conn.close 

       
        
        
def ActivateRedoLogShipping():
        print( "***********************Standby: Connection ******************************" )

        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias_standby,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()

        sqlActivateRedoShipping = 'ALTER DATABASE RECOVER MANAGED STANDBY DATABASE DISCONNECT NODELAY'
        print (sqlActivateRedoShipping)
        curs.execute(sqlActivateRedoShipping)
      

 
        curs.close
        conn.close


def DropTransientPdb():    
        
        print( "***********************Primary: Connection ******************************" )

        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()

        transientpdb = pdbdest + '_transient'

        sqlSetContainertoRoot = 'ALTER SESSION SET CONTAINER=CDB$ROOT'
        print (sqlSetContainertoRoot)
        curs.execute(sqlSetContainertoRoot)

        sqlClosePdb = 'ALTER PLUGGABLE DATABASE ' + transientpdb + ' CLOSE IMMEDIATE INSTANCES=ALL '
        print (sqlClosePdb)
        curs.execute(sqlClosePdb)

        sqlDropTransientPdb = 'DROP PLUGGABLE DATABASE ' + transientpdb + ' including datafiles'
        print (sqlDropTransientPdb)
        curs.execute(sqlDropTransientPdb)

        curs.close
        conn.close 

def CheckStandbyPdbFiles():

        print( "***********************Standby: Connection ******************************" )
        
        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias_standby,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()

        print ('Datafile status')
        if dataguardinfo == 'DATAGUARD':
                sqlSetContainertoPdb ='ALTER SESSION SET CONTAINER=' + pdbdest +''
                print (sqlSetContainertoPdb)
                curs.execute(sqlSetContainertoPdb)

                sqlCommand = 'select  con_id,file#,name from v$datafile;'
                v_extra = 'col name format a200'  + ';\n'
                v_extra = 'col file format a20'  + ';\n'
                v_extra = v_extra + 'set linesize 200'  + ';\n'
                v_extra = v_extra + 'set pagesize 100'  + ';\n'
                queryResult, errorMessage = runSqlQuery(sqlCommand, v_connection_string_pdb_sys , v_extra)
                print (queryResult)
        
        curs.close
        conn.close 

def EnableJobQueue():
        print( "***********************Set  job_queue_processes to 1000******************************" )
        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()

        sqlSetContainer = 'ALTER SESSION SET CONTAINER=' + pdbdest +''
        print (sqlSetContainer)
        curs.execute(sqlSetContainer)
        
        sqlSetJobs = 'alter system set job_queue_processes=1000 sid=\'*\' '
        print (sqlSetJobs)
        curs.execute(sqlSetJobs) 
        


        curs.close
        conn.close


# Program Execution Starts here
############################################################################################################
def main(argv=None):
        global destination_host, cdbsource, pdbsource, cdbdest, pdbdest,tns_alias, v_pdb, servicetpye, dataguardinfo, v_extra, check_exists_Xml, dbname, home_dir, tns_alias_source, tns_alias_standby,localclone,password_of_user_with_dba_role,password_sys,password_of_config_rep01,password_of_config_rep02,Fernet,fernet
        cdbsource = sys.argv[1]
        pdbsource = str(sys.argv[2])
        cdbdest = str(sys.argv[3])
        pdbdest = str(sys.argv[4])
        servicetpye = str(sys.argv[5])
        dataguardinfo= str(sys.argv[6])

        # In order to call the script with python. remember to remove the pwd after you work with this code
        password_sys= <<Insertpwd>>
        password_of_user_with_dba_role= <<Insertpwd>>
        password_of_config_rep01= <<Insertpwd>>
        password_of_config_rep02= <<Insertpwd>>
        key = <<InsertSecretKey>>
     
        # In order to call the python script with jenkins
        # password_sys = os.environ['SYS_DBA_USERPASS'] 
        # password_of_user_with_dba_role = os.environ['USER_DBA_USERPASS']
        # password_of_config_rep01 = os.environ['USER_SEC_CRED_USERPASS']
        # password_of_config_rep02 = os.environ['USER_CLONE_CRED_USERPASS']
        # key = os.environ['USER_PWD_SECRET_KEY']
        
        print (password_of_config_rep01)

        fernet = Fernet(key)
        localclone=0

        if cdbsource == cdbdest:
                localclone=1
        
        #where the python code and the yml files reside
        home_dir='<<base_path>>'  

        v_extra = '\n'
        check_exists_Xml = 0

        dbname = cdbdest[2:] + 'R'

        tns_alias = cdbdest + "_S"
        v_pdb = pdbdest

        tns_alias_source = cdbsource + "_S"

        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y_%H_%M_%S")
        print (dt_string)

        logfilename = 'Clone' + pdbsource + 'to' + pdbdest + '_' + dt_string
        start(home_dir + 'logs/' + logfilename + '.txt')

        print ('*****' + tns_alias)
        conn = cx_Oracle.connect(username_sys, password_sys,tns_alias,mode=cx_Oracle.SYSDBA)
        curs = conn.cursor()

        sqlFindScan='select value from v$parameter where name=\'remote_listener\''
        for row in curs.execute(sqlFindScan):
                scanname = row[0]

        #FindtheHostofPrimary
        sqlFindtheHostofPrimary='select HOST_NAME from V$instance'
        print (sqlFindtheHostofPrimary)
        for row in curs.execute(sqlFindtheHostofPrimary):
                destination_host = row[0]        
     
        global tns_alias_pdb,v_connection_string_pdb_sys,v_connection_string_pdb_user_with_dba_role
        tns_alias_pdb = scanname + "/" + pdbdest + "_ADM_S"
        v_connection_string_pdb_sys = username_sys + "/" + password_sys + "@" + tns_alias_pdb + " as sysdba"
        v_connection_string_pdb_user_with_dba_role = user_with_dba_role + "/" + password_of_user_with_dba_role + "@" + tns_alias_pdb

        
       

        SendEmail('start')
        DropDestPdb()
        CreateCloneDbLink()

        #OPEN: V$DATAGUARD_STATUS
        if dataguardinfo == 'DATAGUARD':
                 try:
                        ClonePdbwithDataGuard()
      
                 except  Exception as e:
                        print(e)
                        pass        
        #finally:
        if dataguardinfo == 'NODATAGUARD':
                 CloneRemotePdb()
        
        CreteAdmServiceforDestPdb()
        CommonPostTaskSys()
        v_extra = '\n'
        CommonPostTaskUser_with_dba_role()
        v_extra = '\n'
        SpecificPostTaskSchemaOwner()
        CreateAppServices()
        EnableJobQueue()
        SendEmail('Clonefinish')
        DiskBackup()
        LegatoBackup()
        
        if dataguardinfo == 'DATAGUARD':
                try:
                        ActivateRedoLogShipping()
                        time.sleep(600)
                        
                        # CheckStandbyPdbFiles()
                except  Exception as e:
                        print(e)
                        pass        
        DropTransientPdb()
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y_%H_%M_%S")
        print (dt_string)
        SendEmail('Backupfinish')
        
    

if __name__ == "__main__":
    main(sys.argv)
