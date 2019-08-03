from twilio.rest import Client
import mysql.connector
from django.conf.urls import url
import time
import sys


account_sid = 'ACe211cd3c60243dfe88947ba46d313ab5'
auth_token = '5993d6b53b0bac1c5170ea499cb52797'
client = Client(account_sid, auth_token)

mydb = mysql.connector.connect(
  host="10.98.50.30",
  user="root",
  passwd="Power#10",
  database="INTEGRATION"
)

mycursor = mydb.cursor()
mycursor.execute("SELECT IFID FROM nifi where Status ='0' AND IFID IN('m_mdm_ccej_tzktsd0005_txt_p','m_mdm_ccej_tzktsd0109_txt_p','m_mdm_ccej_tprodgrp_txt_p','m_mdm_a595_txt_p','m_mdm_lfa1_txt_p','m_mdm_ccej_h_knvv_txt_p','m_mdm_ccej_h_lfb1_txt_p','m_mdm_ccej_tzktsd0010_txt_p','t_vm_tldrundet_txt_p','t_vm_tlcommdetailitem_txt_p','m_cokeone_marm_txt_p','m_cokeone_t320_txt_p','m_cokeone_ccej_tzktfi0092_txt_p','m_cokeone_mara_txt_p','t_cokeone_VBRP_txt_p','t_cokeone_mseg_txt_p','t_cokeone_konv_txt_p','t_cokeonereporting_cccjc_compass_orc_p','t_cokeonereporting_superdaiwa_salesreport_orc_p','t_cokeonereporting_dmetool_reg_hdr_orc_p')")
myresultnifi = mycursor.fetchall()
print (myresultnifi),

if ( myresultnifi !=[]) :
    mycursor = mydb.cursor()
    mycursor.execute("SELECT url FROM nifi where Status ='0'")
    urlsqlnifi = mycursor.fetchall()

    call = client.calls.create(
                        
                        url=urlsqlnifi,
                        to='+817012893466',
                        from_='+16027867405',
                    )
    message = client.messages.create(
                              
                         body= myresultnifi ,
                         to='+817012893466',
                         from_='+16027867405',
                         )
    call = client.calls.create(
                        
                        url=urlsqlnifi,
                        to='+817012893466',
                        from_='+16027867405',
                    )
    message = client.messages.create(
                              
                         body= myresultnifi ,
                         to='+817012893466',
                         from_='+16027867405',
                         )

   
    
    
 
else :
 print('Table NIFI is ok ' )

mycursor = mydb.cursor()
mycursor.execute("SELECT GROUP_CONCAT(DISTINCT Process_Name SEPARATOR ', ') FROM API_Process_Exec  WHERE Status = 'ERROR'  AND date(timestamp) = CURRENT_DATE() GROUP BY Status")
myresultsales = mycursor.fetchall()
print(myresultsales)

if ( myresultsales !=[]) :
    mycursor = mydb.cursor()
    mycursor.execute("SELECT url FROM MACHINESALES where Status ='0'")
    urlsqlsales = mycursor.fetchall()
    
    
    message = client.messages.create(
                              
                         body= myresultsales ,
                         to='+817012893466',
                         from_='+16027867405',
                         )
   
    

    call = client.calls.create(
                        
                        url =urlsqlsales,
                        to='+817012893466',
                        from_='+16027867405',
                    
                    )
    message = client.messages.create(
                              
                         body= myresultsales ,
                         to='+817012893466',
                         from_='+16027867405',
                         )
    
    call = client.calls.create(
                        
                        
                        url=urlsqlsales,
                        to='+817012893466',
                        from_='+16027867405',
                    )
    
    
    
else :
  print('Table MACHINESALES is ok ' )

mycursor = mydb.cursor()
mycursor.execute("SELECT  CONCAT(status,'  ',id) FROM atomeboomi where status ='OFFLINE' AND date(timestamp) = CURRENT_DATE() ")
atome = mycursor.fetchall()
print(atome)
  

time.sleep(10)


if ( atome =='OFFLINE') :
    mycursor = mydb.cursor()
    mycursor.execute("SELECT url FROM atomeboomi where status ='OFFLINE' AND date(timestamp) = CURRENT_DATE() ")
    urlsqlsales = mycursor.fetchall()
    print(atome)
    
    message = client.messages.create(
                              
                         body= atome ,
                         to='+817012456726',
                         from_='+16027867405',
                         )
   
    

    call = client.calls.create(
                        
                        url =urlsqlsales,
                        to='+817012456726',
                        from_='+16027867405',
                    
                    )
    message = client.messages.create(
                              
                         body= atome ,
                         to='+817012893466',
                         from_='+16027867405',
                         )
    
    call = client.calls.create(
                        
                        
                        url=urlsqlsales,
                        to='+817012893466',
                        from_='+16027867405',
                    )
    



