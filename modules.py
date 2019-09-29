# -*- coding: utf-8 -*-

import pyodbc 
import pandas as pd
import config
from datetime import datetime
import time


# SQL Connection String
server = config.DATABASE_CONFIG['server']
database = config.DATABASE_CONFIG['database'] 
username = config.DATABASE_CONFIG['username'] 
password = config.DATABASE_CONFIG['password'] 

# Connect
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()


# Mapper
table_mapper = {
        'Factory Information':'dbo.table_2',
        'Salary Information': 'dbo.table_4',
        'Benefits':'dbo.table_5',
        'Occupation':'dbo.table_3',
        'Incentives':'dbo.table_6',
        'Deductions':'dbo.table_8',
        'Work Days/Hours': 'dbo.table_7'
        }

# Formatting Functions

def check_str(ch,limit = 254):
    if pd.isna(ch):
        ch = 'N/A'
    return ch[0:limit]

def check_float(num):
    if pd.isna(num):
        num = 0.0
    else:
        if type(num) == str:
            num = float(''.join(c for c in num if c.isdigit()))     
        elif type(num) == int:
            num = float(num)                    
    return num

def check_int(num):
    if pd.isna(num):
        num = 0
    else:
        if type(num) == str:
            num = int(''.join(c for c in num if c.isdigit()))     
        elif type(num) == float:
            num = int(num)                    
    return num
  
def dateformat(input1):
    if pd.isna(input1) == False:
        if len(input1) > 7:
            return(str(datetime.strptime(input1, '%Y/%m/%d').strftime('%Y-%m-%d')))
        else:
            return('N/A')
    else:
        return('N/A')




# Uploading Functions
def upload_table_2(data1):
    tab_2 = pd.DataFrame(columns={
    'fac_name','date_entry','uploader','email_address',
    'address','city','country','zip_code','local_currency',
    'number_super','number_non_super','number_women','number_men',
    'type_product','yearly_production','rest_days'    
     })
    
    nation = ''
    if pd.isna(data1.iloc[13,5]):
        nation = data1.iloc[12,5]
    else:
        nation = data1.iloc[13,5]
    
    entry_date = ''    
    entry_date = str(int(data1.iloc[5,7]))+'-'+str(int(data1.iloc[5,6]))+'-'+str(int(data1.iloc[5,5]))
    
    
    ad = data1.iloc[10,5]
    if pd.isna(data1.iloc[10,5]):
        ad = ''
    else:
        if len(ad) > 255:
            ad = data1.iloc[10,5][0:255]
            
    tp = data1.iloc[26,5]
    if pd.isna(data1.iloc[26,5]):
        tp = ''
    else:
        if len(tp) > 255:
            tp = data1.iloc[26,5][0:255]

    
    
    
    tab_2 = tab_2.append({
        'fac_name': check_str(data1.iloc[4,5]),
        'date_entry': entry_date,
        'uploader': check_str(data1.iloc[6,5]),
        'email_address': check_str(data1.iloc[7,5]),
        'address': ad,
        'city': check_str(data1.iloc[11,5]),
        'country':nation,
        'zip_code':check_str(data1.iloc[14,5]),
        'local_currency':check_str(data1.iloc[16,5]),
        'number_super':check_int(data1.iloc[19,5]),
        'number_non_super':check_int(data1.iloc[20,5]),
        'number_women':check_int(data1.iloc[22,5]),
        'number_men':check_int(data1.iloc[23,5]),
        'type_product': tp,
        'yearly_production':check_int(data1.iloc[27,5]),
        'rest_days':check_int(data1.iloc[30,5])       
    },ignore_index=True)
    
    tab_2 = tab_2[['fac_name','date_entry','uploader','email_address',
        'address','city','country','zip_code','local_currency',
        'number_super','number_non_super','number_women','number_men',
        'type_product','yearly_production','rest_days'  ]]

    return(tab_2)
    

def insert_rows_table2(tab_2,up,ut,ud,cnxn,cursor):
    q_ins = "INSERT INTO dbo.table_2 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    tab_2 = tab_2.fillna(0)
    for index,row in tab_2.iterrows():
        a1=row['fac_name']
        a2=row['date_entry']
        a3=row['uploader']
        a4=row['email_address']
        a5=row['address']
        a6=row['city']
        a7=row['country']
        a8=row['zip_code']
        a9=row['local_currency']
        a10=row['number_super']
        a11=row['number_non_super']
        a12=row['number_women']
        a13=row['number_men']
        a14=row['type_product']
        a15=row['yearly_production']
        a16=row['rest_days']
        
        
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                cursor.execute(q_ins,(up,ud,ut,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16))
                cnxn.commit()
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(1)
                
                
    return(0)                
                

def upload_table_3(data1):
    tab_3 = pd.DataFrame(columns={'occupation','description'})
    for i in range(0,4):
        if pd.isna(data1.iloc[7+3*i,5]) == False:
            oc = data1.iloc[7+3*i,5]
            if len(oc) > 255:
                oc = data1.iloc[7+3*i,5][0:255]
           
            od = check_str(data1.iloc[8+3*i,5])
            
            tab_3 = tab_3.append({'occupation':oc,'description':od},ignore_index=True)

    tab_3 = tab_3[['occupation','description']]
    tab_3
    return(tab_3)

    
def insert_rows_table3(tab_3,up,ut,ud,cnxn,cursor):
    q_ins =  "INSERT INTO dbo.table_3 VALUES(?,?,?,?,?)"
    tab_3 = tab_3.fillna(0)
    for index,row in tab_3.iterrows():
                
        a1 = row['occupation'] 
        a2 = row['description']
        
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                cursor.execute(q_ins,(up,ud,ut,a1,a2))
                cnxn.commit()
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
                
    return(0)
    
    
    
def upload_sheet_345(data):
    
    
    # This table indicates general information from sheets 3,4
    tab_4 = pd.DataFrame(columns={
    'occupation','pay_period','start','end',
    'number_workers','number_women','number_men',
    'total_regular_hours','total_ot','total_leave_paid',
    'total_regular_paid','total_ot_paid','total_leave_pay'
    })
    
    
    # This table indicates information for incentives
    tab_6 = pd.DataFrame(columns={
        'occupation','pay_period','start','end',
        'name_incentive','incentive_description','total_incentive_pay',
        'during_ot'
    })
        
    # This table indicates work hours and work days information
    tab_7 = pd.DataFrame(columns={
    'pay_period','start','end','regular_work_days','regular_work_hours'
    })
    
    # This table indicates information for deductions    
    tab_8 = pd.DataFrame(columns={
        'occupation','pay_period','start','end',
        'name_ded','ded_description','total_ded'
    })
    
    # Obtaining start and end dates 
    for data1 in data:
        y_s = str(int(data1.iloc[8,9]))
        m_s = str(int(data1.iloc[8,8]))
        d_s = str(int(data1.iloc[8,7]))
        s_date = y_s+'-'+m_s+'-'+d_s
        
        y_e = str(int(data1.iloc[8,12]))
        m_e = str(int(data1.iloc[8,11]))
        d_e = str(int(data1.iloc[8,10]))
        e_date = y_e+'-'+m_e+'-'+d_e
        
    
        ta = [0,1,2,5]
        for i in ta:
                if pd.isna(data1.iloc[14,5+i]) == False:
                    tab_4 = tab_4.append({
                     'occupation': data1.iloc[14,5+i], 'pay_period': data1.iloc[4,3].split('-')[1].strip(), 'start':s_date,'end':e_date ,     
                      'number_workers': data1.iloc[15,5+i],'number_women':data1.iloc[16,5+i],'number_men':data1.iloc[17,5+i],
                        'total_regular_hours':data1.iloc[21,5+i],'total_ot':data1.iloc[22,5+i],'total_leave_paid':data1.iloc[23,5+i],
                    'total_regular_paid':data1.iloc[27,5+i],'total_ot_paid':data1.iloc[28,5+i],'total_leave_pay':data1.iloc[29,5+i]
                            },ignore_index=True)


        
        # Generating table_6
        for i in ta:
            if pd.isna(data1.iloc[32,5+i]) == False: 
                oc = data1.iloc[32,5+i]
                for j in range(0,5):
                    if pd.isna(data1.iloc[33+5*j,5+i]) == False:
                        
                        ni = data1.iloc[33+5*j,5+i]
                        if ((len(ni) > 255)):
                            ni = data1.iloc[33+5*j,5+i][0:255]
                        
                        nd = data1.iloc[34+5*j,5+i]
                        if ((len(nd) > 255)):
                            nd = data1.iloc[34+5*j,5+i][0:255]
                            
                        tab_6 = tab_6.append({
                                'occupation':oc,'pay_period': data1.iloc[4,3].split('-')[1].strip(), 'start':s_date,'end':e_date,     
                                 'name_incentive':ni, 'incentive_description':nd,
                                'total_incentive_pay':data1.iloc[35+5*j,5+i],'during_ot':data1.iloc[36+5*j,5+i]
                            },ignore_index=True)

        # Generating table_8    
        for i in ta:
            if pd.isna(data1.iloc[59,5+i]) == False: 
                oc = data1.iloc[59,5+i]
                for j in range(0,5):
                    if pd.isna(data1.iloc[60+4*j,5+i]) == False:
                        if pd.isna(data1.iloc[61+4*j,5+i]):
                            des = ''
                        else:
                            des = data1.iloc[61+4*j,5+i]
                            if len(des) > 255:
                                des = data1.iloc[61+4*j,5+i][0:255]
                        
                        
                        nd = data1.iloc[60+4*j,5+i]
                        if len(nd) > 255:
                            nd = data1.iloc[60+4*j,5+i][0:255]
                        
                        tab_8 = tab_8.append({
                                'occupation':oc,'pay_period': data1.iloc[4,3].split('-')[1].strip(), 'start':s_date,'end':e_date,     
                                 'name_ded':nd, 'ded_description':des,
                                'total_ded':data1.iloc[62+4*j,5+i]
                            },ignore_index=True)
    
        # Generating table_7
        tab_7 = tab_7.append({
                 'pay_period': data1.iloc[4,3].split('-')[1].strip(), 'start':s_date,'end':e_date ,     
                  'regular_work_days':data1.iloc[10,7], 'regular_work_hours':data1.iloc[11,7]
                },ignore_index=True)
    
    
    # Rearranging columns for table_4
    tab_4 = tab_4[['occupation','pay_period','start','end',
    'number_workers','number_women','number_men',
    'total_regular_hours','total_ot','total_leave_paid',
    'total_regular_paid','total_ot_paid','total_leave_pay']]
    
    # Rearranging columns for table_6
    tab_6 = tab_6[['occupation','pay_period','start','end',
        'name_incentive','incentive_description','total_incentive_pay',
        'during_ot']]
                
    # Rearranging columns for table_7
    tab_7 = tab_7[['pay_period','start','end','regular_work_days','regular_work_hours']]
    
    # Rearranging columns for table_8
    tab_8 = tab_8[['occupation','pay_period','start','end',
         'name_ded','ded_description','total_ded']]
    
    return(tab_4,tab_6,tab_7,tab_8)

    
    
    
def insert_rows_table4(tab_4,up,ut,ud,cnxn,cursor):
    q_ins = "INSERT INTO dbo.table_4 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    tab_4 = tab_4.fillna(0)
    for index,row in tab_4.iterrows():
                
        a1 = row['occupation'] 
        a2 = row['pay_period']
        a3 = row['start']
        a4 = row['end']
        a5 = row['number_workers']
        a6 = row['number_women']
        a7 = row['number_men']
        a8 = row['total_regular_hours']
        a9 = row['total_ot']
        a10 = row['total_leave_paid']
        a11 = row['total_regular_paid']
        a12 = row['total_ot_paid']
        a13 = row['total_leave_pay']
                
                
              
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                cursor.execute(q_ins,(up,ud,ut,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13))
                cnxn.commit()
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
                
    return(0)
 

def upload_table5(data,benfit_type):
    tab_5 = pd.DataFrame(columns = {
    'type_benefit','name_benefit','benefit_des','amount_spent'
    })
    

    for i in range(0,5):
        if pd.isna(data.iloc[5+4*i,5]) == False:
            nb = check_str(data.iloc[5+4*i,5])
            bd = check_str(data.iloc[6+4*i,5])
                
            tab_5 = tab_5.append({
                 'type_benefit':benfit_type, 'name_benefit':nb,
                    'benefit_des':bd,'amount_spent':data.iloc[7+4*i,5]     
                },ignore_index=True)

    tab_5 = tab_5[['type_benefit','name_benefit','benefit_des','amount_spent']]    
    
    return(tab_5)


def insert_rows_table5(tab_5,up,ut,ud,cncx,cursor):
    q_ins = "INSERT INTO dbo.table_5 VALUES(?,?,?,?,?,?,?)"
    tab_5 = tab_5.fillna(0)
    for index,row in tab_5.iterrows():
                
        a1 = row['type_benefit'] 
        a2 = row['name_benefit']
        a3 = row['benefit_des']
        a4 = row['amount_spent']
        
                
                
              
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                cursor.execute(q_ins,(up,ud,ut,a1,a2,a3,a4))
                cnxn.commit()
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
                
    return(0)

 
def insert_rows_table6(tab_6,up,ut,ud,cncx,cursor):
    q_ins = "INSERT INTO dbo.table_6 VALUES(?,?,?,?,?,?,?,?,?,?,?)"
    tab_6 = tab_6.fillna(0)
    
    for index,row in tab_6.iterrows():
                
        a1 = row['occupation'] 
        a2 = row['pay_period']
        a3 = row['start']
        a4 = row['end']
        a5 = row['name_incentive']
        a6 = row['incentive_description']
        a7 = row['total_incentive_pay']
        a8 = row['during_ot']
    
        
                
                
              
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                cursor.execute(q_ins,(up,ud,ut,a1,a2,a3,a4,a5,a6,a7,a8))
                cnxn.commit()
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
                
    return(0)

def insert_rows_table7(tab_7,up,ut,ud,cncx,cursor):
    q_ins = "INSERT INTO dbo.table_7 VALUES(?,?,?,?,?,?,?,?)"
    tab_7 = tab_7.fillna(0)
    
    for index,row in tab_7.iterrows():
                
        a1 = row['pay_period']
        a2 = row['start']
        a3 = row['end']
        a4 = row['regular_work_days']
        a5 = row['regular_work_hours'] 
        
        
    
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                cursor.execute(q_ins,(up,ud,ut,a1,a2,a3,a4,a5))
                cnxn.commit()
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
                
    return(0)





def insert_rows_table8(tab_8,up,ut,ud,cncx,cursor):
    q_ins = "INSERT INTO dbo.table_8 VALUES(?,?,?,?,?,?,?,?,?,?)"
    tab_8 = tab_8.fillna(0)
    
    for index,row in tab_8.iterrows():
                
        a1 = row['occupation'] 
        a2 = row['pay_period']
        a3 = row['start']
        a4 = row['end']
        a5 = row['name_ded']
        a6 = row['ded_description']
        a7 = row['total_ded']
        
    
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                cursor.execute(q_ins,(up,ud,ut,a1,a2,a3,a4,a5,a6,a7))
                cnxn.commit()
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
                
    return(0)
