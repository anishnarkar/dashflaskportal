# -*- coding: utf-8 -*-

from flask import Flask, request, render_template,redirect,url_for,session,g
import pyodbc 
import pandas as pd
import config
import os 
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



# Master userID
master_id = config.DATABASE_CONFIG['master']

# Initialize application
app = Flask(__name__)
app.secret_key = 'K8kZPDynLCYn7l6nIBUIWE5AKyN3'


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


    
    
@app.before_request
def before_request():
    g.user = None
    g.level = None
    if 'user' in session:
        g.user = session.get('user')
        g.level = int(session.get('auth_level'))
        
       
       




@app.route('/', methods=['GET','POST'])
def set_index():
    if request.method == 'POST':
        session.pop('user',None)
        
        q = "SELECT * FROM dbo.users"
        
        

        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                df_users = pd.read_sql(q,cnxn)
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)




        all_users = df_users['user_name'].unique().tolist()


        entered_user = request.form['userid']
        entered_password = request.form['password']
        
        if entered_user in all_users:
            p1 = df_users[df_users['user_name'] == entered_user].iloc[0,1]
            
            auth_l = int(df_users[df_users['user_name'] == entered_user].iloc[0,4])
            
            if entered_password == p1: # This is equivalent to global access
                session['user'] = entered_user
                if entered_user == master_id:
                    session['auth_level'] = auth_l
                    return redirect(url_for('home'))
                else:
                    session['auth_level'] = auth_l
                    if auth_l == 2:
                        return redirect(url_for('home_auth_one'))
                    elif auth_l == 3:
                        return redirect(url_for('home_auth_two'))
                    elif auth_l == 4:
                        return redirect(url_for('home_auth_four'))
            
        
        else:
            return redirect(url_for('relogin'))
            
            
    return render_template('login.html')

@app.route('/relogin', methods = ['GET','POST'])
def relogin():
    if request.method == 'POST':
        session.pop('user',None)
        
        
        q = "SELECT * FROM dbo.users"
       
        retry_flag = True
        retry_count = 0
        while retry_flag :
            try:
                df_users = pd.read_sql(q,cnxn)
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
        
        
        
        
        
        all_users = df_users['user_name'].unique().tolist()
        
        entered_user = request.form['userid']
        entered_password = request.form['password']
        
        if entered_user in all_users:
            p1 = df_users[df_users['user_name'] == entered_user].iloc[0,1]
       
            auth_l = int(df_users[df_users['user_name'] == entered_user].iloc[0,4])
            
            if entered_password == p1: # This is equivalent to global access
                session['user'] = entered_user
                if entered_user == master_id:
                    session['auth_level'] = auth_l
                    return redirect(url_for('home'))
                else:
                    session['auth_level'] = auth_l
                    if auth_l == 2:
                        return redirect(url_for('home_auth_one'))
                    elif auth_l == 3:
                        return redirect(url_for('home_auth_two'))
                    elif auth_l == 4:
                        return redirect(url_for('home_auth_four'))
           
            
            else:
                return redirect(url_for('relogin'))
    
    return render_template('relogin.html')

@app.route('/home', methods=['GET','POST'])
def home():
    if  (g.level == 1):
        if request.method == 'POST':
            if request.form['submit_button'] == 'Add Users':
                return redirect(url_for('adduser'))
            if request.form['submit_button'] == 'View User Data':
                return redirect(url_for('viewdata'))
            elif request.form['submit_button'] == 'Delete User':
                return redirect(url_for('deleteuser'))
            elif request.form['submit_button'] == 'Upload Data':
                return redirect(url_for('uploaddata'))
            elif request.form['submit_button'] == 'View Data':
                return redirect(url_for('view_updated'))
            elif request.form['submit_button'] == 'View Upload Entries':
                return redirect(url_for('viewuploadentries'))
            elif request.form['submit_button'] == 'Delete Data':
                return redirect(url_for('deleteentries'))
            elif request.form['submit_button'] == 'Visualize Data':
                return redirect(url_for('underdevelopment'))
            elif request.form['submit_button'] == 'Add Benchmark':
                return redirect(url_for('addbenchmark'))
            elif request.form['submit_button'] == 'View Benchmark':
                return redirect(url_for('viewbenchmark'))
            elif request.form['submit_button'] == 'Delete Benchmark':
                return redirect(url_for('deletebenchmark'))
            elif request.form['submit_button'] == 'Log Off':
                return redirect(url_for('logout'))
            
            
        if request.method == 'GET':
            return render_template('home.html')
    
    
    return redirect(url_for('relogin'))


@app.route('/home_auth_one', methods=['GET','POST'])
def home_auth_one():
    if  (g.level == 2):
        if request.method == 'POST':
            
            if request.form['submit_button'] == 'Upload Data':
                return redirect(url_for('uploaddata'))
            elif request.form['submit_button'] == 'View Data':
                return redirect(url_for('view_updated'))
            elif request.form['submit_button'] == 'View Upload Entries':
                return redirect(url_for('viewuploadentries'))
            elif request.form['submit_button'] == 'Delete Data':
                return redirect(url_for('deleteentries'))
            elif request.form['submit_button'] == 'Visualize Data':
                return redirect(url_for('underdevelopment'))
            elif request.form['submit_button'] == 'Add Users':
                return redirect(url_for('addseconduser'))
            elif request.form['submit_button'] == 'View User Data':
                return redirect(url_for('viewdata'))
            elif request.form['submit_button'] == 'Delete User':
                return redirect(url_for('deleteuser'))
            elif request.form['submit_button'] == 'Add Benchmark':
                return redirect(url_for('addbenchmark'))
            elif request.form['submit_button'] == 'View Benchmark':
                return redirect(url_for('viewbenchmark'))
            elif request.form['submit_button'] == 'Delete Benchmark':
                return redirect(url_for('deletebenchmark'))
            elif request.form['submit_button'] == 'Log Off':
                return redirect(url_for('logout'))
            
            
        if request.method == 'GET':
            return render_template('home_auth_one.html')
    
    
    return redirect(url_for('relogin'))

@app.route('/home_auth_two', methods=['GET','POST'])
def home_auth_two():
    if g.level == 3:
        if request.method == 'POST':
            
            
            if request.form['submit_button'] == 'View Data':
                return redirect(url_for('view_updated'))
            elif request.form['submit_button'] == 'View Upload Entries':
                return redirect(url_for('viewuploadentries'))
           
            elif request.form['submit_button'] == 'Visualize Data':
                return redirect(url_for('underdevelopment'))
            elif request.form['submit_button'] == 'Log Off':
                return redirect(url_for('logout'))
            
            
        if request.method == 'GET':
            return render_template('home_auth_two.html')
    
    
    return redirect(url_for('relogin'))

@app.route('/home_auth_four', methods=['GET','POST'])
def home_auth_four():
    if  (g.level == 4):
        if request.method == 'POST':
            
            if request.form['submit_button'] == 'Upload Data':
                return redirect(url_for('uploaddata'))
            elif request.form['submit_button'] == 'View Data':
                return redirect(url_for('view_updated'))
            elif request.form['submit_button'] == 'View Upload Entries':
                return redirect(url_for('viewuploadentries'))
            elif request.form['submit_button'] == 'Delete Data':
                return redirect(url_for('deleteentries'))
            elif request.form['submit_button'] == 'Visualize Data':
                return redirect(url_for('underdevelopment'))
            elif request.form['submit_button'] == 'Log Off':
                return redirect(url_for('logout'))
            
            
        if request.method == 'GET':
            return render_template('home_auth_four.html')
    
    
    return redirect(url_for('relogin'))


@app.route('/adduser', methods=['GET','POST'])
def adduser():
    if g.level == 1:
        if request.method == 'POST':
            a = request.form['userid']
            b = request.form['password1']
            c = request.form['password2']
            
            q = "SELECT * FROM dbo.users"
        
        

            retry_flag = True
            retry_count = 0
            while retry_flag:
                try:
                    df_users = pd.read_sql(q,cnxn)
                    retry_flag = False
                except:
                    retry_count = retry_count + 1
                    time.sleep(3)
            
            all_users = df_users['user_name'].unique().tolist()
            
            if a not in all_users:
                
                if b == c :
                    if len(a)>5:
                    
                        q2= "INSERT INTO dbo.users VALUES (?,?,?,?,?,?)"
                    
                        retry_flag = True
                        retry_count = 0
                        while retry_flag:
                            try:
                                cursor.execute(q2,(a,b,c,g.user,2,'Upload, Visualize & SubUsers'))
                                cnxn.commit()
                                retry_flag = False
                            except:
                                retry_count = retry_count + 1
                                time.sleep(3)
                        return redirect(url_for('successentry'))
                else:
                    
                    return redirect(url_for('reenterpassword'))
            else:
                return redirect(url_for('reenterusernamelev1'))
                
                                 
            if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                else:
                    return redirect(url_for('home_auth_two'))

            
        return render_template('adduser.html')
    return redirect(url_for('unauthorized'))


@app.route('/reenterpassword',methods=['GET','POST'])
def reenterpassword():
    if g.level <3:
    
        if request.method == 'POST':
            a = request.form['userid']
            b = request.form['password1']
            c = request.form['password2']
            
            q = "SELECT * FROM dbo.users"
        
        

            retry_flag = True
            retry_count = 0
            while retry_flag:
                try:
                    df_users = pd.read_sql(q,cnxn)
                    retry_flag = False
                except:
                    retry_count = retry_count + 1
                    time.sleep(3)
            
            all_users = df_users['user_name'].unique().tolist()
            
            if a not in all_users:
                
                if b == c :
                    if len(a)>5:
                    
                        q2= "INSERT INTO dbo.users VALUES (?,?,?,?,?,?)"
                    
                        retry_flag = True
                        retry_count = 0
                        while retry_flag:
                            try:
                                cursor.execute(q2,(a,b,c,g.user,2,'Upload, Visualize & SubUsers'))
                                cnxn.commit()
                                retry_flag = False
                            except:
                                retry_count = retry_count + 1
                                time.sleep(3)
                        return redirect(url_for('successentry'))
                else:
                    
                    return redirect(url_for('reenterpassword'))
            else:
                return redirect(url_for('reenterusernamelev1'))
                
                                 
            if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                else:
                    return redirect(url_for('home_auth_two'))

            
        return render_template('reenterpassword.html')
    return redirect(url_for('unauthorized'))

        
@app.route('/reenterusernamelev1',methods=['GET','POST'])
def reenterusernamelev1():
    if g.level <3:
    
        if request.method == 'POST':
            a = request.form['userid']
            b = request.form['password1']
            c = request.form['password2']
            
            q = "SELECT * FROM dbo.users"
        
        

            retry_flag = True
            retry_count = 0
            while retry_flag:
                try:
                    df_users = pd.read_sql(q,cnxn)
                    retry_flag = False
                except:
                    retry_count = retry_count + 1
                    time.sleep(3)
            
            all_users = df_users['user_name'].unique().tolist()
            
            if a not in all_users:
                
                if b == c :
                    if len(a)>5:
                    
                        q2= "INSERT INTO dbo.users VALUES (?,?,?,?,?,?)"
                    
                        retry_flag = True
                        retry_count = 0
                        while retry_flag:
                            try:
                                cursor.execute(q2,(a,b,c,g.user,2,'Upload, Visualize & SubUsers'))
                                cnxn.commit()
                                retry_flag = False
                            except:
                                retry_count = retry_count + 1
                                time.sleep(3)
                        return redirect(url_for('successentry'))
                else:
                    
                    return redirect(url_for('reenterpassword'))
            else:
                return redirect(url_for('reenterusernamelev1'))
                
                                 
            if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                else:
                    return redirect(url_for('home_auth_two'))

            
        return render_template('reenterusernamelev1.html')
    return redirect(url_for('unauthorized'))
        

level_mapper = {'Upload & Visualize': 4, 'Visualize':3}
reverse_mapper = {4:'Upload & Visualize',3:'Visualize'}

@app.route('/addseconduser', methods=['GET','POST'])
def addseconduser():
    if g.level == 2:
        if request.method == 'POST':
            
                
            a = request.form['userid']
            b = request.form['password1']
            c = request.form['password2']
            d = request.form['choice']
            
            if request.method == 'POST':
                a = request.form['userid']
                b = request.form['password1']
                c = request.form['password2']
                
                q = "SELECT * FROM dbo.users"
            
            
    
                retry_flag = True
                retry_count = 0
                while retry_flag:
                    try:
                        df_users = pd.read_sql(q,cnxn)
                        retry_flag = False
                    except:
                        retry_count = retry_count + 1
                        time.sleep(3)
                
                all_users = df_users['user_name'].unique().tolist()
            
            
            
            
            if len(a) > 5:
                if a not in all_users:
                    if b == c:
                        q2= "INSERT INTO dbo.users VALUES (?,?,?,?,?,?)"
                        
                        retry_flag = True
                        retry_count = 0
                        while retry_flag:
                            try:
                                cursor.execute(q2,(a,b,c,g.user,level_mapper[d],d))
                                cnxn.commit()
                                retry_flag = False
                            except:
                                retry_count = retry_count + 1
                                time.sleep(3)
            
                        return redirect(url_for('successentry')) 
                    
                    else:
                        return redirect(url_for('secondincorrectpassword'))
                else:
                    return redirect(url_for('secondincorrectusername'))
            
            if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
           
            
            
    
            
            
        return render_template('addseconduser.html',levels = ['Upload & Visualize', 'Visualize'])
    return redirect(url_for('unauthorized'))


@app.route('/secondincorrectpassword', methods=['GET','POST'])
def secondincorrectpassword():
    if g.level == 2:
        if request.method == 'POST':
            
                
            a = request.form['userid']
            b = request.form['password1']
            c = request.form['password2']
            d = request.form['choice']
            
            if request.method == 'POST':
                a = request.form['userid']
                b = request.form['password1']
                c = request.form['password2']
                
                q = "SELECT * FROM dbo.users"
            
            
    
                retry_flag = True
                retry_count = 0
                while retry_flag:
                    try:
                        df_users = pd.read_sql(q,cnxn)
                        retry_flag = False
                    except:
                        retry_count = retry_count + 1
                        time.sleep(3)
                
                all_users = df_users['user_name'].unique().tolist()
            
            
            
            
            if len(a) > 5:
                if a not in all_users:
                    if b == c:
                        q2= "INSERT INTO dbo.users VALUES (?,?,?,?,?,?)"
                        
                        retry_flag = True
                        retry_count = 0
                        while retry_flag:
                            try:
                                cursor.execute(q2,(a,b,c,g.user,level_mapper[d],d))
                                cnxn.commit()
                                retry_flag = False
                            except:
                                retry_count = retry_count + 1
                                time.sleep(3)
            
                        return redirect(url_for('successentry')) 
                    
                    else:
                        return redirect(url_for('secondincorrectpassword'))
                else:
                    return redirect(url_for('secondincorrectusername'))
            
            if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
           
            
            
    
            
            
        return render_template('secondincorrectpassword.html',levels = ['Upload & Visualize', 'Visualize'])
    return redirect(url_for('unauthorized'))


@app.route('/secondincorrectusername', methods=['GET','POST'])
def secondincorrectusername():
    if g.level == 2:
        if request.method == 'POST':
            
                
            a = request.form['userid']
            b = request.form['password1']
            c = request.form['password2']
            d = request.form['choice']
            
            if request.method == 'POST':
                a = request.form['userid']
                b = request.form['password1']
                c = request.form['password2']
                
                q = "SELECT * FROM dbo.users"
            
            
    
                retry_flag = True
                retry_count = 0
                while retry_flag:
                    try:
                        df_users = pd.read_sql(q,cnxn)
                        retry_flag = False
                    except:
                        retry_count = retry_count + 1
                        time.sleep(3)
                
                all_users = df_users['user_name'].unique().tolist()
            
            
            
            
            if len(a) > 5:
                if a not in all_users:
                    if b == c:
                        q2= "INSERT INTO dbo.users VALUES (?,?,?,?,?,?)"
                        
                        retry_flag = True
                        retry_count = 0
                        while retry_flag:
                            try:
                                cursor.execute(q2,(a,b,c,g.user,level_mapper[d],d))
                                cnxn.commit()
                                retry_flag = False
                            except:
                                retry_count = retry_count + 1
                                time.sleep(3)
            
                        return redirect(url_for('successentry')) 
                    
                    else:
                        return redirect(url_for('secondincorrectpassword'))
                else:
                    return redirect(url_for('secondincorrectusername'))
            
            if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
           
            
            
    
            
            
        return render_template('secondincorrectusername.html',levels = ['Upload & Visualize', 'Visualize'])
    return redirect(url_for('unauthorized'))


@app.route('/deleteuser', methods = ['GET','POST'])
def deleteuser():
    if g.level < 3:
        
        if request.method == 'POST':
            
            if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
            
            
            if request.form['submit_button'] == 'Delete':
                a = request.form['user_name']
                b =  request.form['reenter_user_name']
                if (a != b):
                    return redirect(url_for('reenterusername'))
                
                q_del = "DELETE FROM dbo.users WHERE (user_name = ?) AND  (added_by = ?)"
                data_tuple = (a,g.user)
                retry_flag = True
                while retry_flag:
                    try:
                        cursor.execute(q_del,data_tuple)       
                        cnxn.commit()          
                        retry_flag = False
                    except:
                        time.sleep(1)
            
                return redirect(url_for('successuserdelete'))    
                
            
            
                
            
            
        q_read = " Select * From dbo.users"
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                df = pd.read_sql(q_read,cnxn)
                df = df[df['added_by'] == g.user]
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
                
                
        return render_template('deleteuser.html',tables=[df.to_html(classes='data')], titles=df.columns.values )
        
    elif g.user:
        return redirect(url_for('unauthorized'))
    else:
        return redirect(url_for('relogin'))
 
@app.route('/successuserdelete', methods = ['GET','POST'])
def successuserdelete():
    if ((g.user != None)& (g.level <3)):
        if request.method == 'POST':
            if request.form['submit_button'] == 'Delete More User':
                return redirect(url_for('deleteuser'))
            
            elif request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
        
        return render_template('successuserdelete.html')
    
    elif g.level >=3 :
        return redirect(url_for('unauthorized'))
    else:
        return redirect(url_for('relogin'))

        
@app.route('/reenterusername', methods = ['GET','POST'])
def reenterusername():
    if g.level < 3:
        
        if request.method == 'POST':
            
            if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
            
            
            if request.form['submit_button'] == 'Delete':
                a = request.form['user_name']
                b =  request.form['reenter_user_name']
                if (a != b):
                    return redirect(url_for('reenterusername'))
                
                q_del = "DELETE FROM dbo.users WHERE (user_name = ?) AND  (added_by = ?)"
                data_tuple = (a,g.user)
                retry_flag = True
                while retry_flag:
                    try:
                        cursor.execute(q_del,data_tuple)       
                        cnxn.commit()          
                        retry_flag = False
                    except:
                        time.sleep(1)
            
                return redirect(url_for('successuserdelete'))    
                
            
            
                
            
            
        q_read = " Select * From dbo.users"
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                df = pd.read_sql(q_read,cnxn)
                df = df[df['added_by'] == g.user]
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
                
                
                
                
        return render_template('reenterusername.html',tables=[df.to_html(classes='data')], titles=df.columns.values )
        
    elif g.user:
        return redirect(url_for('unauthorized'))
    else:
        return redirect(url_for('relogin'))
                     
                




@app.route('/unauthorized', methods = ['GET','POST'])
def unauthorized():
    if ((g.level < 5) and (g.level > 0)) :
        if request.method == 'POST':
            if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
                
                
        return render_template('unauthorized.html')
    
    return redirect(url_for('relogin'))

@app.route('/successentry', methods = ['GET','POST'])
def successentry():
    if (g.level < 3):
        if request.method == 'POST':
            if request.form['submit_button'] == 'Add More User':
                if g.level == 1:
                    return redirect(url_for('adduser'))
                elif g.level == 2:
                    return redirect(url_for('addseconduser'))
                    
            elif request.form['submit_button'] == 'Log Out':
                return redirect(url_for('underdevelopment'))
            
            elif request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
            
            elif request.form['submit_button'] == 'See User Information':
                return redirect(url_for('viewdata'))
            
            
        if request.method == 'GET':
            return render_template('successentry.html')
    return redirect(url_for('relogin'))


@app.route('/viewdata',methods=['GET','POST'])
def viewdata():
    if g.level < 3:
        if request.method == 'POST':
            if request.form['submit_button'] == 'Upload More Users':
                return redirect(url_for('adduser'))
            elif request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
        q1 = "SELECT * FROM dbo.users"
        
        
        
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                df = pd.read_sql(q1,cnxn)
                if g.level == 2:
                    df = df[df['added_by'] == g.user]
                    df = df.drop(['added_by','auth_level'],axis = 1)
                    
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
        
        
        
        
        
        
        return render_template('table.html',  tables=[df.to_html(classes='data')], titles=df.columns.values)
    return redirect(url_for('unauthorized'))


@app.route('/viewuploadentries',methods = ['GET','POST'])
def viewuploadentries():
    if g.level !=3:
        if request.method == 'POST':
            if request.form['submit_button'] == 'Upload More Data':
                return redirect(url_for('uploaddata'))
            elif request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
            
        
        
        q1 = "SELECT * FROM dbo.upload_info_a"
        
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                df = pd.read_sql(q1,cnxn)
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
        
        
        
        
        
        
       
        if g.user != master_id: # This will indicate a user who is not a master user
            df = df[df['uploaded_by'] == g.user]
        return render_template('tablemetadata.html',  tables=[df.to_html(classes='data')], titles=df.columns.values)
    
    
    
    return redirect(url_for('relogin'))
        

@app.route('/uploaddata',methods = ['GET','POST'])
def uploaddata():
    if ((g.level != 3)):
        
        if request.method == 'POST':
            
            if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
            
            
            elif request.form['submit_button'] == 'Upload':   
                target_file = request.files.get('file')
                ut = datetime.now().strftime("%H:%M:%S")
                ud = datetime.now().strftime("%Y-%m-%d")
                
                # Input Data for meta data
                uname = g.user
                fname = target_file.filename
                q_meta = "INSERT INTO dbo.upload_info_a VALUES (?,?,?,?)"
                
                
                
                retry_flag = True
                retry_count = 0
                while retry_flag:
                    try:
                        cursor.execute(q_meta,(fname,uname,ut,ud))
                        cnxn.commit()
                        retry_flag = False
                    except:
                        retry_count = retry_count + 1
                        time.sleep(3)
                
                # Datasheet-1
                data2 = pd.read_excel(target_file,sheet_name=2,header=None) 
                tab_2 = upload_table_2(data2)
                insert_rows_table2(tab_2,g.user,ut,ud,cnxn,cursor)
                
                # Datasheet-2
                data6 = pd.read_excel(target_file,sheet_name=3,header=None)
                tab_3 = upload_table_3(data6)
                insert_rows_table3(tab_3,g.user,ut,ud,cnxn,cursor)
                
                # Datasheet-3,4,5
                data_peak = pd.read_excel(target_file,sheet_name=4,header=None) 
                data_reg = pd.read_excel(target_file,sheet_name=5,header=None)
                data_low = pd.read_excel(target_file,sheet_name=6,header=None)
                data_345 = [data_peak,data_reg,data_low]
                tables345 = upload_sheet_345(data_345)
                insert_rows_table4(tables345[0],g.user,ut,ud,cnxn,cursor)
                insert_rows_table6(tables345[1],g.user,ut,ud,cnxn,cursor)
                insert_rows_table7(tables345[2],g.user,ut,ud,cnxn,cursor)
                insert_rows_table8(tables345[3],g.user,ut,ud,cnxn,cursor)
                
                # Datasheet-6,7
                data6 = pd.read_excel(target_file,sheet_name=7,header=None)
                data7 = pd.read_excel(target_file,sheet_name=8,header=None)
                tab_5 = upload_table5(data6,'IN-KIND BENEFITS')
                tab_5a = upload_table5(data7,'CASH BENEFITS')
                insert_rows_table5(tab_5,g.user,ut,ud,cnxn,cursor)
                insert_rows_table5(tab_5a,g.user,ut,ud,cnxn,cursor)
                
                return redirect(url_for('uploadcomplete'))
                
            
            
                
            
    
        return render_template('upload.html')
    return redirect(url_for('relogin'))
    
    
    
@app.route('/uploadcomplete', methods = ['GET','POST'])
def uploadcomplete():
    if ((g.level != 3)):
        if request.method == 'POST':
            if request.form['submit_button'] == 'Upload More Data':
                return redirect(url_for('uploaddata'))
            elif request.form['submit_button'] == 'View Data':
                return redirect(url_for('view_updated'))
            elif request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
            
        if request.method == 'GET':
            return render_template('uploadcomplete.html')
    return redirect(url_for('relogin'))
    
    
    
                
              
@app.route('/deleteentries',methods = ['GET','POST'])
def deleteentries():
    if ((g.user != None)&(g.level != 3)):
        if request.method == 'POST':
            if request.form['submit_button'] == 'Delete':
                session['tar_uid']= request.form['choice']
                
                
                return redirect(url_for('selecteddelete'))
            elif request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
            
        
        
        q1 = "SELECT * FROM dbo.upload_info_a"
        
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                df = pd.read_sql(q1,cnxn)
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
        
        
        
        if g.user != 'master_id': # This will indicate a user who is not a master user
            df = df[df['uploaded_by'] == g.user]
            
        uids = df['upload_id'].unique().tolist()
            
        return render_template('deletemetadata.html',  tables=[df.to_html(classes='data')], titles=df.columns.values,colours = uids )
    
    
    
    return redirect(url_for('relogin'))



@app.route('/selecteddelete',methods = ['GET','POST'])
def selecteddelete():
        if (g.level != 3):
            q1 = "SELECT * FROM dbo.upload_info_a"
            
            retry_flag = True
            retry_count = 0
            while retry_flag:
                try:
                    df = pd.read_sql(q1,cnxn)
                    retry_flag = False
                except:
                    retry_count = retry_count + 1
                    time.sleep(3)
            
            if g.user != 'master_id': # This will indicate a user who is not a master user
                df = df[df['uploaded_by'] == g.user]
            
            target = int(session.get('tar_uid',None))
             
            df_subset = df[df['upload_id'] == target]
            
            uplder = df_subset.iloc[0,2]
            utim = df_subset.iloc[0,3]
            udate = df_subset.iloc[0,4]
             
            q_del = "DELETE FROM dbo.table_2 WHERE (uploader_name = ?) AND  (upload_time = ?) AND (upload_day = ?)"
            data_tuple = (df_subset.iloc[0,2],df_subset.iloc[0,3], df_subset.iloc[0,4])
            retry_flag = True
            while retry_flag:
                try:
                    cursor.execute(q_del,data_tuple)       
                    cnxn.commit()          
                    retry_flag = False
                except:
                    time.sleep(1)
          
            
            q_del = "DELETE FROM dbo.table_3 WHERE (uploader_name = ?) AND  (upload_time = ?) AND (upload_day = ?)"
            data_tuple = (df_subset.iloc[0,2],df_subset.iloc[0,3], df_subset.iloc[0,4])
            retry_flag = True
            while retry_flag:
                try:
                    cursor.execute(q_del,data_tuple)       
                    cnxn.commit()          
                    retry_flag = False
                except:
                    time.sleep(1)
          
            q_del = "DELETE FROM dbo.table_4 WHERE (uploader_name = ?) AND  (upload_time = ?) AND (upload_day = ?)"
            data_tuple = (df_subset.iloc[0,2],df_subset.iloc[0,3], df_subset.iloc[0,4])
            retry_flag = True
            while retry_flag:
                try:
                    cursor.execute(q_del,data_tuple)       
                    cnxn.commit()          
                    retry_flag = False
                except:
                    time.sleep(1)
          
            q_del = "DELETE FROM dbo.table_5 WHERE (uploader_name = ?) AND  (upload_time = ?) AND (upload_day = ?)"
            data_tuple = (df_subset.iloc[0,2],df_subset.iloc[0,3], df_subset.iloc[0,4])
            retry_flag = True
            while retry_flag:
                try:
                    cursor.execute(q_del,data_tuple)       
                    cnxn.commit()          
                    retry_flag = False
                except:
                    time.sleep(1)
          
            q_del = "DELETE FROM dbo.table_6 WHERE (uploader_name = ?) AND  (upload_time = ?) AND (upload_day = ?)"
            data_tuple = (df_subset.iloc[0,2],df_subset.iloc[0,3], df_subset.iloc[0,4])
            retry_flag = True
            while retry_flag:
                try:
                    cursor.execute(q_del,data_tuple)       
                    cnxn.commit()          
                    retry_flag = False
                except:
                    time.sleep(1)
          
            q_del = "DELETE FROM dbo.table_7 WHERE (uploader_name = ?) AND  (upload_time = ?) AND (upload_day = ?)"
            data_tuple = (df_subset.iloc[0,2],df_subset.iloc[0,3], df_subset.iloc[0,4])
            retry_flag = True
            while retry_flag:
                try:
                    cursor.execute(q_del,data_tuple)       
                    cnxn.commit()          
                    retry_flag = False
                except:
                    time.sleep(1)
          
            q_del = "DELETE FROM dbo.table_8 WHERE (uploader_name = ?) AND  (upload_time = ?) AND (upload_day = ?)"
            data_tuple = (df_subset.iloc[0,2],df_subset.iloc[0,3], df_subset.iloc[0,4])
            retry_flag = True
            while retry_flag:
                try:
                    cursor.execute(q_del,data_tuple)       
                    cnxn.commit()          
                    retry_flag = False
                except:
                    time.sleep(1)
          
            
            q_del2 = "DELETE FROM dbo.upload_info_a WHERE (upload_id = ?)"
            retry_flag = True
            while retry_flag:
                try:
                    cursor.execute(q_del2,target)       
                    cnxn.commit()
                    retry_flag = False
                except:
                    time.sleep(1)
          
            return redirect(url_for('successdelete'))           
            

@app.route('/successdelete',methods = ['GET','POST'])
def successdelete():
    if request.method == 'POST':
        if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
    
    return render_template('successdelete.html')

@app.route('/view_updated',methods = ['GET','POST'])
def view_updated():
    uids = ['Factory Information','Salary Information','Benefits','Occupation',
            'Incentives','Deductions','Work Days/Hours'
            ]
    
    if g.level<5:
        if request.method == 'POST':
            if request.form['submit_button'] == 'View':
                session['table'] = table_mapper[request.form['choice']]
                q1 = "SELECT * FROM " + session.get('table',None)
                
                retry_flag = True
                retry_count = 0
                while retry_flag:
                    try:
                        df = pd.read_sql(q1,cnxn)
                        if g.user != 'master_id':
                            if g.level == 2:
                                q1 = " SELECT * FROM dbo.users" 
                                retry_flag_1 = True
                                retry_count = 0
                                while retry_flag_1:
                                    try:
                                        df_users = pd.read_sql(q1,cnxn)
                                       
                                        df_users = df_users[df_users['added_by'] == g.user]['user_name'].tolist()
                                        df_users = df_users + [g.user]
                                        retry_flag_1 = False
                                    except:
                                        print ("Retry after 1 sec")
                                        retry_count = retry_count + 1
                                        time.sleep(1)
                                df = df[df['uploader_name'].isin(df_users)]
                                
                                
                            elif g.level > 2:
                                q1 = " SELECT * FROM dbo.users" 
                                retry_flag_1 = True
                                retry_count = 0
                                while retry_flag_1:
                                    try:
                                        df_users = pd.read_sql(q1,cnxn)
                                        addedby = df_users[df_users['user_name'] == g.user].iloc[0,3]
                                        df_users = df_users[df_users['added_by'] == addedby]['user_name'].tolist()
                                        df_users = df_users + [addedby]
                                        retry_flag_1 = False
                                    except:
                                        print ("Retry after 1 sec")
                                        retry_count = retry_count + 1
                                        time.sleep(1)
                                df = df[df['uploader_name'].isin(df_users)]
                                
                            
                        
                        retry_flag = False
                    except:
                        retry_count = retry_count + 1
                        time.sleep(3)
                    
                
                return redirect(url_for('view_updated_a'))
                
            elif request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))

            
        
            elif request.form['submit_button'] == 'Delete':
                return redirect(url_for('deleteentries'))
            
            
    
        
        
        
        q1 =  "SELECT * FROM dbo.table_3"
        
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                df = pd.read_sql(q1,cnxn)
                if g.user != 'master_id':
                    if g.level == 2:
                        q1 = " SELECT * FROM dbo.users" 
                        retry_flag_1 = True
                        retry_count = 0
                        while retry_flag_1:
                            try:
                                df_users = pd.read_sql(q1,cnxn)
                                
                                df_users = df_users[df_users['added_by'] == g.user]['user_name'].tolist()
                                df_users = df_users + [g.user]
                                retry_flag_1 = False
                            except:
                                print ("Retry after 1 sec")
                                retry_count = retry_count + 1
                                time.sleep(1)
                        df = df[df['uploader_name'].isin(df_users)]
                                
                                
                    elif g.level > 2:
                        q1 = " SELECT * FROM dbo.users" 
                        retry_flag_1 = True
                        retry_count = 0
                        while retry_flag_1:
                            try:
                                df_users = pd.read_sql(q1,cnxn)
                                addedby = df_users[df_users['user_name'] == g.user].iloc[0,3]
                                df_users = df_users[df_users['added_by'] == addedby]['user_name'].tolist()
                                df_users = df_users + [addedby]
                                retry_flag_1 = False
                            except:
                                print ("Retry after 1 sec")
                                retry_count = retry_count + 1
                                time.sleep(1)
                        df = df[df['uploader_name'].isin(df_users)]        
                                
                    
#
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)        
        
       
        
        return render_template('viewmeta.html',  tables=[df.to_html(classes='data')], titles=df.columns.values,colours = uids )
    
    
    
    return redirect(url_for('relogin'))


@app.route('/view_updated_a',methods = ['GET','POST'])
def view_updated_a():
    uids = ['Factory Information','Salary Information','Benefits','Occupation',
            'Incentives','Deductions','Work Days/Hours'
            ]
    if g.level < 5:
        if request.method == 'POST':
            if request.form['submit_button'] == 'View':
                session['table'] = table_mapper[request.form['choice']]
                q1 = "SELECT * FROM " + session.get('table',None)
                
                retry_flag = True
                retry_count = 0
                while retry_flag:
                    try:
                        df = pd.read_sql(q1,cnxn)
                        if g.user != 'master_id':
                            if g.level == 2:
                                q1 = " SELECT * FROM dbo.users" 
                                retry_flag_1 = True
                                retry_count = 0
                                while retry_flag_1:
                                    try:
                                        df_users = pd.read_sql(q1,cnxn)
                                       
                                        df_users = df_users[df_users['added_by'] == g.user]['user_name'].tolist()
                                        df_users = df_users + [g.user]
                                        retry_flag_1 = False
                                    except:
                                        print ("Retry after 1 sec")
                                        retry_count = retry_count + 1
                                        time.sleep(1)
                                df = df[df['uploader_name'].isin(df_users)]
                                
                                
                            elif g.level > 2:
                                q1 = " SELECT * FROM dbo.users" 
                                retry_flag_1 = True
                                retry_count = 0
                                while retry_flag_1:
                                    try:
                                        df_users = pd.read_sql(q1,cnxn)
                                        addedby = df_users[df_users['user_name'] == g.user].iloc[0,3]
                                        df_users = df_users[df_users['added_by'] == addedby]['user_name'].tolist()
                                        df_users = df_users + [addedby]
                                        retry_flag_1 = False
                                    except:
                                        print ("Retry after 1 sec")
                                        retry_count = retry_count + 1
                                        time.sleep(1)
                                df = df[df['uploader_name'].isin(df_users)]
                                
                            
                        retry_flag = False
                    except:
                        retry_count = retry_count + 1
                        time.sleep(3)
                    
                
                return redirect(url_for('view_updated_a'))
                
            elif request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
            
        
            elif request.form['submit_button'] == 'Delete':
                return redirect(url_for('deleteentries'))
            
            
       
        
        
        
        q1 = "SELECT * FROM " + session.get('table',None)
        
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                df = pd.read_sql(q1,cnxn)
                if g.user != 'master_id':
                    if g.level == 2:
                        q1 = " SELECT * FROM dbo.users" 
                        retry_flag_1 = True
                        retry_count = 0
                        while retry_flag_1:
                            try:
                                df_users = pd.read_sql(q1,cnxn)
                                
                                df_users = df_users[df_users['added_by'] == g.user]['user_name'].tolist()
                                df_users = df_users + [g.user]
                                retry_flag_1 = False
                            except:
                                print ("Retry after 1 sec")
                                retry_count = retry_count + 1
                                time.sleep(1)
                        df = df[df['uploader_name'].isin(df_users)]
                                
                                
                    elif g.level > 2:
                        q1 = " SELECT * FROM dbo.users" 
                        retry_flag_1 = True
                        retry_count = 0
                        while retry_flag_1:
                            try:
                                df_users = pd.read_sql(q1,cnxn)
                                addedby = df_users[df_users['user_name'] == g.user].iloc[0,3]
                                df_users = df_users[df_users['added_by'] == addedby]['user_name'].tolist()
                                df_users = df_users + [addedby]
                                retry_flag_1 = False
                            except:
                                print ("Retry after 1 sec")
                                retry_count = retry_count + 1
                                time.sleep(1)
                        df = df[df['uploader_name'].isin(df_users)]        
                                
                    
                
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)        
        
       
        
        return render_template('viewmeta.html',  tables=[df.to_html(classes='data')], titles=df.columns.values,colours = uids )
    
    
    
    return redirect(url_for('relogin'))




@app.route('/underdevelopment',methods = ['GET','POST'])
def underdevelopment():
    if g.level < 5:
        if request.method == 'POST':
            if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
                
                
        
        return render_template('lander.html')
    return redirect(url_for('relogin'))

@app.route('/addbenchmark',methods = ['GET','POST'])
def addbenchmark():
    if ((g.user != None)&(g.level < 3)):
        
        if request.method == 'POST':
            
            if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
            
            
            elif request.form['submit_button'] == 'Upload':   
                target_file = request.files.get('file')
                ut = datetime.now().strftime("%H:%M:%S")
                ud = datetime.now().strftime("%Y-%m-%d")
                
                # Input Data for meta data
                uname = g.user
                
                data = pd.read_csv(target_file,low_memory=False) 
                q_bmark = "INSERT INTO dbo.benchmark VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                
                
                for i in range (0,data.shape[0]):
                    
                    if pd.isna(data.iloc[i,0]) == False:
                        a1 = check_str(data.iloc[i,0])
                        a2 = check_str(data.iloc[i,1])
                        a3 = check_str(data.iloc[i,2],2999)
                        a4 = check_str(data.iloc[i,3],2999)
                        a5 = check_str(data.iloc[i,4],999) 
                        a6 = check_str(data.iloc[i,5])
                        a7 = check_float(data.iloc[i,6])
                        a8 = check_str(data.iloc[i,7])
                        a9 = check_str(data.iloc[i,8])
                        a10 = check_str(data.iloc[i,9])
                        a11 = check_float(data.iloc[i,10])
                        a12 = check_str(data.iloc[i,11])
                        a13 = check_str(data.iloc[i,12])
                        
                
                    
                        retry_flag = True
                        retry_count = 0
                        while retry_flag:
                            try:
                                cursor.execute(q_bmark,(uname,ud,ut,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13))
                                cnxn.commit()
                                retry_flag = False
                            except:
                                retry_count = retry_count + 1
                                time.sleep(3)
                                
            
            
                return redirect(url_for('benchmarksuccess'))
            
    
        return render_template('uploadbenchmark.html')
    return redirect(url_for('relogin'))

@app.route('/benchmarksuccess',methods = ['GET','POST'])
def benchmarksuccess():
    if ((g.user != None)&(g.level < 3)):
        if request.method == 'POST':
            if request.form['submit_button'] == 'Upload Additional Benchmarks':
                return redirect(url_for('addbenchmark'))
                #elif request.form['submit_button'] == 'View Data':
                #    return redirect(url_for('view_updated'))
            elif request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
                
        if request.method == 'GET':
            return render_template('benchmarksuccess.html')
    return redirect(url_for('relogin'))


@app.route('/viewbenchmark',methods = ['GET','POST'])
def viewbenchmark():
    if g.level < 3:
        if request.method == 'POST':
            if request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
            
        
        q1 = "SELECT * FROM dbo.benchmark"
        
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                df = pd.read_sql(q1,cnxn)
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
        
        
        
        if g.user != 'master_id': # This will indicate a user who is not a master user
            df = df[df['uploader_name'] == g.user]
            
        return render_template('viewbenchmark.html',  tables=[df.to_html(classes='data')], titles=df.columns.values )
     
    return redirect(url_for('relogin'))
@app.route('/logout',methods = ['GET','POST'])
def logout():
    if request.method == 'POST':
        if request.form['submit_button'] == 'Login':
            return redirect(url_for('relogin'))
    
    session.pop('user',None)
    return render_template('logoff.html') 
   

              
@app.route('/deletebenchmark',methods = ['GET','POST'])
def deletebenchmark():
    if ((g.user != None)&(g.level < 3)):
        if request.method == 'POST':
            if request.form['submit_button'] == 'Delete':
                session['tar_uid']= request.form['choice']
                return redirect(url_for('selectedbenchmarkdelete'))
            
            elif request.form['submit_button'] == 'Homepage':
                if g.level == 1:
                    return redirect(url_for('home'))
                elif g.level == 2:
                    return redirect(url_for('home_auth_one'))
                elif g.level == 3:
                    return redirect(url_for('home_auth_two'))
                elif g.level == 4:
                    return redirect(url_for('home_auth_four'))
            
        
        
        q1 = "SELECT * FROM dbo.benchmark"
        
        retry_flag = True
        retry_count = 0
        while retry_flag:
            try:
                df = pd.read_sql(q1,cnxn)
                retry_flag = False
            except:
                retry_count = retry_count + 1
                time.sleep(3)
        
        
        
        if g.user != 'master_id': # This will indicate a user who is not a master user
            df = df[df['uploader_name'] == g.user]
            
        uids = df['upload_id'].unique().tolist()
            
        return render_template('deletebenchmark.html',  tables=[df.to_html(classes='data')], titles=df.columns.values,colours = uids )
    
    
    
    return redirect(url_for('relogin'))



@app.route('/selectedbenchmarkdelete',methods = ['GET','POST'])
def selectedbenchmarkdelete():
    if (g.level < 3):
            
        target = int(session.get('tar_uid',None))
            
            
        q_del2 = "DELETE FROM dbo.benchmark WHERE (upload_id = ?)"
        retry_flag = True
        while retry_flag:
            try:
                cursor.execute(q_del2,target)       
                cnxn.commit()
                retry_flag = False
            except:
                time.sleep(1)
          
        return redirect(url_for('successdelete'))           
        
    else:
        return redirect(url_for('relogin'))

if __name__ == '__main__':
    
    app.run(port = 8101)  
    


