# -*- coding: utf-8 -*-

from flask import Flask, request, render_template,redirect,url_for,session,g
import pyodbc 
import pandas as pd
import config
import os 
from datetime import datetime
import time
from module import upload_table_2,insert_rows_table2,upload_table_3,insert_rows_table3,upload_sheet_345,insert_rows_table4,upload_table5,insert_rows_table5,insert_rows_table6,insert_rows_table7,insert_rows_table8

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
    


