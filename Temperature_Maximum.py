import os
import MySQLdb
change_dir=os.chdir(r"/home/vagrant/hadoop-book/ch02-mr-intro/src/main/python")

def read_file():
    with open('1903') as out_file:
        fileread = out_file.readlines()
        db = MySQLdb.connect(host="localhost",    
                     user="root",        
                     passwd="20084462",  
                     db="mysql")
        cur = db.cursor()
        drop="drop table temperature"
        cur.execute(drop)
        create="create table temperature (`year` int ,`temperature` int)"
        cur.execute(create)
        delete="delete from temperature"
        cur.execute(delete)
        for i in fileread:
            list = i.split()
            string = str(list[0])
            year=string[15:19]
            temp=string[87:92]
            year=int(year)
            temp=int(temp)
            sql="insert into `temperature`(`year`,`temperature`) values (%s,%s)"
            cur.execute(sql,(year,temp))
            db.commit()
        show="select year, max(temperature) from temperature where temperature!=9999 group by year"
        show1=cur.execute(show)
        result=cur.fetchall()
        for show1 in result:
            print "%d\t%d" %(show1[0],show1[1])
read_file()
