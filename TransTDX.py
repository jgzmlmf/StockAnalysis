# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 17:58:04 2016
将通达信导出的日线文件转化到本地的MySQL数据库中，并可以自动更新旧数据。
@author: 王韬
"""

import datetime
import MySQLdb
import os
import time
from progressbar import ProgressBar,Percentage,Bar #用于进度显示

#数据初始化区域------------------------------------------
#源数据目录
FilePath="D:\\Export\\"
#获取目录下所有文件列表
FileList=os.listdir(FilePath)
#数据初始化区域------------------------------------------

#读取文件到内存的函数
def GetDataFromTxt(FileName):
    """
    给定文件路径，从通达信导出的日线数据文件中提取出个股的基本信息和日线数据列表
    """
    #最终结果数据表
    tablelist=[]
    
    #打开文件
    f=open(FileName)

    #个股信息
    #信息内容：代码、名称、K线类型、复权方式
    S1=f.readline()
    stockInfo=S1.split(' ')

    #获得表头
    rowcapital=f.readline()
    rowcapital=rowcapital.split('\t')
    rowcapital.append("权重值")

    #获取全部数据
    tmpinfo=f.readlines()
    #print u"已读入",FileName,u"文件。"
    #关闭文件
    f.close()

    #格式化所有数据
    #日期格式为YYYY/MM/DD
    for trow in tmpinfo:
        if len(trow)<20:
            break
        rowitem=[]    
        tmpitem=trow.split('\t')
        #tmp=time.strptime(tmpitem[0],"%Y/%m/%d")
        t=tmpitem[0].split('/')
        tmp=datetime.date(int(t[0]),int(t[1]),int(t[2]))
        
        rowitem.append(tmp)#日期
        rowitem.append(float(tmpitem[1]))#开盘
        rowitem.append(float(tmpitem[2]))#最高
        rowitem.append(float(tmpitem[3]))#最低
        rowitem.append(float(tmpitem[4]))#收盘
        rowitem.append(float(tmpitem[5]))#成交量
        rowitem.append(float(tmpitem[6]))#成交额
        #加入权重点数据
        if rowitem[5]!=0:    
            bt=rowitem[6]/rowitem[5]
        else:
            bt=float(tmpitem[4])
        rowitem.append(bt)
                
        tablelist.append(rowitem)
        #结束格式化数据    
    return (stockInfo,tablelist)
##读取文件到内存的函数的结束
    
    
#将日线数据导入数据库
def UpdateDatabase(stockInfo,tablelist,db):
    cur=db.cursor()
    #选择数据库
    db.select_db('test')
    #初始化参数
    Insert=0
    Update=0

    #更新数据
    for ritem in tablelist:
        dbCommand="select code,date,open,close,high,low,vol,turn,balance from DateLine where code='%s' and date='%s'"%(stockInfo[0],str(ritem[0]))
        try:    
            cur.execute(dbCommand)
        except Exception,err:
            print err
        o=cur.fetchall()
        if len(o)==0:
            #插入数据
            dbCommand="insert into DateLine(code,date,open,close,high,low,vol,turn,balance) \
            values('%s','%s','%s','%s','%s','%s','%s','%s','%s')"%\
            (stockInfo[0],str(ritem[0]),ritem[1],ritem[4],ritem[2],ritem[3],ritem[5],ritem[6],ritem[7])
            try:
                cur.execute(dbCommand)
                Insert+=1
            except Exception,err:
                print err
        elif len(o)==1:
            s=o[0]
            #比对数据
            if s[2]!=ritem[1] and s[3]!=ritem[4]:
                #更新数据
                dbCommand="update DateLine set open='%s',close='%s',high='%s',low='%s',vol='%s',turn='%s',balance='%s' \
                where code='%s' and date='%s'"%(ritem[1],ritem[4],ritem[2],ritem[3],ritem[5],ritem[6],ritem[7],stockInfo[0],str(ritem[0]))        
                try:
                    cur.execute(dbCommand)
                    Update+=1
                except Exception,err:
                    print err
    #print u"新增",Insert,u"条数据，更新",Update,u"条数据。"
    return (Insert,Update)

##将日线数据导入数据库函数的结束
    
    
#顺序处理
#程序执行开始处
#--------------------------------------
#各个计数器
Insert=0
Update=0
All=0
Position=0

pbar = ProgressBar(widgets=[Percentage(),Bar()], maxval=len(FileList)).start() #进度描述 
#计时
print u"开始时间：",time.ctime(time.time())
#打开数据库链接
db=MySQLdb.connect("localhost","dbuser","dbuser","test")

for f in FileList:
    a,b = GetDataFromTxt(FilePath+f)
    i,u=UpdateDatabase(a,b,db)
    All+=len(b)    
    Insert+=i
    Update+=u
    pbar.update(Position)#进度显示
    Position+=1
    

#确认更新数据
db.commit()
#关闭数据库链接
db.close()

print u"\n共处理%s条数据，共新增%s条数据，更新%s条数据！"%(All,Insert,Update)
print u"结束时间：",time.ctime(time.time())
print u"完成！"
