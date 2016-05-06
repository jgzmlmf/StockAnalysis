# -*- coding: utf-8 -*-
"""
Created on Tue May 03 21:24:00 2016
FileName:FindBig.py
用于在日线中寻找大行情阶段
大行情：
1、单边行情，最大回撤不超过10%
2、快速行情，三个月内
3、大幅度，涨幅70%以上
4、行情开始前无停盘——即非重组股
@author: WangTao
"""
import pandas as pd
import datetime as dt
import multiprocessing
import time
import numpy as np


#全局变量
global totalStatus


#加载本地数据路径
def load_local_path():
    """
    判断当前是哪台设备，不同的设备用不同的文件路径加载数据文件
    """
    import os
    hostname=os.popen('hostname').read()

    if 'PrivateBook' in hostname: #家庭笔记本
         filepath = 'c:\\tmp\\'
         #filename = 'c:\\tmp\\save.h5'
    elif 'USER-20160422LP' in hostname:#办公室台式机
        filepath = u'D:\\用户目录\\My Documents\\Python\\'
        #filename = u'D:\\用户目录\\My Documents\\Python\\save.h5'
    else:
        filepath = 'd:\\'
        #filename = 'd:\\save.h5'

    totalStatus['filepath'] = filepath
    return filepath


#检查所有线程是否全部完成
def check_result(result):
    u'检查线程是否全部完成'
    status = {}
    if type(result) == list:
        lst = []
        lst2 = []
        for res in result:
            t = res.ready()
            lst.append(t)
            if t:
                try:
                    lst2.append(res.successful())
                except:
                    lst2.append(False)

        status['all'] = len(result)
        status['finished'] = sum(lst)
        status['successful'] = sum(lst2)
    else:
        status['all'] = len(result)
        status['finished'] = len(result)
        status['successful'] = 0
    return status

#数据合并，将收盘价数据Series和交易日历合并，形成适于分析的DataFrame
def make_data(calender, se = pd.Series()):
    se = se.sort_index()
    if len(se)>0:
        cal = pd.DataFrame(calender)
        da = pd.DataFrame(se)
        da = da.resample('d')
        df = pd.merge(da, cal, left_index=1, right_index=1)
        return df
    else:
        return pd.DataFrame()

'''
#列出高比例结果和比例
def gethighrate(local, se=pd.Series(), period=30, rate=60):
    length = len(se)
    if length > 0:	
        ratelst ={}
        for i in xrange(length):
            if i <= period:
                start = 0
            else:
                start = i - period
            l = se[start:i].min()
            h = se[start:i].max()
            ratelst[se.index[i]] = ((h/l) - 1) * 100
        result = pd.Series(ratelst)
        result = result[result>rate]
        result = result.drop_duplicates()
        return [local, result]
    else:
        return se
'''
#检查是否有停盘
def check_suspend(check=pd.DataFrame(), limit=3):
    if len(check) > 0:
        count = 0
        for i in xrange(len(check)):
            c = check.iloc[i]
            if np.isnan(c.values[0]):
                if c.values[1] == 1:
                    count +=1
        if count > limit:
            return True
        else:
            return False

#查找大行情的函数
def find_big(code, calender, q, drawback=0.1, bigrate=2):
    ''
    
    filepath = q.get()
    filename = filepath + 'save.h5'
    try:
        df = pd.read_hdf(filename, '/qfq/' + code)
        q.put(filepath)
    except Exception as err:
        q.put(filepath)
        return err.message

    check = make_data(calender, df.close)

    result = []
    start = 1
    #开始进行数据处理  
    if len(check) > 0:
        for i in xrange(len(check)): #出发点，遍历全部节点
            p = check.iloc[i] #当前节点P
            if np.isnan(p.values[0]):
                continue
            else:
                if start == 1: #带start的P节点
                    m1 = m2 = m3 = p
                    start = 0
                    continue
                else: #P节点
                    m1 = p
                
                if m1.values[0] >= m2.values[0]: #爬坡
                    m2 = m1
                    continue
                else:
                    if m1.values[0] <= m3.values[0]: #返回起始节点
                        start = 1
                        continue 
                    else:
                        if m2.values[0] != 0: 
                            back = m1.values[0]/m2.values[0]
                        else:
                            back = 1 - drawback + 0.01
                        if back >= (1-drawback):
                            continue
                        else:
                            rate = m2.values[0]/m3.values[0]
                            if rate < bigrate: #非大行情
                                start = 1
                                continue
                            else: #大行情
                                pos = check.index.get_loc(m3.name)
                                if pos > 30:
                                    startpos = pos - 30
                                else:
                                    startpos = 0
                                if check_suspend(check.iloc[startpos:pos]): #停盘行情
                                    start = 1
                                    continue
                                else: #添加到行情列表
                                    mark = {}
                                    mark['code'] = code
                                    mark['starttime'] = m3.name
                                    mark['startprice'] = m3.values[0]
                                    mark['endtime'] = m2.name
                                    mark['endprice'] = m2.values[0]
                                    mark['rate'] = m2.values[0]/m3.values[0]
                                    mark['period'] = (m2.name - m3.name).days
                                    result.append(mark)
                                    start = 1
    return result

#多线程发起程序
def multi_start(calender, filepath='', lst=[]):
    global totalStatus
    if len(filepath) > 0 and len(lst) > 0:
        #创建队列
        manager = multiprocessing.Manager()
        queue = manager.Queue(1)
        queue.put(filepath)

        #创建线程池
        count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=count-1)
        #启动线程/进程池
        result = []
        biginfo = []
        status = {}
        
        #判断状态
        for code in lst:
            result.append(pool.apply_async(find_big, (code, calender, queue)))
        pool.close()
        status = check_result(result)
        oldq = 0
        while status['finished'] < status['all']:
            if status['finished'] != oldq:
                print u'已经完成%s/%s项寻找任务！其中成功%s个！'%(status['finished'], status['all'], status['successful'])
                time.sleep(1)
            oldq = status['finished']
            status = check_result(result)
        print u'已经完成%s/%s项寻找任务！其中成功%s个！'%(status['finished'], status['all'], status['successful'])
        pool.join()
        totalStatus['endinfo'] = u'已经完成%s/%s项寻找任务！其中成功%s个！'%(status['finished'], status['all'], status['successful'])
        print u'计算工作结束，正在整理结果。。。'
        for res in result:
            try:
                t = res.get()
                if len(t) > 0 and type(t) == list:
                    for b in t:
                        biginfo.append(b)
                elif len(t) > 0:
                    print t
            except Exception as err:
                print err.message

        if len(biginfo) > 0:
            bigdf = pd.DataFrame(biginfo, columns=['code', 'starttime', 'startprice', 'endtime', 'endprice', 'rate', 'period'])
        else:
            bigdf = pd.DataFrame()

    	return bigdf

if __name__ == '__main__':

    global totalStatus
    totalStatus = {}
    totalStatus['starttime'] = dt.datetime.now()

    filepath = load_local_path()
    calender = pd.read_hdf(filepath+'info.h5', '/calender')
    totalStatus['calender'] = calender

    #获得本地股票列表和总表
    bi = pd.read_hdf(filepath+'save.h5', '/basicinfo')
    bi = bi.sort_index()
    lst = bi.index.tolist()
    
    #启动多线程计算程序
    bigdf = multi_start(calender, filepath, lst)
    
    #保存结果
    try:
        bigdf.to_hdf(filepath+'big.h5', 'biginfo', mode='w')
    except:
        store = pd.HDFStore(filepath+'big.h5', mode='w')
        store['biginfo'] = bigdf
        store.close()
    
    totalStatus['endtime'] = dt.datetime.now()
    #打印工作结果
    print u'计算结果：', totalStatus['endinfo']
    print u'共获得%s条数据！'%(len(bigdf))
    print u'开始时间：\t', totalStatus['starttime']
    print u'结束时间：\t', totalStatus['endtime']
    print u'耗时：\t', totalStatus['endtime'] - totalStatus['starttime']
    
    
    