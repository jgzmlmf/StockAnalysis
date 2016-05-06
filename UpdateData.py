# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 20:34:08 2016
UpdateData
in new file tructure of HDF5
@author: WangTao
"""
import tushare as ts
import pandas as pd
import datetime as dt
import multiprocessing
import time
import os

#全局变量
global totalStatus
totalStatus = {}


'''
#通联数据获取
#用通联方法
import dataapi
client = dataapi.Client()
client.init('db087ed39012466cf5a797e6a150aaaf788a103423a9ec2b172cc0e55768aed2')

#用tushare方法
import tushare as ts
ts.set_token('db087ed39012466cf5a797e6a150aaaf788a103423a9ec2b172cc0e55768aed2')

#获取个股信息
mkt = ts.Market()
df = mkt.TickRTSnapshot(securityID=‘000001.XSHE’)

#获取一段时间内的日期是否为交易日，isOpen=1是交易日，isOpen=0为休市
mt = ts.Master()
df = mt.TradeCal(exchangeCD='XSHG', beginDate='19901219', endDate='20161231', field='calendarDate,isOpen, isWeekEnd, isMonthEnd, isQuarterEnd, isYearEnd')


#获得股票状态
eq = ts.Equity()
df = eq.Equ(equTypeCD='A', listStatusCD='L', field='ticker,secShortName,totalShares,nonrestFloatShares')
df['ticker'] = df['ticker'].map(lambda x: str(x).zfill(6))

listStatusCD上市状态，可选状态有L——上市，S——暂停，DE——已退市，UN——未上市

'''
#将上市时间的整形变量转换成符合要求的字符串变量
def ChangeDate(i):
	u'将上市时间的整形变量转换成符合要求的字符串变量'
	r = ''
	if i > 0:
		s = str(i)
		r = s[0:4]+'-'+s[4:6]+'-'+s[6:8]
	return r

#确定最后一个交易日
def get_last_day():
	global totalStatus
	#判断最后一个交易日
	nt = dt.datetime.now()
	cls = dt.time(17, 30)
	if nt.time() > cls:
		wd = dt.date.today()
	else:
		wd = dt.date.today() - dt.timedelta(days=1)
	while ts.is_holiday(str(wd)):
		wd = wd-dt.timedelta(days=1)
	totalStatus['lastday'] = pd.Timestamp(wd)
	return pd.Timestamp(wd)


#加载本地数据
def LoadLocalData():
	"""
	判断当前是哪台设备，不同的设备用不同的文件路径加载数据文件
	"""
	hostname = os.popen('hostname').read()

	if 'PrivateBook' in hostname:#家庭笔记本
		filename = 'c:\\tmp\\save.h5'
	elif 'USER-20160422LP' in hostname:#办公室台式机
		filename = u'D:\\用户目录\\My Documents\\Python\\save.h5'
	else:
		filename = 'd:\\save.h5'

	return filename

#检查所有线程是否全部完成
def CheckResult(result):
	u'检查线程是否全部完成'
	if type(result) == list:
		lst = []
		lst2 = []
		status = {}
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

#下载个股指定日期后的前复权日线数据的单线程函数
def DownloadData(local, st, q):
	'''
	code is stockcode in string type
	st is time to market in string 'YYYY-MM-DD' type
	'''
	t = local.split('/')
	kind = t[1]
	code = t[2]

	if kind == 'cq':
		kind = None
	try:
		if len(st) > 8:
			df = ts.get_h_data(code, start=st, autype=kind)
		else:
			df = ts.get_h_data(code, autype=kind)

		if len(df) > 0:
			df = df.sort_index()
			df = df.sort_index(axis=1)
			filename = q.get()
			try:
				df.to_hdf(filename, local)
			except:
				store = pd.HDFStore(filename, mode='a')
				store[local] = df
				store.flush()
				store.close()

			q.put(filename)
			return [True, code]
	except:
		return [False, code]


#检查个股日线数据的情况并更新的单线程函数
def UpdateStockData(local, q):
	u'更新个股最新数据并对清权股票数据更新。如果无需更新则返回原来的df！'
	global totalStatus
	#lastday = totalStatus['lastday']
	lastday = get_last_day()
	#分解基础信息
	t = local.split('/')
	kind = t[1]
	code = t[2]
	if kind == 'cq':
		kind = None

	
	#对比日期
	filename = q.get()
	df = pd.read_hdf(filename, local)
	q.put(filename)
	df = df.dropna()
	df = df.sort_index()
	df = df.sort_index(axis=1)
	check = df.iloc[-1].name

	if lastday > check:
		downday = check.date() - dt.timedelta(days=10)
		df1 = ts.get_h_data(code, start=str(downday), autype=kind)

		if len(df1) > 0 and type(df1) == pd.core.frame.DataFrame:
			#对比是否发生清权
			dp = pd.concat([df, df1])
			dp = dp.drop_duplicates()
			ls = dp.index.duplicated()
			if sum(ls) > 0: #有清权
				#重新下载数据
				return DownloadData(local, str(df.ix[0].name)[:10], q)
			else:#无清权
				if len(dp) > len(df): #未停盘
					dp = dp.sort_index()
					dp = dp.sort_index(axis=1)
					filename = q.get()

					try:
						dp.to_hdf(filename, local)
					except:
						store = pd.HDFStore(filename, mode='a')
						store[local] = dp
						store.flush()
						store.close()

					q.put(filename)
					return [True, code]
				else: #停盘，无数据更新
					return [False, code]
		else:
			#无更新值
			return [False, code]
	else:
		#无更新值
		return [True, code]


#多线程发起函数
def MultiStart(down_dict):
	global totalStatus
	#创建存储句柄
	filename = LoadLocalData()

	#创建队列
	manager = multiprocessing.Manager()
	queue = manager.Queue(1)
	queue.put(filename)

	#创建线程池
	count = multiprocessing.cpu_count()
	pool = multiprocessing.Pool(processes=count)
	#启动线程/进程池
	result = []
	#判断状态
	if down_dict != None: #有个股数据缺失，下载数据------------------------------
		for s in down_dict:
			result.append(pool.apply_async(DownloadData, (s, down_dict[s], queue)))
		pool.close()
		status = CheckResult(result)
		oldq = 0
		while status['finished'] < status['all']:
			if status['finished'] != oldq:
				print u'\n\t\t已经完成%s/%s项下载任务！其中成功%s个！'%(status['finished'], status['all'], status['successful'])
			time.sleep(1)
			oldq = status['finished']
			status = CheckResult(result)
		pool.join()
		status = CheckResult(result)
		totalStatus['downinfo'] = u'已经完成%s/%s项下载任务！其中成功%s个！'%(status['finished'], status['all'], status['successful'])
	else: #开始更新数据------------------------------------------
		filename = LoadLocalData()
		store = pd.HDFStore(filename, mode='r')
		lst = store.keys()
		store.close()
		if '/basicinfo' in lst:
			lst.remove('/basicinfo')
		for s in lst:
			result.append(pool.apply_async(UpdateStockData, (s, queue)))
		pool.close()
		status = CheckResult(result)
		oldq = 0
		while status['finished'] < status['all']:
			if status['finished'] != oldq:
				print u'\n\t\t已经完成%s/%s项更新任务！其中成功%s个！'%(status['finished'], status['all'], status['successful'])
			time.sleep(1)
			oldq = status['finished']
			status = CheckResult(result)
		pool.join()
		status = CheckResult(result)
		totalStatus['upinfo'] = u'已经完成%s/%s项更新任务！其中成功%s个！'%(status['finished'], status['all'], status['successful'])

	#汇总详细信息
	date = {}
	for res in result:
		try:
			t = res.get()
			if len(t) > 0:
				if not t[0]:
					date[t[1]] = t[0]
		except:
			#print err.message
			pass
	
	if down_dict != None:	
		if len(date) > 0:
			se = pd.Series(date)
			totalStatus['download'] = se
		else:
			totalStatus['download'] = ''
	else:
		if len(date) > 0:
			se = pd.Series(date)
			totalStatus['update'] = se
		else:
			totalStatus['update'] = ''



if __name__ == '__main__':

	global totalStatus
	
	totalStatus['starttime'] = dt.datetime.now()
	totalStatus['download'] = ''
	totalStatus['update'] = ''

	#设定最后一个交易日
	get_last_day()
	#获得股票列表
	nbi = ts.get_stock_basics()
	nbi = nbi.sort_index()
	nbi = nbi.drop(nbi[nbi.timeToMarket==0].index) #删除未上市新股
	nlst = nbi.timeToMarket
	nlst = nlst.apply(ChangeDate)

	filename = LoadLocalData()
	store = pd.HDFStore(filename, mode='r')

	#获得本地数据列表
	lst = store.keys()
	store.close()
	if '/basicinfo' in lst:
		lst.remove('/basicinfo')

	#生成应下载列表
	tmplst = nlst.index.tolist()
	#去除特定股票
	killlst = ['000033', '600710', '600732'] #黑名单——退市股
	for s in killlst:
		if s in tmplst:
			tmplst.remove(s)
	
	down_list = []
	for s in tmplst:
		down_list.append('/qfq/'+s)
		down_list.append('/cq/'+s)


	#-------------------------补全数据------------------------------------------
	for s in lst:
		if s in down_list:
			down_list.remove(s)

	if len(down_list) > 0: #有个股数据需要补全
		down_dict = {}
		for s in down_list:
			down_dict[s] = nlst[s[-6:]]
			
		MultiStart(down_dict) #开始多线程数据补全

	else:#无需个股数据补全，开始更新
		totalStatus['downinfo'] = u'已经完成%s/%s项下载任务！其中成功%s个！'%(0, 0, 0)

	#-------------------------检查更新数据--------------------------------------
	MultiStart(None) #开始多线程更新

	#------------------数据下载结束，本地数据处理---------------------------------
	store = pd.HDFStore(filename, mode='a')
	try:
		store.remove('/basicinfo')
	except:
		pass
	store['/basicinfo'] = nbi
	#如果是办公室的计算机，则进行保存文件的清理
	hostname = os.popen('hostname').read()
	if 'USER-20160422LP' in hostname:#办公室台式机
		store.flush()
		tmp_dict={}
		keys = store.keys()
		for key in keys:
			tmp_dict[key] = store[key]
		store.close()
		store = pd.HDFStore(filename, mode='w')
		for key in tmp_dict:
			store[key] = tmp_dict[key]
	
	store.flush()
	store.close()
	
	#------------------------------数据处理结束---------------------------------
	totalStatus['endtime'] = dt.datetime.now()

	si = totalStatus
	print si['downinfo']
	print u'其中%s条错误信息，如下：'%(len(si['download']))
	print si['download'], '\n'
	print si['upinfo']
	print u'其中%s错误信息，如下：'%(len(si['update']))
	print si['update'], '\n'
	print u'开始时间：', si['starttime']
	print u'结束时间：', si['endtime']
	print u'共用时：', si['endtime'] - si['starttime']	
	
	raw_input('\npress enter to quit...')