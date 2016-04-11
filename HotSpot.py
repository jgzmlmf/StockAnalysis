# -*- coding: utf-8 -*-
"""
Created on Tue Apr 05 15:03:08 2016

@author: Taplo
每日热点股票的信息收集
包括：
1)高转送
2)业绩预披露靠前的
3)限售股解禁
"""
import tushare as ts
import datetime as dt
import multiprocessing



#高转送股票信息
def HighShares(t=50):
	df = ts.profit_data()
	if len(df)<2:
		return [u"高转送股票信息：",u"无！"]
	else:
		df=df.sort_values('shares',ascending=False)
		return [u"高转送股票信息：",df[:t]]

#业绩预披露靠前
def ProfitExp():
    year=dt.date.today().year
    month=dt.date.today().month
    if month>9:
        season=4
    elif month>6:
        season=3
    elif month>3:
        season=2
    else:
        season=1

    if season==1:
        year=year-1
        season=4
    else:
        season=season-1

	dp=ts.forecast_data(year,season)
	dp=dp.sort_values('range',ascending=False)
	return [u"\n近期预披露靠前的结果：",dp[:30]]

'''#上期预披露结果
    if season==1:
        year=year-1
        season=4
    else:
        season=season-1

    dp=ts.forecast_data(year,season)
    dp=dp.sort_values('range',ascending=False)
    print u"\n上期预披露靠前的结果："
    print dp[:20]
'''
#限售股解禁
def XsgJj():
	df=ts.xsg_data()
	df['ratio']=df['ratio'].astype(float)
	df=df.sort_values('ratio',ascending=False)
	return [u"限售股解禁：",df[df.ratio>10]]

#每日龙虎榜列表
def EveryDayTop():
	day=dt.date.today()
	while ts.is_holiday(str(day)):
		day=day-dt.timedelta(days=1)
	df=ts.top_list(str(day))
	return [u"%s日龙虎榜列表：(%s条)"%(str(day+dt.timedelta(days=1)),len(df)),df]

#个股上榜统计
def TopTotal():
	df=ts.cap_tops()
	df=df.sort_values('count',ascending=False)
	return [u"个股上榜统计：",df[:20]]

#机构席位追踪
def ChairTotal():
	df=ts.inst_tops()
	df=df.sort_values('bcount',ascending=False)
	return [u"机构席位追踪:",df[:20]]

#机构成交明细
def ChairDetail():
	df=ts.inst_detail()
	df=df.sort_values('bamount',ascending=False)
	return [u"机构成交明细:",df[:50]]

if __name__ == '__main__':
	function_list=[HighShares,ProfitExp,EveryDayTop,TopTotal,ChairTotal,ChairDetail,XsgJj]

	count=multiprocessing.cpu_count()
	if count>2:
		count=count-1
	pool = multiprocessing.Pool(count)
	result=[]
	for func in function_list:
	    result.append(pool.apply_async(func, ()))
	pool.close()
	pool.join()
	print "Sub-process(es) done."
	for res in result:
		t=res.get()
		print t[0]
		print t[1]

'''
	#输出到文件
	filename=".\\"+str(dt.date.today())+".txt"
	f=open(filename,'w')
	for res in result:
		t=res.get()
		f.write(t[0].encode("GBK")+"\n")
		for s in t[1]:
			f.writelines(s)
	f.close()

'''

