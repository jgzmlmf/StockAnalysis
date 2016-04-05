# -*- coding: utf-8 -*-
"""
Created on Tue Apr 05 15:03:08 2016

@author: Taplo
每日热点股票的信息收集
包括：
1)高转送
"""
import tushare as ts
import datetime as dt


#高转送股票信息
def HighShares(ts):
    df=ts.profit_data(top=50)
    df=df.sort_values('shares',ascending=False)
    print u"近期高转送股票："    
    print df[df.shares>=10]

#业绩预披露靠前
def ProfitExp(ts):
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
    print u"\n近期预披露靠前的结果："
    print dp[:20]

    if season==1:
        year=year-1
        season=4
    else:
        season=season-1

    dp=ts.forecast_data(year,season)
    dp=dp.sort_values('range',ascending=False)
    print u"\n上期预披露靠前的结果："
    print dp[:20]
    
#限售股解禁
def XsgJj(ts):
    df=ts.xsg_data()
    df['ratio']=df['ratio'].astype(float)
    df=df.sort_values('ratio',ascending=False)
    print u"限售股解禁："
    print df[df.ratio>10]
    


if __name__ == '__main__':
    #高转送
    HighShares(ts)
    #预披露
    ProfitExp(ts)
    #限售股解禁
    XsgJj(ts)
