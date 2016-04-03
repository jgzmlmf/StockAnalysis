#C:\Users\WangTao\Documents\Python
#FileName:timertest.py
# -*- coding:utf-8-*-

#计时测试
import time
from timeit import timeit,repeat


start_cpu=time.clock()
start_real=time.time()
for i in range(500000):
    a=i**5
end_cpu=time.clock()
end_real=time.time()
print "%f real Seconds"%(end_real-start_real)
print "%f CPU Seconds"%(end_cpu-start_cpu)


t1=timeit('a=9*9*9*9*9*9')
print t1
t2=repeat('a=9*9*9*9*9*9')
print t2
