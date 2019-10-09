# -*- coding:utf-8 -*-

# coding:utf8
import datetime
import time


def doSth():

    print(u'这个程序要开始疯狂的运转啦')


def main(h=15, m=17):
    while True:
        now = datetime.datetime.now()
        print(now.hour, now.minute)
        if now.hour == h and now.minute == m:
            doSth()
        # 每隔60秒检测一次
        time.sleep(60)


main()