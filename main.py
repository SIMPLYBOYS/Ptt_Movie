#! /usr/bin/env python

import urllib.request
import pymongo
import pprint
import redis
import json
import jieba
import jieba.posseg
import jieba.analyse
import os
import schedule
import time
import dateutil.parser as parser
from urllib.request import urlopen
from bs4 import BeautifulSoup
from pymongo import MongoClient
from elasticsearch import Elasticsearch
# scrap_from = 4970 #scraping from page number
scrap_size = 4 #scraping 4 pages per work
es = Elasticsearch()
client = MongoClient('localhost', 27017)
db = client['test']
taiwan = db['taiwan']
ptt = db['ptt']
# headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.57 Safari/537.36'}
user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
headers = {'User-Agent': user_agent}

def transLate():
    print('transLate')
    ret = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
    foo = ret.get('pttHome')
    bar = json.loads(foo.decode('utf-8'))
    words = ','.join(bar)
    tagArray = []
    for tag, score in jieba.analyse.textrank(words, withWeight=True):
        print('%s %s' % (tag, score))
        tagArray.append(tag)
    ret.set('pttHome', ','.join(tagArray))
    return

def parseTopic(url):
    values = {}
    data = urllib.parse.urlencode(values)
    data = data.encode('ascii')
    req = urllib.request.Request(url, data, headers)
    
    try:
        with urlopen(req) as response:
           the_page = response.read()
           bsObj =  BeautifulSoup(the_page,"html.parser")
           container = bsObj.select('.r-ent')
           for each_item in container:
                if (each_item.select('div.title')[0].text.find("本文已被刪除") != -1):
                   continue
                print ("日期："+each_item.select('div.date')[0].text, "作者："+each_item.select('div.author')[0].text)
                print (each_item.select('div.title')[0].text)
                print ("https://www.ptt.cc"+each_item.find('a', href=True)['href'])
                print ("---------------------------------")
                post = {}
                post['autor'] = each_item.select('div.author')[0].text
                post['date'] = each_item.select('div.date')[0].text.replace(" ","")
                post['link'] = "https://www.ptt.cc"+each_item.find('a', href=True)['href']
                post['title'] = each_item.select('div.title')[0].text.splitlines()[1]
                ptt.replace_one({'title': post['title']}, post, True)
                updatePostDate({'title': post['title'], 'link': post['link'], 'author': post['autor'], 'date': post['date']})
           return 0
    except urllib.error.HTTPError as err:
        print(err.code)
        return -1
      
def showPtt():
    for post in ptt.find():
         pprint.pprint(post)

def getPost(title):
    # Convert from string to ObjectId:
    document = ptt.find_one({'title': title})
    print(document)
    
def updateDate_in_DB(obj, year):
      # print(title+" "+year)
      document = ptt.find_one({'title': obj['title']})
      print(document['date'])
      size = len(document['date'].strip('"').split('/'))
      date = document['date'].strip('"').split('/')[size-2:]
      date = year+"/"+'/'.join(date)
      print((parser.parse(date)))
      ptt.update_one({
          '_id': document['_id']
      }, { 
          '$set':{
              'date': (parser.parse(date)) #for mongoDB ISODate
           } 
      }, upsert=False)
      #print("date ===> "+date.isoformat())
      searchIndexing(obj['title'], obj['link'], date, obj['author'], document['_id'])
    
def resetDate_in_DB(obj):
    print(obj['title']+" "+obj['year'])
    document = ptt.find_one({'title': obj['title']})
    size = len(document['date'].strip('"').split('/'))
    bar = document['date'].strip('"').split('/')[size-2:]
    date = '/'.join(bar)
    ptt.update_one({
        'title': obj['title']
    },{
        '$set':{
            'date': date
        }
    }, upsert=True)
    
def scrapPtt():
    pageLink = {}
    
    if os.path.isfile('scrap_from.txt'):
        scrap_from = read_scrap_from()
    else:
        scrap_from = 4995
        
    print("scraping from --->"+ str(scrap_from))
    i = int(scrap_from)
    stopping_at = int(scrap_from)
    end = int(scrap_from) + scrap_size
    
    while i < end: 
        url = "https://www.ptt.cc/bbs/movie/index"+str(i)+".html"
        status = parseTopic(url)
        i = i+1
        if (status == 0):
            stopping_at = i
    print("scraping status ---> "+str(status)+"\n stoping @: "+ str(stopping_at))  
    
    if (status == 0):
        foo = int(scrap_from)
        foo += scrap_size
        scrap_from = foo
    else:
        scrap_from = stopping_at
    
    write_scrap_from(scrap_from)
    
def write_scrap_from(scrap_from):
    fd = open('scrap_from.txt', 'w')
    fd.write(str(scrap_from))   
    return

def read_scrap_from():
    fd = open('scrap_from.txt', 'r')
    scrap_from = fd.read()
    fd.close()
    return scrap_from
                      
def updatePostDate(obj):
    values = {}
    data = urllib.parse.urlencode(values)
    data = data.encode('ascii')  
    try:
        req = urllib.request.Request(obj['link'], data, headers) 
        with urlopen(req) as response:
            the_page = response.read()
            bsObj = BeautifulSoup(the_page,"html.parser")
            container = bsObj.select('.article-meta-value')
            for innerindex, item in enumerate(container):
                print(item.text);
                if (innerindex == 3):
                    if (item.text.find('(') == -1 or item.text.find(')') == -1):
                        print(('※' not in item.text))
                        if (len(item.text) >= 24 and ('※' not in item.text)):
                            updateDate_in_DB(obj, item.text[len(item.text)-4:])
                        elif ('※' in item.text):
                            print(item.text.split('※'))
                            year = item.text.split('※')[0]
                            updateDate_in_DB(obj, year[len(year)-4:])
                    else:
                         item['text'] = item.text.strip("()").split('(')[1]
                         print(item['text'][len(item['text'])-4:])
                         updateDate_in_DB(obj, item['text'][len(item['text'])-4:])  
    except urllib.error.HTTPError as err:
        print(err.code)
        return    
    
def searchIndexing(title, link, date, author, id):
    doc = {
        'link': link,
        'title': title,
        'date': date,
        'author': author
    }
    res = es.index(index="ptt-test", doc_type='ptt', id=id, body=doc)
    print(title+' indexing !')
    return

def scrapjob():
    scrapPtt()

def transLatejob():
    transLate()
    
def job():
    global scrap_from 
    print("I'm working..."+str(scrap_from))
    scrap_from +=1

schedule.every().day.at("3:50").do(scrapjob)
# node.js task perform between the time gap
schedule.every().day.at("4:10").do(transLatejob)
print(__name__)

def main():
    while True:
        schedule.run_pending()
        #time.sleep(1)
    
if __name__ == '__main__':
    main()

# showPtt()
#scrapPtt()
#updateDB()
# getPost("Re:[心得] 美國殺人魔結局")
# searchIndexing()
#transLate()

    