#! /usr/bin/env python

import urllib.request
import pymongo
import pprint
import redis
import json
import jieba
import jieba.posseg
import jieba.analyse
from urllib.request import urlopen
from bs4 import BeautifulSoup
from pymongo import MongoClient
from elasticsearch import Elasticsearch

es = Elasticsearch()
client = MongoClient('localhost', 27017)
db = client['test']
taiwan = db['taiwan']
ptt = db['ptt']
# headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.57 Safari/537.36'}
user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
headers = {'User-Agent': user_agent}
# response = urllib.request.urlopen("https://www.ptt.cc/bbs/movie/index.html").read()
# url = "https://www.ptt.cc/bbs/movie/index4888.html"

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
    
    with urlopen(req) as response:
       the_page = response.read()

    arrayDB = []
    inner = []
    bsObj =  BeautifulSoup(the_page,"html.parser")
    container = bsObj.select('.r-ent')
    
    for each_item in container:
         if (each_item.select('div.title')[0].text.find("本文已被刪除") != -1):
            continue
         print ("日期："+each_item.select('div.date')[0].text, "作者each_item.select('div.title')[0].text.splitlines()[1]："+each_item.select('div.author')[0].text)
         print (each_item.select('div.title')[0].text)
         print ("https://www.ptt.cc"+each_item.find('a', href=True)['href'])
         print ("---------------------------------")
         post = {}
         post['autor'] = each_item.select('div.author')[0].text
         post['date'] = each_item.select('div.date')[0].text.replace(" ","")
         post['link'] = "https://www.ptt.cc"+each_item.find('a', href=True)['href']
         post['title'] = each_item.select('div.title')[0].text.splitlines()[1]
         arrayDB.append(post)
         inner.append({'title': post['title'], 'link': post['link'], 'author': post['autor'], 'date': post['date']})

    result = ptt.insert_many(arrayDB)
    print('insert result: ' + str(result))
    return inner
         
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
      ptt.update_many({
          'title': obj['title']
      },{ 
          '$set':{
              'date': date
           } 
      }, upsert=False)
      document = ptt.find_one({'title': obj['title']})
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
    }, upsert=False)
    
def scrapPtt():
    pageLink = {}
    i = 4943
    end = 4952
    while i < end: 
        url = "https://www.ptt.cc/bbs/movie/index"+str(i)+".html"
        pageLink = parseTopic(url)
        for index, page in enumerate(pageLink):
            updatePostDate(page)
        i = i+1

def updateDB():
    cursor = ptt.find({})
    for document in cursor:
        updatePostDate(document)
        print('update '+document['title']+' finished!')
                        
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
                if (innerindex == 3):
                    if (item.text.find('(') == -1 or item.text.find(')') == -1):
                         # print(item.text[len(item.text)-4:])
                         updateDate_in_DB(obj, item.text[len(item.text)-4:])
                    else:
                         item['text'] = item.text.strip("()").split('(')[1]
                         # print(item['text'][len(item['text'])-4:])
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
    res = es.index(index="test", doc_type='ptt', id=id, body=doc)
    print(title+' indexing !')
    return
         
# showPtt()
# scrapPtt()
# updateDB()
# getPost("Re:[心得] 美國殺人魔結局")
# searchIndexing()
transLate()

