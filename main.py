#! /usr/bin/env python

import urllib.request
import pymongo
import pprint
from urllib.request import urlopen
from bs4 import BeautifulSoup
from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client['test']
taiwan = db['taiwan']
ptt = db['ptt']

# headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.57 Safari/537.36'}
user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
headers = {'User-Agent': user_agent}
# response = urllib.request.urlopen("https://www.ptt.cc/bbs/movie/index.html").read()
# url = "https://www.ptt.cc/bbs/movie/index4888.html"

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
         inner.append({'title': post['title'], 'link': post['link']})

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
    
def updateDate_in_DB(title, year):
      print(title+" "+year)
      document = ptt.find_one({'title': title})
      print(document['date'])
      ptt.update_one({
          'title': title
      },{ 
          '$set':{
              'date': year+"/"+document['date']
           } 
      }, upsert=False)
    
def scrapPtt():
    pageLink = {}
    i = 4929
    end = 4934
    while i < end: 
        url = "https://www.ptt.cc/bbs/movie/index"+str(i)+".html"
        pageLink = parseTopic(url)
        for index, link in enumerate(pageLink):
            updatePostDate(link['link'], link['title'])
        i = i+1

def updateDB():
    cursor = ptt.find({})
    for document in cursor:
        updatePostDate(document['link'], document['title'])
    print('update DB finished!')
                        
def updatePostDate(url, title):
    values = {}
    data = urllib.parse.urlencode(values)
    data = data.encode('ascii')  
    req = urllib.request.Request(url, data, headers)   
    with urlopen(req) as response:
       the_page = response.read()
       bsObj = BeautifulSoup(the_page,"html.parser")
       container = bsObj.select('.article-meta-value')
       for innerindex, item in enumerate(container):
           if (innerindex == 3):
               if (item.text.find('(') == -1 or item.text.find(')') == -1):
                    print(item.text[len(item.text)-4:])
                    updateDate_in_DB(title, item.text[len(item.text)-4:])
               else:
                    item['text'] = item.text.strip("()").split('(')[1]
                    print(item['text'][len(item['text'])-4:])
                    updateDate_in_DB(title, item['text'][len(item['text'])-4:])
                    
# showPtt()
# scrapPtt()
updateDB()
# getPost("Re:[心得] 美國殺人魔結局")

    

    
