#! /usr/bin/env python

import urllib.request
import pymongo
import pprint
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

def parseUrl(url):
    values = {'name': 'Michael Foord',
          'location': 'Northampton',
          'language': 'Python' }
    data = urllib.parse.urlencode(values)
    data = data.encode('ascii')
    req = urllib.request.Request(url, data, headers)
    with urllib.request.urlopen(req) as response:
       the_page = response.read()
       
    array = []    
    soup =  BeautifulSoup(the_page,"html.parser")
    container = soup.select('.r-ent')
    for each_item in container:
         if (each_item.select('div.title')[0].text.find("本文已被刪除") != -1 or each_item.select('div.title')[0].text.find("[公告]") != -1):
            break
         print ("日期："+each_item.select('div.date')[0].text, "作者each_item.select('div.title')[0].text.splitlines()[1]："+each_item.select('div.author')[0].text)
         print (each_item.select('div.title')[0].text)
         print ("https://www.ptt.cc"+each_item.find('a', href=True)['href'])
         print ("---------------------------------")
         post = {}
         post['autor'] = each_item.select('div.author')[0].text
         post['date'] = each_item.select('div.date')[0].text.replace(" ","")
         post['link'] = "https://www.ptt.cc"+each_item.find('a', href=True)['href']
         post['title'] = each_item.select('div.title')[0].text.splitlines()[1]
         array.append(post)
    
    result = ptt.insert_many(array)
    print('insert result: ' + str(result))
         
def showPtt():
    for post in ptt.find():
         pprint.pprint(post)
         
def getPost(title):
    # Convert from string to ObjectId:
    document = ptt.find_one({'title': title})
    print(document)
def scrapPtt():
    i = 1
    while i < 5:
        url = "https://www.ptt.cc/bbs/movie/index"+str(i)+".html"
        parseUrl(url)
        i = i+1
         
         
# showPtt()
scrapPtt()
# getPost("Re:[心得] 美國殺人魔結局")

    