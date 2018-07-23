from bs4 import BeautifulSoup
import urllib
import requests
import pymongo
from pymongo import MongoClient
import datetime
import string
import re
import json

client = MongoClient(host='localhost', port=27017)
db = client.Tony
collection = db.lifestyle_source_app


def get_applink(page_link, meta_collection):    
    
    app_resp = requests.get(url=page_link)
    soup = BeautifulSoup(app_resp.text,'html') 
    
    for col in soup.find_all('div',{'class':'column first'}):

        for row in col.find_all('ul'):

            for item in row.find_all('a'):

                temp = str(item.get('href'))
                data = {'app_URL':temp}
                #out.update(data)
                result = meta_collection.insert_one(data)                
                
    
    for col in soup.find_all('div',{'class':'column'}):

        for row in col.find_all('ul'):

            for item in row.find_all('a'):

                temp = str(item.get('href'))
                data = {'app_URL':temp}
                result = meta_collection.insert_one(data)
                
    
    for col in soup.find_all('div',{'class':'column last'}):

        for row in col.find_all('ul'):

            for item in row.find_all('a'):

                temp = str(item.get('href'))
                data = {'app_URL':temp}
                result = meta_collection.insert_one(data)

    # to crawl next page

    if soup.find('a',{'class':'paginate-more'}) == None:   

        return

    else:

        for row in soup.find_all('a',{'class':'paginate-more'}):

            temp = str(row.get('href'))
            print (temp)
            break
    
        get_applink(temp, meta_collection)



def get_appData(app_link):
    
    m = re.search('id[0-9]+', app_link)
    id_string = m.group(0)
    start = id_string.find('id') + 2
    app_id = id_string[start:]
    
    app_resp = requests.get(url=app_link)
    soup = BeautifulSoup(app_resp.text,'html')
    
    # check if the link is available

    if soup.find('title').get_text() == "Connecting to the iTunes Store.":
        
        return
    
    else:

        app_title = soup.find("h1").get_text()
        app_price = soup.find("div", class_="price").get_text()
        app_genre = soup.find("span", attrs={"itemprop": "applicationCategory"}).get_text()
        app_updated = soup.find("span", attrs={"itemprop": "datePublished"}).get_text()
        app_version = soup.find("span", attrs={"itemprop": "softwareVersion"}).get_text()
        app_desc = soup.find("p", attrs={"itemprop": "description"}).get_text()

        # rating and review count is not alway available
        if(type(soup.find("span", attrs={"itemprop": "ratingValue"}))!=type(None)) : 
            app_currentRating = soup.find("span", attrs={"itemprop": "ratingValue"}).get_text()    
        else:
            app_currentRating = ""

        if(type(soup.find("span", attrs={"itemprop": "reviewCount"}))!=type(None)):
            app_currentRatingCount = soup.find("span", attrs={"itemprop": "reviewCount"}).get_text()
            start_r = 0
            end_r = app_currentRatingCount.find(' ', start_r)
            app_currentRatingCount = app_currentRatingCount[start_r:end_r]
        else:
            app_currentRatingCount = ""

        app_meta = {
            'app_id' : app_id,
            'app_title' : app_title,
            'app_price' : app_price,
            'app_genre' : app_genre,
            'app_updated' : app_updated,
            'app_version' : app_version,
            'app_desc' : app_desc,
            'app_currentRating' : app_currentRating,
            'app_currentRatingCount': app_currentRatingCount,
        }

    return app_meta



def rvs_crawler (app_collection):
    
    app_cusor = app_collection.distinct('app_id')
    
    reviews = []
    
    for appid in app_cusor: 

    	# restrict developer to crawl 10 pages reviews
	    for j in range(1,11):
	        
	        review_url = 'https://itunes.apple.com/us/rss/customerreviews/page=' + str(j) + '/id=' + appid + '/sortby=mostrecent/json'
	        r = requests.get(review_url)
	        text = json.loads(r.text)
	        e = text['feed']
	        
	        if 'entry' not in e :
	            
	            break

	        else:
	            e = text['feed']['entry']
	            num = len(e)
	            #print num
	            if num > 1 :
	                for u in range(1,num) :
	                    #print u
	                    reviews_ = e[u]
	                    re_id = reviews_['id']['label']
	                    re_authorid =  reviews_['author']['uri']['label']
	                    start = re_authorid.find('id') + 2
	                    re_authorid = re_authorid[start:]
	                    re_author = reviews_['author']['name']['label']
	                    #re_version = '1.0'
	                    re_version = reviews_['im:version']['label']
	                    re_rating = reviews_['im:rating']['label']
	                    re_title = reviews_['title']['label']
	                    re_content = reviews_['content']['label']
	                    reviews.append( {'re_authorid':re_authorid,'re_author':re_author,'re_id':re_id,
	                                     're_version': re_version, 're_rating': re_rating, 're_title': re_title, 
	                                     're_content': re_content} )
    
	    app_collection.update_one({'app_id':appid}, { '$set': { 'app_reviews' : reviews} })
	    print (j)



def get_version_update(meta_collection):    
    
    app_cusor = meta_collection.distinct('_id')
    
    for app_obj in app_cusor:
        
        app = meta_collection.find_one({'_id':app_obj})
        app_url = app['app_URL']
        
        app_resp = requests.get(url=app_url)
        soup = BeautifulSoup(app_resp.text,'html')

        for descption in soup.find_all('div',{'class':'product-review'}):
                        
            for text in descption.find_all('p'):
                
                update_info = text.text
        
        meta_collection.update({'_id':app_obj},{'$set':{'update_info':update_info}})
            

# Each category has [A-Z,#] pages
link_list = list(string.ascii_uppercase)
link_list.append('*')
#print link_list

for letter in link_list:

	link = 'https://itunes.apple.com/us/genre/ios-lifestyle/id6012?mt=8&letter='+letter
	get_applink(link,collection)
       
collection_cursor = db.lifestyle_source_app.distinct('_id')

for obj in collection_cursor:
    
    url = collection.find_one({'_id':obj})['app_URL']
    metadata = get_appData(url)
    
    if metadata == None:
        app_meta = {
            'app_id' : "",
            'app_title' : "",
            'app_price' : "",
            'app_genre' : "",
            'app_updated' : "",
            'app_version' : "",
            'app_desc' : "",
            'app_currentRating' : "",
            'app_currentRatingCount': ""
        }
        collection.update_one({'_id':obj},{'$set':{field:app_meta[field] for field in app_meta}})
    else:
        collection.update_one({'_id':obj},{'$set':{field:metadata[field] for field in metadata}})
    


# make sure id is correct
cursor = collection.find({ "app_id": { '$regex': '.*\Q/\E.*', '$options': 'i' } })
for doc in cursor:
    objid = doc['_id']
    temp = doc['app_id']
    start = temp.find('id') + 2
    output = temp[start:]
     
    collection.update_one ({"_id": objid }, { '$set': { 'app_id' : output} })


rvs_crawler (app_collection)