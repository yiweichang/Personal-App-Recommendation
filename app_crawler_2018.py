from pymongo import MongoClient
from bs4 import BeautifulSoup
import concurrent.futures
import requests
import string
import json
import re

client = MongoClient(host='localhost', port=27017)
db = client.Tony
collection = db.lifestyle_source_app


def get_applink(page_link, meta_collection):
    app_resp = requests.get(url=page_link)
    soup = BeautifulSoup(app_resp.text, 'html')
    for col in soup.find_all('div', {'class': 'column first'}):
        for row in col.find_all('ul'):
            for item in row.find_all('a'):
                temp = str(item.get('href'))
                data = {'app_URL': temp}
                meta_collection.insert_one(data)
    for col in soup.find_all('div', {'class': 'column'}):
        for row in col.find_all('ul'):
            for item in row.find_all('a'):
                temp = str(item.get('href'))
                data = {'app_URL': temp}
                meta_collection.insert_one(data)
    for col in soup.find_all('div', {'class': 'column last'}):
        for row in col.find_all('ul'):
            for item in row.find_all('a'):
                temp = str(item.get('href'))
                data = {'app_URL': temp}
                meta_collection.insert_one(data)
    # deprecated
    # to crawl next page
    # if soup.find('a', {'class': 'paginate-more'}) is None:
    #     return
    # else:
    #     for row in soup.find_all('a', {'class': 'paginate-more'}):
    #         temp = str(row.get('href'))
    #         print (temp)
    #         break
    #     get_applink(temp, meta_collection)


def get_appData(app_link):
    m = re.search('id[0-9]+', app_link)
    id_string = m.group(0)
    start = id_string.find('id') + 2
    app_id = id_string[start:]
    app_resp = requests.get(url=app_link)
    soup = BeautifulSoup(app_resp.text, 'lxml')
    # check if the link is available
    app_meta = {}
    if soup.find('title').get_text() != "Connecting to the iTunes Store.":
        script = soup.find('script').get_text()
        info = json.loads(script)
        app_title = info['name']
        app_price = info['offers']['price']
        app_genre = info['applicationCategory']
        app_desc = info['description']
        review_info = info.get('aggregateRating', None)
        if review_info:
            app_currentRating = review_info['ratingValue']
            app_currentRatingCount = review_info['reviewCount']
        else:
            app_currentRating = 0
            app_currentRatingCount = 0
        app_version = soup.find('p', class_='l-column small-6 medium-12 whats-new__latest__version')
        if app_version:
            app_version = app_version.get_text().replace('Version ', '')
        else:
            app_version = ''
        update_tag = soup.find('ul', attrs={'class': ['version-history__items']})
        app_updated = []
        if update_tag:
            for lis in update_tag.find_all('li'):
                tmp = {}
                for li in lis:
                    if li.name == 'h4':
                        tmp['version'] = li.get_text()
                    if li.name == 'div':
                        tmp['update_desc'] = li.get_text()
                app_updated.append(tmp)
        app_meta = {'app_id': app_id,
                    'app_title': app_title,
                    'app_price': app_price,
                    'app_genre': app_genre,
                    'app_updated': app_updated,
                    'app_version': app_version,
                    'app_desc': app_desc,
                    'app_currentRating': app_currentRating,
                    'app_currentRatingCount': app_currentRatingCount}
    if not app_meta:
        app_meta = {'app_id': "",
                    'app_title': "",
                    'app_price': "",
                    'app_genre': "",
                    'app_updated': "",
                    'app_version': "",
                    'app_desc': "",
                    'app_currentRating': "",
                    'app_currentRatingCount': ""}
    return app_meta


def get_reviews(appid):
    rss_base_link = 'https://itunes.apple.com/us/rss/customerreviews'
    reviews = []
    for j in range(1, 11):
        review_url = '/'.join([rss_base_link, 'page={}'.format(j),
                               'id={}'.format(appid), 'sortby=mostrecent', 'json'])
        r = requests.get(review_url)
        text = json.loads(r.text)
        e = text['feed']
        if 'entry' not in e:
            break
        else:
            e = text['feed']['entry']
            num = len(e)
            if num > 1:
                for u in range(1, num):
                    reviews_ = e[u]
                    re_id = reviews_['id']['label']
                    re_authorid = reviews_['author']['uri']['label']
                    start = re_authorid.find('id') + 2
                    re_authorid = re_authorid[start:]
                    re_author = reviews_['author']['name']['label']
                    re_version = reviews_['im:version']['label']
                    re_rating = reviews_['im:rating']['label']
                    re_title = reviews_['title']['label']
                    re_content = reviews_['content']['label']
                    reviews.append({'re_authorid': re_authorid, 're_author': re_author, 're_id': re_id,
                                    're_version': re_version, 're_rating': re_rating, 're_title': re_title,
                                    're_content': re_content})

# deprecated
# def get_version_update(meta_collection):
#     app_cusor = meta_collection.distinct('_id')
#     for app_obj in app_cusor:
#         app = meta_collection.find_one({'_id': app_obj})
#         app_url = app['app_URL']
#         app_resp = requests.get(url=app_url)
#         soup = BeautifulSoup(app_resp.text, 'html')
#         for descption in soup.find_all('div', {'class': 'product-review'}):
#             for text in descption.find_all('p'):
#                 update_info = text.text
#         meta_collection.update({'_id': app_obj}, {'$set': {'update_info': update_info}})


def crawl_app(db_collection, db_obj, app_url):
    app_meta = get_appData(app_url)
    if app_meta['app_id']:
        reviews = get_reviews(app_meta['app_id'])
        app_meta['app_reviews'] = reviews
    db_collection.update_one({'_id': db_obj}, {'$set': {field: app_meta[field] for field in app_meta}})

if __name__ == '__main__':
    # Each category has [A-Z,#] pages
    link_list = list(string.ascii_uppercase)
    link_list.append('*')
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for letter in link_list[:1]:
            # crawl first 10 pages
            for i in range(1, 11):
                link = 'https://itunes.apple.com/us/genre/ios-lifestyle/id6012?mt=8&letter={}&page={}#page'.format(letter, i)
                executor.submit(get_applink, link, collection)
    collection_cursor = db.lifestyle_source_app.distinct('_id')
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for obj in collection_cursor:
            url = collection.find_one({'_id': obj})['app_URL']
            executor.submit(crawl_app, collection, obj, url)

    # deprecated
    # make sure id is correct
    # cursor = collection.find({"app_id": {'$regex': '.*\Q/\E.*', '$options': 'i'}})
    # for doc in cursor:
    #     objid = doc['_id']
    #     temp = doc['app_id']
    #     start = temp.find('id') + 2
    #     output = temp[start:]
    #     collection.update_one({"_id": objid}, {'$set': {'app_id': output}})
