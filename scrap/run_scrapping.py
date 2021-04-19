import urllib
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import pickle
from multiprocessing import Pool, Manager
import time
from google.cloud import storage
import os


storage_client = storage.Client()

filename = 'site1.json'
bucket = storage_client.get_bucket("investnews")
if filename:
    blob = bucket.get_blob(f'org_site/{filename}')
    datastore = json.loads(blob.download_as_string())


hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'none',
        'Accept-Language': 'en-US,en;q=0.8',
        'Connection': 'keep-alive'}


def testando(_key, lista):
    
    base = 'https://investnews.com.br/'
    first_url = base + _key
    print(f'Fetching: {first_url}')

    try:
        request = urllib.request.Request(first_url,headers=hdr)
        html = urllib.request.urlopen(request)
    except:
        print(f'Erro na request da pagina - {first_url}')
        return
    try:          
        bs = BeautifulSoup(html, 'lxml')
        dc = bs.find('div', class_='mvp-main-blog-body left relative')
    except:
        print(f'Erro no mvp-main-blog-body left relative') 
        return
    try:          
        dc = dc.find('ul', class_='mvp-blog-story-list left relative infinite-content')
    except:
        print(f'Erro no mvp-blog-story-list left relative infinite-conten')
        return
    try:
        articles = dc.findAll("li", {"class": "mvp-blog-story-wrap left relative infinite-post"})
    except:
        print(f'Erro no mvp-blog-story-wrap left relative infinite-pos')
        return
        
    for idx, _article in enumerate(articles): 
            try:
                lista.append(_article.find('a', href=True)['href'])
            except:
                continue 

          

def metid2(lista, processes=4):
    
    base = 'https://investnews.com.br/'
    pool = Pool(processes=processes)
    [pool.apply_async(testando, args=(_key, lista)) for _key in datastore[base]]
    pool.close()
    pool.join()
    print(f'Finalizando')


def funcao_marota(link, idx, tam):
    if idx % 5 == 0:
        print(f'fecthing: {idx} of {tam}')
     
    _full_link = link  

    try:
        request = urllib.request.Request(_full_link,headers=hdr)
        html = urllib.request.urlopen(request)
    except:
        print(f'Erro na request da pagina - {_full_link}')
        return 'None'
    try:          
        bs = BeautifulSoup(html, 'lxml')
    except:
        print(f'Erro no BeatifulSoup da pagina - {_full_link}')
        return 'None'
    try:
        dc = bs.find('body')['class']
        _id = dc[3].split('-')[1]   
    except:
        _title = 'no_id'
        return 'None'
    try:
       _title = bs.find('h1', class_="mvp-post-title left entry-title").text 
    except:
        _title = 'no_title'

    try:
        _data = bs.find('time', class_='post-date updated')['datetime']
    except:
        _data = 'no_date'

    _data_scrap = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    try:
        _fonte = bs.find('span', class_='author-name vcard fn author').strong.text
    except:
        _fonte = 'no_font'
    try:
        dc = bs.find('div', id='mvp-content-main')
        _text = dc.find_all('p')
    except:
        print('erro em obter o texto - p')
        return 'None'    
    
    _text_final_interm = []
    for text in _text:
        _text_final_interm.append(text.text)
    _text_final = "/n".join(_text_final_interm)
    
    return _id, link, _title, _fonte,  _data, _data_scrap, _text_final
    


def metid(dici, lista, processes=4):
    pool = Pool(processes=processes)           
    def aggregator(res): 
        if res != 'None':
            if res[0] != 'no_id':
                if res[0] not in dici.keys():
                    dici[res[0]] = {'link': res[1],
                                    'title': res[2],
                                    'fonte': res[3],
                                    'data_article': res[4],
                                    'data_scrap': res[5],
                                    'text': res[6]}

            else:
                print(f'data_id duplicated: {res[0]}')
            
    tam = len(lista)
    [pool.apply_async(funcao_marota, args=(link, idx, tam), callback=aggregator) for idx, link in enumerate(lista)]
    pool.close()
    pool.join()
    print(f'Finalizando')

    return dici.copy()

def _save_file(dici_final):
    date_file = datetime.now().strftime("%d-%m-%Y-%H:%M")
    blob = bucket.blob(f'write_p/{date_file}.pickle')
    blob.upload_from_string(
        data=json.dumps(dici_final),
        content_type='application/json'
       )
    print('Saved')

def main(request):
    manager = Manager()
    lista = manager.list()
    dici = manager.dict()

    inicio = time.asctime(time.localtime(time.time()))
    metid2(lista, processes=2)
    dc = metid(dici, list(lista), processes=3)
    _save_file(dc)
    fim = time.asctime(time.localtime(time.time()))
    print(f'Inicio: {inicio} - Fim: {fim}')
    return {'Sucesso': 'True'} 


                