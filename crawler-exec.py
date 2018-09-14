# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import time

#funcao para transformar lista em array de strings
def listToStr(lista):
    
    aux_array = []
    for x in lista:
        aux_array.append(''.join(str(e) for e in x).lower())

    return aux_array 

#declaracao do array que armazenará os novos links
link = []

#abre o arquivo de saida
link_file = open("./links.csv", "a")

keywords_array = listToStr(pd.read_csv("./keywords.csv").values.tolist())
old_links_array = listToStr(pd.read_csv("./old-links.csv").values.tolist())
rss_array = listToStr(pd.read_csv("./rss.csv").values.tolist())

while (True):

    #abre o arquivo de saida
    link_file = open("./links.csv", "a")
    link_file_old = open("./old-links.csv", "a")

    for feed in rss_array:

        #requisicao 
        #result = requests.get(feed).text
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        result = session.get(feed).text

        soup = BeautifulSoup(result, 'xml')

        #print(soup.prettify())

        result1 = requests.get("https://br.advfn.com/indice/iee").text
        soup1 = BeautifulSoup(result1, 'html.parser')

        #data atual
        data = soup.find('pubDate').text
        
        try:
            cotation = soup1.find(id="quoteElementPiece2").text
        except AttributeError:
            cotation = "-" 

        #encontra todas as noticias
        noticias = soup.find_all('item')
        
        #crawler 
        for kw in keywords_array:
            for news in noticias:
                if kw in news.title.text.lower():
                    link.append(news.link.text)

        #checa se o link ja existe e adiciona no arquivo de saida
        for x in link:
            if(x not in old_links_array):
                link_file_old.write(x + "\n")
                link_file.write(x + ", " + data + ", " + cotation + "\n") #escrita no arquivo final
                old_links_array.append(x)
                print("Link adicionado: {} adicionado.".format(x))

    link_file.close()
    time.sleep(300)
