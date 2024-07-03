import os
import requests
from bs4 import BeautifulSoup as BS
import pandas as pd
from datetime import datetime
import pandas as pd


def make_df():
    """
    It will give you dataframe made from cnn news webpage
    """
    
    url = "https://edition.cnn.com/business"
    r = requests.get(url)
    bs = BS(r.text)
    
    # 기사 추가링크 생성
    alist = bs.find_all('a', class_="container__link container__link--type-article container_lead-plus-headlines__link")
    
    # category
    category = []
    for a in alist:
        category.append(a.get('href').split('/')[-3])
        
    # 새로운 링크로 크롤링
    new_bs = []
    for x in alist:
        r = requests.get(url[:-9] + x.get('href'))
        new_bs.append(BS(r.text))
        
        
    # 데이터프레임 준비작업
    titlelist=[]
    articlelist=[]
    regDate=[]
    for x in new_bs:
        # title
        titlelist.append(x.find('div', class_="headline__wrapper").text.strip())
    
        # contents
        contentlist = x.find('div', class_="article__content").find_all('p', class_="paragraph inline-placeholder vossi-paragraph-primary-core-light")
        for i, a in enumerate(contentlist):
            contentlist[i] = a.text.strip()
        articlelist.append(' '.join(contentlist))
    
        # updated
        regDate.append(' '.join(x.find('div', class_='timestamp vossi-timestamp-primary-core-light').text.strip().split()[1:]))
    
    cnn_news = {"articleTitle": titlelist, "articleContents": articlelist, "regDate": regDate, "category":category, }
    
    
    # 데이터프레임 생성
    cnn_newsdf = pd.DataFrame(cnn_news)
    cnn_newsdf['institution'] = "CNN"
    now = datetime.now().strftime("%Y-%m-%d_%H%M")
    cnn_newsdf['getDate'] = now
    
    return cnn_newsdf