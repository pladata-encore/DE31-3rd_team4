import os
import requests
from bs4 import BeautifulSoup as BS
import pandas as pd
from datetime import datetime
from glob import glob


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
    cnn_newsdf = cnn_newsdf.drop_duplicates(subset=['articleTitle', 'articleContents']).reset_index(drop=True)
    cnn_newsdf['institution'] = "CNN"
    now = datetime.now().strftime("%Y-%m-%d_%H%M")
    cnn_newsdf['getDate'] = now
    
    return cnn_newsdf


# 하나의 데이터프레임으로 생성
def make_total_df(f_path):
    # 파일 경로를 지정합니다.
    # filepath = "/home/hadoop/repository/news_crawling/"
    filepath = f_path
    
    # 파일이 존재하는지 확인합니다.
    if os.path.exists(filepath):
        files = glob(filepath+'cnn*.csv')
        # 데이터프레임으로 변환 후 리스트생성, 하나의 데이터프레임으로 concat
        dfs = [pd.read_csv(file, sep='|', ) for file in files]
        df = pd.concat(dfs, ignore_index=True)
        df = df.drop_duplicates(subset=['articleTitle', 'articleContents']).sort_values(by=['regDate'], ascending=False).reset_index(drop=True)
        return df
    else:
        print('파일이 존재하지 않습니다.')
        return 0
    

