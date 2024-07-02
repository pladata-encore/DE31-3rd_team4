import requests
from kbs_news import kbs_payloads


def get_kbsNews_count(cate_code, startDate, endDate):
    kbs_payloads.payload_kbsCount['datetimeBegin'] = startDate
    kbs_payloads.payload_kbsCount['datetimeEnd'] = endDate
    kbs_payloads.payload_kbsCount['contentsCode'] = kbs_payloads.cate_list[cate_code]

    kbs_count_r = requests.post(kbs_payloads.kbs_news_count_url, data=kbs_payloads.payload_kbsCount, headers=kbs_payloads.header_key)
    if kbs_count_r.status_code != 200:
        print("Fail. The status code is not 200.")
        return False
    else:
        return kbs_count_r.json()['data']


def get_kbsNews(news_count, cate_code, startDate, endDate):
    news_lines = []

    if news_count < 1:
        print("the news is not exist.")
        return False
    else:
        rows_per_page = news_count
        if news_count > 200:
            rows_per_page = 200
        
        end_page = news_count // rows_per_page + 2

        for curr_page in range(1, end_page):
            kbs_news_r = requests.get(kbs_payloads.kbs_news_get_url.format(curr_page, rows_per_page, startDate, endDate, kbs_payloads.cate_list[cate_code]), 
                                      headers=kbs_payloads.header_key)
            for news_context in kbs_news_r.json()['data']:
                tmp_dict = {}
                tmp_dict['regDate'] = news_context['regDate']
                tmp_dict['articleTitle'] = news_context['originNewsTitle']
                tmp_dict['articleContents'] = news_context['originNewsContents']
                tmp_dict['category'] = news_context['contentsName']

                news_lines.append(tmp_dict)
    
    return news_lines