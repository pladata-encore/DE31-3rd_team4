import os
import sys
from datetime import datetime
import pandas as pd
import cnn_functions
import input_to_hadoop


def main():
    

    newsdf = cnn_functions.make_df()
    crawl_time = datetime.now().strftime("%Y-%m-%d_%H%M")
    new_order = ['institution', 'articleTitle', 'articleContents', 'category', 'regDate', 'getDate']

    # 폴더 이름
    folder_name = 'data'

    # 폴더가 없을 경우 생성
    if not os.path.exists("/home/hadoop/"+folder_name):
        os.makedirs("/home/hadoop/"+folder_name)

    newsdf.to_csv(f"/home/hadoop/data/cnn_{crawl_time}.csv", 
                        index=False, sep='|', 
                        header=True, 
                        columns=new_order, 
                        encoding='utf-8'
                    )

    
    
    # hadoop 연결
    client = input_to_hadoop.connect_hadoop()
    
    # 하나로 합쳐 total_df 생성
    total_df = cnn_functions.make_total_df("/home/hadoop/data/")
    
    # 뉴스 기사 중복값 체크 후 hadoop에 put
    input_to_hadoop.duplication_check(client, total_df)
                

                
if __name__ == "__main__":
    main()