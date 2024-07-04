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
    
    
    input_to_hadoop.put_data(newsdf)                    

                
if __name__ == "__main__":
    main()