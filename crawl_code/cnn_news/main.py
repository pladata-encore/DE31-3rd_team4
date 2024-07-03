import os
import sys
from datetime import datetime
import pandas as pd
import cnn_functions


def main():
    
    
    df = cnn_functions.make_df()
    crawl_time = datetime.now().strftime("%Y-%m-%d_%H%M")
    new_order = ['institution', 'articleTitle', 'articleContents', 'category', 'regDate', 'getDate']

    # 폴더 이름
    folder_name = 'data'

    # 폴더가 없을 경우 생성
    if not os.path.exists(folder_name):
        os.makedirs("./"+folder_name)

    df.to_csv(f"./data/cnn_{crawl_time}.csv", 
                        index=False, sep=';', 
                        header=True, 
                        columns=new_order, 
                        encoding='utf-8'
                    )

                

                
if __name__ == "__main__":
    main()