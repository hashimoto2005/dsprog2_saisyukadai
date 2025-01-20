import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import sqlite3
from tqdm import tqdm

# データベース名
db_name = 'job.db'

def DB_init():
    """データベースを初期化する関数"""
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    # テーブルが存在する場合は削除する
    cur.execute('DROP TABLE IF EXISTS job_postings;')

    # テーブルを作成するSQL
    sql = '''
        CREATE TABLE job_postings (
            id INTEGER PRIMARY KEY AUTOINCREMENT, -- 一意の識別子
            job_title TEXT NOT NULL,             -- 求人タイトル
            company_name TEXT,                   -- 企業名
            salary_min INTEGER,                  -- 最低年収
            salary_max INTEGER,                  -- 最高年収
            location TEXT,                       -- 勤務地
            occupation TEXT,                     -- 職種
            industry TEXT,                       -- 業種
            point TEXT,                          -- ポイント
            tags TEXT                            -- タグ
        );
    '''
    cur.execute(sql)
    
    con.close()

def get_detail_info(detail_url):
    """求人詳細ページから情報を取得する関数"""
    response = requests.get(detail_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
   
    # 求人タイトル
    job_title = soup.find("h1", class_="ttl-sub-header bb-gray pt15 pb12 brt-3 mt02").text.strip()
   
    # 求人情報のテーブル
    table = soup.find("table", class_="border-collapse td-px-10 td-py-15 vertical-align-baseline bg_color4 width100p")
   
    # 企業名
    company_name_row = table.find('td', string='企業名')
    company_name = company_name_row.find_next('td').text.strip()

    # 年収
    salary_row = table.find('td', string='年収')
    salary = salary_row.find_next('td').text.strip()

    # 年収の下限と上限を取得
    if '～' in salary:
        salary_min, salary_max = salary.split('～')
        salary_min_str = salary_min.replace('万円', '').replace(',', '')
        if salary_min_str != '':
            salary_min = int(salary_min_str)
        else:
            salary_min = None
        salary_max_str = salary_max.replace('万円', '').replace(',', '')
        if salary_max_str != '':
            salary_max = int(salary_max_str)
        else:
            salary_max = None

    # 勤務地
    location_row = table.find('td', string='勤務地')
    location = location_row.find_next('td').text.strip()

    # 職種
    occupation_row = table.find('td', string='職種')
    occupation = occupation_row.find_next('td').text.strip()
   
    # 業種
    industry_row = table.find('td', string='業種')
    industry = industry_row.find_next('td').text.strip()
    
    # ポイント (説明・備考)
    point_row = table.find('td', string='ポイント')
    point = point_row.find_next('td').text.strip()
   
    # タグ
    tag_list = table.find('div', class_='tag-list')
    tags = tag_list.text.strip()

    return job_title, company_name, salary_min, salary_max, location, occupation, industry, point, tags

def insert_to_db(data):
    """取得したデータをデータベースに挿入する関数"""
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    sql = '''
        INSERT INTO job_postings (
            job_title,
            company_name,
            salary_min,
            salary_max,
            location,
            occupation,
            industry,
            point,
            tags
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    '''
    cur.executemany(sql, data)
    con.commit()
    con.close()

def run_per_page(url):
    """各ページの求人情報を取得してデータベースに保存する関数"""
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    data = []
    recruit_infos = soup.find_all("li", class_="pt15 mt15 bg_color position-relative")
    for recruit_info in recruit_infos:
        detail_url_suffix = recruit_info.find("a")["href"]
        detail_url = urljoin("https://scouting.mynavi.jp", detail_url_suffix)
        data.append(get_detail_info(detail_url))
    insert_to_db(data)

def main():
    """メイン関数"""
    start = time.time()
    MAX_PAGE = 66  # 66ページまである
    for i in tqdm(range(1, MAX_PAGE + 1)):
        # 中国地方の求人情報を取得
        url = f"https://scouting.mynavi.jp/job-list/ar8/?page={i}"
        run_per_page(url)
        time.sleep(1)
    elapsed_time = time.time() - start
    print(f"経過時間：{elapsed_time}秒")

if __name__ == '__main__':
    main()