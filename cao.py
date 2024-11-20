from utils import clean
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep

def fetch(category: str, max_results: int = 5, start: int = 0):
    ''' Fetch papers from the ArXiv for a given category. '''
    params = {
        'search_query': 'cat:' + category,
        'start': start,
        'max_results': max_results,
        'sortBy': 'lastUpdatedDate',
        'sortOrder': 'descending'
    }

    # Perform GET request
    while True:
        try:
            api_url = 'http://export.arxiv.org/api/query'
            response = requests.get(api_url, params=params)
            soup = BeautifulSoup(response.content, 'lxml-xml')
            break
        except requests.exceptions.ConnectionError:
            sleep(1)
            continue

    # Convert data formats and store it in a list
    papers = []
    for entry in soup.find_all('entry'):
        authors = ','.join(clean(name.string)
                           for author in entry.find_all('author')
                           for name in author.find_all('name')
                           if name.string is not None)

        # Lấy tất cả các categories
        categories = ','.join(cat['term'] for cat in entry.find_all('category'))

        papers.append({
            'paper_id': entry.id.string,
            'authors': authors,
            'updated': datetime.fromisoformat(entry.updated.string[:-1]),
            'published': datetime.fromisoformat(entry.published.string[:-1]),
            'title': clean(entry.title.string),
            'abstract': clean(entry.summary.string),
            'categories': categories
        })

    return papers, int(soup.find('opensearch:totalResults').string)

def scrape(categories: list, max_results: int = 100, max_retries: int = 5):
    ''' Scrape papers for given categories and save to CSV. '''
    all_papers = []
    paper_ids = set()  # Set to track paper IDs and avoid duplicates
    total_fetched = 0  # To track the total number of papers fetched


    for category in categories:
        print(f"Scraping category: {category}")
        
        # Bắt đầu từ vị trí start = 0
        start = 0
        total_results = 1  # Khởi tạo tổng số bài báo (tạm thời là 1)
        retries = 0  # Counter for retries
        n = 0
        
        while start < total_results:
            # Fetch dữ liệu cho mỗi phần
            papers, total_results = fetch(category=category, max_results=max_results, start=start)
            
            if not papers:
                if retries < max_retries:
                    retries += 1
                    print(f"Attempt {retries}/{max_retries}: No papers returned, retrying...")
                    sleep(2)  # Thử lại sau 2 giây
                    continue  # Tiếp tục thử lại
                else:
                    print(f"Max retries reached for {category}. Moving to next category.")
                    break  # Dừng thử lại nếu đạt số lần tối đa
            
            # Xử lý các bài báo trả về
            for paper in papers:
                if paper['paper_id'] not in paper_ids:
                    all_papers.append(paper)
                    paper_ids.add(paper['paper_id'])
                    total_fetched += 1
            
            # Cập nhật start để lấy phần tiếp theo
            start += max_results

            # In số lượng bài báo đã cào cho đến thời điểm hiện tại
            print(f"Fetched {total_fetched} papers so far.")
            
            # Tạm dừng một chút để tránh quá tải API
            sleep(1)
            retries = 0
            n = n + 1
            if (n == 20): # mỗi chủ đề lấy khoảng 10000 bài báo
                break

    # Save to DataFrame
    df = pd.DataFrame(all_papers)
    
    # Save DataFrame to CSV
    csv_file = 'arxiv_cs_papers_all.csv'
    df.to_csv(csv_file, index=False)
    print(f'Data saved to {csv_file}')
    print(f"Total papers fetched: {total_fetched}")



if __name__ == '__main__':
    # Define the list of categories under "cs"
    cs_categories = ['cs.AI', 'cs.AR', 'cs.CC', 'cs.CE', 'cs.CG', 'cs.CL', 'cs.CR', 'cs.CV', 'cs.CY',
                     'cs.DB', 'cs.DC', 'cs.DL', 'cs.DM', 'cs.DS', 'cs.ET', 'cs.FL', 'cs.GL', 'cs.GR',
                     'cs.GT', 'cs.HC', 'cs.IR', 'cs.IT', 'cs.LG', 'cs.LO', 'cs.MA', 'cs.MM', 'cs.MS',
                     'cs.NA', 'cs.NE', 'cs.NI', 'cs.OH', 'cs.OS', 'cs.PF', 'cs.PL', 'cs.RO', 'cs.SC',
                     'cs.SD', 'cs.SE', 'cs.SI', 'cs.SY']
    
    # Scrape data for all categories
    scrape(categories=cs_categories, max_results=500)  # Adjust max_results as needed
