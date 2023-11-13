import sqlite3
import time
from bs4 import BeautifulSoup
from urllib.parse import unquote, urlparse
import random
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
import os
import requests
from datetime import datetime


class Crawler:
    def __init__(self):
        pass


    # seed page -> Do the crawl process on that seed page
    def crawl(self, seed_page):
        '''
        1- First update the depth 1 URLs:
                If it has capacity take more depth 1 URLs
                and if not eliminate older of them which are not fresh      
        '''
        
        # Update or add the seed url to db
        seed_url = seed_page.url
        html, depth1_links = self.fetch_url(seed_url, seed_page.need_render)
        now = datetime.now()
        self.add_to_database(seed_url, 0, html, now, seed_url)

        # Update the depth 1 URLs 
        for link in depth1_links:
            html, hyperlinks = self.fetch_url(link, seed_page.need_render)
            # Check if the link you fetched has enough number of hyperlinks in order to validate it's html
            if len(hyperlinks) >= 2:
                now = datetime.now()
                self.add_to_database(link, 1, html, now, seed_url)
        
        # Check if we didn't exceed the limit of pages in depth 1
        self.check_limitation(1, seed_page.cap1, seed_url)
        

        '''
        2- Second update the deeper URLs:
            If it has capacity take more deeper URLs from depth 1 and itself (random samples)
            and if not eliminate older of them which are not fresh
        '''

        # Set how many samples we should take
        # For render sites -> depth1: 10, depth2: 20
        # For request sites -> depth1: 30, depth2: 60

        if seed_page.need_render:
            sample_depth1 = 3
            sample_depth2 = 6
        else:
            sample_depth1 = 6
            sample_depth2 = 12

        samples = set()
        samples = samples.union(self.extract_depth_x_URLs(1,
                                                           seed_url,
                                                             other_information=True,
                                                               randomness=True,
                                                                 sample_size=sample_depth1))
        samples = samples.union(self.extract_depth_x_URLs(2,
                                                seed_url,
                                                other_information=True,
                                                randomness=True,
                                                sample_size=sample_depth2))
    

        
        # Fetch these samples
        for item in samples:
            html, hyperlinks = self.fetch_url(item[0], seed_page.need_render)
            now = datetime.now()
            self.add_to_database(item[0], item[1], html, now, seed_url)
            
            for link in hyperlinks:
                html, hyper_hyperlinks = self.fetch_url(link, seed_page.need_render)
                # Check if the link you fetched has enough number of hyperlinks in order to validate it's html
                if len(hyper_hyperlinks) >= 2:
                    now = datetime.now()
                    self.add_to_database(link, 2, html, now, seed_url)


        # Check if we didn't exceed the limit of pages in depth 2
        self.check_limitation(2, seed_page.cap2, seed_url)


    @classmethod
    def extract_depth_x_URLs(cls, list_depth, seed_url, other_information = False, randomness = False, sample_size = 0):
        depth_x_URLs = set()

        netloc = urlparse(seed_url).netloc
        table_name = ''.join(char for char in netloc if char.isalnum())
        conn =sqlite3.connect('my_database.db')
        cursor = conn.cursor()

        if not randomness:
            cursor.execute(f"SELECT url, depth, extracted_time FROM {table_name} WHERE depth = ?", (list_depth, ))
        else:
            cursor.execute(f"SELECT url, depth, extracted_time FROM {table_name} WHERE depth = ? ORDER BY RANDOM() LIMIT {sample_size}", (list_depth, ))
        rows = cursor.fetchall()

        if other_information:
            for row in rows:
                depth_x_URLs.add((row[0], row[1], row[2]))
        else:
            for row in rows:
                depth_x_URLs.add(row[0])
        
        return depth_x_URLs


    @classmethod
    def check_limitation(cls, list_depth, cap, seed_url):
        '''existed_depth_x = cls.extract_depth_x_URLs(list_depth, seed_url, other_information=True)
        if len(existed_depth_x) > cap:
            lst_existed_depth_x = list(existed_depth_x)
            sorted_lst = sorted(lst_existed_depth_x, key=lambda x: x[2], reverse=True)
            elimination_list = sorted_lst[cap:]

            for item in elimination_list:
                cls.eliminate_from_database(item[0], seed_url)'''
        
        # Connect to the SQLite database (or create it if it doesn't exist)
        conn = sqlite3.connect('my_database.db')

        # Create a cursor object to execute SQL commands
        cursor = conn.cursor()

        netloc = urlparse(seed_url).netloc

        # Sanitize the netloc to create a safe table name
        table_name = ''.join(char for char in netloc if char.isalnum())
        # Specify the table name you want to query

        max_rows_to_keep = cap  # Adjust this to your desired limit

        # SQL query to retrieve and delete older rows with depth 
        query = f"""
            DELETE FROM {table_name}
            WHERE depth = {list_depth}
            AND extracted_time NOT IN (
                SELECT extracted_time
                FROM {table_name}
                WHERE depth = {list_depth}
                ORDER BY extracted_time DESC
                LIMIT {max_rows_to_keep}
            )
        """

        # Execute the query to delete the older rows
        cursor.execute(query)

        # Commit the changes to the database
        conn.commit()

        # Close the cursor and the database connection
        cursor.close()
        conn.close()
                


    @classmethod
    def eliminate_from_database(cls, url, seed_url):

        # Find the name of the table from seed url
        netloc = urlparse(seed_url).netloc
        table_name = ''.join(char for char in netloc if char.isalnum())

        conn = sqlite3.connect("my_database.db")
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM {table_name} WHERE url = ?", (url, ))

        conn.commit()
        cursor.close()
        conn.close()
             

    @ classmethod
    def wait(cls, seconds):
        start_time = time.time()
        wait_interval = 1
        while True:
            if time.time() - start_time > seconds:
                break
            time.sleep(wait_interval)


    @ classmethod
    def breakdown_url(cls, url):
        parsed_url = urlparse(url)
        scheme, domain = parsed_url.scheme, parsed_url.netloc
        base_url = scheme + "://" + domain
        return scheme, domain, base_url


    @ classmethod
    def remove_www(cls, url):
        if 'www' in url:
            index = url.index('www')
            return url[:index] + url[index + 4: ]
        else:
            return url


    @classmethod 
    def fetch_url(cls, url, need_render):
        
        hyperlinks = set()
        html_text = None
        cls.load_logging_config()
        
        if need_render: 
            driver_path = "/usr/bin/chromedriver"
            chrome_binary_path = '/usr/bin/chromium-browser'

            try:
                driver = cls.get_driver(driver_path, chrome_binary_path)
                driver.get(url) 
                waiting_time = 10
                cls.wait(waiting_time)
                html_text = driver.page_source
                soup = BeautifulSoup(html_text, 'html.parser')

                for a in soup.find_all('a', href = True):
                    rel_attribute = a.get('rel')
                    if rel_attribute is None or 'nofollow' not in rel_attribute:
                        complete_link = cls.find_complete_link(a, url)
                        if not complete_link:
                            continue            

                        hyperlinks.add(complete_link)

                driver.quit()
            except Exception as e:
                logging.warning(f"Error in fetching {url}\nMessage {e}")

            
            if hyperlinks:
                logging.info(f"Successfull fetch for {url}")
                
            return html_text, hyperlinks
        
        else:
            try:
                response = requests.get(url, timeout = 30)
                if response.status_code == 200:
                    html_text = response.text
                    soup = BeautifulSoup(html_text, 'html.parser')
                    
                    for a in soup.find_all('a', href = True):
                        rel_attribute = a.get('rel')
                        if rel_attribute is None or 'nofollow' not in rel_attribute:
                            complete_link = cls.find_complete_link(a, url)
                            if not complete_link:
                                continue

                            hyperlinks.add(complete_link)

            except Exception as e:
                logging.warning(f"Error in fetching {url}\nMessage {e}")

            if hyperlinks:
                logging.info(f"Successfull fetch for {url}")
                
            return html_text, hyperlinks

    
    @ classmethod
    def find_complete_link(cls, a, url):
    
        domains = ['upmusics', 'hexdownload', 'isna',
                'tiwall', 'digikala', 'ershaco',
                'aparat', 'farsnews', 'filesell',
                'pnu', 'filimo', 'varzesh3',
                'iranseda', 'wikishia', 'wikipedia',
                'ghatreh', 'yjc', 'vajehyab',
                'abadis', 'blogfa']
        
        scheme, domain, base_url = cls.breakdown_url(url)
        
        href = unquote(a['href'])
        
        if 'wikishia' in href or 'wikipedia' in href:
            if 'fa.' not in href:
                return 0
        
        if href[0:4].lower() == "http":
            complete_link = href
        elif href[0:2] == "//":
            complete_link = scheme + ":" + href
        elif href != '' and href[0] == '.':
            index = 0
            for char in href:
                if char == '.':
                    index += 1
                    
            complete_link = base_url + href[index:]
        elif href != '' and href[0] != "/":
            complete_link = base_url + '/' + href
        else:
            complete_link = base_url + href
            
        if '#' in complete_link:
            parts = complete_link.split('#')
            complete_link = parts[0]
            
    #     if '?' in complete_link:
    #         parts = complete_link.split('?')
    #         complete_link = parts[0]
        
        if complete_link[-1] == '/':
            complete_link = complete_link[:-1]
            
            
        excluded_extensions = ['.mp4', '.mp3', '.apk',
                            '.png', '.jpg', '.apt',
                            '.pdf', '.xlsx', '.zip',
                            '.xml', '.jpeg']
        
        bad_keywords = ['twitter', 'instagram', 'youtube',
                    't.me', 'telegram', 'linkedin',
                    'facebook', 'javascript', 'comtel',
                    '@', 'whatsapp', 'login',
                    'pdf', 'zip', 'xml'] + excluded_extensions
        

        if any(bad_keyword.lower() in complete_link.lower() for bad_keyword in bad_keywords):
            return 0
        if any(dom in complete_link for dom in domains):
            pass
        else:
            return 0

        '''dir_path = os.path.dirname(os.path.abspath(__file__))
        absolute_path = dir_path + f"/{domain}.log"
        with open(absolute_path, 'a') as file:
            file.write(complete_link + "\n")'''
        return cls.remove_www(complete_link)


    @ classmethod
    def get_driver(cls, driver_path, chrome_binary_path):
        user_agents_list = [
            {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1"}, 
            {"User-Agent": "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0"},
            {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0"},
            {"User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"},
            {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36"},
            {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36"}
        ]
        selected_header = random.sample(user_agents_list, 1)[0]
        # Create a ChromeService object and set the path to the Chrome binary
        chrome_service = ChromeService(driver_path, chrome_binary=chrome_binary_path)

        # Initialize a headless Chrome browser
        options = webdriver.ChromeOptions()
        options.add_argument("--enable-javascript")
        options.add_argument('--headless')  # Run in headless mode (no GUI)
        options.add_argument('--disable-gpu')  # Required for headless mode on some systems
        options.add_argument('--no-sandbox')  # Required for headless mode on some systems
        options.add_argument('--disable-dev-shm-usage')  # Required for headless mode on some systems
        options.add_argument('--disable-infobars')  # Disable infobars
        custom_user_agent = selected_header['User-Agent']    
        options.add_argument(f'user-agent={custom_user_agent}')
        
        # Pass the ChromeService instance when creating the Chrome WebDriver
        driver = webdriver.Chrome(service=chrome_service, options=options)
        driver.set_page_load_timeout(90)
        return driver
    

    @ classmethod
    def load_logging_config(cls):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        absolute_path = dir_path + "/my.log"
        logging.basicConfig(
            filename=absolute_path,
            filemode='a',
            format='%(asctime)s, %(process)d, %(name)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )


    @classmethod
    def add_to_database(cls, url, depth,  html, extracted_time, seed_url):

        # Find the name of the table from seed url
        netloc = urlparse(seed_url).netloc
        table_name = ''.join(char for char in netloc if char.isalnum())
        
        # Connect to db
        conn = sqlite3.connect("my_database.db")
        cursor = conn.cursor()

        # Check if the URL already exists in the table
        cursor.execute(f'SELECT url FROM {table_name} WHERE url = ?', (url,))
        existing_url = cursor.fetchone()

        if existing_url:
            # If the URL exists, update the HTML content
            cursor.execute(f'''
                UPDATE {table_name}
                SET html = ?, extracted_time = ?
                WHERE url = ?
            ''', (html, extracted_time, url))
        else:
            # If the URL doesn't exist, insert a new row
            cursor.execute(f'''
                INSERT INTO {table_name} (url, depth, html, extracted_time)
                VALUES (?, ?, ?, ?)
            ''', (url, depth, html, extracted_time))

        conn.commit()
        conn.close()
