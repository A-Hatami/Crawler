from Crawler import Crawler
from Initialization import Initialization
from SeedPage import SeedPage

seed_urls = [
            ("https://digikala.com", 100, 1000, True),
            ("http://pnu.ac.ir", 100, 600, False),
            ("https://varzesh3.com", 200, 1300, False),
            ("https://fa.wikipedia.org", 100, 6150, False),
            ("https://fa.wikishia.net", 100, 9900, False),
            ("https://ghatreh.com", 100, 525, False),
            ("https://filimo.com", 100, 3400, True),
            ("https://yjc.ir", 200, 1300, False),
            ("https://blogfa.com", 100, 2000, False),
            ("https://aparat.com", 200, 1900, True),
            ("https://upmusics.com", 100, 2000, False),
            ("https://tiwall.com", 100, 400, False),
            ("https://abadis.ir", 100, 5900, False),
            ("https://isna.ir", 200, 1000, True),
            ("https://ershaco.com", 100, 900, False),
            ("https://farsnews.ir", 200, 1100, False),
            ("https://filesell.ir", 100, 400, False)
        ]

seed_pages = []

for item in seed_urls:  
    seed_pages.append(SeedPage(item[0], item[1], item[2], item[3]))

Initialization()
crawler = Crawler()

while True:
    for seed_page in seed_pages:
        crawler.crawl(seed_page)
