import scrapy
from urllib.parse import urlencode

from urllib.parse import urljoin
import re

# The following query consists of all the
# Taken from here: https://www.amazon.com/b?ie=UTF8&node=16310101&ref=nav_cs_dsk_grfl_sag
queries = [
#"/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A16323111&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_0", # 1-Baby Food
#"/s?rh=n%3A2983371011&fs=true&ref=lp_2983371011_sar", # 2-Alcohol "/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A2983371011&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_1",
#"/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A16310231&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_2", # 3-Beverages
#"/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A16318751&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_3", # 4-Bread & Bakery
#"/s?rh=n%3A16310251&fs=true&ref=lp_16310251_sar", # 5-Breakfast Food "/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A16310251&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_4"
#"/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A371460011&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_5", # 6-Dairy, Eggs and Plant Based
#"/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A18773724011&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_6", #7-Deli & Prepared Foods
# "/s?rh=n%3A3745171&fs=true&ref=lp_3745171_sar", #8-Flowers "/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A3745171&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_8"
#"/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A2255571011&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_7", # 9-Food & Beverages Gifts
#"/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A15709227011&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_9", #10Fresh Meal Ingredient Kits
#"/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A6459122011&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_10", # 11FROZEN FOODS
#"/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A6506977011&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_15", # 12Fresh Produce
#"/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A6518859011&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_13", # 13Meat Substitutes
#"/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A979861011&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_11" # 14Home Brewing & Winemaking
#"/s?rh=n%3A23759921011&fs=true&ref=lp_23759921011_sar" 15Snacks # "/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A23759921011&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_16" # 60,000
# "/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A371469011&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_12" # 16Meat & Seafood
#"/s?bbn=16310211&amp;rh=n%3A16310101%2Cn%3A18787303011&amp;dc&amp;qid=1664743602&amp;rnid=16310211&amp;ref=lp_16310211_nr_n_14" # Pantry Staples

]

import json

class AmazonSpider(scrapy.Spider):
    name = 'amazon'
    counter = 0
    limit = 500
    def get_url(self,url):
        API_KEY ='d652b34e48635445f6ad45e8f0f62bb7'#'d652b34e48635445f6ad45e8f0f62bb7''424cce8bf47a469cd7bab4546e5fa605'#'OVER:610dbd8ca8843eac862c11c903c13b13'#'OVER:1c6d58043f9ab3646882793df1c9152d' #'OVER:bfa44b9adb46aa34425442423f4fe499'
        payload = {'api_key': API_KEY, 'url': url,'country_code':'us'}
        proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)
        return proxy_url

    def start_requests(self):
        for query in queries:
            url = 'https://www.amazon.com' + query
            yield scrapy.Request(url=self.get_url(url), callback=self.parse_keyword_response)

    def parse_keyword_response(self, response):
        products = response.xpath('//*[@data-asin]')
        if self.counter > self.limit:
            return
        for product in products:
            asin = product.xpath('@data-asin').extract_first()
            product_url = f"https://www.amazon.com/dp/{asin}"
            yield scrapy.Request(url=self.get_url(product_url), callback=self.parse_product_page, meta={'asin': asin})
            self.counter+=1
        next_page = response.xpath('//a[contains(@class,"s-pagination-next") and contains(@class,"s-pagination-item") and contains(@class,"s-pagination-button") and contains(@class,"s-pagination-separator")]/@href').extract_first()
        print(next_page)
        if next_page:
            url = urljoin("https://www.amazon.com",next_page)
            yield scrapy.Request(url=self.get_url(url), callback=self.parse_keyword_response)

    def parse_product_page(self, response):
            asin = response.meta['asin']
            title = response.xpath('//*[@id="productTitle"]/text()').extract_first()
            image = re.search('"large":"(.*?)"',response.text).groups()[0]
            rating = response.xpath('//*[@id="acrPopover"]/@title').extract_first()
            number_of_reviews = response.xpath('//*[@id="acrCustomerReviewText"]/text()').extract_first()
            try:
                aboutitem =  response.xpath('//div[@id="feature-bullets"]/ul/li')
            except:
                aboutitem = ""
            try:
                aboutitem_array = response.xpath('.//ul[contains(@class,"a-unordered-list") and contains(@class,"a-vertical") and contains(@class,"a-spacing-mini")]//span/text()').extract()
            except:
                aboutitem_array = ""

            try:
                moreinfo = response.xpath('.//table[contains(@class,"a-normal") and contains(@class,"a-spacing-micro")]//td[@class="a-span3"]//span/text()').extract()
            except:
                moreinfo = ""

            try:
                moreinfo_1 = response.xpath('.//table[contains(@class,"a-normal") and contains(@class,"a-spacing-micro")]//td[@class="a-span9"]//span/text()').extract()
            except:
                moreinfo_1 = ""

            more_info_final = [": ".join(entry) for entry in zip(moreinfo, moreinfo_1)]

            try:
                unit =  response.xpath('.//div[contains(@class,"inline-twister-dim-title-value-truncate-expanded") and contains(@class,"a-padding-none") and contains(@class,"a-spacing-none") and contains(@class,"a-section")]/span/text()').extract()
            except:
                unit = ""

            try:
                category =  response.xpath('.//a[contains(@class,"a-color-tertiary") and contains(@class,"a-link-normal")]/text()').extract()
            except:
                category = ""

            #price = response.xpath('//*[@id="priceblock_ourprice"]/text()').extract_first()
            #availability = response.xpath('//*[@id="availability"]//span/text()').extract_first()
            #options = response.xpath('//div[@class="inline-twister-dim-title-value-truncate-expanded"]/span/text()').extract_first()

            #aboutitem =  response.xpath('//div[@id="feature-bullets"]//ul[contains(@class,"a-unordered-list") and contains(@class,"a-vertical a-spacing-mini")]/li')
            #if not price:
            #   price = response.xpath('//*[@data-asin-price]/@data-asin-price').extract_first() or \
            #           response.xpath('//*[@id="price_inside_buybox"]/text()').extract_first()
            #temp = response.xpath('//*[@id="twister"]')
            # sizes = []
            # colors = []
            # if temp:
            #    s = re.search('"variationValues" : ({.*})', response.text).groups()[0]
            #    json_acceptable = s.replace("'", "\"")
            #    di = json.loads(json_acceptable)
            #    sizes = di.get('size_name', [])
            #    colors = di.get('color_name', [])
            #bullet_points = response.xpath('//*[@id="feature-bullets"]//li/span/text()').extract()
            #seller_rank = response.xpath('//*[text()="Amazon Best Sellers Rank:"]/parent::*//text()[not(parent::style)]').extract()
            yield {'asin': asin, 'Title': title, 'MainImage': image, 'Rating': rating, 'NumberOfReviews': number_of_reviews,
                   #'Price': price#, 'AvailableSizes': sizes, 'AvailableColors': colors, 'BulletPoints': bullet_points,
                   #'SellerRank': seller_rank#,'Availability':availability,
                   'Items':aboutitem_array,'MoreInfo':more_info_final,'unit':unit,'category':category
                   }
