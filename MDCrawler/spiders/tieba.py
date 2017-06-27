# -*- coding: utf-8 -*-
import re
import json
import MySQLdb
import scrapy
import scrapy.cmdline
from datetime import datetime
from scrapy.selector import Selector
from MDCrawler.items import TiebaItem
from MDCrawler.settings import MIN_REPLY_NUM,MAX_POSTS_NUM,PER_PAGE_NUM,\
    MYSQL_HOST,MYSQL_DBNAME,MYSQL_USER,MYSQL_PASSWD


class TiebaSpider(scrapy.Spider):
    name = 'tieba'
    allowed_domains = ['tieba.baidu.com']

    def start_requests(self):
        conn = MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASSWD, db=MYSQL_DBNAME, charset='utf8', use_unicode=True)
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('SELECT tieba_link FROM tieba_link')
        rows = cursor.fetchall()
        for row in rows:
            for pn in range(0, MAX_POSTS_NUM, PER_PAGE_NUM):
                url = '{}&pn={}'.format(row['tieba_link'].encode('utf8'), pn)
                yield scrapy.Request(url, callback=self.parse)
        cursor.close()

    def parse(self, response):
        sel = Selector(response)

        domain = 'http://tieba.baidu.com'
        links = sel.xpath('//span[@class="threadlist_rep_num center_text"][text() > $num]/../following-sibling::div//a[contains(@class,"j_th_tit")]/@href' ,num = MIN_REPLY_NUM).extract()
        for link in links:
            meta = {}
            label = sel.xpath('//a[@href=$link]/../i/@title', link=link).extract()
            if label:
                meta['label'] = ' '.join(label)
            last_reply_time = sel.xpath('//a[@href=$link]/../../..//span[@class="threadlist_reply_date pull_right j_reply_data"]/text()', link=link).extract_first()
            if last_reply_time:
                meta['last_reply_time'] = last_reply_time.strip()
            yield scrapy.Request(domain + link, callback=self.parse_item, meta=meta)

    def parse_item(self, response):
        sel = Selector(response)
        item = TiebaItem()

        self._extract_forum_name(sel, item)
        self._extract_post_id(sel, item)
        self._extract_lzonly_link(sel, item)
        self._extract_title(sel, item)
        self._extract_reply_num(sel, item)
        self._extract_author(sel, item)
        self._extract_post_time(sel, item)
        self._extract_last_reply_time(sel, item)
        self._extract_content(sel, item)
        self._extract_content_text(sel, item)
        self._extract_label(sel, item)

        return item

    def _remove_html_tags(self, text):
        """Remove html tags from a string"""
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)

    def _convert_time(self, time):
        """ convert time to real datetime """
        if ':' in time:
            return datetime.now().strftime('%Y-%m-%d') + ' ' + time + ':00'
        elif '-' in time:
            return str(datetime.now().year) + '-' + time + ' ' + datetime.now().strftime('%H:%M:%S')

    def _extract_forum_name(self, sel, item):
        tieba_name_xpath = '//a[@class="card_title_fname"]/text()'
        data = sel.xpath(tieba_name_xpath).extract()
        if data:
            item['forum_name'] = data[0].strip()

    def _extract_post_id(self, sel, item):
        item['post_id'] = sel.response.url.split('/p/')[1].split('?')[0]

    def _extract_lzonly_link(self, sel, item):
        item['lzonly_link'] = 'https://tieba.baidu.com/p/{}?see_lz=1'.format(item['post_id'])

    def _extract_title(self, sel, item):
        title_xpath = '//*[contains(@class, "core_title_txt")]/text()'
        data = sel.xpath(title_xpath).extract()
        if data:
            item['title'] = data[0].strip()

    def _extract_reply_num(self, sel, item):
        reply_num_xpath = '//li[@class="l_reply_num"]/span/text()'
        data = sel.xpath(reply_num_xpath).extract()
        if data:
            item['reply_num'] = data[0].strip()

    def _extract_author(self, sel, item):
        author_xpath = '//div[contains(@class,"j_louzhubiaoshi")]/@author'
        data = sel.xpath(author_xpath).extract()
        if data:
            item['author'] = data[0].strip()

    def _extract_post_time(self, sel, item):
        try:
            dataField = json.loads(sel.xpath('//div[@id="j_p_postlist"]/div[1]/@data-field').extract_first())
            item['post_time'] = dataField['content']['date'].strip() + ':00'
        except:
            post_time_xpath = '//div[@class="post-tail-wrap"]/span[last()]/text()'
            data = sel.xpath(post_time_xpath).extract()
            if data:
                item['post_time'] = data[0].strip() + ':00'

    def _extract_last_reply_time(self, sel, item):
        if sel.response.meta and 'last_reply_time' in sel.response.meta:
            item['last_reply_time'] = self._convert_time(sel.response.meta['last_reply_time'].strip())
        else:
            item['last_reply_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def _extract_content(self, sel, item):
        content_xpath = '//cc/div'
        data = sel.xpath(content_xpath).extract()
        if data:
            item['content'] = data[0].strip()

    def _extract_content_text(self, sel, item):
        item['content_text'] = self._remove_html_tags(item['content']).replace('\r','').replace('\n','').strip()

    def _extract_label(self, sel, item):
        if sel.response.meta and 'label' in sel.response.meta:
            item['label'] = sel.response.meta['label'].strip()
        else:
            item['label'] = ''

def main():
    scrapy.cmdline.execute(['scrapy', 'crawl', 'tieba'])

if __name__ == '__main__':
    main()