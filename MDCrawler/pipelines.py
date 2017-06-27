# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
from datetime import datetime
from MDCrawler.settings import MYSQL_HOST,MYSQL_DBNAME,MYSQL_USER,MYSQL_PASSWD

class MdcrawlerPipeline(object):
    def process_item(self, item, spider):
        return item

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        pass

class MySQLStorePipeline(object):
    """A pipeline to store the item in a MySQL database."""

    def __init__(self):
        self.conn = MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASSWD, db=MYSQL_DBNAME, charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor() #MySQLdb.cursors.DictCursor

    def process_item(self, item, spider):
        self.cursor.execute('SELECT EXISTS(SELECT 1 FROM tieba WHERE post_id = %s)' ,(item['post_id'],))
        ret = self.cursor.fetchone()[0]

        if ret:
            self.cursor.execute('UPDATE tieba SET reply_num = %s, last_reply_time = %s, updated_at = %s WHERE post_id = %s',
                                (item['reply_num'], item['last_reply_time'], datetime.now().strftime("%Y-%m-%d %H:%M:%S"), item['post_id']))
            self.conn.commit()
            spider.log("Item updated in db: %s %r" % (item['post_id'], item))
        else:
            self.cursor.execute("""
                INSERT INTO tieba(forum_name, post_id, lzonly_link, title, reply_num, author, post_time, last_reply_time, content, content_text, label, created_at, updated_at)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,(item['forum_name'], item['post_id'], item['lzonly_link'], item['title'], item['reply_num'], item['author'], item['post_time'], item['last_reply_time'], item['content'], item['content_text'], item['label'], datetime.now().strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            self.conn.commit()
            spider.log("Item stored in db: %s %r" % (item['post_id'], item))


        return item