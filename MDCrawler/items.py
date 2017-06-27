# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Item, Field

class MdcrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class TiebaItem(Item):
    forum_name = Field()
    post_id = Field()
    lzonly_link = Field()
    title = Field()
    reply_num = Field()
    author = Field()
    post_time = Field()
    last_reply_time = Field()
    content = Field()
    content_text = Field()
    label = Field()
