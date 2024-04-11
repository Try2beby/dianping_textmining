# -*- coding: utf-8 -*-
"""
Created on Mon Jul  9 16:42:52 2018

@author: bin
"""

# 目标爬取店铺的评论

import requests
from bs4 import BeautifulSoup
import time, random
import mysqls
import re
from fake_useragent import UserAgent
import os
import json

cacheDir = "./cache/"

ua = UserAgent()

# 读取JSON文件中的Cookies
with open(os.path.join(cacheDir, "cookies.json"), "r") as f:
    cookies = json.load(f)

# 构造Cookies字符串
cookie = "; ".join([f"{item['name']}={item['value']}" for item in cookies])

# 修改请求头
headers = {
    "User-Agent": ua.random,
    "Cookie": cookie,
    "Connection": "keep-alive",
    "Host": "www.dianping.com",
    "Referer": "http://www.dianping.com/shop/l9qwmkX3FoD9tExc/review_all",
}

# # 从ip代理池中随机获取ip
# ips = open("proxies.txt", "r").read().split("\n")


# def get_random_ip():
#     ip = random.choice(ips)
#     pxs = {ip.split(":")[0]: ip}
#     return pxs


# 获取html页面
def getHTMLText(url, code="utf-8"):
    try:
        time.sleep(random.random() * 6 + 2)
        r = requests.get(
            url,
            timeout=5,
            headers=headers,
            #                       proxies=get_random_ip()
        )
        r.raise_for_status()
        r.encoding = code
        return r.text
    except:
        print("产生异常")
        return "产生异常"


# 因为评论中带有emoji表情，是4个字符长度的，mysql数据库不支持4个字符长度，因此要进行过滤
def remove_emoji(text):
    try:
        highpoints = re.compile("[\U00010000-\U0010ffff]")
    except re.error:
        highpoints = re.compile("[\uD800-\uDBFF][\uDC00-\uDFFF]")
    return highpoints.sub("", text)


# 从html中提起所需字段信息
def parsePage(html, shpoID):
    infoList = []  # 用于存储提取后的信息，列表的每一项都是一个字典
    soup = BeautifulSoup(html, "html.parser")

    # # 找到所有页码的链接
    # page_links = soup.find_all('a', class_='PageLink')

    # # 假设最后一个PageLink包含的是最大页数
    # total_pages = int(page_links[-1].text) if page_links else 0

    for item in soup("div", "main-review"):
        cus_id = item.find("a", "name").text.strip()
        comment_time = item.find("span", "time").text.strip()
        try:
            comment_star = item.find("span", re.compile("sml-rank-stars")).get("class")[
                1
            ]
        except:
            comment_star = "NAN"
        cus_comment = item.find("div", "review-words").text.strip()
        scores = str(item.find("span", "score"))
        try:
            kouwei = re.findall(r"口味：([\u4e00-\u9fa5]*)", scores)[0]
            huanjing = re.findall(r"环境：([\u4e00-\u9fa5]*)", scores)[0]
            fuwu = re.findall(r"服务：([\u4e00-\u9fa5]*)", scores)[0]
            shicai = re.findall(r"食材：([\u4e00-\u9fa5]*)", scores)[0]
        except:
            kouwei = huanjing = fuwu = shicai = "NAN"

        infoList.append(
            {
                "cus_id": cus_id,
                "comment_time": comment_time,
                "comment_star": comment_star,
                "cus_comment": remove_emoji(cus_comment),
                "kouwei": kouwei,
                "huanjing": huanjing,
                "fuwu": fuwu,
                "shicai": shicai,
                "shopID": shpoID,
            }
        )
    return infoList


# 构造每一页的url，并且对爬取的信息进行存储
def getCommentinfo(shop_url, shpoID, page_begin, page_end):
    for i in range(page_begin, page_end):
        try:
            url = shop_url + "p" + str(i)
            html = getHTMLText(url)
            infoList = parsePage(html, shpoID)
            print("成功爬取第{}页数据,有评论{}条".format(i, len(infoList)))
            for info in infoList:
                mysqls.save_data(info)
            time.sleep(random.randint(5, 12))
            # 断点续传中的断点
            if (html != "产生异常") and (len(infoList) != 0):
                download_info = {"nowpage": i}
                with open(os.path.join(cacheDir, "download_info.json"), "w") as f:
                    json.dump(download_info, f)
            else:
                print("休息60s...")
                time.sleep(60)
        except:
            print("跳过本次")
            continue
    return


def get_download_info():
    if os.path.exists(os.path.join(cacheDir, "download_info.json")):
        with open(os.path.join(cacheDir, "download_info.json"), "r") as f:
            download_info = json.load(f)
    else:
        download_info = {}
    return download_info


# 根据店铺id，店铺页码进行爬取
def craw_comment(shopID="l9qwmkX3FoD9tExc", page=1064):
    shop_url = "http://www.dianping.com/shop/" + shopID + "/review_all/"
    # 读取断点续传中的续传断点
    download_info = get_download_info()
    if download_info:
        nowpage = download_info["nowpage"]
    else:
        nowpage = 0
    getCommentinfo(shop_url, shopID, page_begin=nowpage + 1, page_end=page + 1)
    mysqls.close_sql()
    return


if __name__ == "__main__":
    craw_comment()
