import requests
from bs4 import BeautifulSoup
import time, random, re
import os
import json
import logging
from fake_useragent import UserAgent
import mysqls


class DianpingScraper:
    def __init__(self, shop_id, page_end, config_path="./config.json"):
        self.shop_id = shop_id
        self.page_end = page_end
        self.config_path = config_path
        self.load_config()
        self.setup_logging()

    def load_config(self):
        with open(self.config_path, "r") as f:
            config = json.load(f)
        self.base_url = config["base_url"]
        self.cache_dir = config["cache_dir"]
        os.makedirs(self.cache_dir, exist_ok=True)
        self.headers = {
            # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "User-Agent": UserAgent().random,
            "Cookie": "",
            "Connection": "keep-alive",
            "Host": "www.dianping.com",
            "Referer": f"{self.base_url}/shop/{self.shop_id}/review_all",
        }

    def setup_logging(self):
        if not os.path.exists("./logs"):
            os.makedirs("./logs")

        # 创建日志器
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # 创建控制台处理器和文件处理器
        handler_console = logging.StreamHandler()
        handler_file = logging.FileHandler(
            f"./logs/{self.shop_id}_scraper.log", mode="a+"
        )  # 'a' 为追加模式

        # 设置格式化器
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler_console.setFormatter(formatter)
        handler_file.setFormatter(formatter)

        # 添加处理器到日志器
        self.logger.addHandler(handler_console)
        self.logger.addHandler(handler_file)

    def load_cookies(self):
        with open(os.path.join(self.cache_dir, "cookies.txt"), "r") as f:
            self.all_cookies = [line.strip() for line in f.readlines()]
            random.shuffle(self.all_cookies)
        # check if cookies are valid
        for cookies in self.all_cookies:
            proxy_retry_count = 3
            while proxy_retry_count > 0:
                proxy = self.get_proxy()
                # log proxy info
                self.logger.info(f"Using proxy to test cookies: {proxy}")
                self.add_cookies(cookies)
                try:
                    response = requests.get(
                        self.base_url,
                        headers=self.headers,
                        proxies={"http": f"http://{proxy}"},
                    )
                    # check 403 error
                    if response.status_code == 403:
                        self.logger.error("Cookies are invalid, retrying...")
                        break
                    elif response.status_code == 200:
                        self.logger.info("Cookies are valid.")
                        time.sleep(random.uniform(1, 2))
                        return
                except requests.RequestException as e:
                    self.logger.error(f"Request failed: {e}, retrying...")
                    proxy_retry_count -= 1

    def add_cookies(self, cookies):
        self.headers["Cookie"] = cookies

    def get_proxy(self):
        return requests.get("http://127.0.0.1:5010/get/").json().get("proxy")

    def delete_proxy(self, proxy):
        requests.get(f"http://127.0.0.1:5010/delete/?proxy={proxy}")

    def get_html_text(self, url, encoding="utf-8"):
        # change Referer setting
        try:
            if self.current_page > 1:
                self.headers["Referer"] = (
                    f"{self.base_url}/shop/{self.shop_id}/review_all/p{self.current_page - 1}"
                )
        except AttributeError:
            pass

        proxy_retry_count = 3
        while proxy_retry_count > 0:
            proxy = self.get_proxy()
            # log proxy info
            self.logger.info(f"Using proxy: {proxy}")
            self.load_cookies()
            retry_count = 3
            error_msg = ""
            while retry_count > 0:
                try:
                    response = requests.get(
                        url,
                        timeout=5,
                        headers=self.headers,
                        proxies={"http": f"http://{proxy}"},
                    )
                    response.raise_for_status()
                    response.encoding = encoding
                    return response.text, ""
                except requests.RequestException as e:
                    error_msg = str(e)
                    self.logger.error(f"Request failed: {e}, retrying...")
                    retry_count -= 1
            self.delete_proxy(proxy)
            self.logger.error("Proxy failure, switching proxy...")
            proxy_retry_count -= 1
        return "failed", error_msg

    def parse_page(self, html):
        soup = BeautifulSoup(html, "html.parser")
        all_review = []
        for review in soup.find_all("div", class_="main-review"):
            try:
                review_username = review.select(".name")[0].text.strip()
            except:
                review_username = "-"

            try:
                user_id = review.select(".name")[0]["href"].split("/")[-1]
            except:
                user_id = "-"

            review_total_score = ""
            try:
                review_score_detail = {}
                review_avg_price = ""
                review_score_detail_temp = (
                    review.select(".score")[0]
                    .text.replace(" ", "")
                    .replace("\n", " ")
                    .strip()
                    .split()
                )
                try:
                    review_total_score = str(
                        float(review.select(".sml-rank-stars")[0]["class"][1][-2:]) / 10
                    )
                except:
                    review_total_score = ""

                for each in review_score_detail_temp:
                    if "人均" in each:
                        review_avg_price = each.split("：")[1].replace("元", "")
                    else:
                        temp = each.split("：")
                        review_score_detail[temp[0]] = temp[1]
            except:
                review_score_detail = {}
                review_avg_price = ""

            try:
                review_text = (
                    review.select(".review-words")[0]
                    .text.replace(" ", "")
                    .replace("收起评价", "")
                    .replace("\r", " ")
                    .replace("\n", " ")
                    .strip()
                )
            except:
                review_text = "-"
            try:
                review_like_dish = (
                    review.select(".review-recommend")[0]
                    .text.replace(" ", "")
                    .replace("\r", " ")
                    .replace("\n", " ")
                    .strip()[5:]
                    .split()
                )
            except:
                review_like_dish = []
            try:
                review_publish_time = review.select(".time")[0].text.strip()
            except:
                review_publish_time = "-"
            try:
                review_id = review.select(".actions")[0].select("a")[0].attrs["data-id"]
            except:
                review_id = "-"

            try:
                review_pic_list = []
                review_pic_list_temp = review.select(".review-pictures")[0].select("a")
                for each in review_pic_list_temp:
                    url = each["href"]
                    review_pic_list.append("http://www.dianping.com" + str(url))
            except:
                review_pic_list = []

            try:
                review_merchant_reply = review.select(".shop-reply-content")[
                    0
                ].text.strip()
            except:
                review_merchant_reply = ""

            # 检查是否为vip用户
            try:
                # 检查是否存在VIP标记
                vip_status = (
                    review.find("div", class_="dper-info").find("span", class_="vip")
                    is not None
                )
            except:
                vip_status = False

            each_review = {
                "店铺id": self.shop_id,
                "评论id": review_id,
                "用户id": user_id,
                "用户名": review_username,
                "是否VIP": vip_status,
                "用户总分": review_total_score,
                "用户打分": review_score_detail,
                "评论内容": review_text,
                "人均价格": review_avg_price,
                "喜欢的菜": review_like_dish,
                "发布时间": review_publish_time,
                "商家回复": review_merchant_reply,
                "评论图片": review_pic_list,
            }
            all_review.append(each_review)
        return all_review

    def get_star_rating(self, item):
        try:
            return item.find("span", class_=re.compile("sml-rank-stars")).get("class")[
                1
            ]
        except:
            return "NAN"

    def clean_text(self, text):
        # remove emoji
        return re.compile("[\U00010000-\U0010ffff]").sub("", text)

    def extract_detail(self, item, pattern):
        try:
            return re.findall(pattern, str(item.find("span", class_="score")))[0]
        except:
            return "NAN"

    def get_comment_info(self):
        download_info = self.get_download_info()
        current_page = download_info.get("nowpage", 0) + 1
        for i in range(current_page, self.page_end + 1):
            self.current_page = i
            url = f"{self.base_url}/shop/{self.shop_id}/review_all/p{i}"
            if i == 1:
                url = f"{self.base_url}/shop/{self.shop_id}/review_all"
            html, error_msg = self.get_html_text(url)
            if html == "failed":
                self.logger.error("Failed to fetch page, skipping...")
                self.update_download_info(i, success=False, error_msg=error_msg)
                # sleep 10-15 min
                time.sleep(random.uniform(600, 900))
                continue
            info_list = self.parse_page(html)
            # record how many items are fetched
            self.logger.info(f"Page {i} fetched {len(info_list)} items")
            for info in info_list:
                mysqls.save_data(info)
            self.update_download_info(i, success=True)
            time.sleep(random.uniform(6, 12))

    def get_download_info(self):
        try:
            with open(os.path.join(self.cache_dir, "download_info.json"), "r") as f:
                download_info = json.load(f)
            # 确保每个shop_id的信息独立管理
            shop_info = download_info.get(self.shop_id, {})
            return shop_info
        except FileNotFoundError:
            return {}

    def update_download_info(self, page, success=True, error_msg=""):
        try:
            with open(os.path.join(self.cache_dir, "download_info.json"), "r") as f:
                download_info = json.load(f)
        except FileNotFoundError:
            download_info = {}

        shop_info = download_info.get(self.shop_id, {})
        shop_info.update(
            {
                "nowpage": page,
                "last_attempt_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "last_success": success,
                "error_message": error_msg,
                "failed_attempts": shop_info.get("failed_attempts", 0)
                + (0 if success else 1),
            }
        )
        download_info[self.shop_id] = shop_info

        with open(os.path.join(self.cache_dir, "download_info.json"), "w") as f:
            json.dump(download_info, f, indent=4)

    def run(self):
        self.get_comment_info()
        mysqls.close_sql()


if __name__ == "__main__":
    scraper = DianpingScraper(shop_id="l9qwmkX3FoD9tExc", page_end=400)
    scraper.run()
