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
            "User-Agent": UserAgent().random,
            "Cookie": self.load_cookies(),
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
        with open(os.path.join(self.cache_dir, "cookies.json"), "r") as f:
            cookies = json.load(f)
        return "; ".join([f"{item['name']}={item['value']}" for item in cookies])

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
            retry_count = 3
            error_msg = ""
            while retry_count > 0:
                try:
                    time.sleep(random.uniform(6, 12))
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
        info_list = []
        for item in soup.find_all("div", class_="main-review"):
            try:
                info = {
                    "shop_id": self.shop_id,
                    "user_id": item.find("a", class_="name")["data-user-id"],
                    "customer_name": item.find("a", class_="name").text.strip(),
                    "comment_time": item.find("span", class_="time").text.strip(),
                    "comment_star": item.find("span", class_="sml-rank-stars")["class"][
                        1
                    ],
                    "cus_comment": self.clean_text(
                        item.find("div", class_="review-words").text.strip()
                    ),
                    "kouwei": self.extract_detail(
                        item, r"口味：(\d\.\d)"
                    ),  # Extract taste rating
                    "huanjing": self.extract_detail(
                        item, r"环境：(\d\.\d)"
                    ),  # Extract environment rating
                    "fuwu": self.extract_detail(
                        item, r"服务：(\d\.\d)"
                    ),  # Extract service rating
                    "shicai": self.extract_detail(
                        item, r"食材：(\d\.\d)"
                    ),  # Extract ingredients rating
                    "renjun": self.extract_detail(
                        item, r"人均：(\d+)元"
                    ),  # Extract average cost per person
                }
                info_list.append(info)
            except Exception as e:
                self.logger.warning(f"Failed to parse item: {e}")
        return info_list

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
    scraper = DianpingScraper(shop_id="l9qwmkX3FoD9tExc", page_end=1064)
    scraper.run()
