import pymysql
import json

# 连接MYSQL数据库
db = pymysql.connect(
    host="localhost",  # 数据库地址，本地为localhost
    user="root",  # 数据库用户
    password="2902054141",  # 用户密码
    database="spider",  # 要连接的数据库名
    charset="utf8mb4",  # 字符编码
)
cursor = db.cursor()


def create_table():
    cursor.execute("DROP TABLE IF EXISTS Reviews")
    sql = """CREATE TABLE Reviews(
            shopID varchar(100),
            reviewID varchar(100),
            userID varchar(100),
            username varchar(255),
            isVIP boolean,
            totalScore varchar(50),
            scoreDetails text,  
            comment text,
            avgPrice varchar(100),
            likeDish text,
            publishTime varchar(100),
            merchantReply text,
            commentPics text
            );"""
    cursor.execute(sql)


def save_data(data_dict):
    sql = """INSERT INTO Reviews(shopID, reviewID, userID, username, isVIP, totalScore, scoreDetails, comment, avgPrice, likeDish, publishTime, merchantReply, commentPics) 
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    value_tup = (
        data_dict["店铺id"],
        data_dict["评论id"],
        data_dict["用户id"],
        data_dict["用户名"],
        data_dict["是否VIP"],
        data_dict["用户总分"],
        json.dumps(data_dict["用户打分"]),  # Serializing dict to JSON string
        data_dict["评论内容"],
        data_dict["人均价格"],
        json.dumps(data_dict["喜欢的菜"]),  # Serializing list to JSON string
        data_dict["发布时间"],
        data_dict["商家回复"],
        json.dumps(data_dict["评论图片"]),  # Serializing list to JSON string
    )
    try:
        cursor.execute(sql, value_tup)
        db.commit()
    except Exception as e:
        print(f"Failed to save data: {e}")


# export data to csv
def export_csv():
    sql = "SELECT * FROM Reviews;"
    cursor.execute(sql)
    results = cursor.fetchall()
    with open("./data/reviews.csv", "w", encoding="utf-8") as f:
        f.write(
            "shopID,reviewID,userID,username,isVIP,totalScore,scoreDetails,comment,avgPrice,likeDish,publishTime,merchantReply,commentPics\n"
        )
        for row in results:
            f.write(",".join([str(i) for i in row]) + "\n")


def close_sql():
    db.close()
