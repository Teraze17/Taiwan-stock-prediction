"""
tii_company 很容易沒抓到欸
可以加入
1. 自動幫我抓到今天
2. 檢查log 重抓一次
"""
import requests
# import pandas as pd
from io import StringIO
from datetime import date, timedelta
import sqlite3
import os.path
from tqdm import tqdm
from time import sleep
import random


class Crawler:
    def __init__(self, info_name="stock"):
        """
        輸入需要抓取的資料類型
        :param info_name: stock：公司股價 tii_net：三大法人大盤 tii_company：三大法人買賣各公司
        """
        self.info_name = info_name

        self.request_params = {"response": "csv", "date": 20190812, "type": "ALL"}
        self.all_info = [("價格指數(臺灣證券交易所)", "price_index"), ("價格指數(跨市場)", "price_index"),
                         ("價格指數(臺灣指數公司)", "price_index"),
                         ("報酬指數(臺灣證券交易所)", "return_index"), ("報酬指數(跨市場)", "return_index"),
                         ("報酬指數(臺灣指數公司)", "return_index"),
                         ("大盤統計資訊", "total"), ("漲跌證券數合計", "stock_total"), ("每日收盤行情(全部)", "stock"), ("down", "down")]
        self.r_name = "exchangeReport/MI_INDEX"

        if info_name == "tii_net":
            self.request_params = {"response": "csv", "dayDate": 20190812, "type": "day"}
            self.all_info = [("三大法人買賣金額統計表", "tii_net"), ("down", "down")]
            self.r_name = "fund/BFI82U"
        elif info_name == "tii_company":
            self.request_params = {"response": "csv", "date": 20190812, "selectType": "ALL"}
            self.all_info = [("三大法人買賣超日報", "tii_company"), ("down", "down")]
            self.r_name = "fund/T86"

    def craw(self, start_date=0, end_date=0, coverage=True):
        """
        爬所需資料，從start_dat ~ end_date (含)
        :param start_date: start day of craw (yyyymmdd)
        :param end_date:   end day od craw (yyyymmdd)
        :param coverage:   coverage file or not
        :return: none
        """
        # log 分一下段
        log_name = "data/log/" + date.today().strftime("%Y%m%d") + ".csv"
        if os.path.isfile(log_name):
            with open(log_name, "a") as write_file:
                write_file.write("Crawer,,\n")

        start = date(int(str(start_date)[0:4]), int(str(start_date)[4:6]), int(str(start_date)[6:]))
        end = date(int(str(end_date)[0:4]), int(str(end_date)[4:6]), int(str(end_date)[6:]))
        # 從start_date ~ end_date 各天
        for pass_day in tqdm(range(0, (end - start).days + 1)):
            # (起始日 + 經過天數) 再將這天轉換成post 需要的格式 (yyyymmdd)
            r_date = (start + timedelta(days=pass_day))

            # 5,6 為周六、日 (股市休息啦)
            if r_date.weekday() not in [5, 6]:
                if "date" in self.request_params:
                    self.request_params["date"] = r_date.strftime("%Y%m%d")
                elif "dayDate" in self.request_params:
                    self.request_params["dayDate"] = r_date.strftime("%Y%m%d")

                # Check if folder exist or not
                do_craw = True
                for name, folder in self.all_info:
                    if name == "down":
                        continue
                    if os.path.isfile("data/" + folder + "/" + r_date.strftime("%Y%m%d") + ".csv"):
                        if coverage is True:
                            os.remove("data/" + folder + "/" + r_date.strftime("%Y%m%d") + ".csv")
                        else:
                            do_craw = False
                    elif do_craw is False:
                        do_craw = True

                # 不想重作但有缺值的時候，需要去除掉原檔案
                if coverage is False and do_craw is True:
                    for name, folder in self.all_info:
                        if os.path.isfile("data/" + folder + "/" + r_date.strftime("%Y%m%d") + ".csv"):
                            os.remove("data/" + folder + "/" + r_date.strftime("%Y%m%d") + ".csv")

                # split and save
                if do_craw:
                    r = requests.post("https://www.twse.com.tw/" + self.r_name, data=self.request_params)
                    empty_r = ["", "\r\n", "\r", "\n"]
                    if r.text not in empty_r:
                        self._split_and_save_file(r)
                    else:
                        # log
                        log_name = "data/log/" + date.today().strftime("%Y%m%d") + ".csv"
                        if os.path.isfile(log_name):
                            with open(log_name, "a") as write_file:
                                write_file.write("can not craw," + self.info_name + "," + r_date.strftime("%Y%m%d") + "\n")
                        else:
                            with open(log_name, "w") as write_file:
                                write_file.write("error,info_name,date\n")
                                write_file.write("Crawer,,\n")
                                write_file.write("can not craw," + self.info_name + "," + r_date.strftime("%Y%m%d") + "\n")
                        del log_name

                    sleep(random.randint(1, 5))  # 防止被ban

        # log 分一下段
        log_name = "data/log/" + date.today().strftime("%Y%m%d") + ".csv"
        if os.path.isfile(log_name):
            with open(log_name, "a") as write_file:
                write_file.write("=====,=====,=====\n")

    def recraw(self, recraw_list):
        """
        Not finish, need to hand input the day which need to recraw
        :param recraw_list: [need_date1, need_date2,...]
        """
        for drop_date in tqdm(recraw_list):
            # (起始日 + 經過天數) 再將這天轉換成post 需要的格式 (yyyymmdd)
            r_date = date(int(str(drop_date)[0:4]), int(str(drop_date)[4:6]), int(str(drop_date)[6:]))

            # 5,6 為周六、日 (股市休息啦)
            if r_date.weekday() not in [5, 6]:
                if "date" in self.request_params:
                    self.request_params["date"] = r_date.strftime("%Y%m%d")
                elif "dayDate" in self.request_params:
                    self.request_params["dayDate"] = r_date.strftime("%Y%m%d")

                # Check if folder exist or not
                for name, folder in self.all_info:
                    if os.path.isfile("data/" + folder + "/" + r_date.strftime("%Y%m%d") + ".csv"):
                        os.remove("data/" + folder + "/" + r_date.strftime("%Y%m%d") + ".csv")

                r = requests.post("https://www.twse.com.tw/" + self.r_name, data=self.request_params)
                empty_r = ["", "\r\n", "\r", "\n"]
                if r.text not in empty_r:
                    self._split_and_save_file(r)
                else:
                    log_name = "data/log/" + date.today().strftime("%Y%m%d") + ".csv"
                    if os.path.isfile(log_name):
                        with open(log_name, "a") as write_file:
                            write_file.write("can not craw," + self.info_name + "," + r_date.strftime("%Y%m%d") + "\n")
                    else:
                        with open(log_name, "w") as write_file:
                            write_file.write("error,info_name,date\n")
                            write_file.write("can not craw, " + self.info_name + "," + r_date.strftime("%Y%m%d") + "\n")
                    del log_name

            sleep(random.randint(0, 5))  # 被ban啦

        # log 分一下段
        log_name = "data/log/" + date.today().strftime("%Y%m%d") + ".csv"
        if os.path.isfile(log_name):
            with open(log_name, "a") as write_file:
                write_file.write("=====,=====,=====\n")

    def _split_and_save_file(self, request_file):
        """
        將post 回來的檔案進行切割以及儲存
        :return: none
        """
        contain_flag, now_info_list = 0, []

        for line in StringIO(request_file.text.replace("=", "").replace("\r", "")):
            if line != "\n":
                # line = line.replace("\"", "")
                if self.all_info[contain_flag][0] in line:
                    if contain_flag != 0:
                        self._write_file(self.all_info[contain_flag - 1], now_info_list)
                    now_info_list = []
                    contain_flag += 1
                else:
                    now_info_list.append(line)

        self._write_file(self.all_info[contain_flag - 1], now_info_list)  # 寫入最後一行

    def _write_file(self, name, write_info):
        """
        write information into file
        :param name: (information, folder name)
        :param write_info: need write information
        """
        file_name = "data/" + name[1] + "/"
        if "date" in self.request_params:
            file_name += str(self.request_params["date"]) + ".csv"
        elif "dayDate" in self.request_params:
            file_name += str(self.request_params["dayDate"]) + ".csv"

        # 如果不分成有檔案和沒檔案在抓價格指數、報酬指數的時候會被覆蓋
        # 那些[x:y] 是檔案有雜訊要去除
        if os.path.isfile(file_name):
            with open(file_name, "a") as write_file:
                if name[0] == "三大法人買賣金額統計表":
                    write_file.writelines(write_info[1:-9])
                elif name[0] == name[0] == "三大法人買賣超日報":
                    write_file.writelines(write_info[:-8])
                elif name[0] == "每日收盤行情(全部)":
                    write_file.writelines(write_info[2:])
                elif name[0] == "漲跌證券數合計":
                    write_file.writelines(write_info[1:-4])
                else:
                    write_file.writelines(write_info[1:])
        else:
            with open(file_name, "w") as write_file:
                if name[0] == "三大法人買賣金額統計表":
                    write_file.writelines(write_info[:-9])
                elif name[0] == name[0] == "三大法人買賣超日報":
                    write_file.writelines(write_info[:-8])
                elif name[0] == "每日收盤行情(全部)":
                    write_file.writelines(write_info[1:])
                elif name[0] == "漲跌證券數合計":
                    write_file.writelines(write_info[:-4])
                else:
                    write_file.writelines(write_info)

    def preprocess_company_data(self):
        pass
        # header 代表從那行開始，作法為以 \n(空白) 分割string，如果分出來的那個line有"證券代號" 就從那行(index-1)開始
        # df = pd.read_csv(StringIO(r.text.replace("=", "")),
        #                  header=["證券代號" in l for l in r.text.split("\n")].index(True) - 1)
        # df = df.drop(columns="Unnamed: 16")
        # df.to_csv("gg.csv")


class DBController:
    def __init__(self, db):
        self.conn = sqlite3.connect(db)

    def create_table(self, table_name):
        tn = '"' + table_name + '"'
        create_instruction = 'CREATE TABLE IF NOT EXISTS {0} (代號 integer, 名稱 text, 日期 date, 收盤 float, 漲跌 float, 開盤 float, 最高 float, 最低 float, 均價 float, 成交股數 integer, 成交金額"("元")" integer, 成交筆數 integer, 最後買價 float, 最後賣價 float, 發行股數 integer, 次日參考價 float, 次日漲停價 float);'.format(tn)
        # print(create_instruction)
        self.conn.cursor()
        self.conn.execute(create_instruction)

    def insert_value(self, *value):
        pass


if __name__ == "__main__":
    from preprocessing import Preprocesser
    print("大盤資訊")
    # crawler = Crawler(info_name="stock")
    # crawler.craw(start_date=20180101, end_date=20190101)
    # crawler = Crawler(info_name="stock")
    # crawler.recraw([20180125])
    print("三大法人")
    # crawler = Crawler(info_name="tii_net")
    # crawler.craw(start_date=20180131, end_date=20190101)
    # crawler = Crawler(info_name="tii_company")
    # crawler.craw(start_date=20180101, end_date=20190101, coverage=False)
    preprocess = Preprocesser()
    preprocess.init_company_file()
    # with open("big3.txt", "w") as gg:
    #     gg.write(r.text)
    # https: // www.twse.com.tw / fund / BFI82U?response = json & dayDate = 20190816 & weekDate = 20190812 & monthDate = 20190816 & type = day & _ = 1566102498986
    # cnx = sqlite3.connect('data.db')
    # df = pd.read_csv("data/RSTA3104_1080814.csv", skiprows=[0, 1])
    # dbc = DBController("data.db")
    # for name in df.名稱[:-7]:
    #     print(name)
    #     dbc.create_table(name)
    # df2 = pd.read_csv("data/20190102.csv", encoding="ms950", low_memory=False)
    # print(df2.head())
#

