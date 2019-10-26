"""
需要添加當資料加入時可以更新而不是全部重寫
"""
import pandas as pd
import os
from tqdm import tqdm
from datetime import date
# 為了美觀QQ
DF_COL = ["日期","證券代號", "證券名稱", "產業別", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌(+/-)",
          "漲跌價差", "最後揭示買價", "最後揭示買量", "最後揭示賣價", "最後揭示賣量", "本益比", "外陸資買進股數(不含外資自營商)",
          "外陸資賣出股數(不含外資自營商)", "外陸資買賣超股數(不含外資自營商)", "外資自營商買進股數", "外資自營商賣出股數",
          "外資自營商買賣超股數", "投信買進股數", "投信賣出股數", "投信買賣超股數", "自營商買賣超股數", "自營商買進股數(自行買賣)",
          "自營商賣出股數(自行買賣)", "自營商買賣超股數(自行買賣)", "自營商買進股數(避險)", "自營商賣出股數(避險)",
          "自營商買賣超股數(避險)", "三大法人買賣超股數"]

# Windows is awesome
# NOT_IN_CP950 = {"喆": "吉吉"}


class Preprocesser:
    def __init__(self):
        pass

    def init_company_file(self):
        """
        將以日期分的資料轉為以公司分
        :return:
        """
        global DF_COL
        # log 分一下段
        log_name = "data/log/" + date.today().strftime("%Y%m%d") + ".csv"
        if os.path.isfile(log_name):
            with open(log_name, "a") as write_file:
                write_file.write("Preprocesser,,\n")

        company_pd = pd.read_csv("data/company_list.csv", index_col=False)
        tqdm_sf = tqdm(os.listdir("data/stock"))

        # load stock and tii information
        info_list, tii_file_list = [], []
        for stock_file in tqdm_sf:
            tqdm_sf.set_description(stock_file)

            # 收盤資訊
            each_day_stock_pd = pd.read_csv("data/stock/" + stock_file, encoding="cp950")
            each_day_stock_pd = self.remove_empty(each_day_stock_pd)
            each_day_stock_pd.set_index("證券代號")
            stock_mask = (each_day_stock_pd["證券代號"].isin(company_pd["證券代號"].tolist()))

            # 三大法人資訊
            each_day_tii_pd = pd.read_csv("data/tii_company/" + stock_file, encoding="cp950")
            each_day_tii_pd = self.remove_empty(each_day_tii_pd)
            each_day_tii_pd.set_index("證券代號")
            tii_mask = (each_day_tii_pd["證券代號"].isin(company_pd["證券代號"].tolist()))

            info_list.append({"file_name": stock_file, "stock": each_day_stock_pd.loc[stock_mask], "tii": each_day_tii_pd.loc[tii_mask]})

        # write data according to company
        for company_index, company in company_pd.iterrows():
            if company_index <= 830:
                continue
            print(company["證券代號"], company["證券名稱"], company_index, "/", len(company_pd))
            company_stock_pd = pd.DataFrame(columns=DF_COL)
            append_dict = {}

            for info in tqdm(info_list):
                try:
                    # Extract data
                    company_stock_info = info["stock"].loc[info["stock"]["證券代號"] == company["證券代號"]].iloc[0]
                    try:
                        company_tii_info = info["tii"].loc[info["tii"]["證券代號"] == company["證券代號"]].iloc[0]
                    except:
                        company_tii_info = pd.Series(0, index=info["tii"].columns)

                    if not company_stock_info.empty:
                        # 寫入
                        append_dict.update(company_stock_info.to_dict())
                        append_dict.update(company_tii_info.to_dict())

                        append_dict["證券代號"] = company["證券代號"]
                        append_dict["證券名稱"] = company_stock_info["證券名稱"]
                        append_dict["產業別"] = company["產業別"]
                        append_dict["日期"] = info["file_name"][:8]
                        company_stock_pd = company_stock_pd.append(append_dict, ignore_index=True)

                except:
                    print("!!!", company["證券名稱"], info["file_name"][:8], " 的靈壓消失了")
                    # 寫log
                    # log_name = "data/log/" + date.today().strftime("%Y%m%d") + ".csv"
                    # if os.path.isfile(log_name):
                    #     with open(log_name, "a") as write_file:
                    #         write_file.write("no data," + company["證券代號"] + "," + info["file_name"][:8] + "\n")
                    # else:
                    #     with open(log_name, "w") as write_file:
                    #         write_file.write("error,info_name,date\n")
                    #         write_file.write("Preprocesser,,\n")
                    #         write_file.write("no data," + company["證券代號"] + "," + info["file_name"][:8] + "\n")
                    # del log_name

                    # tqdm_sf.close()
                    # break

            company_stock_pd.to_csv("data/company/" + company["證券代號"] + ".csv", encoding="cp950", index=False)

    def update_company_file(self):
        pass
    def remove_empty(self, df):
        """
        Remove Unname column and NAN row
        :param df: Want remove data frame
        :return: New data frame
        """
        # 去掉空col, row
        # df.set_index('類型', inplace=True)
        drop_col = []
        for col in df.columns:
            if "Unnamed" in col:
                drop_col.append(col)
        df.drop(columns=drop_col, inplace=True)
        df.dropna(how="all", inplace=True)
        return df


if __name__ == "__main__":
    print("  ")
    preprocesser = Preprocesser()
    preprocesser.init_company_file()
    # for file in os.listdir("data/stock"):
    #     print(file)

    # print("元大台灣50" in list(df2["證券名稱"]))
    # print(company_pd.head())