# -*- coding: utf-8 -*-
"""雨量データ取得
気象庁の1時間ごとの雨量データを指定した期間取得
"""
import os
import datetime
import csv
import urllib.request
import time
from bs4 import BeautifulSoup


# メイン処理(処理フローの集約)
def main():
    # ユーザーの入力
    user_input_tuple = user_input()

    # 雨量取得(スクレイピング)
    create_csv_hour(user_input_tuple)        

    # 取得完了後に終了通知
    input("何かをキーを押したら終了いたします...")


def create_csv_hour(user_input_tuple):
    """
    CSVファイルを作成
        指定期間の気象データをスクレイピングしてCSV保存

    Args:
        user_input_tuple (tuple): ユーザー入力を含むタプル。要素は下記順序
        - str: CSV保存先ディレクトリパス
        - str: 観測所のprec_no
        - str: 観測所のblock_no
        - list of str: 取得開始日 (年, 月, 日)
        - list of str: 取得終了日 (年, 月, 日)

    Raises:
        Exception: スクレイピング時にエラーが発生した場合
    """
    print("雨量データを取得中...")

    # 計測開始時間
    start_time = time.time()
    
    # 入力データを変数に格納
    output_dir, prec_no, block_no, start_input_list, end_input_list = user_input_tuple

    # 気象台の種類を判定
    facility_type = select_weather_observatory(block_no)

    # 出力ファイル
    output_file = "weather_per_hour.csv"
    output_dir_name = os.path.join(output_dir, output_file)

    # データ取得開始日・終了日
    start_date = datetime.date(int(start_input_list[0]), int(start_input_list[1]), int(start_input_list[2]))
    end_date   = datetime.date(int(end_input_list[0]), int(end_input_list[1]), int(end_input_list[2]))

    # CSV作成
    fields = ["年月日", "時間", "降水量"]
    with open(output_dir_name, 'w', encoding='utf_8_sig') as f:
        writer = csv.writer(f, lineterminator='\n')
        writer.writerow(fields)

        date = start_date
        while date <= end_date:
            # 1秒間処理を中断(サイト負荷軽減)
            time.sleep(1)
            print(date)

            # 対象url
            url = f"https://www.data.jma.go.jp/obd/stats/etrn/view/hourly_{facility_type}.php?" \
                f"prec_no={prec_no}&block_no={block_no}&year={date.year}&month={date.month}&day={date.day}&view="

            # 1日の時間毎データ取得
            data_per_day = scraping_day_per_hour(url, date, facility_type)
            
            # CSV書込み
            for dpd in data_per_day:
                writer.writerow(dpd)

            date += datetime.timedelta(1)

    # 処理時間を表示
    print(processing_time(start_time)) 
    print(">>完了<<")


def select_weather_observatory(block_no):
    """
    気象台の種類を判定
        block_noから気象台の種類を返す

    Args:
        block_no (str): 観測所の識別番号(5桁か4桁の文字列)

    Returns:
        str: URL上の気象台の種類("s1"か"a1")
    """
    if len(block_no) == 5:
        facility_type = "s1"
    elif len(block_no) == 4:
        facility_type = "a1"
    else:
        print("block_noが無効なため停止")
        print("block_noを5桁か4桁で入力してください")
        return # 動作停止
    
    return facility_type


def scraping_day_per_hour(url, date, facility_type):
    """スクレイピングの関数
        指定URLから雨量データをスクレイピングし、1日分のデータを取得

    Args:
        url (str): スクレイピング対象のURL
        date (datetime.datetime): 日付
        facility_type (str): 気象台の種類"s1"か"a1"

    Returns:
        list: 1時間毎のリスト(各要素は[年, 月, 日, 時, 雨量]で1日分(24個)を返す)
    """

    # 気象ページ取得
    html = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(html ,'html.parser')
    trs = soup.find("table", { "class" : "data2_s" })

    data_list = []
    data_list_per_hour = []

    # tableの中身を取得
    for tr in trs.findAll('tr')[2:]:
        tds = tr.findAll('td')

        if facility_type == "s1":
            if tds[3].string == None:
                break;
            data_list.append(date)
            data_list.append(tds[0].string)
            data_list.append(string_to_float(tds[3].string))
            data_list_per_hour.append(data_list)        
            data_list = []
        elif facility_type == "a1":
            if tds[1].string == None:
                break;
            data_list.append(date)
            data_list.append(tds[0].string)
            data_list.append(string_to_float(tds[1].string))
            data_list_per_hour.append(data_list)        
            data_list = []

    return data_list_per_hour


def string_to_float(weather_data):
    """文字列を浮動小数点数に変換する関数

    Args:
        weather_data (str): 変換対象の文字列

    Returns:
        float: 変換した浮動小数点数、変換失敗時は0を返す
    """
    try:
        return float(weather_data)
    except:
        return 0


def user_input():
    """
    ユーザーインプット
        ユーザーの入力[保存先や取得年月など]受付け

    Returns:
        list: 入力情報のリスト
        - str: CSV出力先
        - str: 観測所のprec_no
        - str: 観測所のblock_no
        - list of str: 取得開始日 (年, 月, 日)
        - list of str: 取得終了日 (年, 月, 日)
    """

    print("■■■ 気象庁の雨量データ取得ツール ■■■\n")

    print("◆出来る事")
    print("  気象庁の1時間の雨量データを指定した期間取得")
    print("   ※サイトor回線が重い場合は取得に時間がかかります\n")

    print("◆事前準備")
    print("下記サイトで観測所を選択し、URLに表示されるprec_noとblock_noを確認してください")
    print("https://www.data.jma.go.jp/obd/stats/etrn/index.php?prec_no=&block_no=&year=&month=&day=&view=\n\n")
    
    print("----入力項目----\n")

    output_dir = input("CSVの出力先を入力してください・・・ ")
    prec_no = input("prec_noを入力してください - 入力例 [44]・・・ ")
    block_no = input("block_noを入力してください - 入力例 [47662]・・・ ")
    start_input = input("取得開始日を入力してください - 入力例 [2020/1/7]・・・ ")
    end_input = input("取得終了日を入力してください - 入力例 [2020/1/9]・・・ ")

    print("\nprec_no=" + str(prec_no) + ", " + "block_no=" + str(block_no) + 
            ", " + start_input + " ～ " + end_input + "の" + "雨量データを取得しますがよろしいでしょうか？")
    input("はいの場合、何かキーを押してください...\n")
    
    print("雨量データの取得中...")
    print(" ※気象庁サイトへの負荷軽減のため、データを1日取得ごとに1秒停止してます")

    start_input_list = start_input.split('/')
    end_input_list = end_input.split('/')

    return [output_dir, prec_no, block_no, start_input_list, end_input_list]


def processing_time(start_time):
    """処理時間を計測する関数

    Args:
        start_time (float): 処理の開始時間

    Returns:
        str: 処理時間を表す文字列 例: "-合計処理時間5.3秒-"
    """
    end = time.time() - start_time
    processing_time_str = " -合計処理時間" + str(round(end,1)) + "秒-" + "\n"
    return processing_time_str


if __name__ == '__main__':
    main()
