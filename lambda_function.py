"""app/lambda_function.py
"""
import psycopg2
import common
import pandas as pd
from datetime import datetime
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import requests


####################################
# 設定
# デフォルトの登録内容 **変更しない
now = datetime.now()
user_id = "user01"
error_flag = "0"
delete_flg = "0"
api_url = "https://"
####################################

db_cred = common.getCredentialJson()

def get_record(conn):
    sql_query = "SELECT id from TEMP"
    result = pd.read_sql(sql=sql_query, con=conn)
    return result
def update_record(conn, record_id, value):
    logger.info("TEMPテーブルへ更新：Record ID {}, value {}".format(record_id, value))
    sql_query = "UPDATE TEMP \
        SET value={0}, update_datetime=timestamp '{2}' \
        WHERE record_id={1}".format(value, record_id, now)
    logger.info(sql_query)
    with conn.cursor() as cur:
        cur.execute (sql_query) # クエリ実行
        print("result count:", cur.rowcount)  # 1が返ってくればOK
        if cur.rowcount == 0:
            logger.error("TEMP テーブルへの更新が失敗しました")
            conn.rollback()
def delete_record(conn, record_id):
    logger.info("TEMPテーブルへ削除：Record ID {0}".format(record_id))
    sql_query = "UPDATE TEMP \
        SET delete_flag='1', update_user_id={0}, update_datetime=timestamp '{2}' \
        WHERE record_id={1}".format(user_id, record_id, now)
    logger.info(sql_query)
    with conn.cursor() as cur:
        cur.execute (sql_query) # クエリ実行
        print("result count:", cur.rowcount)  # 1が返ってくればOK
        if cur.rowcount == 0:
            logger.error("TEMP テーブルへ削除が失敗しました")
            conn.rollback()
def call_and_process_api(conn, record_id):
    # パラメータ準備
    PARAMS = {'record_id':record_id}
    r = requests.get(url = api_url, params = PARAMS)
    if r.status_code == 200:
        delete_record(record_id)
    else:
        logger.error(f"{record_id}のAPI処理が失敗しました。")
    
def lambda_handler(event, context):
    try:
        with psycopg2.connect(**db_cred) as conn:
            # Check project id exits
            record_ids = get_record(conn)
            if len(record_ids) == 0:
                logger.warning("Tempテーブルにrecord id が一個も存在しません")
                return
            for index, row in record_ids.iterrows():
                id = row["record_id"]
                call_and_process_api(conn, id)
            conn.commit()  # commitする
        return "登録成功"

    except Exception as e:
        logger.error("エラーが発生しました。エラー：{}".format(str(e)))
        return "失敗"