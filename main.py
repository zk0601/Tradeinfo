import time
import multiprocessing

from Mysql_connect import MYSQL
from get_trade_info import InfoFromAPI
from log import log

TRADE_SUIT_LIST = ['insur_eth', 'insur_btc', 'insur_usdt']
COLLECT_INTERVAL_TIME = 10
logger = log()


def main():
    logger.info("START!!")
    for trade_suit in TRADE_SUIT_LIST:
        db_con = MYSQL(trade_suit)
        if not db_con.is_table_exist():
            db_con.create_table()
            db_con.db_shut_off()
    pool = multiprocessing.Pool(processes=len(TRADE_SUIT_LIST))
    for trade_suit in TRADE_SUIT_LIST:
        pool.apply_async(func=collect_and_store, args=(trade_suit,))
    pool.close()
    pool.join()


def change_timestamp(date_ms):
    time_str = str(date_ms)
    time_date = int(time_str[:10])
    time_ms = time_str[-3:]
    time_date = time.strftime("%Y%m%d-%H%M%S", time.localtime(time_date))
    return time_date + '.' + time_ms


def collect_and_store(trade_suit):
    while True:
        logger.info("Start collect trade data on %s" % trade_suit)
        try:
            data = InfoFromAPI(trade_suit).get_trade_info()
        except Exception:
            continue
        db_conn = MYSQL(trade_suit)
        for info_dict in data:
            datetime = change_timestamp(info_dict["date_ms"])
            db_conn.store_data_in_table(trade_id=info_dict["tid"], datetime='"{0}"'.format(datetime), price=info_dict["price"],
                                        amount=info_dict["amount"], trade_type='"{0}"'.format(info_dict["type"]))
        print("complete a storage on %s" % trade_suit)
        time.sleep(COLLECT_INTERVAL_TIME)


if __name__ == '__main__':
    main()
