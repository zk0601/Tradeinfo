import pymysql
from log import log

logger = log()


class MYSQL(object):
    def __init__(self, trade_suit):
        self.server_ip = "47.91.252.155"
        self.username = 'root'
        self.passwd = '123456'
        self.database_name = 'trade_info'
        self.trade_suit = trade_suit
        try:
            self.db = pymysql.connect(self.server_ip, self.username, self.passwd, self.database_name)
            self.cursor = self.db.cursor()
        except Exception as e:
            logger.info(e)
            logger.info("Connect database failed!!")

    def is_table_exist(self):
        sql = "show tables"
        self.cursor.execute(sql)
        ret = [self.cursor.fetchall()]
        tables = []
        if not ret[0]:
            return False
        for element in ret[0]:
            tables.append(element[0])
        if self.trade_suit in tables:
            return True
        else:
            return False

    def create_table(self):
        sql = """CREATE TABLE %s (
                 TRADE_ID int PRIMARY KEY,
                 DATETIME varchar(255),
                 PRICE DECIMAL(16,8),  
                 AMOUNT DECIMAL(16,8),
                 TRADE_TYPE varchar(255) )""" % self.trade_suit
        try:
            self.cursor.execute(sql)
            logger.info("Create table %s" % self.trade_suit)
        except Exception as e:
            logger.info(e)
            logger.info("Create table failed!")

    def store_data_in_table(self, **kwargs):
        trade_id = kwargs.get('trade_id')
        datetime = kwargs.get('datetime', None)
        price = kwargs.get('price', None)
        amount = kwargs.get('amount', None)
        trade_type = kwargs.get('trade_type', None)
        sql = """INSERT INTO %s(TRADE_ID,
                 DATETIME, PRICE, AMOUNT, TRADE_TYPE)
                 VALUES (%d, %s, %e, %g, %s)""" \
              % (self.trade_suit, trade_id, datetime, price, amount, trade_type)
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            logger.info(e)
            self.db.rollback()
            logger.info("store data failed!")

    def select_data_from_db(self, trade_type, trade_date):
        sql = """SELECT * FROM {0} 
                 WHERE TRADE_TYPE = '{1}' 
                 AND DATETIME LIKE "{2}%" """.format(self.trade_suit, trade_type, trade_date)
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        return results

    def db_shut_off(self):
        self.db.close()


# if __name__ == '__main__':
#     a= MYSQL('insur_usdt')
#     print(a.is_table_exist())
#     a.create_table()
#     a.store_data_in_table(trade_id=6, datetime='"20180703-1648"', price=0.000025, amount=124.115, trade_type='"buy"')
#     print(a.is_table_exist())
#     a.db_shut_off()
#
#     b = a.select_data_from_db("sell", "20180706")
#     print(b)
