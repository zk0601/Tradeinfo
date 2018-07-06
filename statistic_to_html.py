import datetime
import os
import pandas
import numpy
import decimal
from pandas import DataFrame

from Mysql_connect import MYSQL

TRADE_SUIT_LIST = ['insur_eth', 'insur_btc', 'insur_usdt']


def main():
    yesterday_date = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    for trade_suit in TRADE_SUIT_LIST[2:]:
        data_dict = {}
        sell_data = MYSQL(trade_suit).select_data_from_db('sell', yesterday_date)
        buy_data = MYSQL(trade_suit).select_data_from_db('buy', yesterday_date)
        data_dict['sell_data'] = statistic_data(sell_data)
        data_dict['buy_data'] = statistic_data(buy_data)
        html_file = os.path.join('statistic_html', '%s_%s.html' % (trade_suit, yesterday_date))
        yesterday_str = yesterday_date[:4] + '-' + yesterday_date[4:6] + '-' + yesterday_date[6:]
        with open(html_file, 'w') as f:
            f.write('<h1>%s:%s</h1>' % (yesterday_str, trade_suit))
            f.write('<h2>Sell_info</h2>')
            for line in data_dict['sell_data'].to_html():
                f.write(line)
            f.write('<br>  </br>')
            f.write('<h2>Buy_info</h2>')
            for line in data_dict['buy_data'].to_html():
                f.write(line)
        print("finish %s" % trade_suit)


def statistic_data(data_tuple):
    time_data_dict = {}
    for data in data_tuple:
        for hour in range(12):
            data_hour = data[1].split('-')[1][:2]
            if int(data_hour) in [2*hour, 2*hour + 1]:
                if str(2*hour) in time_data_dict:
                    time_data_dict[str(2*hour)].append(data)
                    break
                else:
                    time_data_dict.setdefault(str(2*hour), [data])
                    break
    sum_trade, sum_amount, sum_price, high, low, average = [], [], [], [], [], []
    index = []
    for time, value_list in time_data_dict.items():
        index.append("%s-%s" % (time, str(int(time)+2)))
        trade, amount, price = 0, decimal.Decimal(0), decimal.Decimal(0)
        price_list = []
        for value in value_list:
            trade += 1
            amount += round(value[3], 2)
            price += round(value[2] * value[3], 8)
            price_list.append(value[2])
        sum_trade.append(trade)
        sum_amount.append(round(amount, 2))
        sum_price.append(round(price, 8))
        high.append(max(price_list))
        low.append(min(price_list))
        average.append(float(numpy.mean(price_list)))
    # 汇总当日数据
    index.append('全天数据总结')
    sum_trade.append(sum(sum_trade))
    sum_amount.append(sum(sum_amount))
    sum_price.append(sum(sum_price))
    high.append(max(high))
    low.append(min(low))
    average.append(float(numpy.mean(average)))

    df_dict = {
        '交易操作总数': sum_trade,
        '交易总数量': sum_amount,
        '交易总金额': sum_price,
        '最高交易价格': high,
        '最低交易价格': low,
        '平均交易价格': average
    }
    trade_info = DataFrame(df_dict, index=index)
    trade_info = trade_info.T
    trade_info.columns.name = '统计项\\时间段(hour)'
    print(trade_info)
    return trade_info


if __name__ == '__main__':
    main()