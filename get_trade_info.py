import requests
import json


class InfoFromAPI(object):
    def __init__(self, trade_suit, timeout=10):
        self.trade_suit = trade_suit
        self.timeout = timeout

    def get_trade_info(self):
        url = 'https://www.okex.com'
        api_op = '/api/v1/trades.do?symbol=%s' % self.trade_suit
        response = requests.get(url+api_op, timeout=self.timeout)
        content = response.content
        content = content.decode('utf-8')
        content = json.loads(content)
        return content


# if __name__ == '__main__':
#     a = InfoFromAPI('insur_btc')
#     print(a.get_trade_info())
#     print(InfoFromAPI('insur_eth').get_trade_info())
#     a = InfoFromAPI('insur_eth').get_trade_info()
#     print(a[0])

