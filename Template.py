import signal
import requests
import time

shutdown = False

#global vars
bookES={}
bookWM={}
bookWA={}
bookMM={}
bookMA={}
bookCM={}
bookCA={}
bookETF={}

class ApiException(Exception):
    pass

class Book(object):
    def __init__(self, sym, json):
        self.sym = sym
        self.json = json
    def bid_price(self):
        if self.json['bids']:
            return self.json['bids'][0]['price']
        else:
            return float('nan')
    def ask_price(self):
        if self.json['asks']:
            return self.json['asks'][0]['price']
        else:
            return float('nan')
    def vwap(self):
        if self.json['bids']:
            if self.json['bids'][0]['vwap'] != None:
                return self.json['bids'][0]['vwap']
            return float('nan')
        else:
            return float('nan')

class Session(object):
    def __init__(self, url, key):
        self.url = url
        self.key = key
        self.tick = -1
    def __enter__(self):
        self.session = requests.Session()
        self.session.headers.update({'X-API-Key': self.key })
        return self
    def __exit__(self, type, value, traceback):
        self.session.close()
    def get_tick(self):
        while True:
            resp = self.session.get(self.url + '/v1/case', params = None)
            if not resp.ok:
                raise ApiException('could not get tick: ' + str(resp))
            json = resp.json()
            if json['status'] == 'STOPPED' or shutdown:
                return False
            if json['tick'] != self.tick:
                self.tick = json['tick']
                print('got tick', self.tick)
                return True
    def get_book(self, sym):
        resp = self.session.get(self.url + '/v1/securities/book', params = { 'ticker': sym })
        if not resp.ok:
            raise ApiException('could not get book: ' + str(resp))
        return Book(sym, resp.json())
    def send_order(self, sym, side, price, size):
        resp = self.session.post(self.url + '/v1/orders', params = { 'ticker': sym, 'type': 'LIMIT', 'action': side, 'quantity': size, 'price': price })
        if resp.ok:
            print('sent order', side, sym, size, '@', price)
        else:
            print('failed to send order', side, sym, size, '@', price, ':', resp.text)

def main():
    with Session('http://localhost:9999', '7PFL9YEE') as session:
        while session.get_tick():
            global bookES,bookWM,bookWA,bookMM,bookMA,bookCM,bookCA,bookETF
            bookES=session.get_book('ES')
            bookWM=session.get_book('WMT-M')
            bookWA=session.get_book('WMT-A')
            bookMM=session.get_book('MMM-M')
            bookMA=session.get_book('MMM-A')
            bookCM=session.get_book('CAT-M')
            bookCA=session.get_book('CAT-A')
            bookETF=session.get_book('ETF')
            #arbitrage_bot(session)
            # naive_bot(session)


def naive_bot(session):
    global bookES,bookWM,bookWA,bookMM,bookMA,bookCM,bookCA,bookETF

    if bookWM.bid_price() > bookWM.vwap()-0.002:
        session.send_order('WMT-M', 'SELL', bookWM.bid_price(),  1000)
    if bookWM.ask_price() < bookWM.vwap()+0.0065:
        session.send_order('WMT-M', 'BUY', bookWM.ask_price(),  1000)
    if bookMM.bid_price() > bookMM.vwap()-0.002:
        session.send_order('MMM-M', 'SELL', bookMM.bid_price(),  1000)
    if bookMM.ask_price() < bookMM.vwap()+0.0065:
        session.send_order('MMM-M', 'BUY', bookMM.ask_price(),  1000)
    if bookCM.bid_price() > bookCM.vwap()-0.002:
        session.send_order('CAT-M', 'SELL', bookCM.bid_price(),  1000)
    if bookCM.ask_price() < bookCM.vwap()+0.0065:
        session.send_order('CAT-M', 'BUY', bookCM.ask_price(),  1000)

    if bookWA.bid_price() > bookWA.vwap()-0.0035:
        session.send_order('WMT-A', 'SELL', bookWA.bid_price(),  1000)
    if bookWA.ask_price() < bookWA.vwap()+0.005:
        session.send_order('WMT-A', 'BUY', bookWA.ask_price(),  1000)
    if bookMA.bid_price() > bookMA.vwap()-0.0035:
        session.send_order('MMM-A', 'SELL', bookMA.bid_price(),  1000)
    if bookMA.ask_price() < bookMA.vwap()+0.005:
        session.send_order('MMM-A', 'BUY', bookMA.ask_price(),  1000)
    if bookCA.bid_price() > bookCA.vwap()-0.0035:
        session.send_order('CAT-A', 'SELL', bookCA.bid_price(),  1000)
    if bookCA.ask_price() < bookCA.vwap()+0.005:
        session.send_order('CAT-A', 'BUY', bookCA.ask_price(),  1000)


    return 0

def arbitrage_bot(session):
    global bookES,bookWM,bookWA,bookMM,bookMA,bookCM,bookCA,bookETF
    if bookWA.bid_price() > bookWM.ask_price() -0.001:
        session.send_order('WMT-A', 'SELL', bookWA.bid_price(),  1000)
        session.send_order('WMT-M', 'BUY', bookWM.ask_price(), 1000)
    if bookWA.ask_price() < bookWM.bid_price() + 0.007:
        session.send_order('WMT-M', 'SELL', bookWM.bid_price(), 1000)
        session.send_order('WMT-A', 'BUY', bookWA.ask_price(),  1000)
    if bookMA.bid_price() > bookMM.ask_price() -0.001:
        session.send_order('MMM-A', 'SELL', bookMA.bid_price(),  1000)
        session.send_order('MMM-M', 'BUY', bookMM.ask_price(), 1000)
    if bookMA.ask_price() < bookMM.bid_price() + 0.007:
        session.send_order('MMM-M', 'SELL', bookMM.bid_price(), 1000)
        session.send_order('MMM-A', 'BUY', bookMA.ask_price(),  1000)
    if bookCA.bid_price() > bookCM.ask_price() -0.001:
        session.send_order('CAT-A', 'SELL', bookCA.bid_price(),  1000)
        session.send_order('CAT-M', 'BUY', bookCM.ask_price(), 1000)
    if bookCA.ask_price() < bookCM.bid_price() + 0.007:
        session.send_order('CAT-M', 'SELL', bookCM.bid_price(), 1000)
        session.send_order('CAT-A', 'BUY', bookCA.ask_price(),  1000)
    if bookETF.bid_price() > bookWM.ask_price() + bookMM.ask_price() + bookCM.ask_price()-0.0215:
        session.send_order('ETF', 'SELL', bookETF.bid_price(),  1000)
        session.send_order('WMT-M', 'BUY', bookWM.ask_price(), 1000)
        session.send_order('MMM-M', 'BUY', bookMM.ask_price(), 1000)
        session.send_order('CAT-M', 'BUY', bookCM.ask_price(), 1000)
    if bookETF.ask_price() < bookWM.bid_price() + bookMM.bid_price() + bookCM.bid_price()+0.0125:
        session.send_order('WMT-M', 'SELL', bookWM.bid_price(), 1000)
        session.send_order('MMM-M', 'SELL', bookMM.bid_price(), 1000)
        session.send_order('CAT-M', 'SELL', bookCM.bid_price(), 1000)
        session.send_order('ETF', 'BUY', bookETF.ask_price(),  1000)
    return 0

def momentum_bot():
    return 0

def forward_looking():
    return 0

def dynamic_weighting():
    return 0

def news_adjusted_price():
    return 0

def money_manager(amount):
    return 0



def sigint(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True

if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint)
    main()
