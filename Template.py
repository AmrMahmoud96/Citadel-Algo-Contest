import signal
import requests
import time
import numpy as np
import scipy.stats
import math

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
adjusted_price={}
lastnews=0
firstnews=True
limita=0
limitb=0

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
    def get_news(self):
        global lastnews,firstnews
        resp = self.session.get(self.url + '/v1/news', params = {'since':lastnews,'limit':1})
        if not resp.ok:
            raise ApiException('could not get news: ' + str(resp))
        else:
            r = resp.json()
            if len(r)>0 and (r[0]['news_id']!=lastnews or firstnews):
                firstnews=False
                lastnews = r[0]['news_id']
                news_adjusted_price(r[0])

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
            session.get_news()
            arbitrage_bot(session)
            probability_bot(session,1500000)
            # naive_bot(session)

def probability_bot(session,assignedlimit):
    global bookES,bookWM,bookWA,bookMM,bookMA,bookCM,bookCA,limita
    pWdown = scipy.stats.norm.cdf(bookWA.bid_price(),7+adjusted_price.get('WMT',0),math.sqrt(12))
    pWup = scipy.stats.norm.sf(bookWA.ask_price(),7+adjusted_price.get('WMT',0),math.sqrt(12))
    if pWdown>0.502 and limita - 5000>-1*assignedlimit:
        limita -= 5000
        session.send_order('WMT-A', 'SELL', bookWA.bid_price()+0.01,  700)
    elif(pWup > 0.502 and limita + 5000<assignedlimit):
        limita += 5000
        session.send_order('WMT-A', 'BUY', bookWA.ask_price()-0.01,  700)
    pWMdown = scipy.stats.norm.cdf(bookWM.bid_price(),7+adjusted_price.get('WMT',0),math.sqrt(12))
    pWMup = scipy.stats.norm.sf(bookWM.ask_price(),7+adjusted_price.get('WMT',0),math.sqrt(12))
    if(pWMdown>0.502 and limita - 5000>-1*assignedlimit):
        limita -= 5000
        session.send_order('WMT-M', 'SELL', bookWM.bid_price()+0.01,  700)
    elif(pWMup > 0.502 and limita + 5000<assignedlimit ):
        limita += 5000
        session.send_order('WMT-M', 'BUY', bookWM.ask_price()-0.01,  700)

    pMdown = scipy.stats.norm.cdf(bookMA.bid_price(),20+adjusted_price.get('MMM',0),math.sqrt(20))
    pMup = scipy.stats.norm.sf(bookMA.ask_price(),20+adjusted_price.get('MMM',0),math.sqrt(20))
    if(pMdown>0.502 and limita - 5000>-1*assignedlimit):
        limita -= 5000
        session.send_order('MMM-A', 'SELL', bookMA.bid_price()+0.01,  250)
    elif(pMup > 0.502 and limita + 5000<assignedlimit):
        limita += 5000
        session.send_order('MMM-A', 'BUY', bookMA.ask_price()-0.01,  250)
    pMMdown = scipy.stats.norm.cdf(bookMM.bid_price(),20+adjusted_price.get('MMM',0),math.sqrt(20))
    pMMup = scipy.stats.norm.sf(bookMM.ask_price(),20+adjusted_price.get('MMM',0),math.sqrt(20))
    if(pMMdown>0.502 and limita - 5000>-1*assignedlimit):
        limita -= 5000
        session.send_order('MMM-M', 'SELL', bookMM.bid_price()+0.01,  250)
    elif(pMMup > 0.502 and limita + 5000<assignedlimit):
        limita += 5000
        session.send_order('MMM-M', 'BUY', bookMM.ask_price()-0.01,  250)

    pCdown = scipy.stats.norm.cdf(bookCA.bid_price(),15+adjusted_price.get('CAT',0),math.sqrt(26))
    pCup = scipy.stats.norm.sf(bookCA.ask_price(),15+adjusted_price.get('CAT',0),math.sqrt(26))
    if(pCdown>0.502 and limita - 5000>-1*assignedlimit):
        limita -= 5000
        session.send_order('CAT-A', 'SELL', bookCA.bid_price()+0.01,  333)
    elif(pCup > 0.502 and limita + 5000<assignedlimit):
        limita += 5000
        session.send_order('CAT-A', 'BUY', bookCA.ask_price()-0.01,  333)
    pCMdown = scipy.stats.norm.cdf(bookCM.bid_price(),15+adjusted_price.get('CAT',0),math.sqrt(26))
    pCMup = scipy.stats.norm.sf(bookCM.ask_price(),15+adjusted_price.get('CAT',0),math.sqrt(26))
    if(pCMdown>0.502 and limita - 5000>-1*assignedlimit):
        limita -= 5000
        session.send_order('CAT-M', 'SELL', bookCM.bid_price()+0.01,  333)
    elif(pCMup > 0.502 and limita + 5000<assignedlimit):
        limita += 5000
        session.send_order('CAT-M', 'BUY', bookCM.ask_price()-0.01,  333)

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
    if bookWA.bid_price() > bookWM.ask_price() -0.01:
        session.send_order('WMT-A', 'SELL', bookWA.bid_price(),  1000)
        session.send_order('WMT-M', 'BUY', bookWM.ask_price(), 1000)
    if bookWA.ask_price() < bookWM.bid_price() + 0.01:
        session.send_order('WMT-M', 'SELL', bookWM.bid_price(), 1000)
        session.send_order('WMT-A', 'BUY', bookWA.ask_price(),  1000)
    if bookMA.bid_price() > bookMM.ask_price() -0.01:
        session.send_order('MMM-A', 'SELL', bookMA.bid_price(),  1000)
        session.send_order('MMM-M', 'BUY', bookMM.ask_price(), 1000)
    if bookMA.ask_price() < bookMM.bid_price() + 0.01:
        session.send_order('MMM-M', 'SELL', bookMM.bid_price(), 1000)
        session.send_order('MMM-A', 'BUY', bookMA.ask_price(),  1000)
    if bookCA.bid_price() > bookCM.ask_price() -0.01:
        session.send_order('CAT-A', 'SELL', bookCA.bid_price(),  1000)
        session.send_order('CAT-M', 'BUY', bookCM.ask_price(), 1000)
    if bookCA.ask_price() < bookCM.bid_price() + 0.01:
        session.send_order('CAT-M', 'SELL', bookCM.bid_price(), 1000)
        session.send_order('CAT-A', 'BUY', bookCA.ask_price(),  1000)
    # if bookETF.bid_price() > bookWM.ask_price() + bookMM.ask_price() + bookCM.ask_price()-0.0215:
    #     session.send_order('ETF', 'SELL', bookETF.bid_price(),  1000)
    #     session.send_order('WMT-M', 'BUY', bookWM.ask_price(), 1000)
    #     session.send_order('MMM-M', 'BUY', bookMM.ask_price(), 1000)
    #     session.send_order('CAT-M', 'BUY', bookCM.ask_price(), 1000)
    # if bookETF.ask_price() < bookWM.bid_price() + bookMM.bid_price() + bookCM.bid_price()+0.0125:
    #     session.send_order('WMT-M', 'SELL', bookWM.bid_price(), 1000)
    #     session.send_order('MMM-M', 'SELL', bookMM.bid_price(), 1000)
    #     session.send_order('CAT-M', 'SELL', bookCM.bid_price(), 1000)
    #     session.send_order('ETF', 'BUY', bookETF.ask_price(),  1000)
    return 0

def momentum_bot():
    return 0

def forward_looking():
    return 0

def dynamic_weighting():
    return 0

def news_adjusted_price(jsonresp):
    global adjusted_price
    mindex=jsonresp['headline'].index('$')
    if jsonresp['headline'][mindex-1]=='-':
        print(jsonresp['ticker'],'down',float(jsonresp['headline'][mindex+1:]))
        adjusted_price[jsonresp['ticker']]=adjusted_price.get(jsonresp['ticker'],0)-float(jsonresp['headline'][mindex+1:])
    else:
        print(jsonresp['ticker'],'up',float(jsonresp['headline'][mindex+1:]))
        adjusted_price[jsonresp['ticker']]=adjusted_price.get(jsonresp['ticker'],0)+float(jsonresp['headline'][mindex+1:])

def money_manager(amount):
    return 0
# def risk_manager():
    #for i in ['WMT-M','WMT-A',]



def sigint(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True

if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint)
    main()
