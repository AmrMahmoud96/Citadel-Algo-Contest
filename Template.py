import signal
import requests
# import time
# import numpy as np
# import pandas as pd
# import scipy.stats
# import math

# import time

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
position={}

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
    # def vwap(self):
    #     if self.json['bids']:
    #         if self.json['bids'][0]['vwap'] != None:
    #             return self.json['bids'][0]['vwap']
    #         return float('nan')
    #     else:
    #         return float('nan')

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
    def get_news(self,session):
        global lastnews,firstnews
        resp = self.session.get(self.url + '/v1/news', params = {'since':lastnews,'limit':1})
        if not resp.ok:
            raise ApiException('could not get news: ' + str(resp))
        else:
            r = resp.json()
            if len(r)>0 and (r[0]['news_id']!=lastnews or firstnews):
                firstnews=False
                lastnews = r[0]['news_id']
                session.kill_all(r[0]['ticker']+'-M')
                session.kill_all(r[0]['ticker']+'-A')
                news_adjusted_price(r[0],session)


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
    def get_history(self, sym):
        resp = self.session.get(self.url + '/v1/securities/history', params = { 'ticker': sym })
        if not resp.ok:
            raise ApiException('could not get history: ' + str(resp))
        return resp.json()
    def send_order(self, sym, side, price, size):
        resp = self.session.post(self.url + '/v1/orders', params = { 'ticker': sym, 'type': 'LIMIT', 'action': side, 'quantity': size, 'price': price })
        if resp.ok:
            print('sent order', side, sym, size, '@', price)
        else:
            print('failed to send order', side, sym, size, '@', price, ':', resp.text)
    def kill_all(self,ticker):
        resp = self.session.post(self.url + '/v1/commands/cancel', params = {'ticker':ticker,'query':'volume>0'})
        if resp.ok:
            print('killed all orders')
        else:
            print('failed to kill order:', resp.text)

def main():
    with Session('http://localhost:9999', '7PFL9YEE') as session:
        while session.get_tick():
            global bookES,bookWM,bookWA,bookMM,bookMA,bookCM,bookCA,bookETF
            # start = time.time()
            bookES=session.get_book('ES')
            bookWM=session.get_book('WMT-M')
            bookWA=session.get_book('WMT-A')
            bookMM=session.get_book('MMM-M')
            bookMA=session.get_book('MMM-A')
            bookCM=session.get_book('CAT-M')
            bookCA=session.get_book('CAT-A')
            bookETF=session.get_book('ETF')

            session.get_news(session)

            # start = time.time()

            spread_bot(session)

            # mean_reversion_bot(session)
            # end = time.time()
            # print(end - start)
            # arbitrage_bot(session)
            # probability_bot(session,1500000)
            # # naive_bot(session)

def mean_reversion_bot(session):
    global bookES,bookWM,bookWA,bookMM,bookMA,bookCM,bookCA,bookETF,position
    try:
        x = np.array(session.get_history('WMT-M'))
        y = np.array(session.get_history('WMT-A'))
        z = []
        for i in range(0,len(x)-1):
            z.append(x[i]['open'] - y[i]['open'])
        df = pd.Series(z)
        zscore=(df-df.mean())/np.std(df)
        print('zscore: ', zscore[0])
        if zscore[0]>=1:
            session.send_order('WMT-M', 'SELL', bookWM.ask_price()-0.01,  3000)
            session.send_order('WMT-A', 'BUY', bookWA.bid_price()+0.01,  3000)
            position['WMT-M']=position.get('WMT-M',0)-3000
            position['WMT-A']=position.get('WMT-A',0)+3000
        elif zscore[0]<=0.5 and zscore[0]>=-0.5:
            if bookWA.bid_price() >= bookWM.ask_price():
                session.send_order('WMT-M', 'BUY', bookWM.ask_price(),  3000)
                session.send_order('WMT-A', 'SELL', bookWA.bid_price(),  3000)
                position['WMT-M']=position.get('WMT-M',0)+3000
                position['WMT-A']=position.get('WMT-A',0)-3000
            else:
                session.send_order('WMT-M', 'SELL', bookWM.bid_price(),  3000)
                session.send_order('WMT-A', 'BUY', bookWA.ask_price(), 3000)
                position['WMT-M']=position.get('WMT-M',0)-3000
                position['WMT-A']=position.get('WMT-A',0)+3000
        elif zscore[0]<=-1:
            session.send_order('WMT-M', 'BUY', bookWM.bid_price()+0.01,  3000)
            session.send_order('WMT-A', 'SELL', bookWA.ask_price()-0.01,  3000)
            position['WMT-M']=position.get('WMT-M',0)+3000
            position['WMT-A']=position.get('WMT-A',0)-3000

    except:
        pass




    return 0
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
        session.send_order('WMT-M', 'SELL', bookWM.bid_price(),  2000)
    if bookWM.ask_price() < bookWM.vwap()+0.0065:
        session.send_order('WMT-M', 'BUY', bookWM.ask_price(),  2000)
    if bookMM.bid_price() > bookMM.vwap()-0.002:
        session.send_order('MMM-M', 'SELL', bookMM.bid_price(),  2000)
    if bookMM.ask_price() < bookMM.vwap()+0.0065:
        session.send_order('MMM-M', 'BUY', bookMM.ask_price(),  2000)
    if bookCM.bid_price() > bookCM.vwap()-0.002:
        session.send_order('CAT-M', 'SELL', bookCM.bid_price(),  2000)
    if bookCM.ask_price() < bookCM.vwap()+0.0065:
        session.send_order('CAT-M', 'BUY', bookCM.ask_price(),  2000)

    if bookWA.bid_price() > bookWA.vwap()-0.0035:
        session.send_order('WMT-A', 'SELL', bookWA.bid_price(),  2000)
    if bookWA.ask_price() < bookWA.vwap()+0.005:
        session.send_order('WMT-A', 'BUY', bookWA.ask_price(),  2000)
    if bookMA.bid_price() > bookMA.vwap()-0.0035:
        session.send_order('MMM-A', 'SELL', bookMA.bid_price(),  2000)
    if bookMA.ask_price() < bookMA.vwap()+0.005:
        session.send_order('MMM-A', 'BUY', bookMA.ask_price(),  2000)
    if bookCA.bid_price() > bookCA.vwap()-0.0035:
        session.send_order('CAT-A', 'SELL', bookCA.bid_price(),  2000)
    if bookCA.ask_price() < bookCA.vwap()+0.005:
        session.send_order('CAT-A', 'BUY', bookCA.ask_price(),  2000)
    return 0

def arbitrage_bot(session):
    global bookES,bookWM,bookWA,bookMM,bookMA,bookCM,bookCA,bookETF
    if bookWA.bid_price() > bookWM.ask_price():
        session.send_order('WMT-A', 'SELL', bookWA.bid_price(),  2000)
        session.send_order('WMT-M', 'BUY', bookWM.ask_price(), 2000)
    if bookWA.ask_price() < bookWM.bid_price():
        session.send_order('WMT-M', 'SELL', bookWM.bid_price(), 2000)
        session.send_order('WMT-A', 'BUY', bookWA.ask_price(),  2000)
    if bookMA.bid_price() > bookMM.ask_price():
        session.send_order('MMM-A', 'SELL', bookMA.bid_price(),  333)
        session.send_order('MMM-M', 'BUY', bookMM.ask_price(), 333)
    if bookMA.ask_price() < bookMM.bid_price():
        session.send_order('MMM-M', 'SELL', bookMM.bid_price(), 333)
        session.send_order('MMM-A', 'BUY', bookMA.ask_price(),  333)
    if bookCA.bid_price() > bookCM.ask_price():
        session.send_order('CAT-A', 'SELL', bookCA.bid_price(),  500)
        session.send_order('CAT-M', 'BUY', bookCM.ask_price(), 500)
    if bookCA.ask_price() < bookCM.bid_price():
        session.send_order('CAT-M', 'SELL', bookCM.bid_price(), 500)
        session.send_order('CAT-A', 'BUY', bookCA.ask_price(),  500)
    # if bookETF.bid_price() > bookWM.ask_price() + bookMM.ask_price() + bookCM.ask_price()-0.0215:
    #     session.send_order('ETF', 'SELL', bookETF.bid_price(),  2000)
    #     session.send_order('WMT-M', 'BUY', bookWM.ask_price(), 2000)
    #     session.send_order('MMM-M', 'BUY', bookMM.ask_price(), 2000)
    #     session.send_order('CAT-M', 'BUY', bookCM.ask_price(), 2000)
    # if bookETF.ask_price() < bookWM.bid_price() + bookMM.bid_price() + bookCM.bid_price()+0.0125:
    #     session.send_order('WMT-M', 'SELL', bookWM.bid_price(), 2000)
    #     session.send_order('MMM-M', 'SELL', bookMM.bid_price(), 2000)
    #     session.send_order('CAT-M', 'SELL', bookCM.bid_price(), 2000)
    #     session.send_order('ETF', 'BUY', bookETF.ask_price(),  2000)
    return 0

def momentum_bot():
    return 0

def forward_looking():
    return 0

def risk_manager():
    global bookWM,bookWA,bookMM,bookMA,bookCM,bookCA,bookETF
    ##get current positions, see if theres risk of loss
    return 0

def spread_bot(session):
    global bookWM,bookWA,bookMM,bookMA,bookCM,bookCA,bookETF

    ##potentially remove the tight constraints to counter momentum

    #not tight
    if bookWM.bid_price()+0.01 < bookWM.ask_price()-0.01:
        session.send_order('WMT-M', 'SELL', bookWM.ask_price()-0.01,  15000)
        session.send_order('WMT-M', 'BUY', bookWM.bid_price()+0.01, 15000)
    # #tight up
    if bookWM.bid_price()+0.01 < bookWM.ask_price():
        session.send_order('WMT-M', 'SELL', bookWM.ask_price(),  5000)
        session.send_order('WMT-M', 'BUY', bookWM.bid_price()+0.01, 5000)
    #tight down
    if bookWM.bid_price() < bookWM.ask_price()-0.01:
        session.send_order('WMT-M', 'SELL', bookWM.ask_price()-0.01,  5000)
        session.send_order('WMT-M', 'BUY', bookWM.bid_price(), 5000)

    #not tight
    if bookWA.bid_price()+0.01 < bookWA.ask_price()-0.01:
        session.send_order('WMT-A', 'SELL', bookWA.ask_price()-0.01,  15000)
        session.send_order('WMT-A', 'BUY', bookWA.bid_price()+0.01, 15000)
    # #tight up
    if bookWA.bid_price()+0.01 < bookWA.ask_price():
        session.send_order('WMT-A', 'SELL', bookWA.ask_price(),  5000)
        session.send_order('WMT-A', 'BUY', bookWA.bid_price()+0.01, 5000)
    #tight down
    if bookWA.bid_price() < bookWA.ask_price()-0.01:
        session.send_order('WMT-A', 'SELL', bookWA.ask_price()-0.01,  5000)
        session.send_order('WMT-A', 'BUY', bookWA.bid_price(), 5000)

    #not tight
    if bookCM.bid_price()+0.01 < bookCM.ask_price()-0.01:
        session.send_order('CAT-M', 'SELL', bookCM.ask_price()-0.01,  4000)
        session.send_order('CAT-M', 'BUY', bookCM.bid_price()+0.01, 4000)
    #tight up
    if bookCM.bid_price()+0.01 < bookCM.ask_price():
        session.send_order('CAT-M', 'SELL', bookCM.ask_price(),  2000)
        session.send_order('CAT-M', 'BUY', bookCM.bid_price()+0.01, 2000)
    #tight down
    if bookCM.bid_price() < bookCM.ask_price()-0.01:
        session.send_order('CAT-M', 'SELL', bookCM.ask_price()-0.01,  2000)
        session.send_order('CAT-M', 'BUY', bookCM.bid_price(), 2000)

    #not tight
    if bookCA.bid_price()+0.01 < bookCA.ask_price()-0.01:
        session.send_order('CAT-A', 'SELL', bookCA.ask_price()-0.01,  4000)
        session.send_order('CAT-A', 'BUY', bookCA.bid_price()+0.01, 4000)
    #tight up
    if bookCA.bid_price()+0.01 < bookCA.ask_price():
        session.send_order('CAT-A', 'SELL', bookCA.ask_price(),  2000)
        session.send_order('CAT-A', 'BUY', bookCA.bid_price()+0.01, 2000)
    #tight down
    if bookCA.bid_price() < bookCA.ask_price()-0.01:
        session.send_order('CAT-A', 'SELL', bookCA.ask_price()-0.01,  2000)
        session.send_order('CAT-A', 'BUY', bookCA.bid_price(), 2000)

    #not tight
    if bookMM.bid_price()+0.01 < bookMM.ask_price()-0.01:
        session.send_order('MMM-M', 'SELL', bookMM.ask_price()-0.01,  5000)
        session.send_order('MMM-M', 'BUY', bookMM.bid_price()+0.01, 5000)
    # #tight up
    if bookMM.bid_price()+0.01 < bookMM.ask_price():
        session.send_order('MMM-M', 'SELL', bookMM.ask_price(),  2000)
        session.send_order('MMM-M', 'BUY', bookMM.bid_price()+0.01, 2000)
    #tight down
    if bookMM.bid_price() < bookMM.ask_price()-0.01:
        session.send_order('MMM-M', 'SELL', bookMM.ask_price()-0.01,  2000)
        session.send_order('MMM-M', 'BUY', bookMM.bid_price(), 2000)

    #not tight
    if bookMA.bid_price()+0.01 < bookMA.ask_price()-0.01:
        session.send_order('MMM-A', 'SELL', bookMA.ask_price()-0.01,  5000)
        session.send_order('MMM-A', 'BUY', bookMA.bid_price()+0.01, 5000)
    #tight up
    if bookMA.bid_price()+0.01 < bookMA.ask_price():
        session.send_order('MMM-A', 'SELL', bookMA.ask_price(),  2000)
        session.send_order('MMM-A', 'BUY', bookMA.bid_price()+0.01, 2000)
    #tight down
    if bookMA.bid_price() < bookMA.ask_price()-0.01:
        session.send_order('MMM-A', 'SELL', bookMA.ask_price()-0.01,  2000)
        session.send_order('MMM-A', 'BUY', bookMA.bid_price(), 2000)

    #not tight
    if bookETF.bid_price()+0.01 < bookETF.ask_price()-0.01:
        session.send_order('ETF', 'SELL', bookETF.ask_price()-0.01,  2000)
        session.send_order('ETF', 'BUY', bookETF.bid_price()+0.01, 2000)
    #tight up
    if bookETF.bid_price()+0.01 < bookETF.ask_price():
        session.send_order('ETF', 'SELL', bookETF.ask_price(),  500)
        session.send_order('ETF', 'BUY', bookETF.bid_price()+0.01, 500)
    #tight down
    if bookETF.bid_price() < bookETF.ask_price()-0.01:
        session.send_order('ETF', 'SELL', bookETF.ask_price()-0.01,  500)
        session.send_order('ETF', 'BUY', bookETF.bid_price(), 500)



def dynamic_weighting():
    return 0

def news_adjusted_price(jsonresp,session):
    global adjusted_price,bookWM,bookWA,bookMM,bookMA,bookCM,bookCA,bookETF,position

##potentially put trade here to account for news
    mindex=jsonresp['headline'].index('$')
    #session.kill_all()
    val=0
    if jsonresp['headline'][mindex-1]=='-':
        val=-1*float(jsonresp['headline'][mindex+1:])
        # adjusted_price[jsonresp['ticker']]=adjusted_price.get(jsonresp['ticker'],0)-val
    else:
        val=float(jsonresp['headline'][mindex+1:])
        # adjusted_price[jsonresp['ticker']]=adjusted_price.get(jsonresp['ticker'],0)+val
    bookA={}
    bookM={}
    if(jsonresp['ticker'] == 'WMT'):
        bookM = bookWM
        bookA = bookWA
    if(jsonresp['ticker'] == 'CAT'):
        bookM = bookCM
        bookA = bookCA
    if(jsonresp['ticker'] == 'MMM'):
        bookM = bookMM
        bookA = bookMA
    if position.get(jsonresp['ticker']+'-M',0)*val<0:
        if position.get(jsonresp['ticker']+'-M') > 0:
            session.send_order(jsonresp['ticker']+'-M', 'SELL', bookM.bid_price()+0.01 + val, position.get(jsonresp['ticker']+'-M'))
        else:
            session.send_order(jsonresp['ticker']+'-M', 'BUY', bookM.ask_price()+0.01 + val, position.get(jsonresp['ticker']+'-M'))
    if position.get(jsonresp['ticker']+'-A',0)*val<0:
        if position.get(jsonresp['ticker']+'-A') > 0:
            session.send_order(jsonresp['ticker']+'-A', 'SELL', bookA.bid_price()+0.01 + val, position.get(jsonresp['ticker']+'-A'))
        else:
            session.send_order(jsonresp['ticker']+'-A', 'BUY', bookA.ask_price()+0.01 + val, position.get(jsonresp['ticker']+'-A'))
    position[jsonresp['ticker']+'-M']=0
    position[jsonresp['ticker']+'-A']=0
    if val!=0:
        session.send_order(jsonresp['ticker']+'-M', 'BUY', bookM.bid_price()+0.01 + val, (abs(val))/(0.01*bookM.bid_price())*200000)
        session.send_order(jsonresp['ticker']+'-M', 'SELL', bookM.ask_price()-0.01 + val,  (abs(val))/(0.01*bookM.bid_price())*200000)
        session.send_order(jsonresp['ticker']+'-A', 'BUY', bookA.ask_price()+0.01 + val,  (abs(val))/(0.01*bookM.bid_price())*200000)
        session.send_order(jsonresp['ticker']+'-A', 'SELL', bookA.ask_price()-0.01 + val,  (abs(val))/(0.01*bookM.bid_price())*200000)
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
