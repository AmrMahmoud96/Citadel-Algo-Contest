import signal
import requests
import scipy.stats
import math
import threading

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
limita=0
position={}
threads=[]

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

            process = threading.Thread(target=arbitrage_bot, args=[session])
            process.start()
            threads.append(process)
            process = threading.Thread(target=probability_bot, args=[session,3000000])
            process.start()
            threads.append(process)
            for process in threads:
                process.join()


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
    try:
        x = np.array(session.get_history('MMM-M'))
        y = np.array(session.get_history('MMM-A'))
        z = []
        for i in range(0,len(x)-1):
            z.append(x[i]['open'] - y[i]['open'])
        df = pd.Series(z)
        zscore=(df-df.mean())/np.std(df)
        print('zscore: ', zscore[0])
        if zscore[0]>=1:
            session.send_order('MMM-M', 'SELL', bookMM.ask_price()-0.01,  3000)
            session.send_order('MMM-A', 'BUY', bookMA.bid_price()+0.01,  3000)
            position['MMM-M']=position.get('MMM-M',0)-3000
            position['MMM-A']=position.get('MMM-A',0)+3000
        elif zscore[0]<=0.5 and zscore[0]>=-0.5:
            if bookMA.bid_price() >= bookMM.ask_price():
                session.send_order('MMM-M', 'BUY', bookMM.ask_price(),  3000)
                session.send_order('MMM-A', 'SELL', bookMA.bid_price(),  3000)
                position['MMM-M']=position.get('MMM-M',0)+3000
                position['MMM-A']=position.get('MMM-A',0)-3000
            else:
                session.send_order('MMM-M', 'SELL', bookMM.bid_price(),  3000)
                session.send_order('MMM-A', 'BUY', bookMA.ask_price(), 3000)
                position['MMM-M']=position.get('MMM-M',0)-3000
                position['MMM-A']=position.get('MMM-A',0)+3000
        elif zscore[0]<=-1:
            session.send_order('MMM-M', 'BUY', bookMM.bid_price()+0.01,  3000)
            session.send_order('MMM-A', 'SELL', bookMA.ask_price()-0.01,  3000)
            position['MMM-M']=position.get('MMM-M',0)+3000
            position['MMM-A']=position.get('MMM-A',0)-3000

    except:
        pass
    try:
        x = np.array(session.get_history('CAT-M'))
        y = np.array(session.get_history('CAT-A'))
        z = []
        for i in range(0,len(x)-1):
            z.append(x[i]['open'] - y[i]['open'])
        df = pd.Series(z)
        zscore=(df-df.mean())/np.std(df)
        print('zscore: ', zscore[0])
        if zscore[0]>=1:
            session.send_order('CAT-M', 'SELL', bookCM.ask_price()-0.01,  3000)
            session.send_order('CAT-A', 'BUY', bookCA.bid_price()+0.01,  3000)
            position['CAT-M']=position.get('CAT-M',0)-3000
            position['CAT-A']=position.get('CAT-A',0)+3000
        elif zscore[0]<=0.5 and zscore[0]>=-0.5:
            if bookCA.bid_price() >= bookCM.ask_price():
                session.send_order('CAT-M', 'BUY', bookCM.ask_price(),  3000)
                session.send_order('CAT-A', 'SELL', bookCA.bid_price(),  3000)
                position['CAT-M']=position.get('CAT-M',0)+3000
                position['CAT-A']=position.get('CAT-A',0)-3000
            else:
                session.send_order('CAT-M', 'SELL', bookCM.bid_price(),  3000)
                session.send_order('CAT-A', 'BUY', bookCA.ask_price(), 3000)
                position['CAT-M']=position.get('CAT-M',0)-3000
                position['CAT-A']=position.get('CAT-A',0)+3000
        elif zscore[0]<=-1:
            session.send_order('CAT-M', 'BUY', bookCM.bid_price()+0.01,  3000)
            session.send_order('CAT-A', 'SELL', bookCA.ask_price()-0.01,  3000)
            position['CAT-M']=position.get('CAT-M',0)+3000
            position['CAT-A']=position.get('CAT-A',0)-3000

    except:
        pass
    return 0

def probability_bot(session,assignedlimit):
    global bookES,bookWM,bookWA,bookMM,bookMA,bookCM,bookCA,limita
    pWdown = scipy.stats.norm.cdf(bookWA.bid_price(),7,math.sqrt(12))
    pWup = scipy.stats.norm.sf(bookWA.ask_price(),7,math.sqrt(12))
    if pWdown>0.502 and limita - 5000>-1*assignedlimit:
        limita -= 5000
        session.send_order('WMT-A', 'SELL', bookWA.bid_price()+0.01,  700)
    elif(pWup > 0.502 and limita + 5000<assignedlimit):
        limita += 5000
        session.send_order('WMT-A', 'BUY', bookWA.ask_price()-0.01,  700)
    pWMdown = scipy.stats.norm.cdf(bookWM.bid_price(),7,math.sqrt(12))
    pWMup = scipy.stats.norm.sf(bookWM.ask_price(),7,math.sqrt(12))
    if(pWMdown>0.502 and limita - 5000>-1*assignedlimit):
        limita -= 5000
        session.send_order('WMT-M', 'SELL', bookWM.bid_price()+0.01,  700)
    elif(pWMup > 0.502 and limita + 5000<assignedlimit ):
        limita += 5000
        session.send_order('WMT-M', 'BUY', bookWM.ask_price()-0.01,  700)

    pMdown = scipy.stats.norm.cdf(bookMA.bid_price(),20,math.sqrt(20))
    pMup = scipy.stats.norm.sf(bookMA.ask_price(),20,math.sqrt(20))
    if(pMdown>0.502 and limita - 5000>-1*assignedlimit):
        limita -= 5000
        session.send_order('MMM-A', 'SELL', bookMA.bid_price()+0.01,  250)
    elif(pMup > 0.502 and limita + 5000<assignedlimit):
        limita += 5000
        session.send_order('MMM-A', 'BUY', bookMA.ask_price()-0.01,  250)
    pMMdown = scipy.stats.norm.cdf(bookMM.bid_price(),20,math.sqrt(20))
    pMMup = scipy.stats.norm.sf(bookMM.ask_price(),20,math.sqrt(20))
    if(pMMdown>0.502 and limita - 5000>-1*assignedlimit):
        limita -= 5000
        session.send_order('MMM-M', 'SELL', bookMM.bid_price()+0.01,  250)
    elif(pMMup > 0.502 and limita + 5000<assignedlimit):
        limita += 5000
        session.send_order('MMM-M', 'BUY', bookMM.ask_price()-0.01,  250)

    pCdown = scipy.stats.norm.cdf(bookCA.bid_price(),15,math.sqrt(26))
    pCup = scipy.stats.norm.sf(bookCA.ask_price(),15,math.sqrt(26))
    if(pCdown>0.502 and limita - 5000>-1*assignedlimit):
        limita -= 5000
        session.send_order('CAT-A', 'SELL', bookCA.bid_price()+0.01,  333)
    elif(pCup > 0.502 and limita + 5000<assignedlimit):
        limita += 5000
        session.send_order('CAT-A', 'BUY', bookCA.ask_price()-0.01,  333)
    pCMdown = scipy.stats.norm.cdf(bookCM.bid_price(),15,math.sqrt(26))
    pCMup = scipy.stats.norm.sf(bookCM.ask_price(),15,math.sqrt(26))
    if(pCMdown>0.502 and limita - 5000>-1*assignedlimit):
        limita -= 5000
        session.send_order('CAT-M', 'SELL', bookCM.bid_price()+0.01,  333)
    elif(pCMup > 0.502 and limita + 5000<assignedlimit):
        limita += 5000
        session.send_order('CAT-M', 'BUY', bookCM.ask_price()-0.01,  333)

def arbitrage_bot(session):
    global bookES,bookWM,bookWA,bookMM,bookMA,bookCM,bookCA,bookETF
    if bookWA.bid_price() > bookWM.ask_price():
        session.send_order('WMT-A', 'SELL', bookWA.bid_price(),  3000)
        session.send_order('WMT-M', 'BUY', bookWM.ask_price(), 3000)
    if bookWA.ask_price() < bookWM.bid_price():
        session.send_order('WMT-M', 'SELL', bookWM.bid_price(), 3000)
        session.send_order('WMT-A', 'BUY', bookWA.ask_price(),  3000)
    if bookMA.bid_price() > bookMM.ask_price():
        session.send_order('MMM-A', 'SELL', bookMA.bid_price(),  1000)
        session.send_order('MMM-M', 'BUY', bookMM.ask_price(), 1000)
    if bookMA.ask_price() < bookMM.bid_price():
        session.send_order('MMM-M', 'SELL', bookMM.bid_price(), 1000)
        session.send_order('MMM-A', 'BUY', bookMA.ask_price(),  500)
    if bookCA.bid_price() > bookCM.ask_price():
        session.send_order('CAT-A', 'SELL', bookCA.bid_price(),  1000)
        session.send_order('CAT-M', 'BUY', bookCM.ask_price(), 1000)
    if bookCA.ask_price() < bookCM.bid_price():
        session.send_order('CAT-M', 'SELL', bookCM.bid_price(), 1000)
        session.send_order('CAT-A', 'BUY', bookCA.ask_price(), 1000)
    return 0


def sigint(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True

if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint)
    main()
