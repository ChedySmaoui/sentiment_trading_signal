"""
Code for my article: Using Sentiment Analysis as a Trading Signal? Beat the market with Transformers!

@author Chedy Smaoui
@link https://www.linkedin.com/in/chedy-smaoui/
"""

import backtrader as bt
import pandas as pd
from datetime import datetime
import quantstats

#data_raw = DataGenerator.get_data('GOOG')
#data_raw.to_csv('GOOG.csv')

data = pd.read_csv('GOOG.csv')

# Dataframe editing
data['Date'] = pd.to_datetime(data['Date'])
data = data.rename(columns={'finbert_sentiment_score': 'sentiment'})
data.set_index('Date', inplace=True)

# Edit the PandasData feeder to accept the last column as holding the sentiment scores
class PandasSent(bt.feeds.PandasData):
    lines = (('sentiment'),)
    params = (('sentiment',-1),)

# Pass the data into the Backtrader data feeder
data = PandasSent(dataname=data)

class SentimentStrat(bt.Strategy):
    params = (
        ('exitbars', 3),
    )
    
    def log(self, txt, dt=None):
        """
        Logging function for this strategy.

        Args:
            txt (_type_): text variable to output to the screen.
            dt (_type_, optional): Attempt to grab datetime values from the most recent data point if available and log it to the screen. Defaults to None.
        """
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} {txt}') # Print date and close

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close
        self.datasentiment = self.datas[0].sentiment
            
        # To keep track of pending orders and buy price/commision
        self.order = None
        self.buyprice = None
        self.buycomm = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return
            
        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                        order.executed.value,
                        order.executed.comm)
                )
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else: # Sell
                self.log(
                    'SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                        order.executed.value,
                        order.executed.comm)
                )
                
            self.bar_executed = len(self)
            
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')
            
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
            
        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                    (trade.pnl, trade.pnlcomm))

    def next(self):
        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.dataclose[0])
            
        # Check if an order is pending ... if yes we cannot send a 2nd one
        if self.order:
            return
            
        # Check if we are in the market
        if not self.position:
                
            # If the sentiment score is over 0.6, we buy
            if self.datasentiment[0] > 0.6:
                    
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                    
                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy(size=1000)
            
        else:
            # Already in the market, we sell three days (bars) after buying:
            if len(self) >= (self.bar_executed + self.params.exitbars):
                    
                self.log('SELL CREATE, %.2f' % self.dataclose[0])
                    
                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell(size=1000)


# Instantiate the Cerebro engine
cerebro = bt.Cerebro()

# Add the strategy to Cerebro
cerebro.addstrategy(SentimentStrat)

# Add the data feed to cerebro
cerebro.adddata(data)

# Add an analyzer to get the return data
cerebro.addanalyzer(bt.analyzers.PyFolio, _name='PyFolio')

# set initial porfolio value at 100,000$
cerebro.broker.setcash(100000.0)
start_value = cerebro.broker.getvalue() # should be 100,000$

# Run the Backtest
results = cerebro.run()

# Print out the final result
print('Starting Portfolio Value: ', start_value)
print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

# Plot the results using cerebro's plotting facilities
cerebro.plot()

strat = results[0]

# Uses PyFolio to obtain the returns from the Backtest
portfolio_stats = strat.analyzers.getbyname('PyFolio')
returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()
returns.index = returns.index.tz_convert(None)

import webbrowser
# Feeds the returns to Quantstats to obtain a more complete report over the Backtest
quantstats.reports.html(returns, output='stats.html', title=f'{ticker} Sentiment')
webbrowser.open('stats.html')