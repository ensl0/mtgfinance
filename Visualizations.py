import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import sqlite3

# Given a uuid, graph that card's price
def plot_price_over_time(uuid):
    db = sqlite3.connect("mtgfinance.sqlite")
    cur = db.cursor()
    cur.execute("SELECT * FROM Prices WHERE Prices.uuid="+uuid)

    # price_data is a tuple containing as much price data as is available

    dates = [i[0] for i in cur.description[6:]]
    datarow = cur.fetchone()
    price_data = datarow[6:]
    mpldates = mdates.datestr2num(dates)
    plt.plot_date(mpldates, price_data, '-')
    plt.title(datarow[0]+" Price History ("+dates[0]+" to "+dates[-1]+")")
    plt.xlabel('Date')
    plt.ylabel('Price (USD)')
    plt.show()

plot_price_over_time("'fdccaf49-4190-5740-bf6c-2fc9139489f8'")
plot_price_over_time("'b0a94e61-889f-5265-8a18-ca1271694239'")