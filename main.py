import json
import sqlite3
import statistics
import os

# The benefit of using a sql database is that it can be expanded pretty easily if there is new data
# TODO: I could set this script to run every month or so to extend the Prices table

# This script uses the oracle cards json file supplied by Scryfall.com and the price history supplied by mtgjson.com
# to create a sqlite database which contains card data and price data

# Creates a sqlite table named Cards for oracle cards using a preestablished connection to some db
def init_oraclecard_db(connection):
    cur = connection.cursor()
    try:
        cur.execute(
            "CREATE TABLE Cards (id TEXT, name TEXT, colors TEXT, mana_cost TEXT, set_name TEXT, edhrec_rank TEXT, "
            "type_line TEXT, scryfall_uri TEXT)")
    except:
        pass

    # Make sure Cards is empty
    cur.execute("DELETE FROM Cards")

    # This json is from scryfall and it doesn't contain the mtgjson uuid, so we need to use another table to grab it
    # It's an issue because mtgjson doesnt have a database for oracle only cards, only scryfall does.
    cards = open("C://Datasets//oracle-cards.json", encoding='utf8')
    cards_json = json.load(cards)

    # for each oracle card, fill the CARDS table with the following information
    fields = ['id', 'name', 'colors', 'mana_cost', 'set_name', 'edhrec_rank', 'type_line', 'scryfall_uri']

    # cleaning text
    for row in cards_json:
        datastring = ''
        for field in fields:

            # Just in case a field is null
            try:
                if isinstance(row[field], list):
                    datastring += '"' + ''.join(row[field]) + '", '
                else:
                    datastring += '"' + str(row[field]).replace('"', '') + '", '
            except:
                datastring += "NULL, "

        # cleaning and inserting
        datastring = datastring[:-2]
        commandstring = "INSERT INTO Cards (id, name, colors, mana_cost, set_name, edhrec_rank, type_line, " \
                        "scryfall_uri) VALUES (" + datastring + "); "
        cur.execute(commandstring)
    cards.close()
    connection.commit()

    # Joins the scryfall id with the uuid which we need for the price json
    # The Master Table contains both the sfid and uuid
    commandstring = ''' CREATE TABLE Master AS 
    SELECT Cards.*, allprintings.uuid
    FROM Cards
	JOIN allprintings
    ON Cards.id = allprintings.scryfallId'''

    cur.execute(commandstring)
    connection.commit()

# Creates a sqlite table named AllPrintings for oracle cards using a preestablished connection to some db
def add_all_printings(connection):

    all_printings_path = "'c:\Datasets\AllPrintings.sqlite'"
    connection.execute("ATTACH DATABASE "+all_printings_path+" AS TempAP")
    connection.execute("CREATE TABLE allprintings AS SELECT * FROM TempAP.c")

    connection.commit()

# For each row in the Cards table, find its price history and insert into the Price table
def init_price_db(connection):
    prices = open("C://Datasets//AllPrices.json", encoding='utf8')
    json_prices = json.load(prices)['data']

    # 2 cursors since we r/w to the same db
    readcur = connection.cursor()
    writecur = connection.cursor()

    # This database needs a lot of columns
    try:
        writecur.execute("CREATE TABLE Prices (name TEXT, uuid TEXT, scryfall_id TEXT)")
        writecur.execute("ALTER TABLE Prices ADD Median_Price FLOAT;")
        writecur.execute("ALTER TABLE Prices ADD Mean_Price FLOAT;")
        writecur.execute("ALTER TABLE Prices ADD Percent_Change FLOAT;")

        # Returns a dictionary where dates map to prices
        vorstclaw = json_prices['4dc8ad93-2ba1-5417-b4c6-77f93293c1b3']['paper']['tcgplayer']['retail']['normal']
        dates = list(vorstclaw.keys())

        for d in dates:
            writecur.execute("ALTER TABLE Prices ADD '" + d + "' FLOAT;")
    except:
        pass

    readcur.execute('SELECT * FROM Master')
    for card_row in readcur:
        scryfall_id = card_row[0]
        target_id = card_row[8]
        name = card_row[1]
        # Some cards in the database don't have normal printing prices, which is all I care about
        try:
            pricehistory = json_prices[target_id]

            # paperprice is a dictionary containing the last 93 days worth of prices
            # all_dates is a list containing the past 93 days of card price data
            paperprice = pricehistory['paper']['tcgplayer']['retail']['normal']
            all_dates = list(paperprice.values())

            # some simple statistics
            median_price = statistics.median(all_dates)
            mean_price = statistics.mean(all_dates)
            percent_change = 100 * (all_dates[-1] - all_dates[0]) / all_dates[-1]

            # Pad array with Null strings if not enough price data
            if len(all_dates) != 93:
                all_dates = ['NULL'] * (93 - len(all_dates)) + all_dates

            insertrow = [name, target_id, scryfall_id, median_price, mean_price, percent_change] + all_dates
            string = str(insertrow)[1:-1]

            writecur.execute("INSERT INTO Prices VALUES (" + string + ");")

        except:
            pass
    connection.commit()

def build_new_DB():
    if os.path.exists('mtgfinance.sqlite'):
        os.remove('mtgfinance.sqlite')
    con = sqlite3.connect('mtgfinance.sqlite')
    print("Grabbing Card Data...")
    add_all_printings(con)
    print("Done")
    print("Joining Tables...")
    init_oraclecard_db(con)
    print("Done")
    print("Grabbing Prices...")
    init_price_db(con)
    print("Done")

    con.close()

build_new_DB()