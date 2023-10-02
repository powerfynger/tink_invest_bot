"""
sudo systemctl status supervisor
sudo systemctl restart supervisor
sudo nano /etc/supervisor/conf.d/tinkbot.conf
TODO:
    Добавить возможность пользователям оформлять подписку
    Добавить возможность изменения минимальной суммы для уведомления, связанную с БД
"""

# Необходимо для работы с АПИ tinkoff
from tinkoff.invest.services import InstrumentsService, MarketDataService
from tinkoff.invest.utils import quotation_to_decimal
from tinkoff.invest.constants import INVEST_GRPC_API
from tinkoff.invest.utils import now
from tinkoff.invest import Client, OrderDirection, OrderType

# Необходимо для хранения данных
import pandas as pd

# Необходимо для расчета периода отслеживания сделок
from datetime import timedelta, datetime

# Возможно переделать для многопоточного обращения к АПИ тинькова
import threading

# Необходимо для уведомления в ТГ
import telebot

# Необходимо для импортирования токенов тинькова и ТГ-бота
import os
from dotenv import load_dotenv, find_dotenv 

# Если нет сделок за указанное время -- спим
from time import sleep

# Проверка индиктаторов на tradingview
from tradingview_ta import TA_Handler, Interval, Exchange
import uuid

# База данных для хранения подписанных пользователей
import sqlite3

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# Для подгрузки переменных среды  
load_dotenv(find_dotenv())

IS_SANDBOX = True

TOKEN_TINKOFF = os.getenv('TOKEN_TINKOFF')
TOKEN_TELEBOT = os.getenv('TOKEN_TELEBOT')

MY_ID_TELEBOT = 1149967740
# Сумма, начиная с которой сделки будут отправлены в ТГ и помечены как крупные
MIN_TOTAL_MONEY = 5000000
# Сумма, начиная с которой по крупной сделке будет отправлено уведомление
MIN_TOTAL_MONEY_NOTIF = 10000000

EXEL_TMP_FILENAME = 'Тестовый_Вывод.xlsx'
EXEL_TICEKRS_FILENAME = 'tickers.xlsx'

# Словарь, который хранит пары ключ-значение: figi -> ticekr
# Необходим для отправки уведомления в ТГ
figi_to_ticker = {}
# Массив, уже обработанных крупных сделок, чтобы избежать дублирования сообщений в ТГ
big_trades = []
# Открытые позиции [trade]
open_positions = []
# Приоритетные тикеры активов для отслеживания (если не пусто, то уведомления будут приходить только по активам из списка)
priority_tickers = []

users = []

bot = telebot.TeleBot(TOKEN_TELEBOT)

commands ={
    'track' : 'Отслеживать актив по тикеру.',
    'help' : 'Показывает список доступных команд.',
    'ping' : 'Пинг.',
    'clear' : 'Очистить отслеживаемые активы.',
    'list' : 'Список отслеживаемых активов.'
}

#ТЕСТОВЫЙ СЕГМЕНТ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# r = client.orders.post_order(
#     order_id=str(datetime.utcnow().timestamp()),
#     figi=FIGI,
#     quantity=1,
#     account_id=creds.account_id_test,
#     direction=OrderDirection.ORDER_DIRECTION_BUY,
#     order_type=OrderType.ORDER_TYPE_MARKET
# )

def _post_order(client, figi, quantity, price, direction, account_id, order_type):
    """Posts order to Tinkoff API
    Args:
        figi (str): FIGI of share
        quantity (int): quantity of share
        price (schemas.Quotation): price of share
        direction (schemas.OrderDirection): order direction (SELL or BUY)
        account_id (str): account ID for order
        order_type (schemas.OrderType): type of order (MARKET or LIMIT)

    Returns:
        schemas.PostOrderResponse: response from Tinkoff API or None

    Если OrderType.ORDER_TYPE_MARKET без указания цены, то купит по текущей рыночной
    """
    # order_id=str(datetime.utcnow().timestamp())
    order_id = uuid.uuid4().hex

    if IS_SANDBOX:
        response = client.sandbox.post_sandbox_order(
            figi=figi,
            quantity=quantity, 
            direction=direction, 
            account_id=account_id, 
            order_type=order_type, 
            order_id=order_id
            )
    else:
        response = client.orders.post_order(
            figi=figi, 
            quantity=quantity, 
            direction=direction, 
            account_id=account_id, 
            order_type=order_type, 
            order_id=order_id
            )
    # _db.put_order_in_db(figi=figi, quantity=quantity, price=_quotation_to_float(response.initial_order_price_pt), direction=int(direction), account_id=account_id, order_type=int(order_type), order_id=order_id, news_id=news_id)
    return response

def check_sell(trade):
    """
    type: (trade) --> None
    """
    pass



def check_buy(trade):
    """
    type: (trade) --> None
    """
    # if 
    pass

def check_TA(ticker, interval=Interval.INTERVAL_4_HOURS):
    analysis = TA_Handler(
    symbol=ticker,
    screener="russia",
    exchange="MOEX",
    interval=interval,
    )
    return analysis.get_analysis().summary['RECOMMENDATION']
    # if analysis.get_analysis().summary['RECOMMENDATION'] == "BUY":
    #     return 1
    # if analysis.get_analysis().summary['RECOMMENDATION'] == "STRONG_BUY":
    #     return 2
    # if analysis.get_analysis().summary['RECOMMENDATION'] == "SELL":
    #     return -1
    # if analysis.get_analysis().summary['RECOMMENDATION'] == "STRONG_SELL":
    #     return -2
    # if analysis.get_analysis().summary['RECOMMENDATION'] == "NEUTRAL":
    #     return 0
    # if analysis.get_analysis().summary['RECOMMENDATION'] == "ERROR":
    #     return None

# СЕГМЕНТ БД~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def connect_db():
    # Create a new SQLite database
    db = sqlite3.connect("users.db")
    cur = db.cursor()

    # Create a table for the key-value pairs
    # cur.execute("CREATE TABLE mytable (key INTEGER PRIMARY KEY)")
    db.execute(""" CREATE TABLE IF NOT EXISTS users (
                          user_id INTEGER PRIMARY KEY,
            );""")

    # Commit changes and close the database
    db.commit()
    db.close()

def insert_data(user_id):
    # Open the database
    db = sqlite3.connect("users.db")
    cur = db.cursor()

    # Insert the data into the table
    cur.execute("INSERT INTO mytable (user_id) VALUES (?)", (user_id))

    # Commit changes and close the database
    db.commit()
    db.close()

def retrieve_data(user_id):
    db = sqlite3.connect("users.db")
    cur = db.cursor()

    cur.execute("SELECT value FROM mytable WHERE user_id = ?", (user_id,))
    rows = cur.fetchall()

    # Return the data as a list of tuples
    return [(row[0], row[1]) for row in rows]

def delete_data(user_id):
    # Open the database
    db = sqlite3.connect("users.db")
    cur = db.cursor()

    # Delete the data from the table
    cur.execute("DELETE FROM mytable WHERE user_id = ?", (user_id,))

    # Commit changes and close the database
    db.commit()
    db.close()

def select_all():
    db = sqlite3.connect("users.db")
    cur = db.cursor()

    cur.execute("SELECT * from users")
    return list(cur.fetchall())

#ТИНЬКОФ СЕГМЕНТ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_figi(client, ticker):
    """
    type: (Client, Str) --> Str
    Возвращает значение figi по тикеру бумаги.
    Это знаечние может изменяться с течением времени.
    """
    instruments: InstrumentsService = client.instruments
    l = []
    for method in ['shares', 'bonds', 'etfs']: # , 'currencies', 'futures']:
        for item in getattr(instruments, method)().instruments:
            l.append({
                'ticker': item.ticker,
                'figi': item.figi,
                'type': method,
                'name': item.name,
            })
 
    df = pd.DataFrame(l)

    df = df[df['ticker'] == ticker]
    if df.empty:
        print(f"Нет тикера {ticker}")
        return
    return df['figi'].iloc[0]
 


def quotation_to_float(price):
    """
    type: (Quotation) --> Float
    Превращает quotation, тип данных который хранит цену, как целую и вещественную часть(units=xxx, nano=yyy)
    во float.
    """
    return float(quotation_to_decimal(price))

def process_trade(trade):
    """
    type: (Trade) --> DataFrame
    Возращает dataframe, созданный по объекту trade.
    Для direction 1 -- Покупка; 2 -- Продажа.
    """
    if trade is None:
        return
    price = quotation_to_float(trade.price)
    data = pd.DataFrame.from_records([
        {
            "figi": trade.figi,
            "direction": trade.direction,
            "price": price,
            "quantity": trade.quantity,
            # "time": pd.to_datetime(str(trade.time), utc=True)
            "total_money": float(price) * float(trade.quantity)
        }
    ])
    return data

def create_empty_df() -> pd.DataFrame:
    """
    type: (None) --> DataFrame
    Создает пустой DataFrame с нужными полями.
    """
    df = pd.DataFrame(columns=["figi", "direction", "price", "quantity", "total_money"])
    # df.time = pd.to_datetime(df.time, utc=True, unit="ms")
    df.price = pd.to_numeric(df.price)
    df.quantity = pd.to_numeric(df.quantity)
    df.total_money = pd.to_numeric(df.total_money) 
    return df

def get_history_trades(client, ticker=None, figi=None, time_minutes=60):
    """
    type: (Client, Str, Int) --> List[Trade]
    Возращает обезличенные сделки по активу[figi or ticker] за последние time_minutes
    """
    if figi is None:
        if ticker is None:
            return 
        figi = get_figi(client, ticker)    
    interval_from = now() - timedelta(minutes=time_minutes)
    interval_to = now()
    r =  client.market_data.get_last_trades(
            figi=figi,
            from_=interval_from,
            to=interval_to,
        )
    return r.trades

def get_position(trade):
    """
    (trade) --> trade
    Поиск информации, по которой была открыта данная позиция.
    """
    for position in open_positions:
        if position['figi'][0] == trade['figi'][0]:
            if position['quantity'][0] == trade['quantity'][0]:
                return position
    return None


def check_unusual(trade):
    """
    (trade) --> None
    Проверка, если были был перемещен большой капитал.
    Отправка уведомления в ТГ, если да.
    """
    if trade['total_money'][0] > MIN_TOTAL_MONEY:
        is_in_list = False
        for big_trade in big_trades:
            if trade.equals(big_trade):
                is_in_list = True
                break
        if is_in_list:
            return
        big_trades.append(trade)
        if len(big_trades) >= 250:
            big_trades.pop()
        notification = str(get_ticker(trade['figi'][0])) + '\n' + str(trade['total_money'][0]) + ' RUB\n'
        if trade['direction'][0] == 1:
            notification += f"Покупка по {(trade['price'][0])} RUB\n"
            open_positions.append(trade)
        else:
            position = get_position(trade)
            print(position)
            if position is not None:
                notification += f"Закрытие покупки по {position['price'][0]}, выручка \
{trade['total_money'][0] - position['total_money'][0]}/\
{round((trade['price'][0] - position['price'][0]) / 100.0, 3)} %"
                try:
                    open_positions.remove(position)
                except:
                    pass
            else:
                notification += f"Продажа по {(trade['price'][0])} RUB\n"
        notification += check_TA(get_ticker(trade['figi'][0]), Interval.INTERVAL_1_HOUR)
        if trade['total_money'][0] >= MIN_TOTAL_MONEY_NOTIF:
            send_notification(notification, is_silent=False)
        else:
            send_notification(notification)
 

def get_save_tickers(client):
    """
    (client) -> None
    Получает тикеры, фиги и имена российских акций и записывает в excel таблицу в виде:
    | ticker | figi | name |
    """
    instruments: InstrumentsService = client.instruments
    l = []
    for item in getattr(instruments, 'shares')().instruments:
        if item.currency == 'rub':
            l.append({
                'ticker': item.ticker,
                'figi': item.figi,
                # 'type': method,
                'name': item.name,
                # 'currency': item.currency,
            })
    df = pd.DataFrame(l)
    df.to_excel(EXEL_TICEKRS_FILENAME)

def load_tickers():
    """
    (None) -> DataFrame 
    Загружает тикеры российских акций из excel таблицы и возращает в виде DataFrame
    """
    df = pd.read_excel(EXEL_TICEKRS_FILENAME)
    return df

def get_ticker(figi):
    """
    (str) -> str
    Возращает тикер актива по figi;
    Нужно для отправки уведомления в ТГ 
    """
    return figi_to_ticker[figi]

#ТГ СЕГМЕНТ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def send_notification(message, is_silent=True):
    try:
        bot.send_message(MY_ID_TELEBOT, message,  disable_notification=is_silent)
    except:
        pass 


@bot.message_handler(commands=['ping'])
def start_message(message):
    bot.send_message(message.chat.id, f'Онлайн.\n{message.chat.id}')

@bot.message_handler(commands=['track'])
def add_track(message):
    chat_id = message.chat.id
    if chat_id != MY_ID_TELEBOT:
        return
    ticker_track = bot.send_message(chat_id, 'Отправьте тикер, который необходимо отслеживать.')
    bot.register_next_step_handler(ticker_track, sub_add_track)

def sub_add_track(message):
    chat_id = message.chat.id
    if chat_id != MY_ID_TELEBOT:
        return
    if message.text.upper() in figi_to_ticker.values():
        priority_tickers.append(message.text)
        bot.send_message(chat_id, f'Отслеживаю {message.text}.')

@bot.message_handler(commands=['clear'])
def add_track(message):
    chat_id = message.chat.id
    if chat_id != MY_ID_TELEBOT:
        return
    priority_tickers.clear()
    bot.send_message(chat_id, 'Список очищен.')

@bot.message_handler(commands=['list'])
def add_track(message):
    chat_id = message.chat.id
    if chat_id != MY_ID_TELEBOT:
        return
    msg = "Отслеживается:\n"
    for priority_ticker in priority_tickers:
        msg += f"{priority_ticker}\n"
    bot.send_message(chat_id, msg)

@bot.message_handler(commands=['help'])
def command_help(message):
    chat_id = message.chat.id
    bot.reply_to(message, 'Список доступных команд:')
    for i in commands:
        bot.send_message(chat_id, f'/{i} - {commands[i]}')
    return 



def check_thread_alive(thr):
    thr.join(timeout=0.0)
    return thr.is_alive()

def main():
    print("** Started **\n")
    # Подписанные пользователи 
    # connect_db()
    # global users
    # users = select_all()


    with Client(TOKEN_TINKOFF, target=INVEST_GRPC_API) as client:
        # Получение данных аккаунта  
        response = client.users.get_accounts()
        account, *_ = response.accounts
        account_id = account.id
        
        # ТИКЕРЫ D: 
        get_save_tickers(client)
        tickers = load_tickers()
        figis = tickers['figi']

        # Заполняем словарь, который нужен для получения тикера по фиги
        for row in tickers.itertuples():
            figi_to_ticker[row.figi] = row.ticker
            
        bot_thread = threading.Thread(target=bot.polling)
        bot_thread.start()
        while True:
            if not check_thread_alive(bot_thread):
                bot_thread = threading.Thread(target=bot.polling)
                bot_thread.start()
            # Если есть приоритетные сделки, список фиги на проверку состоит только из них
            if priority_tickers:
                figis = [get_figi(priority_ticker) for priority_ticker in priority_tickers]
            else:
                figis = tickers['figi']
            for figi in figis:
                try:
                    trades = get_history_trades(client, figi=figi, time_minutes=60)
                except:
                    sleep(60*2)
                    send_notification("Нет крупных сделок за послендний час:(", is_silent=True)
                for trade in trades:
                    # type(trade)
                    # Если что-то необычное в сделках отправляем уведомление в ТГ
                    processed_trade = process_trade(trade)
                    check_unusual(processed_trade)
                # print("Checked = " + figi)
            # sleep(60)
                
if __name__ == '__main__':
    main()
    
            
        # trades = get_history_trades(client, TICKER, 600)
    #     df_trades = create_empty_df()
    #     for trade in trades:
    #         df_trade = process_trade(trade)
    #         df_trades = pd.concat([df_trades, df_trade])
    # df_trades = df_trades.sort_values("total_money", ascending=False)
    # df_trades.to_excel(EXEL_TMP_FILENAME, index=True)
