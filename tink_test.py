"""
1. Подождать и положительно выйти с каждой позиции;
2. Сфокусироваться на одном активе и заходить в шорт на большом капитале.
test
"""

# Необходимо для работы с АПИ tinkoff
from tinkoff.invest.services import InstrumentsService, MarketDataService
from tinkoff.invest.utils import quotation_to_decimal
from tinkoff.invest.utils import now
from tinkoff.invest import Client

# Необходимо для хранения данных
import pandas as pd

# Необходимо для расчета периода отслеживания сделок
from datetime import timedelta

# Возможно переделать для многопоточного обращения к АПИ тинькова
import threading

# Необходимо для уведомления в ТГ
import telebot

# Необходимо для импортирования токенов тинькова и ТГ-бота
import os
from dotenv import load_dotenv, find_dotenv 


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
 
load_dotenv(find_dotenv())

TOKEN_TINKOFF = os.getenv('TOKEN_TINKOFF')
TOKEN_TELEBOT = os.getenv('TOKEN_TELEBOT')

MY_ID_TELEBOT = '1149967740'
# Сумма, начиная с которой сделки будут отправлены в ТГ и помечены как крупные
MIN_TOTAL_MONEY = 4000000

EXEL_TMP_FILENAME = 'Тестовый_Вывод.xlsx'
EXEL_TICEKRS_FILENAME = 'tickers.xlsx'

# Словарь, который хранит пары ключ-значение: figi -> ticekr
# Необходим для отправки уведомления в ТГ
figi_to_ticker = {}
# Массив, уже обработанных крупных сделок, чтобы избежать дублирования сообщений в ТГ
big_trades = []


bot = telebot.TeleBot(TOKEN_TELEBOT)

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

def check_unusual(trade):
    """
    (trade) --> None
    Проверка, если были был перемещен большой капитал.
    Отправка уведомления в ТГ, если да.
    """
    # print(trade['total_money'][0], type(trade['total_money']))
    if trade['total_money'][0] > MIN_TOTAL_MONEY:
        print(big_trades)
        is_in_list = False
        for big_trade in big_trades:
            if trade.equals(big_trade):
                is_in_list = True
                break
        if is_in_list:
            return
        big_trades.append(trade)
        notification = str(get_ticker(trade['figi'][0])) + '\n' + str(trade['total_money'][0]) + " RUB"
        notification += (lambda x: '\nПродажа' if x == 2 else '\nПокупка')(trade['direction'][0])
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
 
def send_notification(message):
    bot.send_message(MY_ID_TELEBOT, message)


@bot.message_handler(commands=['start'])
def start_message(message):
    bot.send_message(message.chat.id, f'Привет, я бот! Введите /send для отправки сообщения.\n{message.chat.id}')

def main():
    print("** Started **\n")


    with Client(TOKEN_TINKOFF) as client:
        get_save_tickers(client)
        tickers = load_tickers()
        figis = tickers['figi']
        # Заполняем словарь, который нужен для получения тикера по фиги
        for row in tickers.itertuples():
            figi_to_ticker[row.figi] = row.ticker
            
        bot_thread = threading.Thread(target=bot.polling)
        bot_thread.start()
        while True:
            for figi in figis:
                trades = get_history_trades(client, figi=figi, time_minutes=600)
                for trade in trades:
                    # Если что-то необычное в сделках отправляем уведомление в ТГ
                    processed_trade = process_trade(trade)
                    check_unusual(processed_trade)
                print("Checked = " + figi)
                
if __name__ == '__main__':
    main()
    
            
        # trades = get_history_trades(client, TICKER, 600)
    #     df_trades = create_empty_df()
    #     for trade in trades:
    #         df_trade = process_trade(trade)
    #         df_trades = pd.concat([df_trades, df_trade])
    # df_trades = df_trades.sort_values("total_money", ascending=False)
    # df_trades.to_excel(EXEL_TMP_FILENAME, index=True)
