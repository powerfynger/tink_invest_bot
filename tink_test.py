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

# Для подгрузки переменных среды  
load_dotenv(find_dotenv())

TOKEN_TINKOFF = os.getenv('TOKEN_TINKOFF')
TOKEN_TELEBOT = os.getenv('TOKEN_TELEBOT')

MY_ID_TELEBOT = '1149967740'
# Сумма, начиная с которой сделки будут отправлены в ТГ и помечены как крупные
MIN_TOTAL_MONEY = 2500000

EXEL_TMP_FILENAME = 'Тестовый_Вывод.xlsx'
EXEL_TICEKRS_FILENAME = 'tickers.xlsx'

# Словарь, который хранит пары ключ-значение: figi -> ticekr
# Необходим для отправки уведомления в ТГ
figi_to_ticker = {}
# Массив, уже обработанных крупных сделок, чтобы избежать дублирования сообщений в ТГ
big_trades = []
# Открытые позиции [trade]
open_positions = []


bot = telebot.TeleBot(TOKEN_TELEBOT)

#ТЕСТОВЫЙ СЕГМЕНТ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
Покупает и продает активы по эталонным ценам. Следует решить:
Вопрос стоит ли ждать эталонной цены или покупать по текущей в реальных сделках.
Вопрос с хранением открытых позиций (возможно получится получать через апи, в таком случае и объекты для добавления при покупке изменяться)
Вопрос частоты проверки сделок по купленным активам.

На данном этапе запоминаем по количеству и наименованию актива большой сделки.
"""
TESTSUM_FILENAME = "money.txt"
MIN_TOTAL_MONEY_BUY = 5000000

my_open_positions = []
test_sum = -1


def get_sum():
    try:
        with open(TESTSUM_FILENAME, 'r+') as file:
            test_sum =  float(file.readline().strip())
    except Exception as e:
        print(e)


# Изменяет текущее значение суммы, вызывается только при покупке или продаже  
def update_sum(value):
    try:
        # Открываем файл для чтения и записи
        with open(TESTSUM_FILENAME, 'r+') as file:
            # Захватываем блокировку
            with lock:
                # Считываем первую строку и преобразуем её в int
                current_sum = float(file.readline().strip())

                # Меняем значение на новое
                current_sum += value

                # Перемещаем указатель файла в начало
                file.seek(0)

                # Перезаписываем новое значение в файл
                file.write(str(current_sum))
                file.truncate()

    except Exception as e:
        print(e)


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
    # print(trade['total_money'][0], type(trade['total_money']))
    if trade['total_money'][0] > MIN_TOTAL_MONEY:
        # print(big_trades)
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
        # notification += (lambda x: '\nПродажа' if x == 2 else '\nПокупка')(trade['direction'][0])
        if trade['direction'][0] == 1:
            check_to_buy(trade)
            notification = str(get_ticker(trade['figi'][0])) + '\n' + str(trade['total_money'][0]) + ' RUB\n'
            notification += "Покупка " + "по "+str(trade['price'][0])+ " RUB\n"
            open_positions.append(trade)
        else:
            check_to_sell(trade)
            notification = str(get_ticker(trade['figi'][0])) + '\n' + str(trade['total_money'][0]) + ' RUB\n'
            position = get_position(trade)
            print(position)
            if position is not None:
                notification += f"Закрытие покупки по {position['price'][0]}, выручка \
{trade['total_money'][0] - position['total_money'][0]}/\
{round(float(trade['total_money'][0]) / position['total_money'][0], 3) % 100} %"
                try:
                    open_positions.remove(position)
                except:
                    pass
                # print(open_positions)
            else:
                notification += "Продажа " + "по "+str(trade['price'][0])+ " RUB\n"
        send_notification(notification)

def check_to_buy(trade):
    """
    (trade) --> None
    Проверяет, если сумма сделки выше указанной и на счёте достаточно денег, то совершает покупку по текущей (в тестовом варианте эталонной) 
    цене на макс. возможную сумму, не превышающую 20% от всего капитала. 
    Добавляет в список открытых позиций
    """
    try:
        if test_sum <= 20000:
            return

        if trade['total_money'][0] > MIN_TOTAL_MONEY_BUY:
            quantity = 1
            total_money = 20000
            while total_money < quantity * trade['price'][0]:
                quantity += 1
            
            quantity -= 1
            # ИМИТАЦИЯ ПОКУПКИ
            test_sum -= quantity * trade['price'][0] 
            update_sum(-(quantity * trade['price'][0]))
            
            notification = f"Произошла покупка {get_ticker(trade['figi'][0])} \n {trade['total_money'][0]} RUB\n\
По {trade['price'][0]}\nБаланс:{test_sum}"
        send_notification(notification, is_silent=False)

        my_open_positions.append({
            'total_sum' : quantity * trade['price'][0],
            'quantity': quantity,
            'price' : price, 
            'quantity_big' : trade['quantity'][0],
            'figi' : trade['figi'][0]
            })
    except:
        pass


def check_to_sell(trade):
    """
    (trade) --> None
    Находит купленные активы в открытых сделках и продает про текущей (в тестовом варианте эталонной) цене
    Удаляет из списка открытых позиций
    """
    try:
        for my_position in my_open_positions:
            if my_position['figi'] == trade['figi'][0] and my_position['quantity_big'] == trade['quantity']:
                # ИМИТАЦИЯ ПРОДАЖИ
                test_sum += trade['price'][0] * my_position['quantity']
                
                update_sum(trade['price'][0] * my_position['quantity'])
                notification = f"Произошла продажа {get_ticker(trade['figi'][0])} \n {trade['total_money'][0]} RUB\n\
    По {trade['price'][0]}\nБаланс:{test_sum}"
                send_notification(notification, is_silent=False)

                my_open_positions.remove(my_position)
    except:
        pass

    

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

def main():
    print("** Started **\n")


    with Client(TOKEN_TINKOFF, target=INVEST_GRPC_API) as client:
        # Создание замка для доступа к файлу с тестовой суммой
        money_lock = threading.Lock()
        test_sum = get_sum()

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
            for figi in figis:
                trades = get_history_trades(client, figi=figi, time_minutes=600)
                for trade in trades:
                    type(trade)
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
