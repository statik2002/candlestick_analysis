import asyncio
import datetime
import logging
from logging.handlers import RotatingFileHandler
import requests
from datetime import date


async def get_ticker_info(ticker: str):
    url = f'https://iss.moex.com/iss/securities.json?q={ticker}'

    response = requests.get(url)
    response.raise_for_status()

    return response.json()


async def get_day_aggregate_by_day(ticker: str, date: str) -> dict:
    url = f'https://iss.moex.com/iss/securities/{ticker}/aggregates.json?date={date}'

    response = requests.get(url)
    response.raise_for_status()

    return response.json()


async def get_ticket_by_date_range(ticket: str, start_date: str, end_date: str, interval: str) -> dict:
    url = (f'https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticket}/'
           f'candles.json?from={start_date}&till={end_date}&interval={interval}')

    response = requests.get(url)
    response.raise_for_status()

    return response.json()


def check_bear_eat_theory(data):
    index = 1
    downtrend_proof_data = []
    proof_counter = 0
    antiproof_counter = 0
    while index <= len(data)-1:
        if data[index]['open'] > data[index]['close']:
            # bear bar
            if data[index-1]['open'] < data[index-1]['close']:
                # prev bar is bull
                if data[index]['open'] > data[index-1]['close'] and data[index]['close'] < data[index-1]['open']:
                    # bear bar eat bull bar
                    if data[index+1]['open'] > data[index+1]['close']:
                        # downtrend proof
                        downtrend_proof_data.append(data[index])
                        proof_counter += 1
                    else:
                        antiproof_counter += 1
        index += 1

    return proof_counter, antiproof_counter, downtrend_proof_data


def pin_bar(data):
    index = 1
    proof_pinbar_down_counter = 0
    proof_pinbar_up_counter = 0
    proof_data_down = []
    proof_data_up = []

    while index <= len(data)-1:
        if data[index]['open'] > data[index]['close']:
            # bear bar
            bar_body = data[index]['open'] - data[index]['close']
            bar_up_tail = data[index]['high'] - data[index]['open']
            bar_down_tail = data[index]['close'] - data[index]['low']
            if bar_body*2 < bar_up_tail:
                # tail bigger than body 2x
                if data[index]['open'] < data[index-1]['high'] and data[index]['close'] > data[index-1]['low']:
                    # body pinbar between hi and low previous bar
                    if data[index]['high'] > data[index-1]['high']:
                        # high pin bar bigger than previous bar
                        if bar_up_tail > bar_down_tail:
                            proof_pinbar_down_counter += 1
                            proof_data_down.append(data[index])

        else:
            # bull bar
            bar_body = data[index]['close'] - data[index]['open']
            bar_up_tail = data[index]['high'] - data[index]['close']
            bar_down_tail = data[index]['open'] - data[index]['low']
            if bar_body*2 < bar_down_tail:
                if data[index]['open'] > data[index-1]['low'] and data[index]['close'] < data[index-1]['high']:
                    if data[index]['low'] < data[index-1]['low']:
                        if bar_down_tail > bar_up_tail:
                            proof_pinbar_up_counter += 1
                            proof_data_up.append(data[index])

        index += 1

    return proof_pinbar_down_counter, proof_pinbar_up_counter, proof_data_down, proof_data_up


def get_hummers(data):
    index = 1
    proof_hammer_down_counter = 0
    proof_hummer_up_counter = 0
    proof_data_down = []
    proof_data_up = []

    while index <= len(data):
        if data[index-1]['open'] > data[index-1]['close']:
            # up trend
            bar_body = abs(data[index]['close'] - data[index]['open'])
            if data[index]['open'] > data[index]['close']:
                bar_up_tail = data[index]['high'] - data[index]['open']
                bar_down_tail = data[index]['close'] - data[index]['low']
                if bar_down_tail > bar_body * 2:
                    if bar_down_tail > bar_up_tail * 2:
                        if data[index]['close'] > data[index-1]['close']:
                            proof_hammer_down_counter += 1
                            proof_data_down.append(data[index])
            else:
                bar_up_tail = data[index]['high'] - data[index]['close']
                bar_down_tail = data[index]['open'] - data[index]['low']
                if bar_down_tail > bar_body * 2:
                    if bar_down_tail > bar_up_tail * 2:
                        if data[index]['close'] > data[index-1]['close']:
                            proof_hammer_down_counter += 1
                            proof_data_down.append(data[index])

        else:
            # downtrend
            bar_body = abs(data[index]['close'] - data[index]['open'])
            if data[index]['open'] > data[index]['close']:
                bar_up_tail = data[index]['high'] - data[index]['open']
                bar_down_tail = data[index]['close'] - data[index]['low']
                if bar_up_tail > bar_body * 2:
                    if bar_up_tail > bar_down_tail * 2:
                        if data[index]['open'] < data[index - 1]['close']:
                            proof_hummer_up_counter += 1
                            proof_data_up.append(data[index])
            else:
                bar_up_tail = data[index]['high'] - data[index]['close']
                bar_down_tail = data[index]['open'] - data[index]['low']
                if bar_up_tail > bar_body * 2:
                    if bar_up_tail > bar_down_tail * 2:
                        if data[index]['close'] < data[index - 1]['close']:
                            proof_hummer_up_counter += 1
                            proof_data_up.append(data[index])

        index += 1

        return proof_hammer_down_counter, proof_hummer_up_counter, proof_data_down, proof_data_up


def get_inner_harami(data):
    index = 1
    proof_harami_down_counter = 0
    proof_harami_up_counter = 0
    proof_data_down = []
    proof_data_up = []

    while index < len(data):
        if data[index-1]['open'] > data[index-1]['close']:
            # downtrend
            if data[index - 1]['close'] < data[index]['open'] < data[index]['close'] < data[index - 1]['open']:
                proof_harami_up_counter += 1
                proof_data_up.append(data[index])
            if data[index - 1]['open'] > data[index]['open'] > data[index]['close'] > data[index - 1]['close']:
                proof_harami_down_counter += 1
                proof_data_down.append(data[index])

        else:
            if data[index - 1]['close'] > data[index]['open'] > data[index]['close'] > data[index - 1]['open']:
                proof_harami_down_counter += 1
                proof_data_down.append(data[index])
            if data[index - 1]['open'] < data[index]['open'] < data[index]['close'] < data[index - 1]['close']:
                proof_harami_up_counter += 1
                proof_data_up.append(data[index])

        index += 1

    return proof_harami_up_counter, proof_harami_down_counter, proof_data_up, proof_data_down


def get_eaters(data):
    index = 1
    proof_eaters_down_counter = 0
    proof_eaters_up_counter = 0
    proof_data_down = []
    proof_data_up = []

    while index < len(data):
        if data[index-1]['open'] < data[index-1]['close']:
            # uptrend
            if data[index]['open'] > data[index-1]['close'] and data[index]['close'] < data[index-1]['open']:
                proof_eaters_down_counter += 1
                proof_data_down.append(data[index])
        else:
            # downtrend
            if data[index]['open'] < data[index-1]['close'] and data[index]['close'] > data[index-1]['open']:
                proof_eaters_up_counter += 1
                proof_data_up.append(data[index])

        index += 1

    return proof_eaters_up_counter, proof_eaters_down_counter, proof_data_up, proof_data_down


def get_morning_stars(data):

    index = 1
    morning_star_up_counter = 0
    morning_star_up = []
    morning_star_down_counter = 0
    morning_star_down = []

    while len(data) > index:

        if data[index]['open'] < data[index]['close']:
            # uptrend
            if data[index-1]['open'] < data[index-1]['close']:
                mx = data[index-1]['close']
            else:
                mx = data[index-1]['open']

            if mx < data[index]['open'] and mx < data[index-2]['close'] and mx < data[index-2]['open']:
                print(len(data))
                print(f'mx={mx}, prev_open={data[index-2]["open"]}, prev_close={data[index-2]["close"]}, date_prev={data[index-2]["end"]}')
                morning_star_up_counter += 1
                morning_star_up.append(data[index-1])

        else:
            # downtrend
            if data[index - 1]['open'] < data[index - 1]['close']:
                mn = data[index - 1]['open']
            else:
                mn = data[index - 1]['close']

            if mn > data[index]['open'] and mn > data[index - 2]['close'] and mn > data[index-2]['open']:
                morning_star_down_counter += 1
                morning_star_down.append(data[index-1])

        index += 1

    return morning_star_down_counter, morning_star_up_counter, morning_star_down, morning_star_up


async def analysis(tickets: list, start_date: str, end_date: str, interval: int):
    for ticket in tickets:
        ticket_by_dates = await get_ticket_by_date_range(ticket, start_date, end_date, str(interval))
        data = []
        for day in ticket_by_dates['candles']['data']:
            data.append(
                {
                    'open': day[0],
                    'close': day[1],
                    'high': day[2],
                    'low': day[3],
                    'value': day[4],
                    'volume': day[5],
                    'begin': day[6],
                    'end': day[7],
                }
            )

        proof_pinbar_down_counter, proof_pinbar_up_counter, proof_data_down, proof_data_up = pin_bar(data)
        if proof_pinbar_up_counter:
            print(f'{ticket}---Proved up pinbars={proof_pinbar_up_counter}')
            for day in proof_data_up:
                print(f'{ticket}---{day["end"]}')
        if proof_pinbar_down_counter:
            print(f'{ticket}---Proved down pinbars={proof_pinbar_down_counter}')
            for day in proof_data_down:
                print(f'{ticket}---{day["end"]}')

        proof_hammer_down_counter, proof_hummer_up_counter, proof_data_down, proof_data_up = get_hummers(data)
        if proof_hammer_down_counter:
            print(f'{ticket}---Proved down hammers={proof_hammer_down_counter}')
            for day in proof_data_down:
                print(f'{ticket}---{day["end"]}')
        if proof_hummer_up_counter:
            print(f'{ticket}---Proved up hammers={proof_hummer_up_counter}')
            for day in proof_data_up:
                print(f'{ticket}---{day["end"]}')

        proof_harami_up_counter, proof_harami_down_counter, proof_data_up, proof_data_down = get_inner_harami(data)
        if proof_harami_up_counter:
            print(f'{ticket}---Proved up harami={proof_harami_up_counter}')
            for day in proof_data_up:
                print(f'{ticket}---{day["end"]}')
        if proof_harami_down_counter:
            print(f'{ticket}---Proved down harami={proof_harami_down_counter}')
            for day in proof_data_down:
                print(f'{ticket}---{day["end"]}')

        proof_eaters_up_counter, proof_eaters_down_counter, proof_data_up, proof_data_down = get_eaters(data)
        if proof_eaters_up_counter:
            print(f'{ticket}---Proved up eaters={proof_eaters_up_counter}')
            for day in proof_data_up:
                print(f'{ticket}---{day["end"]}')
        if proof_eaters_down_counter:
            print(f'{ticket}---Proved down eaters={proof_eaters_down_counter}')
            for day in proof_data_down:
                print(f'{ticket}---{day["end"]}')

        '''
        morning_star_down_counter, morning_star_up_counter, morning_star_down, morning_star_up = get_morning_stars(data)
        if morning_star_up_counter:
            print(f'{ticket}---Proved up morning star [*]={morning_star_up_counter}')
            for day in morning_star_up:
                print(f'{ticket}---{day["end"]}')
        if morning_star_down_counter:
            print(f'{ticket}---Proved down morning star [*]={morning_star_down_counter}')
            for day in morning_star_down:
                print(f'{ticket}---{day["end"]}')
        '''


async def main():

    current_date = date.today() - datetime.timedelta(days=1)
    days_to_analyses = current_date - datetime.timedelta(days=4)

    await analysis(
        [
            'GAZP', 'AFLT', 'PIKK', 'UNAC', 'GECO',
            'PHOR', 'APTK', 'RNFT', 'SBER', 'YNDX',
            'SGZH', 'SNGS', 'ROSN', 'HYDR', 'NLMK'
        ],
        days_to_analyses,
        current_date,
        24
    )


if __name__ == '__main__':

    # получение пользовательского логгера и установка уровня логирования
    py_logger = logging.getLogger(__name__)
    py_logger.setLevel(logging.INFO)

    # настройка обработчика и формировщика в соответствии с нашими нуждами
    py_handler = logging.FileHandler(f"{__name__}.log", mode='a')
    py_formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
    # py_handler. .handlers.RotatingFileHandler(f"{__name__}.log", 10000, 3)

    # добавление формировщика к обработчику
    py_handler.setFormatter(py_formatter)
    # добавление обработчика к логгеру
    py_logger.addHandler(py_handler)
    rotate_handler = RotatingFileHandler(f"{__name__}.log", maxBytes=500000, backupCount=5)
    py_logger.addHandler(rotate_handler)

    asyncio.run(main())
