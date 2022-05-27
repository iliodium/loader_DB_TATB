"""Программа скачивает базу данных Tokyo Polytechnic University
в отдельную директорию 'mat files'.
"""

import os
import time
import urllib.request
import multiprocessing

import requests


def check_url(url):
    """Проверка ссылки на существование."""
    if requests.head(url, timeout=5).status_code == 404:  # 404 - Not Found
        return False
    else:
        return True


def check_urls(url, initial_urls: list):
    """Проверка ссылки из массива на существование.

    Удаляет ссылку из массива если ее не существует.
    """
    if check_url(url) is False:
        initial_urls.remove(url)


def generator_urls(url, urls: list):
    """Перебирает углы и задает им нужный формат.

    1 -> 001
    11 -> 011
    111 -> 111

    Если ссылка не существует, завершает цикл.
    """
    for angle in range(0, 360, 5):
        if len(str(angle)) == 1:
            angle = f'00{angle}'
        elif len(str(angle)) == 2:
            angle = f'0{angle}'
        else:
            angle = f'{angle}'
        url = f'{url[:-7]}{angle}.mat'
        urls.append(url)


def downloader(url):
    """Скачивает файл по заданной ссылке.
    Создает папку под разные углы параметра, если она не существует.
    """
    folder = f'{os.getcwd()}/mat files Wind Pressure Database of Two Adjacent Tall Buildings'
    ratio = f'A{url[-18:-15]}'

    path_folder = f'{folder}/{ratio}'
    if not os.path.isdir(path_folder):
        os.mkdir(path_folder)

    pos = f'case{url[-10:-8]}'  # Позиция модели
    path_folder = f'{folder}/{ratio}/{pos}'
    if not os.path.isdir(path_folder):
        os.mkdir(path_folder)

    path_file = f'{path_folder}/{url[-24:]}'
    urllib.request.urlretrieve(url, path_file)


if __name__ == '__main__':
    print('Начало работы:', end=' ')
    print(time.strftime("%m/%d/%Y %H:%M:%S", time.localtime()))
    t1 = time.time()
    urls = multiprocessing.Manager().list()  # Массив доступный всем процессам
    count_core = os.cpu_count()  # Количество логических ядер

    folder = f'{os.getcwd()}/mat files Wind Pressure Database of Two Adjacent Tall Buildings'
    if not os.path.isdir(folder):
        os.mkdir(folder)

    # Генерация ссылок mat файлов с углом 0.
    for Hr in (1, '05', '07', 15, 20):
        for case in range(1, 50):
            if case // 10 == 0:
                case = f'0{case}'
            for A in range(1, 10):
                url = f'http://www.wind.arch.t-kougei.ac.jp/info_center/windpressure/interference/New_time_series/Hr_{Hr}/case{case}/P114_A11{A}_case{case}_000.mat'
                urls.append(url)

    # Проверка ссылок mat файлов с углом 0 на существование.
    with multiprocessing.Pool(processes=count_core * 3) as pool:
        pool.starmap(check_urls, [(url, urls,) for url in urls])

    # Генерация ссылок mat файлов с углом от 0 до 355 и проверка их существованния.
    with multiprocessing.Pool(processes=count_core * 2) as pool:
        pool.starmap(generator_urls, [(url, urls,) for url in urls])

    # Скачивание ссылок
    with multiprocessing.Pool(processes=count_core * 4) as pool:
        pool.map(downloader, [url for url in urls])

    print(f'Окончание скачивания:', end=' ')
    print(time.strftime("%m/%d/%Y %H:%M:%S", time.localtime()))
    print("Затраченное время:", time.time() - t1)
