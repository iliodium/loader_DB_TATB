import os
import time
import urllib.request
import multiprocessing
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
import scipy.io as sio


def check_url(url):
    """Проверка ссылки на существование."""
    if requests.head(url, timeout=5).status_code == 404:  # 404 - Not Found
        return False
    else:
        return True


def check_urls(url):
    """Проверка ссылки из массива на существование.

    Удаляет ссылку из массива если ее не существует.
    """
    if check_url(url):
        return url


def generator_urls(url):
    """Перебирает углы и задает им нужный формат.

    1 -> 001
    11 -> 011
    111 -> 111

    Если ссылка не существует, завершает цикл.
    """
    urls = []

    for angle in range(0, 360, 5):
        if len(str(angle)) == 1:
            angle = f'00{angle}'
        elif len(str(angle)) == 2:
            angle = f'0{angle}'
        else:
            angle = f'{angle}'
        url = f'{url[:-7]}{angle}.mat'
        urls.append(url)
    return urls


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


def download_TATB():
    print('Начало скачивания:', end=' ')
    print(time.strftime("%m/%d/%Y %H:%M:%S", time.localtime()))
    t1 = time.time()

    urls = []

    folder = f'{os.getcwd()}/mat files Wind Pressure Database of Two Adjacent Tall Buildings'
    if not os.path.isdir(folder):
        os.mkdir(folder)

    # Генерация ссылок mat файлов с углом 0.
    for Hr in (1,):  # , '05', '07', 15, 20):
        # for case in range(1, 50):
        for case in [16, 20, 23]:
            if case // 10 == 0:
                case = f'0{case}'
            # for A in range(1, 10):
            A = '4'
            url = f'http://www.wind.arch.t-kougei.ac.jp/info_center/windpressure/interference/New_time_series/Hr_{Hr}/case{case}/P114_A11{A}_case{case}_000.mat'
            urls.append(url)

    # Проверка ссылок mat файлов с углом 0 на существование.
    with ThreadPoolExecutor(max_workers=20) as executor:
        urls = list(executor.map(check_urls, urls))

    urls_all = []
    # Генерация ссылок mat файлов с углом от 0 до 355 и проверка их существованния.
    with ThreadPoolExecutor(max_workers=20) as executor:
        urls_temp = list(executor.map(generator_urls, urls))

    for i in urls_temp:
        urls_all.extend(i)

    urls_all = urls_all[0:-1:50]

    # Скачивание ссылок
    with ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(downloader, urls_all)

    print(f'Окончание скачивания:', end=' ')
    print(time.strftime("%m/%d/%Y %H:%M:%S", time.localtime()))
    print("Затраченное время:", time.time() - t1)


def generator_directory_csv():
    """Создание директории под csv файлы.

    Функция исследует папку 'mat files' и создает директорию.
    """
    path = os.getcwd()
    path_csv_files = f'{path}\\csv files Wind Pressure Database of Two Adjacent Tall Buildings'
    folder_name = 'mat files Wind Pressure Database of Two Adjacent Tall Buildings'
    if not os.path.isdir(path_csv_files):
        os.mkdir(path_csv_files)

    for level_1 in os.listdir(f'{os.getcwd()}\\{folder_name}'):
        if not os.path.isdir(f'{path_csv_files}\\{level_1}'):
            os.mkdir(f'{path_csv_files}\\{level_1}')

        for level_2 in os.listdir(f'{path}\\{folder_name}\\{level_1}'):
            if not os.path.isdir(f'{path_csv_files}\\{level_1}\\{level_2}'):
                os.mkdir(f'{path_csv_files}\\{level_1}\\{level_2}')


def detection_mat_files():
    """Перебирает папку 'mat files Wind Pressure Database of Two Adjacent Tall Buildings'

    Возвращает массив с путями mat файлов.
    """
    path_mat_files = f'{os.getcwd()}\\mat files Wind Pressure Database of Two Adjacent Tall Buildings'
    paths_mat_files = []
    for level_1 in os.listdir(path_mat_files):
        for level_2 in os.listdir(f'{path_mat_files}\\{level_1}'):
            for file in os.listdir(f'{path_mat_files}\\{level_1}\\{level_2}'):
                paths_mat_files.append(f'{path_mat_files}\\{level_1}\\{level_2}\\{file}')
    return paths_mat_files


def worker(path_file):
    file_name = path_file[-24:]
    param = file_name[5:9]
    case = file_name[10:16]
    path_out = f'{os.getcwd()}\\csv files Wind Pressure Database of Two Adjacent Tall Buildings\\{param}\\{case}\\'
    file_out = f'{path_out}{file_name[:-3]}.csv'

    mat_file = sio.loadmat(path_file)
    Turbulence_intensity_top = mat_file['Turbulence_intensity_top']
    Sample_period = mat_file['Sample_period']
    Sample_frequecny = mat_file['Sample_frequecny']
    Principal_building_BDH = mat_file['Principal_building_BDH']
    Mean_wind_speed_top = mat_file['Mean_wind_speed_top']
    Interfering_building_BDH = mat_file['Interfering_building_BDH']
    Pressure_coefficients = mat_file['Pressure_coefficients']
    data = [['%.4f' % j for j in i] for i in Pressure_coefficients]

    Turbulence_intensity_top = pd.DataFrame(Turbulence_intensity_top)
    Sample_period = pd.DataFrame(Sample_period)
    Sample_frequecny = pd.DataFrame(Sample_frequecny)
    Principal_building_BDH = pd.DataFrame(Principal_building_BDH)
    Mean_wind_speed_top = pd.DataFrame(Mean_wind_speed_top)
    Interfering_building_BDH = pd.DataFrame(Interfering_building_BDH)
    data = pd.DataFrame(data)

    Turbulence_intensity_top.to_csv(file_out, index=False, header=False, sep=',')
    Sample_period.to_csv(file_out, index=False, header=False, sep=',', mode='a')
    Sample_frequecny.to_csv(file_out, index=False, header=False, sep=',', mode='a')
    Principal_building_BDH.to_csv(file_out, index=False, header=False, sep=',', mode='a')
    Mean_wind_speed_top.to_csv(file_out, index=False, header=False, sep=',', mode='a')
    Interfering_building_BDH.to_csv(file_out, index=False, header=False, sep=',', mode='a')
    data.to_csv(file_out, index=False, header=False, sep=',', mode='a')


if __name__ == '__main__':
    t1 = time.time()
    generator_directory_csv()
    mat_files = detection_mat_files()
    with multiprocessing.Pool(processes=os.cpu_count()) as pool:
        pool.map(worker, mat_files)
    print(time.time() - t1)
