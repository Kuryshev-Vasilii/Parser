import requests
import re
import psycopg2

from bs4 import BeautifulSoup

# Библиотека для многопоточной работы в несколько процессов
# from multiprocessing.dummy import Pool as ThreadPool


# Метод получения html-кода со страницы
def get_html(url):
    r = requests.get(url, timeout=10)
    return r.text  # Возвращает HTML-код страницы


# Метод возвращает список ссылок докторов конкретной больницы
def get_all_links_doctors(url):
    soup = BeautifulSoup(get_html(url), 'lxml')
    links = []
    trs = soup.find('table').find_all('tr')

    for tr in trs:
        try:
            url = str(tr.select('td:nth-of-type(2) > a[href]'))
            url = re.search(re.compile('".*"'), url).group()
            links.append('https://k-vrachu.volmed.org.ru' + url[1:-1])
        except Exception as error:
            continue
    return links


# Метод возвращает названия и адреса больниц и их подразделений
def get_hospital_unit(obj):
    try:
        ''''
            Проверка является ли больница главной и если условие выполняется возвращается ее 
            название и адрес, в противном случае возвращаются сведенья о подразделении больницы
        '''
        if obj.get('class')[0] == 'moDetail' and len(obj.get('class')) == 1:
            return obj.find('span', class_='lpu-name').get_text(), ' '.join(
                obj.find('span', class_='lpu-address').get_text().split()), True
        else:
            return obj.find('span', class_='lpu-name').get_text(), ' '.join(
                obj.find('span', class_='lpu-address').get_text().split()), False
    except Exception as error:
        return '', '', False


# Метод возвращает список ссылок больниц и записывает в базу названия и адреса больниц и их подразделения
def get_all_links_hospital(url, base_url):
    soup = BeautifulSoup(get_html(url), 'lxml')
    links = []
    elements = soup.find(class_='medOrganizationsTable').find_all('a')

    # Подключение к базе
    con = psycopg2.connect(user='vasilii', password='', host='localhost', port='5432', database='hospital_db')
    cursor = con.cursor()


    for element in elements:
        a = element.get('href')
        value, address, flag = get_hospital_unit(element)
        links.append(a.replace('view/', 'doctors/'))

        if flag:
            if value != '':
                # Заполение таблицы hospital
                cursor.execute("INSERT INTO hospital (hospital_name, address) VALUES (%s, %s) RETURNING id",
                               (value, address))
                index = cursor.fetchone()[0]
                con.commit()

        elif flag is False and value != '':
            if value != '':
                # Заполение таблицы hospital
                cursor.execute("INSERT INTO hospital (hospital_name, address, unit_id) "
                               "VALUES (%s, %s, %s) RETURNING id", (value, address, index))
                con.commit()

    cursor.close()
    con.close()
    return list(base_url + link for link in links)


# Метод для извлечения данных из тегов
def get_data_from_tag(data, field='', tag=None):
    array_data = []
    try:
        info = data.find(class_='docInfoBlock').find_all('p')
        flag = False

        for p in info:
            '''
                Случай, когда требуется получить данные из всех родительских p-тегов до элемента, 
                содержащих класс, свидетельствующий, что начинается следующий блок
            '''
            if field == '' and tag is not None:
                if p.get('class') is not None:
                    break
                tags_all = p.find_all(tag)
                array_data.append({tags_all[0].get_text(): tags_all[2].get_text()})
                continue
            '''
                Проверка соответсятвует ли значение текущего элемента переданному значению тега field,  
                в случае, если условие выполняется flag=True
            '''
            if p.get_text() == field:
                flag = True
                continue

            if flag:
                '''
                    Проверка наличия класса у тега, если условие выполняется это свидельствует, 
                    что начинается следующая секция
                '''
                if p.get('class') is not None:
                    break

                '''
                    Получение данных из определенного тега внутри родительского элемента, 
                    в случае отсутствия tag=None данные просто записываются в массив 
                '''
                if tag is not None:
                    info_additional_work = (iter(filter(None, list(i.text.strip() for i in p.find_all('span')))))
                    info_additional_work = dict(zip(info_additional_work, info_additional_work))
                    array_data.append(info_additional_work)
                    continue
                array_data.append(p.get_text())
            else:
                continue
    except Exception as error:
        array_data = []
    return array_data


# Метод получения данных из словаря
def get_data_from_dict(*data):
    arr = ['Организация', 'Отделение', 'Специальность', 'Должность', 'Адрес']
    data = list(filter(None, data))
    result = []

    try:
        result.append(next((item[arr[0]] for item in data[0] if list(item.keys())[0] == arr[0]), ''))
    except Exception as error:
        result.append('')

    try:
        result.append(next((item[arr[1]] for item in data[0] if list(item.keys())[0] == arr[1]), ''))
    except Exception as error:
        result.append('')

    try:
        result.append(next((item[arr[2]] for item in data[0] if list(item.keys())[0] == arr[2]), ''))
    except Exception as error:
        result.append('')

    try:
        result.append(next((item[arr[3]] for item in data[0] if list(item.keys())[0] == arr[3]), ''))
    except Exception as error:
        result.append('')

    try:
        result.append(next((item[arr[4]] for item in data[0] if list(item.keys())[0] == arr[4]), ''))
    except Exception as error:
        result.append('')

    return result


# Метод получения данных о врачах и запись в базу
def get_doctor_data(url):
    soup = BeautifulSoup(get_html(url), 'lxml')
    try:
        name = soup.find(class_='docname').text
    except:
        name = ''

    try:
        docspec = get_data_from_tag(soup, tag='span')[0]['Основная специализация']
    except:
        docspec = ''
    try:
        category = get_data_from_tag(soup, tag='span')[1]['Категория']
    except:
        category = ''

    base_company = get_data_from_tag(soup, 'ОСНОВНОЕ МЕСТО РАБОТЫ', 'span')
    dop_work = get_data_from_tag(soup, 'ДОПОЛНИТЕЛЬНЫЕ МЕСТА РАБОТЫ', 'span')

    b_w = get_data_from_dict(base_company)
    ad_work = get_data_from_dict(dop_work)

    education = get_data_from_tag(soup, 'ОБРАЗОВАНИЕ')

    if name == '':
        return

    # Подключение к базе
    con = psycopg2.connect(user='vasilii', password='', host='localhost', port='5432', database='hospital_db')
    cursor = con.cursor()

    # Заполение таблицы doctors
    cursor.execute(
        "INSERT INTO doctors_new (doctor_name, specialization, category) " +
        "VALUES (%s, %s, %s) RETURNING id",
        (name, docspec, category))
    index = cursor.fetchone()[0]

    # Заполение таблицы main_place_work
    cursor.execute("INSERT INTO main_place_work (work_name, department, specialisation, profile, address)" +
                   " VALUES (%s, %s, %s, %s, %s) RETURNING id",
                   (b_w[0], b_w[1], b_w[2], b_w[3], b_w[4]))
    index_main = cursor.fetchone()[0]
    # Заполение таблицы additional_place_job
    cursor.execute(
        "INSERT INTO additional_place_job (work_name, department, specialisation, profile, address) " +
        "VALUES (%s, %s, %s, %s, %s) RETURNING id",
        (ad_work[0], ad_work[1], ad_work[2], ad_work[3], ad_work[4]))
    index_addition = cursor.fetchone()[0]

    # Заполнение таблицы education
    for item in education:
        cursor.execute(
            "INSERT INTO education (education_background, doctor_id) " +
            "VALUES (%s, %s)", (item, index))

    # Заполение таблицы intermediate_table_main_work_place
    cursor.execute(
        "INSERT INTO intermediate_table_main_work_place (doctor_main_work_id, main_work_id) " +
        "VALUES (%s, %s)", (index, index_main))

    # Заполение таблицы intermediate_table_additional_work_place
    cursor.execute("INSERT INTO intermediate_table_additional_work_place " +
                   "(doctor_additional_work_id, additional_work_id) VALUES (%s, %s)",
                   (index, index_addition))

    con.commit()
    cursor.close()
    con.close()


# Метод для многопоточного парсинга в несколько процессов
# def make_all(url):
#     try:
#         list_links_doctors = get_all_links_doctors(url)
#         with ThreadPool(12) as p:
#             p.map(get_doctor_data, list_links_doctors)
#     except Exception as error:
#         print(error, 'make_all')


def main():
    all_links_hospital = get_all_links_hospital('https://k-vrachu.volmed.org.ru/service/hospitals',
                                                                'https://k-vrachu.volmed.org.ru')
    # Уникальные url в списке
    all_links_hospital = list(set(all_links_hospital))

    # Многопоточная работа в несколько процессов
    # with ThreadPool(2) as p:
    #     p.map(make_all, all_links_hospital)

    for url in all_links_hospital:
        list_links_doctors = get_all_links_doctors(url)
        for i in list_links_doctors:
            get_doctor_data(i)


main()
