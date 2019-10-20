import re
import pandas as pd
from tqdm import tqdm as bar
from multiprocessing import Pool


big_2_reg = r'(?P<big_one>[А-Я]+[а-я]{2,})'
small_2_reg = r'(?P<small_one>(?:(?:дом,? ?)|(?:[^а-я]д,?)|(?:дв)|(?:кор,?)|(?:корп,?)|(?:корпус,?)|(?:строен,?)|(?:строение,?)|(?:вл,?)|(?:сооружение,?)|(?:соор,?)|(?:домовл,?))? ?[0-9/]+(?P<letter>(?:а)|(?:б)|(?:в)|(?:г)|(?:д)|(?:ж)|(?:А)|(?:Б)|(?:В)|(?:Г)|(?:Д)(?:Ж)|(?:с)|(?:С)|(?:к)|(?:К))*[0-9/, ]*)'

small_1_reg = r'(?P<small_one>((владение)|(вл\.?)|(строение)|(дом)|(корпус)|(сооружение)|(домовладение)|(д\.?)) ?[0-9][0-9А-Яа-я\"\/-]*)'
big_1_reg = r'(?P<big_one>[А-Я]+[а-я]{2,})'


def reg_func_2(value):
    if type(value) is str:
        ans_big = []
        ans_small = []
        for_big = value[:]
        for_small = value[:]
        while re.search(big_2_reg, for_big) is not None:
            ans_big.append(re.search(big_2_reg, for_big).group("big_one"))
            for_big = re.sub(big_2_reg, "", for_big, count=1)

        while re.search(small_2_reg, for_small) is not None:
            ans_small.append(re.search(small_2_reg, for_small).group("small_one"))
            for_small = re.sub(small_2_reg, "", for_small, count=1)
        ans_all = []
        for elem in ans_small:
            temp = re.sub(r',', "", elem)
            temp = re.sub(r'(?:дом,? ?)|(?:[^а-я]д,?)', "дом ", temp)
            temp = re.sub(r'(?:домовл,?)|(?:дв)', "домовладение ", temp)
            temp = re.sub(r'(?:вл,?)', "владение ", temp)

            temp1 = re.sub(r'(?:корпус,?)|(?:корп,?)|(?:кор,?)', "корпус ", temp)
            if temp1 == temp:
                temp1 = re.sub(r'к', "корпус ", temp)
                if temp1 != temp and temp1.find("корпус ") != 0:
                    ans_all.append("дом " + temp1.split("корпус")[0])
                    temp = "корпус" + temp1.split("корпус")[1]

                temp1 = re.sub(r'К', "корпус ", temp)
                if temp1 != temp and temp1.find("корпус ") != 0:
                    ans_all.append("дом " + temp1.split("корпус")[0])
                    temp = "корпус" + temp1.split("корпус")[1]
            else:
                temp = temp1[:]

            temp = re.sub(r'(?:сооружение,?)|(?:соор,?)', "сооружение ", temp)

            temp1 = re.sub(r'(?:строение,?)|(?:строен,?)', "строение ", temp)
            if temp1 == temp:
                temp1 = re.sub(r'c[^о]', "строение ", temp)
                if temp1 != temp and temp1.find("строение ") != 0:
                    ans_all.append("дом " + temp1.split("строение")[0])
                    temp = "строение" + temp1.split("строение")[1]

                temp1 = re.sub(r'С[^о]', "строение", temp)
                if temp1 != temp and temp1.find("строение ") != 0:
                    ans_all.append("дом " + temp1.split("строение")[0])
                    temp = "строение" + temp1.split("строение")[1]
            else:
                temp = temp1[:]
            ans_all.append(temp)
        ans_all += ans_big
        ans_all = list(map(lambda x: x.lower().replace(" ", "").rstrip().strip(), ans_all))
        return set(ans_all)
    else:
        return set()


def reg_func_1(value):
    if type(value) is str:
        ans_big = []
        ans_small = []
        for_big = value[:]
        for_small = value[:]
        while re.search(big_1_reg, for_big) is not None:
            ans_big.append(re.search(big_1_reg, for_big).group("big_one"))
            for_big = re.sub(big_1_reg, "", for_big, count=1)

        while re.search(small_1_reg, for_small) is not None:
            ans_small.append(re.search(small_1_reg, for_small).group("small_one"))
            for_small = re.sub(small_1_reg, "", for_small, count=1)

        # ans_all = []
        # for elem in ans_small:
        #     # temp = re.sub(r'(?:дом,? ?)|(?:[^а-я]д,?.?[^а-я])', "дом ", elem)
        #     temp = re.sub(r'(владение)|(вл\.?)', "владение ", elem)
        #
        #     ans_all.append(temp)
        #
        # ans_small_temp = ans_all[:]
        # for elem in ans_small_temp:
        #     if "дом " in elem:
        #         ans_all.append(elem.replace("дом", "домовладение"))
        ans_all = list(map(lambda x: x.lower().replace(" ", "").rstrip().strip(), ans_big + ans_small))
        return set(ans_all)
    else:
        return set()


def distance(a, b):
    """Calculates the Levenshtein distance between a and b."""
    n, m = len(a), len(b)
    if n > m:
        a, b = b, a
        n, m = m, n

    current_row = range(n + 1)
    for i in range(1, m + 1):
        previous_row, current_row = current_row, [i] + [0] * n
        for j in range(1, n + 1):
            add, delete, change = previous_row[j] + 1, current_row[j - 1] + 1, previous_row[j - 1]
            if a[j - 1] != b[i - 1]:
                change += 1
            current_row[j] = min(add, delete, change)

    return current_row[n]


def worker(value):
    believability = 0
    index_of_max = None
    for i, current in enumerate(excel_1["real_reg"]):
        if type(value) is set and type(current) is set and len(value) > 0 and len(current) > 0:
            dist = len(value & current) / len(value | current)
            if dist > believability:
                believability = dist
                index_of_max = i
    print(believability, index_of_max)
    return index_of_max


if __name__ == "__main__":
    excel_1 = pd.read_excel("1.xlsx", header=0)
    excel_2 = pd.read_excel("2.xlsx", header=0)
    excel_1["real_reg"] = excel_1["АДРЕС"].apply(reg_func_1)
    excel_2["real_reg"] = excel_2[" Адрес ОКС"].apply(reg_func_2)

    true_ans = excel_2["real_reg"].apply(worker)
    # parameters = [(ind, search, excel_1["АДРЕС"]) for ind, search in enumerate(excel_2[" Адрес ОКС"])]
    # with Pool(processes=4) as pool:
    #     ans = pool.map(worker, parameters)

    # true_ans = {"index": [elem[0] for elem in ans], "believability": [elem[1] for elem in ans],
    #             "index_of_max": [elem[2] for elem in ans]}

    colums = ["АДРЕС", "MAIN_ADR", "КЛАСС", "ТИП СТРОЕНИЯ", "НАЗНАЧЕНИЕ СТРОЕНИЯ", "СОСТОЯНИЕ_СТАТКАРТЫ",
              "ДАТА_УСТАНОВКИ_СОСТОЯНИЯ_СТАТКАРТЫ", "САМОВОЛЬНО_ВОЗВЕДЕННОЕ", "_КВАРТАЛА_БТИ", "ВИД_ФОНДА",
              "МАТЕРИАЛ СТЕН", "ГРУППА_КАПИТАЛЬНОСТИ", "ЭТАЖНОСТЬ", "КОЛ_ВО_МИНИМАЛЬНЫХ_ЭТАЖЕЙ", "МАТЕРИАЛ ПЕРЕКТЫТИЙ",
              "МАТЕРИАЛ КРОВЛИ", "ОТДЕЛКА ФАСАДА", "ГОД ПОСТРОЙКИ", "ГОД НАДСТРОЙКИ", "ГОД ПРИСТРОЙКИ",
              "ГОД ПЕРЕОБОРУДОВАНИЯ", "ГОД КАПИТАЛЬНОГО РЕМОНТА", "ПРИЗНАК ПАМЯТНИКА АРХИТЕКТУРЫ",
              "ПЛОЩАДЬ ПО НАРУЖНОМУ ОБМЕРУ", "ОБЪЕМ", "ПРОЦЕНТ ИЗНОСА", "ГОД УСТАНОВКИ ИЗНОСА", "НАЛИЧИЕ ВОДОПРОВОДА",
              "НАЛИЧИЕ КАНАЛИЗАЦИИ", "ВИД ОТОПЛЕНИЯ", "ПРИЗНАК НАЛИЧИЯ ГОРЯЧЕЙ ВОДЫ", "КОЛИЧЕСТВО ГАЗОВЫХ ПЛИТ",
              "КОЛИЧЕСТВО ЭЛЕКТРИЧЕСКИХ ПЛИТ", "КОЛИЧЕСТВО МУСОРОПРОВОДОВ", "КОЛИЧЕСТВО ПОДЬЕЗДОВ",
              "КОЛИЧЕСТВО ЛИФТОВ ПАССАЖИРСКИХ", "КОЛИЧЕСТВО ЛИФТОВ ГРУЗО ПАССАЖИРСКИХ", "КОЛИЧЕСТВО ЛИФТОВ ГРУЗОВЫХ",
              "ПЛОЩАДЬ ОБЩАЯ", "ПЛОЩАДЬ ЛОДЖИЙ", "ПЛОЩАДЬ БАЛКОНОВ", "ПЛОЩАДЬ ПРОЧИХ ХОЛОДНЫХ ПОМЕЩЕНИЙ", "ПЛОЩАДЬ НЕЖИЛАЯ",
              "ПЛОЩАДЬ ОБЩАЯ НЕЖИЛЫХ ПОДВАЛОВ", "ПЛОЩАДЬ ОБЩАЯ НЕЖИЛЫХ ЦОКОЛЬНЫХ", "ПЛОЩАДЬ ОБЩАЯ ЖИЛЫХ ПОМЕЩЕНИЙ",
              "ПЛОЩАДЬ ЖИЛАЯ ЖИЛЫХ ПОМЕЩЕНИЦ", "КОЛИЧЕСТВО ЖИЛЫХ ПОМЕЩЕНИЙ", "КОЛИЧЕСТВО ЖИЛЫХ КОМНАТ",
              "КОЛИЧЕСТВО ОДНОКОМНАТНЫХ КВАРТИО", "ОБЩАЯ ПЛОЩАДЬ ОДНОКОМНАТНЫХ КВАРТИР",
              "ЖИЛАЯ ПЛОЩАДЬ ОДНОКОМНАТНЫХ КВАРТИР", "КОЛИЧЕСТВО ДВУХКОМНАТНЫХ КВАРТИО",
              "ОБЩАЯ ПЛОЩАДЬ ДВУХКОМНАТНЫХ КВАРТИР", "ЖИЛАЯ ПЛОЩАДЬ ДВУХКОМНАТНЫХ КВАРТИР",
              "КОЛИЧЕСТВО ТРЕХКОМНАТНЫХ КВАРТИО", "ОБЩАЯ ПЛОЩАДЬ ТРЕХКОМНАТНЫХ КВАРТИР",
              "ЖИЛАЯ ПЛОЩАДЬ ТРЕХКОМНАТНЫХ КВАРТИР", "КОЛИЧЕСТВО ЧЕТЫРЕХКОМНАТНЫХ КВАРТИО",
              "ОБЩАЯ ПЛОЩАДЬ ЧЕТЫРЕХКОМНАТНЫХ КВАРТИР", "ЖИЛАЯ ПЛОЩАДЬ ЧЕТЫРЕХКОМНАТНЫХ КВАРТИР",
              "КОЛИЧЕСТВО ПЯТИКОМНАТНЫХ КВАРТИР", "ОБЩАЯ ПЛОЩАДЬ ПЯТИКОМНАТНЫХ КВАРТИР",
              "ЖИЛАЯ ПЛОЩАДЬ ПЯТИКОМНАТНЫХ КВАРТИР", "ВСЕГО КВАРТИР КОЛИЧЕСТВО", "ВСЕГО В КВАРТИРАХ КОМНАТ",
              "ВСЕГО В КВАРТИРАХ ПЛОЩАДЬ ОБЩАЯ", "ВСЕГО В КВАРТИРАХ ПЛОЩАДЬ ЖИЛАЯ", "СЕРИЯ ПРОЕКТА", "КОЛИЧЕСТВО БАССЕЙНОВ",
              "ПРИЗНАК СТУДЕНТЧЕСКОГО ОБЩЕЖИТИЯ", "СЧИТАТЬ ОТДЕЛЬНЫМ КОРПУСОМ", "НАЛИЧИЕ МАНСАРДЫ", "UNOM 2"]

    for ind, val in bar(enumerate(true_ans)):
        if not pd.isna(val):
            excel_2.loc[ind, colums] = excel_1.iloc[int(val)][colums]

    excel_2.to_excel("5.xlsx")

