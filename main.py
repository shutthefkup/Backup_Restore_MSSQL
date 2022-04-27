import pyodbc
import datetime
# Указываем основную папку куда складывать бэкапы и откуда их потом брать
path_backup = 'B:\\Backup'


def main():
    #Просим ввести имя бд(опять же можно указать и руками и автоматом юзать его)
    dbname = input("Введите имя базы : ")
    backorres = input("Укажите действие бэкап 1, рестор 2")
    # Старт и финиш берём для процедуры подсчёта времени
    start = datetime.datetime.now()
    full_path = f'{path_backup}\{dbname}{date_today()}.bak'
    if backorres == 1:
        backupdb(dbname, full_path, start)
    elif backorres == 2:
        restoredb(dbname, full_path, start)
    else:
        print("Вы ввели некорректное значение")
        pass


def restoredb(dbname, full_path, start):
    # указываем драйвер
    driver = 'DRIVER={ODBC Driver 17 for SQL Server}'
    # SQL сервер к примеру 192.168.0.1
    server = 'SERVER='
    # указываем порт
    port = 'PORT=1433'
    # указываем имя бэкапируемой базы
    db = f'DATABASE={dbname}'
    # учетная запись, у которой есть права на backup к примеру sa
    user = 'UID='
    # пароль для учетной записи
    pw = 'PWD='
    # соберем строку подключения к серверу
    conn_str = ';'.join([driver, server, port, db, user, pw])
    # подключаемся к базе
    base_conn = pyodbc.connect(conn_str)
    connect_to_base = f'Подключились к базе {dbname} для восстановления - {now_time()}'
    print(connect_to_base)
    # Создаём курсор для работы с базой
    cursor = base_conn.cursor()
    # устанавливаем режим автосохранения транзакций
    base_conn.autocommit = True
    '''
    Заправшиваем через файл бэкапа логические имена файлов(иначе будет ошибка о несовпадении)
        при бэкапе вписывается логичкие имена файлов и при ресторе они остаются теми же несмотря на то
        что физически файлы называются по другому
     '''
    # Делаем запрос
    cursor.execute(f"RESTORE FILELISTONLY FROM DISK = 'B:\Backup\{dbname}{date_today()}.bak'")
    # Получаем сразу все данные
    logicalnames = cursor.fetchall()
    # Делаем магию)))
    # Переводим полученный список в строку
    tmp = str(logicalnames[0])
    tmp1 = str(logicalnames[1])
    # Отрезаем лишние символы слева
    logdbname = tmp.lstrip("('").split("'", 1)[0]
    logdblog = tmp1.lstrip("('").split("'", 1)[0]
    print(f'Логическое имя базы: {logdbname} айла лога: {logdblog}')
    # оповещаем о начале восстановления базы
    restore_database = f'Начали восстановление базы {dbname} в - {now_time()}'
    print(restore_database)
    # выполняем запрос
    cursor.execute('SELECT name FROM sys.databases')
    # получаем в список кортежей, где на первой позиции, в каждом из кортежей имя базы
    databases = cursor.fetchall()
    # список для баз
    databases_list = []
    # перебираем список картежей и добавляем имена баз в список databases_list
    for base in databases:
        databases_list.append(base[0])
    if dbname in databases_list:
        # выводим оповещения об устанавливке SINGLE_USER
        single_user = f'Устанавливаем SINGLE_USER - {now_time()}'
        print(single_user)
        # выполняем запрос на установку SINGLE_USER
        cursor.execute(f'ALTER DATABASE [{dbname}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE')
        # выводим оповещение об удалении базы
        print(f'Удаляем базу {dbname} - {now_time()}')
        # удаляем базу
        cursor.execute(f'use master DROP DATABASE [{dbname}]')
        time.sleep(20)
    else:
        # если база не обнаружена оповещаем об этом
        print(f'База с именем {dbname} не обнаружена приступаем к восстановлению - {now_time()}')
    # вводим переменную для "правильных" процентов
    stats = -2
    # выполняем запрос
    cursor.execute(
        f"RESTORE DATABASE [{dbname}] FROM  DISK = N'{full_path}' WITH  FILE = 1, MOVE N'{logdbname}' TO N'{db_path}\{dbname}.mdf', MOVE N'{logdblog}' TO N'{log_path}\{dbname}_log.ldf', NOUNLOAD, STATS=1")
    # получаем ответ от сервера SQL и оповещаем о статусе выполнения
    while cursor.nextset():
        stats += 1
        if stats > 0:
            print(f'Выполненно {stats}% - {now_time()}')
        pass
    base_conn.close()
    finish = datetime.datetime.now()
    time_delta = delta_hour_min_sec(start=start, finish=finish)
    print(f'Для восстановления базы CRM_UT потребовалось {time_delta}')


def backupdb(dbname, full_path, start):
    # указываем драйвер
    driver = 'DRIVER={ODBC Driver 17 for SQL Server}'
    # SQL сервер
    server = 'SERVER='
    # указываем порт
    port = 'PORT=1433'
    # указываем имя бэкапируемой базы
    db = f'DATABASE={dbname}'
    # учетная запись, у которой есть права на backup
    user = 'UID='
    # пароль для учетной записи
    pw = 'PWD='
    # соберем строку подключения к серверу
    conn_str = ';'.join([driver, server, port, db, user, pw])
    # подключаемся к базе
    base_conn = pyodbc.connect(conn_str)
    connect_to_base = f'Подключились к базе {dbname} для создания бэкапа - {now_time()}'
    print(connect_to_base)
    # Создаём курсор для работы с базой
    cursor = base_conn.cursor()
    # Указываем автокомит для запросов
    base_conn.autocommit = True
    # Переходим непосредственно к созданию бэкапа
    print(f'Начали создание бэкапа - {now_time()}')
    # вводим переменную для "красивого" отображения процентов
    stats = -2
    # выполняем команду для создания бэкапа
    cursor.execute(f"BACKUP DATABASE [{dbname}] TO DISK = N'{full_path}' WITH COMPRESSION, COPY_ONLY, STATS=1")
    # получаем ответ от сервера SQL и оповещаем о статусе выполнения
    while cursor.nextset():
        stats += 1
        if stats > 0:
            print(f'Выполненно {stats}% - {now_time()}')
        pass
    base_conn.close()
    finish = datetime.datetime.now()
    time_delta = delta_hour_min_sec(start=start, finish=finish)
    print(f'Для создания бэкапа потребовалось {time_delta}')


def delta_hour_min_sec(start, finish):
    # Проведём подсчёт затраченного времени на операцию для вывода отчёта
    delta = finish - start
    total_sec = delta.total_seconds()
    total_hours = int(total_sec // 3600)
    total_min = int((total_sec % 3600) // 60)
    final_sec = int((total_sec % 3600) % 60)
    return f'{total_hours} час. {total_min: 02} мин. {final_sec: 02} сек.'


def date_today():
    # Тут мы получаем очередную переменную с которой нужно будет работать
    daytoday = datetime.datetime.now()
    bcpdate = daytoday.strftime("%d%m%Y")
    return bcpdate


def now_time():
    hour = datetime.datetime.now().hour
    minute = datetime.datetime.now().minute
    seconds = datetime.datetime.now().second
    now = f'{hour:02}:{minute:02}:{seconds:02}'
    return now


if __name__ == '__main__':
    main()
