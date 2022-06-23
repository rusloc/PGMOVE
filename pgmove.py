import io
import sys
import time
import logging
import configparser
import psycopg2 as pg
from datetime import datetime as dt
from datetime import timedelta as td


class pgmove:
    
    '''
            *******
            ! DOC !
            *******
    
            Класс используется для переноса таблиц из одной бд в другую (линейно).
            
            1. При инициализации класса описывается набор переменных через __slots__
            2. Затем происходит инциализация переменных из файла config.ini
            3. При инициализации из файла 
                3.1. забираются мэппинги таблиц
                3.2. данные для создания подключений
                3.3. скрипты SQL для проверки DDL и колонок таблиц
            4. После считывания данных конфигурации происходит:
                4.1. инициализация логгеров и установка подключений с передачей в переменные инстанса соединений и курсоров
            5. Транспорт таблиц происходит в синхронном режиме
                5.1. Перед переносом выполняется проверка существования сооветствующих таблиц в БД источнике и БД назначения
		    5.2. Также проверяется схема таблицы источника из мэппинга: если схема табл изменилась то таблица назначения удаляется
				и создается заново на основании нового DDL из источника.
            6. Перенос таблиц проихводится через фукнцию MOVE которая в свою очередь использует:
                6.1. инициализированные подключения (курсоры)
                6.2. соответствующие функции GET_TABLE и PUT_TABLE для забора и встравки данных таблиц
            7. Добавлен кастомный HTTP логгер через отдельный класс который отправляет сообщения в Telegram через HTTPS API
            
    '''
    
    __slots__ = '_src_ddl','_chat','_key', '_tg_logger','_error_logger','_info_logger','_dst_con','_src_con', '_dst_cursor','_src_cursor', '_buffer','_mapping','_config','_parser','_src_host','_src_port','_src_dbname','_src_user','_src_password','_dst_host','_dst_port','_dst_dbname','_dst_user','_dst_password'
    
    def __init__(s, config_file):

		'''
			Инициализация базовых переменных подключения, мэппингов и прочих атрибутов
		'''
        
        s._config = configparser.ConfigParser()
        s._config.read(config_file)
        
        s._src_ddl = s._config.get('ddl', 'sql')
        
        s._src_host = s._config.get('src','host')
        s._src_port = s._config.get('src','port')
        s._src_user = s._config.get('src','user')
        s._src_password = s._config.get('src','password')
        s._src_dbname = s._config.get('src','dbname')
        
        s._dst_host = s._config.get('dst','host')
        s._dst_port = s._config.get('dst','port')
        s._dst_user = s._config.get('dst','user')
        s._dst_password = s._config.get('dst','password')
        s._dst_dbname = s._config.get('dst','dbname')
        
        s._key = '5250617487:AAF9j4edtPjY-_d92In0z6FiGijgSrFjAUo'

        s._chat = '-725709893'
        
        s._mapping = {x.split(':')[0]:x.split(':')[1] for x in s._config.get('mapping', 'tables').split('\n')}
        
        s._info_logger, s._error_logger, s._tg_logger = s.set_loggers()
        
        s._src_con, s._src_cursor = s.set_con(s._src_host, s._src_port, s._src_user, s._src_password, s._src_dbname)
        s._dst_con, s._dst_cursor = s.set_con(s._dst_host, s._dst_port, s._dst_user, s._dst_password, s._dst_dbname)
        
        s._buffer = io.StringIO() 
        
    def set_loggers(s):
                
        # loggers setup (tglog - измененный класс логера для отправки сообщений в Телеграм
        
        _handler = logging.FileHandler('_etl_logger.txt', 'a')
        _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - func_name:  %(funcName)s - %(message)s')
        
        #info logger
        
        info_logger = logging.getLogger('info_logger')
        info_logger.setLevel(logging.INFO)

        _handler.setFormatter(_formatter)
        info_logger.addHandler(_handler)

        # error logger

        error_logger = logging.getLogger('error_logger')
        error_logger.setLevel(logging.ERROR)

        _handler.setFormatter(_formatter)
        error_logger.addHandler(_handler)
        
        # telegram logger
        
        telegram_logger = logging.getLogger('telegram_logger')
        telegram_logger.setLevel(logging.INFO)

        http_Handler = tgLog(
            chat='-725709893',
            key='5250617487:AAF9j4edtPjY-_d92In0z6FiGijgSrFjAUo')

        http_Handler.setFormatter(_formatter)

        telegram_logger.addHandler(http_Handler)

        return info_logger, error_logger, telegram_logger
        
    def set_con(s, _host, _port, _user, _password, _dbname):
        
        try:
        
            connection = pg.connect(host = _host, port = _port, user = _user, password = _password, dbname = _dbname)
            cursor = connection.cursor()
            
            s._info_logger.info(f'Connection successful {_dbname}')
            
            return connection, cursor
            
        except Exception:
            
            s._error_logger.error(f'Failed to connect {sys.exc_info()}')
    
    def get_table(s, _table = None): 
        
        '''
            Получение данных из таблицы в буфер через инциализиваронное подключение
        '''
        
        s._src_cursor.copy_to(s._buffer, table = str(_table), sep='^')

        s._buffer.seek(0)


    def put_table(s, _table= None, _truncate= False):
        
        '''
            Вставка данных в таблицу назначения из буфера через инциализиваронное подключение
        '''

        if _truncate:
            s._dst_cursor.execute(f'truncate table {_table}')
            
        try:

            s._dst_cursor.copy_from(s._buffer, sep = '^', table = str(_table))
            
            s._buffer.seek(0)
            s._buffer.truncate()
            
        except Exception:
            
            s._error_logger.error(f'FAIL to insert. Crash info: {sys.exc_info()}')
                
    def make_table(s, _src_table, _dst_table):
        
        '''
            Функция удаляет таблицу в базе назначения и создает таблицу заново на основании DDL источника.
            Игнорируются CONSTRAINTS источника.
        '''
                
        # удалить таблицу назначение если колонки не совпадают
        s._dst_cursor.execute(f'drop table if exists {_dst_table}')

        # получить DDL источника
        s._src_cursor.execute(s._src_ddl.replace('{_table_name_}', _src_table))

        # сохраняем DDL источника
        _ddl = s._src_cursor.fetchone()
        
        # меняем название таблицы в DDL в соответствии с мэппингом
        s._dst_cursor.execute(_ddl[0].replace(f'>{_src_table}<' , _dst_table))

        # записываем изменения в базе назначения
        s._dst_con.commit()
                
    def check_table(s, _cursor, _table):
        
        '''
        Проверка существует ли таблица. 
        Ответ 1 или 0.
        '''
        
        _query = f'''
                SELECT EXISTS (
                    SELECT FROM 
                        pg_tables
                    WHERE 
                        schemaname = 'public' AND 
                        tablename  = '{_table}'
                    )::int
                '''
        try:
            
            _cursor.execute(_query)
            _status = _cursor.fetchone()
            
            s._info_logger.info(f'Recieved table status {_table} OK')
        
            return _status[0]
    
        except Exception:
            
            s._error_logger.error(f'Failed to check table status crash info: {sys.exc_info()}')
            
            return False     
        
    def get_table_schema(s, _src_table, _dst_table):
        
        '''
            Получение схемы таблиц источника и назначения и возвращение списков колонок
        '''
        
        s._src_cursor.execute(f'''select 
                                    column_name
                                    ,data_type
                                from information_schema.columns
                                where 1=1 
                                    and table_name = '{_src_table}' 
                                order by column_name
                        ''')
        s._dst_cursor.execute(f'''select 
                                    column_name
                                    ,data_type
                                from information_schema.columns
                                where 1=1 
                                    and table_name = '{_dst_table}' 
                                order by column_name
                        ''')
        
        src_cols = s._src_cursor.fetchall()
        dst_cols = s._dst_cursor.fetchall()
        
        return src_cols, dst_cols
            
    def check_cols(s, _src_schema, _dst_schema):

        '''
            Проверка (сопоставление) списка колонок в таблице назначения и таблице истоничке
        '''
        
        return True if _src_schema == [(x[0].lstrip('_'), x[1]) for x in _dst_schema] else False
    
    def cols_dif(s, _src_schema, _dst_schema):
    
        sym_dif = []

        src_dict = {x[0]:x[1] for x in _src_schema}
        dst_dict = {x[0].lstrip('_'):x[1] for x in _dst_schema}

        for i in src_dict:

            if i not in dst_dict or src_dict[i] != dst_dict[i]:
                sym_dif.append((i, src_dict[i]))

        return sym_dif
                
    def move(s):
        
        _start = time.time()
        
        for t in s._mapping.items():
            
            _src_schema, _dst_schema = s.get_table_schema(t[0], t[1])
            
            _check_cols = s.check_cols(_src_schema, _dst_schema)
            
            _cols_dif = s.cols_dif(_src_schema, _dst_schema)
            
            if s.check_table(s._src_cursor, t[0]) and s.check_table(s._dst_cursor, t[1]) and _check_cols:
                
                s._info_logger.info(f'Tables {t[0]} and {t[1]} and schemas OK')
            
                try:
                    s.get_table(t[0])
                    s._info_logger.info(f'Get table {t[0]} OK')
                
                except Exception:
                    
                    s._error_logger.error(f'crash info: {sys.exc_info()}')
                    #s._tg_logger.info(f'FAIL to get table: {t[0]}')
                
                try:
                    
                    s.put_table(t[1], _truncate = True)
                    s._info_logger.info(f'Put table {t[1]} OK')
                    
                except Exception:
                    
                    s._error_logger.error(f'Failed to put table {t[1]} crash info: {sys.exc_info()}')
                    #s._tg_logger.info(f'FAIL to put table: {t[1]}')
                
            else:
                
                s._error_logger.error(f'Could not process table {t[1]} FAIL')
                s._info_logger.info(f'Reset table {t[1]}')
                s._info_logger.info(f'Check columns {_cols_dif}')
                s._tg_logger.info(f'Table schema changed: {t[0]} check server LOG')
                
                try:
                    s.make_table(t[0], t[1])
                    s._info_logger.info(f'Table created {t[1]}')
                    
                except Exception:
                    
                    s._error_logger.error(f'Failed create {t[1]} crash info: {sys.exc_info()}')
                    
                try:
                    s.get_table(t[0])
                    s.put_table(t[1], _truncate = True)
                    s._info_logger.info(f'Table replaced {t[1]} OK')
                    
                except Exception:
                    s._error_logger.error(f'Failed to reset and replace {t[1]} crash info: {sys.exc_info()}')
                    
        
        s._src_con.commit()
        s._src_con.close()
        
        s._dst_con.commit()
        s._dst_con.close()
        
        _end = time.time()
        _delta = _end - _start
        
        s._info_logger.info(f'Finished in {_delta//60:.0f} min {_delta%60:.0f} sec')
        s._tg_logger.info(f'Finished in {_delta//60:.0f} min {_delta%60:.0f} sec')
        
        logging.shutdown()
