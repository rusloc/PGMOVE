class tgLog(logging.Handler):
    
    '''
            *******
            ! DOC !
            *******
            
            Создание кастомного Handler для отправки логов в группу через телеграм бота
            
            1. Наследие через __super__ класса хэндлеров
            2. создание сэссии через requests
            3. Замена базовой функции EMIT на кастомную которая отправляет сообщения через TG API
            
    '''
    
    def __init__(s, chat, key):
        
        super().__init__()
        
        s._chat = chat
        s._key = key
        s._session = requests.Session()
        
        
    def emit(s, message):
        
        _text = s.format(message)
        s._session.post(f'https://api.telegram.org/bot{s._key}/sendMessage?chat_id={s._chat}&text={_text}')
