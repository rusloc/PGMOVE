class tgLog(logging.Handler):
    
    '''
            *******
            ! DOC !
            *******
            
            Создание кастомного Handler для отправки логов в группу через телеграм бота
            Custom handler for logging into TG channel
            
            1. inherit __super__
            2. need to import REQUESTS for session setup
            3. change original EMIT function
            
    '''
    
    def __init__(s, chat, key):
        
        super().__init__()
        
        s._chat = chat
        s._key = key
        s._session = requests.Session()
        
        
    def emit(s, message):
        
        _text = s.format(message)
        s._session.post(f'https://api.telegram.org/bot{s._key}/sendMessage?chat_id={s._chat}&text={_text}')
