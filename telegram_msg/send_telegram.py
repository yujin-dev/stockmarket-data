import telegram


class telegram_bot():

    def __init__(self):


        bot_token = '1265467593:AAEFmuREZWe4ZItQGGeIdmh9KDxkPpdvHdI'
        self.bot = telegram.Bot(token=bot_token)

        ### 처음 Bot 등록해서 chat id 가져옴
        #chat_id = self.bot.getUpdates()[-1].message.chat.id

    def send_msg(self, text):

        self.bot.sendMessage(chat_id = '651947034', text=text)
