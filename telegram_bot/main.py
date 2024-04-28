import telebot
from telebot import types
import requests
from bs4 import BeautifulSoup as bs
import random as rd
import os

API_TOKEN = "6175109575:AAG5tQ77KRTZOCGj0mAcGOPjbdu76devtu8"
bot = telebot.TeleBot(API_TOKEN)

# Parser function to fetch a random joke/anecdote from a specified URL
def parse(url):
    try:
        result = {'title': []}
        r = requests.get(url)
        r.raise_for_status()  # Check for HTTP errors
        soup = bs(r.text, 'html.parser')
        anecdotes_name = soup.find_all(class_='anekdot')
        for name in anecdotes_name:
            result['title'].append(name.a['title'])
        return rd.choice(result['title'])
    except Exception as e:
        print(f"An error occurred while fetching anecdotes: {e}")
        return "Failed to fetch anecdotes"

# Function to download an image (e.g., waifu) from a specified URL
def waifu(url, save_path="image.jpg"):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors
        if 'image' in response.headers['Content-Type']:  # Check if response is an image
            with open(save_path, "wb") as f:
                f.write(response.content)
                return True  # Image successfully downloaded
        else:
            print("Unexpected content type received. Not saving the file.")
            return False  # Image not saved due to unexpected content type
    except Exception as e:
        print(f"An error occurred while downloading the image: {e}")
        return False  # Image not saved due to error

# Handle /start command
@bot.message_handler(commands=['start'])
def welcome(message):
    markup = types.ReplyKeyboardMarkup()
    item1 = types.KeyboardButton('Рандомное число')
    markup.add(item1)
    item2 = types.KeyboardButton('Рандомное число 1')
    markup.add(item2)
    item3 = types.KeyboardButton('Daily dose of анекдотов')
    markup.add(item3)
    item4 = types.KeyboardButton('Waifu')
    markup.add(item4)
    bot.send_message(message.chat.id, 'Салам', reply_markup=markup)

# Handle text messages
@bot.message_handler(content_types=['text'])
def handle_text(message):
    if message.chat.type == 'private':
        if message.text == 'Рандомное число':
            bot.send_message(message.chat.id, str(rd.randrange(1, 100)))
        elif message.text == 'Рандомное число 1':
            bot.send_message(message.chat.id, '1')
        elif message.text == 'Daily dose of анекдотов ебана':
            joke = parse('https://anekdotov.net/sms/?ysclid=lkcp4hij44395382291')
            bot.send_message(message.chat.id, f'Daily dose of анекдотов: {joke}')
        elif message.text == 'Waifu':
            success = waifu('https://api.waifu.pics/sfw/waifu', 'image.jpg')
            if success:
                photo = open('image.jpg', 'rb')
                bot.send_photo(message.chat.id, photo)
                photo.close()
            else:
                bot.send_message(message.chat.id, 'Failed to fetch waifu image')
        else:
            bot.send_message(message.chat.id, 'на кнопочки нажимай, brah')

# Polling for messages
bot.polling(none_stop=True)
