#!/usr/bin/env python3

import asyncio
import logging
import random
import uuid
import sqlite3
import json
import re
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from enum import Enum

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, 
    InlineKeyboardButton, URLInputFile
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
import aiohttp
from aiohttp import ClientTimeout, ClientSession

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneNumberInvalidError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('casino_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Config:
    BOT_TOKEN = "8500749765:AAFzMmN_blh6e7AjI4EblL1C6SmQn7hdI9k"
    CRYPTOBOT_API_TOKEN = "528236:AAidGbqFOGlxEAA2mcOgE3Ql72L30K9IFeo"
    TELEGRAM_API_ID = 29898848
    TELEGRAM_API_HASH = "5c73f25097ed580ea178f7e97018548d"
    TELEGRAM_PHONE = "+79942922128"
    
    CRYPTO_BOT_ID = 1559501630
    DB_PATH = "casino_bot.db"
    DB_BACKUP_PATH = "backups/"
    
    EXCHANGE_RATE = 76.79
    
    MIN_DEPOSIT = 100.0
    MAX_DEPOSIT = 1000000.0
    MIN_WITHDRAWAL = 100.0
    MAX_WITHDRAWAL = 500000.0
    MIN_BET = 20.0
    MAX_BET = 500000.0
    
    BONUS_WAGERING_REQUIREMENT = 10.0
    MIN_DEPOSIT_FOR_BONUS = 20.0
    BONUS_EXPIRY_DAYS = 30
    
    ADMIN_IDS = [8272278969, 7199344406]
    
    PHOTOS = {
        "menu": "AgACAgIAAxkBAAICb2mDT4vvJSoOi6nzFZfspqxVIxPnAALID2sbxmAZSDWZ5QzI1LiOAQADAgADeQADOAQ",
        "profile": "AgACAgIAAxkBAAICa2mDTwjNZsyF5zor1gd3MDNOrc8bAALCD2sbxmAZSNXJVtIhVpC1AQADAgADeQADOAQ",
        "deposit": "AgACAgIAAxkBAAICbWmDTx8MWC4zec278CfA7jezfz01AALED2sbxmAZSK1MlIxrA74bAQADAgADeQADOAQ",
        "withdraw": "AgACAgIAAxkBAAICammDTud2bSPdeTyynBwYY3P1f9ifAALAD2sbxmAZSKmwPnp3bQemAQADAgADeQADOAQ",
        "bet": "AgACAgIAAxkBAAMRaYhjTfqG-B4JnVqIFRA1F8nUgjgAAgQmaxuAekhI_UbBzvDELusBAAMCAAN5AAM6BA",
        "win": "AgACAgIAAxkBAAMPaYhjTVB50DXNIxr0r1_3qn5fSncAAgMmaxuAekhIgAABUJ313fKrAQADAgADeQADOgQ",
        "lose": "AgACAgIAAxkBAAMNaYhjTWgPHlpOHJVVkODyTPfjgfgAAgEmaxuAekhIT_vhQSB-i-8BAAMCAAN5AAM6BA",
        "draw": "AgACAgIAAxkBAAOCaYiQxKGnxdMEFERIhj2rumx4N38AAiIoaxuAekhID_LHho1tpN4BAAMCAAN5AAM6BA",
        "bonus": "AgACAgIAAxkBAAICvGmEyk-7eQfUQeCt8nP-omT8DF3sAALhEmsbbOUhSFtWh2x5JpqVAQADAgADeQADOAQ"
    }
    
    COEFFICIENTS = {
        'dice': {
            'odd': 2.0, 'even': 2.0,
            '1': 3.0, '2': 3.0, '3': 3.0,
            '4': 3.0, '5': 3.0, '6': 3.0
        },
        'darts': {
            'miss': 2.5, 'center': 2.5,
            'red': 2.0, 'white': 2.0
        },
        'rps': {
            'rock': 3.0, 'scissors': 3.0, 'paper': 3.0
        },
        'bowling': {
            'strike': 2.5,
            'miss': 2.5,
            '2_pins': 1.1, '3_pins': 1.2, '4_pins': 1.3, '5_pins': 1.4
        },
        'basketball': {
            'goal': 2.0, 'stuck': 2.0,
            'miss': 2.0, 'clean': 3.0
        },
        'football': {
            'goal': 1.5, 'miss': 1.5
        },
        'slots': {
            '777': 4.0,
            'triple': 2.0
        },
        'kb': {
            'red': 1.5, 'white': 1.5
        }
    }
    
    EMOJI_MAP = {
        'dice': '🎲',
        'darts': '🎯',
        'basketball': '🏀',
        'football': '⚽',
        'bowling': '🎳',
        'slots': '🎰'
    }
    
    SUPPORT_LINK = "https://t.me/casinomayami"
    NEWS_CHANNEL = "https://t.me/casinomayami_news"
    
    MESSAGES = {
        'welcome': "<tg-emoji emoji-id='5217822164362739968'>👑</tg-emoji> <b>Добро пожаловать, {username}!</b>\n\n<tg-emoji emoji-id='5316727448644103237'>👤</tg-emoji> <b>Тех. поддержка</b> - <a href='{support}'>тык</a>\n<tg-emoji emoji-id='5258474669769497337'>❗️</tg-emoji><b>Новостной канал</b> - <a href='{news}'>тык</a>\n<tg-emoji emoji-id='5316832074047441823'>🌐</tg-emoji> <b>Как играть</b> - <a href='{news}'>тык</a>\n\n<tg-emoji emoji-id='5258179403652801593'>❤️</tg-emoji> Для продолжения выберите кнопки ниже.",
        
        'play_menu': "<tg-emoji emoji-id='5436386989857320953'>🤑</tg-emoji> <b>Выберите игру, на которую желаете сделать ставку</b>\n\n<tg-emoji emoji-id='5353025608832004653'>🤩</tg-emoji> После выбора, выберите режим игры, в который желаете играть",
        
        'profile': "<tg-emoji emoji-id='5454371323595744068'>🥸</tg-emoji> <b>Ваш профиль</b> ›\n├ Баланс: {balance:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n├ Бонусный баланс: {bonus_balance:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n├ Оборот: {turnover:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n└ Осталось до бонуса {remaining:.2f}<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> из 50000.0<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>",
        
        'deposit_menu': "<tg-emoji emoji-id='5417924076503062111'>💰</tg-emoji> <b>Пополнение баланса</b>\n\nВаш баланс: {balance:.2f} <tg-emoji emoji-id='5417924076503062111'>💰</tg-emoji>\nБонусный баланс: {bonus_balance:.2f} <tg-emoji emoji-id='5417924076503062111'>💰</tg-emoji>\n\n<tg-emoji emoji-id='5449800250032143374'>🎁</tg-emoji> <b>Бонусы доступны после депозита!</b>\n\nМинимальный депозит: {min_deposit} <tg-emoji emoji-id='5417924076503062111'>💰</tg-emoji>\nМаксимальный депозит: {max_deposit} <tg-emoji emoji-id='5417924076503062111'>💰</tg-emoji>\n\nВыберите сумму или введите свою:",

        'withdraw_menu': "<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Вывод средств</b>\n\nВаш баланс: {balance:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\nБонусный баланс: {bonus_balance:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n\n⚠️ <b>Вывод доступен только с реального баланса!</b>\n\nМинимальный вывод: {min_withdraw} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\nМаксимальный вывод: {max_withdraw} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n\nВыберите сумму или введите свою:",

        'stats': "<tg-emoji emoji-id='5429651785352501917'>↗️</tg-emoji> <b>Статистика @{username}:</b>\n\n<tg-emoji emoji-id='5436386989857320953'>🤑</tg-emoji> Сыграно — {games_played} ставок\n\n<tg-emoji emoji-id='5402186569006210455'>💱</tg-emoji> Оборот — {turnover:.1f}<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n\n<tg-emoji emoji-id='5386367538735104399'>⌛️</tg-emoji> Аккаунту — {account_age} дней\n<tg-emoji emoji-id='5443127283898405358'>📥</tg-emoji> Пополнений — {deposits}<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n\n<tg-emoji emoji-id='5445355530111437729'>📤</tg-emoji> Выводов — {withdrawals}<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>",
        
        'insufficient_balance': "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Недостаточно средств на балансе",

        'min_bet': "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Минимальная ставка: {min} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>",
        'max_bet': "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Максимальная ставка: {max} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>",

        'min_deposit': "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Минимальный депозит: {min} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>",
        'max_deposit': "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Максимальный депозит: {max} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>",

        'min_withdrawal': "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Минимальный вывод: {min} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>",
        'max_withdrawal': "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Максимальный вывод: {max} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>",

        'invalid_amount': "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Введите корректную сумму (например: 100 или 50.5)",

        'deposit_success': "<tg-emoji emoji-id='5206607081334906820'>✅</tg-emoji> Депозит зачислен!\n\nСумма: {amount:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> Новый баланс: {new_balance:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>",

        'withdrawal_success': "<tg-emoji emoji-id='5206607081334906820'>✅</tg-emoji> Заявка на вывод создана!\n\nСумма: {amount:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n\n<tg-emoji emoji-id='5294087731134082941'>📝</tg-emoji> Ожидайте выплаты от поддержки в течение 24 часов.",
        
        'withdrawal_request_created': "<tg-emoji emoji-id='5206607081334906820'>✅</tg-emoji> <b>Заявка на вывод создана!</b>\n\nСумма: {amount:.2f} ₽\nСтатус: ⏳ На рассмотрении\n\n<tg-emoji emoji-id='5294087731134082941'>📝</tg-emoji> Администратор получил уведомление. Ожидайте подтверждения.",
        
        'withdrawal_approved': "<tg-emoji emoji-id='5206607081334906820'>✅</tg-emoji> <b>Заявка на вывод одобрена!</b>\n\nСумма: {amount:.2f} ₽\nСтатус: ✅ Одобрено\n\n<tg-emoji emoji-id='5294087731134082941'>📝</tg-emoji> Ожидайте отправки средств администратором.",
        
        'withdrawal_completed': "<tg-emoji emoji-id='5206607081334906820'>✅</tg-emoji> <b>Вывод средств выполнен!</b>\n\nСумма: {amount:.2f} ₽\nСтатус: 💰 Выполнено\n\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Текущий баланс:</b> {balance:.2f} ₽\n\n🎮 <b>Благодарим за игру!</b>",
        
        'withdrawal_rejected': "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> <b>Заявка на вывод отклонена</b>\n\nСумма: {amount:.2f} ₽\nСтатус: ❌ Отклонено\n\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Текущий баланс:</b> {balance:.2f} ₽",
        
        'withdrawal_admin_notification': "📤 <b>НОВАЯ ЗАЯВКА НА ВЫВОД</b>\n\n👤 Пользователь: {username} (ID: {user_id})\n💰 Сумма: {amount:.2f} ₽\n📅 Дата: {date}\n\nСтатус: ⏳ Ожидает решения",
        
        'withdrawal_admin_approve_request': "💬 <b>ОТПРАВЬТЕ ЧЕК ПОЛЬЗОВАТЕЛЮ</b>\n\n👤 Пользователь: {username} (ID: {user_id})\n💰 Сумма: {amount:.2f} ₽\n📅 Дата: {date}\n\n<b>Отправьте чек или сообщение пользователю. Бот перешлет его автоматически.</b>",
        
        'game_result_win': (
            "<tg-emoji emoji-id='5258508428212445001'>🎮</tg-emoji> <b>Результат игры</b><tg-emoji emoji-id='5372949601340897128'>✨</tg-emoji>\n\n"
            "<tg-emoji emoji-id='5258501105293205250'>👏</tg-emoji> <b>Победа!</b>\n\n"
            "{bet_info}\n"
            "{coefficient_info}\n"
            "{bet_amount_info}\n\n"
            "{balance_info}"
        ),
        
        'game_result_loss': (
            "<tg-emoji emoji-id='5258508428212445001'>🎮</tg-emoji> <b>Результат игры</b><tg-emoji emoji-id='5372949601340897128'>✨</tg-emoji>\n\n"
            "<tg-emoji emoji-id='5258105663359294787'>❌</tg-emoji> <b>Проигрыш!</b>\n\n"
            "{bet_info}\n"
            "{bet_amount_info}\n\n"
            "{balance_info}"
        ),
        
        'game_result_draw': (
            "<tg-emoji emoji-id='5258508428212445001'>🎮</tg-emoji> <b>Результат игры</b><tg-emoji emoji-id='5372949601340897128'>✨</tg-emoji>\n\n"
            "<tg-emoji emoji-id='5258501105293205250'>👏</tg-emoji> <b>Ничья!</b>\n\n"
            "{bet_info}\n"
            "{bet_amount_info}\n\n"
            "{balance_info}"
        ),
        
        'check_detected': "<tg-emoji emoji-id='5206607081334906820'>✅</tg-emoji> <b>Чек обнаружен!</b>\n\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Сумма:</b> {amount} {currency}\n🆔 <b>Код:</b> {check_code}\n\n⏳ <b>Обрабатываю активацию...</b>\nСредства будут зачислены на ваш баланс в течение 1-2 минут.",
        
        'check_activated': "<tg-emoji emoji-id='5206607081334906820'>✅</tg-emoji> <b>Чек успешно активирован!</b>\n\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Сумма:</b> {amount} {currency}\n🆔 <b>Код чека:</b> {check_code}\n👤 <b>Зачислено на баланс</b>\n\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Текущий баланс:</b> {balance:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n\n🎮 <b>Можете начинать играть!</b>",
        
        'check_failed': "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> <b>Ошибка при активации чека</b>\n\n🆔 <b>Код:</b> {check_code}\n📝 <b>Причина:</b> {error}\n\n⚠️ <b>Пожалуйста, обратитесь в поддержку</b>",
        
        'bonus_received': "<tg-emoji emoji-id='5206607081334906820'>✅</tg-emoji> <b>Бонус получен!</b>\n\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Сумма:</b> {amount:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n🎯 <b>Требуемый оборот:</b> x{multiplier} ({required_turnover:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>)\n⏳ <b>Истекает:</b> {expiry_date}\n\n🎮 <b>Используйте бонусный баланс для ставок!</b>",
        
        'bonus_activated': "<tg-emoji emoji-id='5206607081334906820'>✅</tg-emoji> <b>Бонус активирован!</b>\n\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Сумма:</b> {amount:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n🎯 <b>Требуемый оборот:</b> x{multiplier} ({required_turnover:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>)\n📊 <b>Текущий оборот:</b> {current_turnover:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> ({progress:.1f}%)\n\n🎮 <b>Делайте ставки, чтобы отыграть бонус!</b>",
        
        'bonus_converted': "<tg-emoji emoji-id='5206607081334906820'>✅</tg-emoji> <b>Бонус конвертирован в реальные средства!</b>\n\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Бонусная сумма:</b> {bonus_amount:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n🎯 <b>Выполненный оборот:</b> {actual_turnover:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Зачислено на баланс:</b> {converted_amount:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Текущий баланс:</b> {new_balance:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>",
        
        'bonus_expired': "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> <b>Бонус истек!</b>\n\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Сумма:</b> {amount:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n🎯 <b>Требуемый оборот:</b> x{multiplier} ({required_turnover:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>)\n📊 <b>Текущий оборот:</b> {current_turnover:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> ({progress:.1f}%)\n\n⏳ <b>Время на отыгрыш истекло.</b>",
        
        'bonus_info_message': "<tg-emoji emoji-id='5449800250032143374'>🎁</tg-emoji> <b>Бонусная система</b>\n\nДля получения бонусов необходимо сделать депозит на любую сумму.\n\n<tg-emoji emoji-id='5417924076503062111'>💰</tg-emoji> <b>Как получить бонус:</b>\n1. Сделайте депозит (любая сумма)\n2. Обратитесь к администратору за бонусом\n3. Или используйте промокод: /promo КОД\n\n<tg-emoji emoji-id='5224257782013769471'>🎯</tg-emoji> <b>Условия отыгрыша:</b>\n• Вейджеринг: x{wagering_requirement}\n• Срок: {expiry_days} дней\n• После отыгрыша бонус конвертируется в реальные средства",

        'no_active_bonus': "<tg-emoji emoji-id='5294087731134082941'>📝</tg-emoji> <b>Активных бонусов нет</b>\n\n🎁 Для получения бонуса:\n1. Сделайте депозит\n2. Обратитесь к администратору\n3. Используйте промокод",
        
        'promo_code_not_found': "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Промокод не найден или истек",
        
        'promo_code_used': "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Вы уже использовали этот промокод",
        
        'promo_code_success': "<tg-emoji emoji-id='5206607081334906820'>✅</tg-emoji> <b>Промокод активирован!</b>\n\n🎁 <b>Бонус:</b> {amount:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n🎯 <b>Требуемый оборот:</b> x{multiplier}\n📝 <b>Код:</b> {promo_code}\n\n🎮 <b>Используйте бонусный баланс для ставок!</b>",
        
        'use_bonus_balance': "<tg-emoji emoji-id='5294087731134082941'>📝</tg-emoji> <b>Использовать бонусный баланс?</b>\n\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Доступно:</b> {bonus_balance:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Реальный баланс:</b> {real_balance:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>\n\nВыберите, откуда делать ставку:"
    }

class GameType(Enum):
    DICE = "dice"
    DARTS = "darts"
    RPS = "rps"
    BOWLING = "bowling"
    BASKETBALL = "basketball"
    FOOTBALL = "football"
    SLOTS = "slots"
    KB = "kb"

class BonusStatus(Enum):
    ACTIVE = "active"
    COMPLETED = "completed"
    EXPIRED = "expired"
    CANCELLED = "cancelled"

class WithdrawalStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    COMPLETED = "completed"
    REJECTED = "rejected"

class CurrencyConverter:
    @staticmethod
    def usdt_to_rub(amount_usdt: float) -> float:
        return amount_usdt * Config.EXCHANGE_RATE
    
    @staticmethod
    def rub_to_usdt(amount_rub: float) -> float:
        return amount_rub / Config.EXCHANGE_RATE
    
    @staticmethod
    def format_currency(amount: float) -> str:
        return f"{amount:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>"

class RealEmojiGame:
    @staticmethod
    async def throw_real_dice(bot: Bot, chat_id: int, game_type: str) -> Dict[str, Any]:
        try:
            emoji = Config.EMOJI_MAP.get(game_type, '🎲')
            dice_message = await bot.send_dice(chat_id=chat_id, emoji=emoji)
            real_value = dice_message.dice.value
            logger.info(f"Real Telegram dice: game={game_type}, value={real_value}, emoji={emoji}")
            return {
                'value': real_value,
                'emoji': emoji,
                'message_id': dice_message.message_id
            }
        except Exception as e:
            logger.error(f"Error throwing real dice: {e}")
            return {
                'value': random.randint(1, 6) if game_type in ['dice', 'darts', 'basketball', 'football', 'bowling'] else random.randint(1, 64),
                'emoji': Config.EMOJI_MAP.get(game_type, '🎲'),
                'message_id': None
            }
    
    @staticmethod
    def check_game_result(game_type: str, dice_value: int, user_choice: str = None) -> Dict[str, Any]:
        game_handlers = {
            GameType.DICE.value: RealEmojiGame._check_dice_result,
            GameType.DARTS.value: RealEmojiGame._check_darts_result,
            GameType.BASKETBALL.value: RealEmojiGame._check_basketball_result,
            GameType.FOOTBALL.value: RealEmojiGame._check_football_result,
            GameType.BOWLING.value: RealEmojiGame._check_bowling_result,
            GameType.SLOTS.value: RealEmojiGame._check_slots_result,
            GameType.RPS.value: RealEmojiGame._check_rps_result,
            GameType.KB.value: RealEmojiGame._check_kb_result,
        }
        handler = game_handlers.get(game_type)
        if handler:
            return handler(dice_value, user_choice)
        return {'win': False, 'coefficient': 0.0, 'result_text': 'Неизвестная игра', 'bot_choice': ''}
    
    @staticmethod
    def _check_dice_result(dice_value: int, user_choice: str) -> Dict[str, Any]:
        if not user_choice:
            return {
                'win': False,
                'coefficient': 0.0,
                'result_text': f'🎲 Выпало: {dice_value}',
                'bot_choice': str(dice_value)
            }
        if user_choice == 'odd':
            win = (dice_value % 2 == 1)
            coefficient = Config.COEFFICIENTS['dice']['odd'] if win else 0.0
            result_text = f'🎲 Выпало: {dice_value} ({"Нечет" if dice_value % 2 == 1 else "Чет"})'
        elif user_choice == 'even':
            win = (dice_value % 2 == 0)
            coefficient = Config.COEFFICIENTS['dice']['even'] if win else 0.0
            result_text = f'🎲 Выпало: {dice_value} ({"Чет" if dice_value % 2 == 0 else "Нечет"})'
        elif user_choice in ['1', '2', '3', '4', '5', '6']:
            win = (dice_value == int(user_choice))
            coefficient = Config.COEFFICIENTS['dice'][user_choice] if win else 0.0
            result_text = f'🎲 Выпало: {dice_value} (Ставка: {user_choice})'
        else:
            win = False
            coefficient = 0.0
            result_text = f'🎲 Выпало: {dice_value}'
        return {
            'win': win,
            'coefficient': coefficient,
            'result_text': result_text,
            'bot_choice': str(dice_value)
        }
    
    @staticmethod
    def _check_darts_result(dice_value: int, user_choice: str) -> Dict[str, Any]:
        if dice_value == 1:
            result_text = f'🎯 Мимо! (значение: {dice_value})'
            win = (user_choice == 'miss')
            coefficient = Config.COEFFICIENTS['darts']['miss'] if win else 0.0
            actual_choice = 'miss'
        elif 2 <= dice_value <= 5:
            result_text = f'🎯 Почти попал! (значение: {dice_value})'
            win = (user_choice in ['red', 'white'])
            coefficient = Config.COEFFICIENTS['darts'][user_choice] if win else 0.0
            actual_choice = 'red' if dice_value == 5 else 'white'
        elif dice_value == 6:
            result_text = f'🎯 Центр! Победа! 🎯'
            win = (user_choice == 'center')
            coefficient = Config.COEFFICIENTS['darts']['center'] if win else 0.0
            actual_choice = 'center'
        else:
            result_text = f'🎯 Неизвестный результат: {dice_value}'
            win = False
            coefficient = 0.0
            actual_choice = 'miss'
        return {
            'win': win,
            'coefficient': coefficient,
            'result_text': result_text,
            'bot_choice': actual_choice
        }
    
    @staticmethod
    def _check_basketball_result(dice_value: int, user_choice: str) -> Dict[str, Any]:
        if dice_value in [1, 2]:
            result_text = f'🏀 Мимо! (значение: {dice_value})'
            win = (user_choice == 'miss')
            coefficient = Config.COEFFICIENTS['basketball']['miss'] if win else 0.0
            actual_choice = 'miss'
        elif dice_value == 3:
            result_text = f'🏀 Дужка! Почти попал! (значение: {dice_value})'
            win = (user_choice == 'stuck')
            coefficient = Config.COEFFICIENTS['basketball']['stuck'] if win else 0.0
            actual_choice = 'stuck'
        elif dice_value in [4, 5]:
            result_text = f'🏀 Гол! Победа! 🏀'
            win = (user_choice == 'goal' or user_choice == 'clean')
            if user_choice == 'clean':
                coefficient = Config.COEFFICIENTS['basketball']['clean']
            else:
                coefficient = Config.COEFFICIENTS['basketball']['goal']
            actual_choice = 'goal'
        else:
            result_text = f'🏀 Неизвестный результат: {dice_value}'
            win = False
            coefficient = 0.0
            actual_choice = 'miss'
        return {
            'win': win,
            'coefficient': coefficient,
            'result_text': result_text,
            'bot_choice': actual_choice
        }
    
    @staticmethod
    def _check_football_result(dice_value: int, user_choice: str) -> Dict[str, Any]:
        if dice_value in [1]:
            result_text = f'⚽ Мимо! (значение: {dice_value})'
            win = (user_choice == 'miss')
            coefficient = Config.COEFFICIENTS['football']['miss'] if win else 0.0
            actual_choice = 'miss'
        elif dice_value in [2, 3]:
            result_text = f'⚽ Штанга! Почти попал! (значение: {dice_value})'
            win = False
            coefficient = 0.0
            actual_choice = 'miss'
        elif dice_value in [4, 5, 6]:
            result_text = f'⚽ Гол! Победа! ⚽'
            win = (user_choice == 'goal')
            coefficient = Config.COEFFICIENTS['football']['goal'] if win else 0.0
            actual_choice = 'goal'
        else:
            result_text = f'⚽ Неизвестный результат: {dice_value}'
            win = False
            coefficient = 0.0
            actual_choice = 'miss'
        return {
            'win': win,
            'coefficient': coefficient,
            'result_text': result_text,
            'bot_choice': actual_choice
        }
    
    @staticmethod
    def _check_bowling_result(dice_value: int, user_choice: str = None) -> Dict[str, Any]:
        if user_choice is None:
            return RealEmojiGame._calculate_bowling_result(dice_value)
        if user_choice == 'miss':
            win = (dice_value == 1)
            coefficient = Config.COEFFICIENTS['bowling']['miss'] if win else 0.0
            result_text = f'🎳 Мимо! (значение: {dice_value})'
            actual_choice = 'miss'
        elif user_choice == 'strike':
            win = (dice_value == 6)
            coefficient = Config.COEFFICIENTS['bowling']['strike'] if win else 0.0
            result_text = f'🎳 Все кегли! СТРАЙК! 🎳' if win else f'🎳 Не страйк! (значение: {dice_value})'
            actual_choice = 'strike'
        else:
            return RealEmojiGame._calculate_bowling_result(dice_value)
        return {
            'win': win,
            'coefficient': coefficient,
            'result_text': result_text,
            'bot_choice': actual_choice
        }
    
    @staticmethod
    def _calculate_bowling_result(dice_value: int) -> Dict[str, Any]:
        if dice_value == 1:
            result_text = f'🎳 Мимо! (значение: {dice_value})'
            win = False
            coefficient = 0.0
            actual_choice = 'miss'
        elif 2 <= dice_value <= 5:
            coefficient_map = {2: 1.1, 3: 1.2, 4: 1.3, 5: 1.4}
            result_text = f'🎳 Выбито {dice_value} кеглей'
            win = True
            coefficient = coefficient_map.get(dice_value, 1.0)
            actual_choice = f'{dice_value}_pins'
        elif dice_value == 6:
            result_text = f'🎳 СТРАЙК! Все кегли! 🎳'
            win = True
            coefficient = Config.COEFFICIENTS['bowling']['strike']
            actual_choice = 'strike'
        else:
            result_text = f'🎳 Неизвестный результат: {dice_value}'
            win = False
            coefficient = 0.0
            actual_choice = 'miss'
        return {
            'win': win,
            'coefficient': coefficient,
            'result_text': result_text,
            'bot_choice': actual_choice
        }
    
    @staticmethod
    def _check_slots_result(dice_value: int, user_choice: str = None) -> Dict[str, Any]:
        slots_map = {
            1: {'symbol': 'BAR_BAR_BAR', 'name': 'BAR BAR BAR', 'emoji': '💰'},
            22: {'symbol': 'GRAPE_GRAPE_GRAPE', 'name': 'Три грозди винограда', 'emoji': '🍇'},
            43: {'symbol': 'LEMON_LEMON_LEMON', 'name': 'Три лимона', 'emoji': '🍋'},
            64: {'symbol': 'SEVEN_SEVEN_SEVEN', 'name': '777 Джекпот', 'emoji': '💰💰💰'}
        }
        if dice_value in slots_map:
            symbol_info = slots_map[dice_value]
            symbol = symbol_info['symbol']
        else:
            symbol_info = {'name': 'Проигрыш', 'emoji': '❌'}
            symbol = 'LOSS'
        if symbol == 'SEVEN_SEVEN_SEVEN':
            result_text = f'🎰 {symbol_info["emoji"]} ДЖЕКПОТ! 777! {symbol_info["emoji"]} 🎰'
            win = True
            coefficient = Config.COEFFICIENTS['slots']['777']
            actual_choice = '777'
        elif symbol in ['BAR_BAR_BAR', 'GRAPE_GRAPE_GRAPE', 'LEMON_LEMON_LEMON']:
            result_text = f'🎰 {symbol_info["emoji"]} {symbol_info["name"]} - Выигрыш! {symbol_info["emoji"]} 🎰'
            win = True
            coefficient = Config.COEFFICIENTS['slots']['triple']
            actual_choice = 'triple'
        else:
            result_text = f'🎰 {symbol_info["emoji"]} {symbol_info["name"]} (значение: {dice_value})'
            win = False
            coefficient = 0.0
            actual_choice = 'loss'
        return {
            'win': win,
            'coefficient': coefficient,
            'result_text': result_text,
            'bot_choice': actual_choice
        }
    
    @staticmethod
    def _check_rps_result(dice_value: int = None, user_choice: str = None) -> Dict[str, Any]:
        if user_choice is None:
            return {
                'win': False,
                'coefficient': 0.0,
                'result_text': 'Не выбрана ставка',
                'bot_choice': ''
            }
        choices = ['rock', 'scissors', 'paper']
        random_value = random.random()
        if random_value < 0.15:
            bot_choice = user_choice
            return {
                'win': False,
                'coefficient': 1.0,
                'result_text': f'🤝 Ничья! Бот выбрал: {RealEmojiGame._rps_to_text(bot_choice)}',
                'bot_choice': bot_choice
            }
        if random_value < 0.45:
            win_map = {'rock': 'scissors', 'scissors': 'paper', 'paper': 'rock'}
            bot_choice = win_map[user_choice]
            win = True
            coefficient = Config.COEFFICIENTS['rps'][user_choice]
            result_emoji = '👏'
        else:
            lose_map = {'rock': 'paper', 'scissors': 'rock', 'paper': 'scissors'}
            bot_choice = lose_map[user_choice]
            win = False
            coefficient = 0.0
            result_emoji = '❌'
        return {
            'win': win,
            'coefficient': coefficient,
            'result_text': f'{result_emoji} {"Победа" if win else "Проигрыш"}! Бот выбрал: {RealEmojiGame._rps_to_text(bot_choice)}',
            'bot_choice': bot_choice
        }
    
    @staticmethod
    def _check_kb_result(dice_value: int = None, user_choice: str = None) -> Dict[str, Any]:
        if user_choice is None:
            return {
                'win': False,
                'coefficient': 0.0,
                'result_text': 'Не выбрана ставка',
                'bot_choice': ''
            }
        bot_choice = random.choice(['red', 'white'])
        if user_choice == bot_choice:
            win = True
            coefficient = Config.COEFFICIENTS['kb'][user_choice]
            color_emoji = '❤️' if bot_choice == 'red' else '🤍'
            result_text = f'{color_emoji} {bot_choice.capitalize()} - Победа!'
        else:
            win = False
            coefficient = 0.0
            color_emoji = '❤️' if bot_choice == 'red' else '🤍'
            result_text = f'{color_emoji} {bot_choice.capitalize()} - Проигрыш'
        return {
            'win': win,
            'coefficient': coefficient,
            'result_text': result_text,
            'bot_choice': bot_choice
        }
    
    @staticmethod
    def _rps_to_text(choice: str) -> str:
        translations = {
            'rock': 'Камень',
            'scissors': 'Ножницы',
            'paper': 'Бумага'
        }
        return translations.get(choice, choice)

class UserStates(StatesGroup):
    waiting_deposit_amount = State()
    waiting_withdraw_amount = State()
    waiting_bet_amount = State()
    waiting_bet_source = State()
    waiting_promo_code = State()
    admin_waiting_check = State()

class AdminStates(StatesGroup):
    waiting_admin_check = State()

class WithdrawalRequest:
    def __init__(self, request_id: str, user_id: int, amount: float, status: str = "pending"):
        self.request_id = request_id
        self.user_id = user_id
        self.amount = amount
        self.status = status
        self.created_at = datetime.now()
        self.updated_at = datetime.now()

class CryptoReceiptParser:
    @staticmethod
    def parse_receipt_message(text: str) -> Optional[Dict[str, Any]]:
        try:
            text = ' '.join(text.split())
            logger.info(f"Parsing receipt message: {text[:200]}")
            patterns = [
                r'([\d\.]+)\s*USDT',
                r'[\$\€\£]?\s*([\d\.]+)\s*[\$\€\£]?',
                r'(\d+\.\d+)',
                r'(\d+)',
            ]
            for pattern in patterns:
                matches = re.findall(pattern, text)
                for match in matches:
                    if isinstance(match, tuple):
                        match = match[0]
                    try:
                        amount = float(match)
                        if 0.001 <= amount <= 100000:
                            logger.info(f"Amount parsed from receipt: {amount}")
                            currency = 'USDT'
                            if 'USDT' in text.upper():
                                currency = 'USDT'
                            elif '$' in text:
                                currency = 'USD'
                            elif '€' in text:
                                currency = 'EUR'
                            elif '£' in text:
                                currency = 'GBP'
                            return {
                                'amount': amount,
                                'currency': currency,
                                'raw_text': text
                            }
                    except ValueError:
                        continue
            logger.warning(f"No amount found in receipt message")
            return None
        except Exception as e:
            logger.error(f"Error parsing receipt message: {e}")
            return None
    
    @staticmethod
    def parse_check_info_from_user_message(text: str) -> Optional[Dict[str, Any]]:
        try:
            check_code = CryptoReceiptParser.extract_check_code(text)
            if not check_code:
                return None
            amount = 0.0
            amount_patterns = [
                r'([\d\.,]+)\s*USDT',
                r'[\$\€\£]?\s*([\d\.,]+)\s*[\$\€\£]?',
                r'(\d+\.\d+)',
                r'(\d+)',
            ]
            for pattern in amount_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, tuple):
                        match = match[0]
                    try:
                        amount_str = match.replace(',', '.')
                        parsed_amount = float(amount_str)
                        if 0.001 <= parsed_amount <= 100000:
                            amount = parsed_amount
                            logger.info(f"Amount parsed from text: {amount}")
                            break
                    except ValueError:
                        continue
                if amount > 0:
                    break
            currency = 'USDT'
            if 'USDT' in text.upper():
                currency = 'USDT'
            elif '$' in text:
                currency = 'USD'
            elif '€' in text:
                currency = 'EUR'
            elif '£' in text:
                currency = 'GBP'
            elif 'RUB' in text.upper() or '₽' in text or 'руб' in text.lower():
                currency = 'RUB'
            logger.info(f"Parsed check info: code={check_code}, amount={amount}, currency={currency}")
            return {
                'check_code': check_code,
                'estimated_amount': amount,
                'currency': currency,
                'raw_text': text[:200]
            }
        except Exception as e:
            logger.error(f"Error parsing check info: {e}")
            return None
    
    @staticmethod
    def extract_check_code(text: str) -> Optional[str]:
        try:
            text = text.strip()
            patterns = [
                r't\.me/CryptoBot\?start=([A-Za-z0-9_\-]+)',
                r'CryptoBot\?start=([A-Za-z0-9_\-]+)',
                r'/start\s+([A-Za-z0-9_\-]+)',
                r'([A-Za-z][A-Za-z0-9_\-]{9,})',
            ]
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    code = match.group(1)
                    if len(code) >= 10 and ' ' not in code:
                        logger.info(f"Extracted check code: {code} from pattern {pattern}")
                        return code
            return None
        except Exception as e:
            logger.error(f"Error extracting check code: {e}")
            return None
    
    @staticmethod
    def extract_all_check_codes(text: str, urls: List[str] = None) -> List[str]:
        check_codes = []
        patterns = [
            r't\.me/CryptoBot\?start=([A-Za-z0-9_\-]+)',
            r'CryptoBot\?start=([A-Za-z0-9_\-]+)',
            r'/start\s+([A-Za-z0-9_\-]+)',
            r'([A-Za-z][A-Za-z0-9_\-]{9,})',
        ]
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]
                if match and ' ' not in match and len(match) >= 10:
                    check_codes.append(match)
        if urls:
            for url in urls:
                url_codes = CryptoReceiptParser.extract_check_code(url)
                if url_codes:
                    check_codes.append(url_codes)
        unique_codes = list(set(check_codes))
        logger.info(f"Найдены коды чеков: {unique_codes}")
        return unique_codes

    @staticmethod
    def extract_check_code_from_url(url: str) -> Optional[str]:
        try:
            url = url.strip()
            patterns = [
                r't\.me/CryptoBot\?start=([A-Za-z0-9_\-]+)',
                r'https?://t\.me/CryptoBot/\?start=([A-Za-z0-9_\-]+)',
                r'cryptobot\?start=([A-Za-z0-9_\-]+)',
                r'start=([A-Za-z0-9_\-]+)',
            ]
            for pattern in patterns:
                match = re.search(pattern, url, re.IGNORECASE)
                if match:
                    code = match.group(1)
                    if len(code) >= 10:
                        logger.info(f"Extracted check code from URL: {code}")
                        return code
            return None
        except Exception as e:
            logger.error(f"Error extracting check code from URL: {e}")
            return None

class TelethonCryptoBot:
    def __init__(self, api_id: int, api_hash: str, phone: str):
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self.client = None
        self.is_connected = False
        self.check_responses = {}
        self.last_command = None
        self.message_count = 0
    
    async def connect(self):
        try:
            logger.info(f"Connecting Telethon with phone: {self.phone}")
            self.client = TelegramClient(
                f'session_{self.phone}',
                self.api_id,
                self.api_hash,
                connection_retries=3,
                timeout=10
            )
            await self.client.start(phone=self.phone)
            self.is_connected = True
            
            @self.client.on(events.NewMessage(from_users=Config.CRYPTO_BOT_ID))
            async def crypto_bot_handler(event):
                self.message_count += 1
                await self.handle_crypto_bot_message(event)
            
            logger.info("Telethon connected successfully")
            return True
        except SessionPasswordNeededError:
            logger.error("Two-factor authentication required!")
            return False
        except PhoneNumberInvalidError:
            logger.error("Invalid phone number.")
            return False
        except FloodWaitError as e:
            logger.error(f"Flood wait: {e.seconds} seconds.")
            return False
        except Exception as e:
            logger.error(f"Error connecting Telethon: {e}")
            return False
    
    async def disconnect(self):
        if self.client and self.is_connected:
            try:
                await self.client.disconnect()
                self.is_connected = False
                logger.info("Telethon disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting Telethon: {e}")
    
    async def handle_crypto_bot_message(self, event):
        try:
            message_text = event.raw_text
            message_id = self.message_count
            logger.info(f"[{message_id}] CryptoBot: {message_text[:200]}")
            check_code = None
            code_pattern = r'([A-Za-z0-9_\-]{10,})'
            matches = re.findall(code_pattern, message_text)
            for match in matches:
                if len(match) >= 10 and not match.isdigit():
                    if re.match(r'^[A-Za-z]', match) and '_' not in match:
                        check_code = match
                        logger.info(f"[{message_id}] Found check code in message: {check_code}")
                        break
            if not check_code and self.last_command:
                last_match = re.search(r'/start\s+([A-Za-z0-9_\-]+)', self.last_command)
                if last_match:
                    check_code = last_match.group(1)
                    logger.info(f"[{message_id}] Using check code from last command: {check_code}")
            check_keywords = ['чек', 'check', 'received', 'получил', 'активировал', 'activated', 
                             'получен', 'зачислен', 'чек', 'чек', 'чека', 'amount', 'сумма']
            message_lower = message_text.lower()
            is_check_related = any(keyword in message_lower for keyword in check_keywords)
            if is_check_related:
                logger.info(f"[{message_id}] Check-related message detected")
                parser = CryptoReceiptParser()
                receipt_info = parser.parse_receipt_message(message_text)
                if receipt_info:
                    amount = receipt_info['amount']
                    currency = receipt_info.get('currency', 'USDT')
                    logger.info(f"[{message_id}] Amount parsed: {amount} {currency}")
                    if check_code:
                        self.check_responses[check_code] = {
                            'amount': amount,
                            'currency': currency,
                            'message': message_text,
                            'timestamp': datetime.now(),
                            'message_id': message_id
                        }
                        logger.info(f"[{message_id}] Response saved for check {check_code}")
                    else:
                        temp_key = f"unknown_{message_id}"
                        self.check_responses[temp_key] = {
                            'amount': amount,
                            'currency': currency,
                            'message': message_text,
                            'timestamp': datetime.now(),
                            'message_id': message_id,
                            'raw_text': message_text
                        }
                        logger.info(f"[{message_id}] Response saved without check code: {amount} {currency}")
                    return {
                        'check_code': check_code,
                        'amount': amount,
                        'currency': currency,
                        'message': message_text,
                        'success': True
                    }
                else:
                    logger.warning(f"[{message_id}] No amount found in check message")
            return None
        except Exception as e:
            logger.error(f"[{message_id}] Error handling CryptoBot message: {e}")
            return None
    
    async def activate_check_and_get_amount(self, check_code: str, timeout: int = 15) -> Dict[str, Any]:
        try:
            if not self.is_connected or not self.client:
                logger.error("Telethon not connected")
                return {'success': False, 'error': 'Telethon not connected'}
            self.last_command = f"/start {check_code}"
            logger.info(f"Sending to CryptoBot: {self.last_command}")
            if check_code in self.check_responses:
                del self.check_responses[check_code]
            await self.client.send_message(
                Config.CRYPTO_BOT_ID,
                self.last_command
            )
            start_time = datetime.now()
            max_wait = timeout
            logger.info(f"Waiting for response to check {check_code}...")
            while (datetime.now() - start_time).seconds < max_wait:
                await asyncio.sleep(1)
                if check_code in self.check_responses:
                    response = self.check_responses[check_code]
                    logger.info(f"Found direct response for check {check_code}: {response['amount']} {response.get('currency')}")
                    del self.check_responses[check_code]
                    return {
                        'success': True,
                        'check_code': check_code,
                        'amount': response['amount'],
                        'currency': response.get('currency', 'USDT'),
                        'message': response.get('message', ''),
                        'response_type': 'direct'
                    }
                now = datetime.now()
                for key, response in list(self.check_responses.items()):
                    if 'timestamp' in response:
                        age = (now - response['timestamp']).seconds
                        if age < 10:
                            amount = response.get('amount')
                            if amount and amount > 0:
                                logger.info(f"Found recent response {key}: {amount} {response.get('currency')}")
                                del self.check_responses[key]
                                return {
                                    'success': True,
                                    'check_code': check_code,
                                    'amount': amount,
                                    'currency': response.get('currency', 'USDT'),
                                    'message': response.get('message', ''),
                                    'response_type': 'recent',
                                    'original_key': key
                                }
            logger.warning(f"No response for check {check_code} after {max_wait} seconds")
            if self.message_count == 0:
                return {
                    'success': False,
                    'error': 'No messages received from CryptoBot',
                    'check_code': check_code
                }
            recent_responses = self.get_recent_responses(max_age_seconds=30)
            if recent_responses:
                last_response = recent_responses[0]
                amount = last_response.get('amount')
                if amount and amount > 0:
                    logger.info(f"Using last recent response: {amount}")
                    return {
                        'success': True,
                        'check_code': check_code,
                        'amount': amount,
                        'currency': last_response.get('currency', 'USDT'),
                        'message': last_response.get('message', ''),
                        'response_type': 'fallback',
                        'note': 'Using most recent response'
                    }
            return {
                'success': False,
                'error': 'No response from CryptoBot',
                'check_code': check_code,
                'note': f'Sent command but no valid response. Message count: {self.message_count}'
            }
        except FloodWaitError as e:
            logger.error(f"Flood wait during check activation: {e.seconds} seconds")
            return {
                'success': False,
                'error': f'Flood wait: {e.seconds} seconds',
                'check_code': check_code
            }
        except Exception as e:
            logger.error(f"Error activating check: {e}")
            return {
                'success': False,
                'error': str(e),
                'check_code': check_code
            }
    
    def get_recent_responses(self, max_age_seconds: int = 60) -> List[Dict]:
        recent = []
        now = datetime.now()
        for key, response in self.check_responses.items():
            if isinstance(response, dict) and 'timestamp' in response:
                age = (now - response['timestamp']).seconds
                if age < max_age_seconds:
                    recent.append({
                        'key': key,
                        'check_code': key if not key.startswith('unknown_') else None,
                        'amount': response.get('amount'),
                        'currency': response.get('currency', 'USDT'),
                        'message': response.get('message'),
                        'age_seconds': age,
                        'message_id': response.get('message_id')
                    })
        return sorted(recent, key=lambda x: x.get('age_seconds', 999))
    
    def clear_old_responses(self, max_age_seconds: int = 300):
        now = datetime.now()
        to_delete = []
        for key, response in self.check_responses.items():
            if isinstance(response, dict) and 'timestamp' in response:
                age = (now - response['timestamp']).seconds
                if age > max_age_seconds:
                    to_delete.append(key)
        for key in to_delete:
            del self.check_responses[key]
        if to_delete:
            logger.info(f"Cleared {len(to_delete)} old responses")

class CryptoPayAPI:
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.base_url = "https://pay.crypt.bot/api"
        self.timeout = ClientTimeout(total=15)
        self.session: Optional[ClientSession] = None
        
    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            headers = {
                "Crypto-Pay-API-Token": self.api_token,
                "Content-Type": "application/json"
            }
            self.session = aiohttp.ClientSession(headers=headers, timeout=self.timeout)
    
    async def _make_request(self, method: str, endpoint: str, **kwargs):
        try:
            await self._ensure_session()
            url = f"{self.base_url}/{endpoint}"
            async with self.session.request(method, url, **kwargs) as response:
                response_text = await response.text()
                if response.status == 200:
                    result = json.loads(response_text)
                    if result.get('ok'):
                        return result.get('result')
                    else:
                        error = result.get('error', {}).get('name', 'Unknown error')
                        logger.error(f"CryptoPay API error: {error}")
                        return None
                else:
                    logger.error(f"CryptoPay API HTTP error {response.status}: {response_text}")
                    return None
        except asyncio.TimeoutError:
            logger.error(f"CryptoPay API timeout for {endpoint}")
            return None
        except Exception as e:
            logger.error(f"CryptoPay API connection error for {endpoint}: {e}")
            return None
        finally:
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
    
    async def create_invoice(self, asset: str, amount: float, description: str = ""):
        data = {
            "asset": asset,
            "amount": str(amount),
            "expires_in": 3600,
            "paid_btn_name": "openBot",
            "paid_btn_url": "https://t.me/CryptoBot",
            "allow_comments": False,
            "allow_anonymous": True
        }
        if description:
            data["description"] = description
        logger.info(f"Creating invoice: {asset} {amount}")
        result = await self._make_request("POST", "createInvoice", json=data)
        if result:
            return {
                'pay_url': result.get('pay_url', ''),
                'invoice_id': str(result.get('invoice_id', '')),
                'amount': amount,
                'asset': asset
            }
        return None
    
    async def get_invoice(self, invoice_id: str):
        params = {"invoice_ids": invoice_id}
        try:
            logger.info(f"Getting invoice status: {invoice_id}")
            result = await self._make_request("GET", "getInvoices", params=params)
            if result and 'items' in result and len(result['items']) > 0:
                invoice = result['items'][0]
                logger.info(f"Invoice {invoice_id} status: {invoice.get('status')}")
                return invoice
            else:
                logger.warning(f"Invoice {invoice_id} not found in API response")
                return None
        except Exception as e:
            logger.error(f"Error getting invoice {invoice_id}: {e}")
            return None
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None

class SQLiteDatabase:
    def __init__(self, db_path: str = Config.DB_PATH):
        self.db_path = db_path
        self._init_db()
        self._create_backup_dir()

    def _create_backup_dir(self):
        Path(Config.DB_BACKUP_PATH).mkdir(exist_ok=True, parents=True)

    def _init_db(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA foreign_keys=ON")
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        telegram_id INTEGER UNIQUE NOT NULL,
                        username TEXT,
                        first_name TEXT,
                        last_name TEXT,
                        balance REAL DEFAULT 0.0 CHECK(balance >= 0),
                        bonus_balance REAL DEFAULT 0.0 CHECK(bonus_balance >= 0),
                        turnover REAL DEFAULT 0.0 CHECK(turnover >= 0),
                        bonus_turnover REAL DEFAULT 0.0 CHECK(bonus_turnover >= 0),
                        total_deposits REAL DEFAULT 0.0 CHECK(total_deposits >= 0),
                        total_withdrawals REAL DEFAULT 0.0 CHECK(total_withdrawals >= 0),
                        games_played INTEGER DEFAULT 0,
                        games_won INTEGER DEFAULT 0,
                        status TEXT DEFAULT 'active' CHECK(status IN ('active', 'banned', 'suspended')),
                        is_admin BOOLEAN DEFAULT 0,
                        has_deposited BOOLEAN DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_deposit_at TIMESTAMP,
                        last_withdrawal_at TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS transactions (
                        id TEXT PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        type TEXT NOT NULL CHECK(type IN ('deposit', 'withdrawal', 'bet', 'win', 'refund', 'bonus', 'withdrawal_reserve', 'bonus_conversion', 'withdrawal_request')),
                        amount REAL NOT NULL,
                        before_balance REAL NOT NULL,
                        after_balance REAL NOT NULL,
                        is_bonus BOOLEAN DEFAULT 0,
                        description TEXT,
                        metadata TEXT DEFAULT '{}',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS withdrawals (
                        id TEXT PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        amount REAL NOT NULL CHECK(amount > 0),
                        status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'approved', 'completed', 'rejected')),
                        admin_id INTEGER,
                        admin_message TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        approved_at TIMESTAMP,
                        completed_at TIMESTAMP,
                        rejected_at TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS bonuses (
                        id TEXT PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        amount REAL NOT NULL CHECK(amount > 0),
                        wagering_requirement REAL DEFAULT 10.0 CHECK(wagering_requirement > 0),
                        required_turnover REAL NOT NULL CHECK(required_turnover > 0),
                        current_turnover REAL DEFAULT 0.0 CHECK(current_turnover >= 0),
                        status TEXT DEFAULT 'active' CHECK(status IN ('active', 'completed', 'expired', 'cancelled')),
                        is_converted BOOLEAN DEFAULT 0,
                        converted_amount REAL DEFAULT 0.0,
                        expiry_date TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP,
                        converted_at TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS promo_codes (
                        id TEXT PRIMARY KEY,
                        code TEXT UNIQUE NOT NULL,
                        bonus_amount REAL NOT NULL CHECK(bonus_amount > 0),
                        wagering_requirement REAL DEFAULT 10.0 CHECK(wagering_requirement > 0),
                        max_uses INTEGER DEFAULT 1,
                        current_uses INTEGER DEFAULT 0,
                        expiry_date TIMESTAMP,
                        is_active BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS used_promo_codes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        promo_code_id TEXT REFERENCES promo_codes(id) ON DELETE CASCADE,
                        used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_id, promo_code_id)
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS crypto_invoices (
                        id TEXT PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        asset TEXT NOT NULL,
                        amount REAL NOT NULL CHECK(amount > 0),
                        amount_rub REAL NOT NULL CHECK(amount_rub > 0),
                        pay_url TEXT NOT NULL,
                        status TEXT DEFAULT 'active' CHECK(status IN ('active', 'paid', 'expired', 'cancelled')),
                        paid_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS game_sessions (
                        id TEXT PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        game_type TEXT NOT NULL,
                        bet_amount REAL NOT NULL CHECK(bet_amount >= 0),
                        is_bonus_bet BOOLEAN DEFAULT 0,
                        user_choice TEXT,
                        bot_choice TEXT,
                        result TEXT CHECK(result IN ('win', 'loss', 'draw')),
                        win_amount REAL DEFAULT 0.0 CHECK(win_amount >= 0),
                        coefficient REAL DEFAULT 1.0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS crypto_checks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        check_code TEXT UNIQUE NOT NULL,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        estimated_amount REAL DEFAULT 0.0 CHECK(estimated_amount >= 0),
                        real_amount REAL DEFAULT 0.0 CHECK(real_amount >= 0),
                        currency TEXT DEFAULT 'USDT',
                        status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'activated', 'failed', 'credited')),
                        activation_result TEXT,
                        activation_error TEXT,
                        credited_amount REAL DEFAULT 0.0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        activated_at TIMESTAMP,
                        credited_at TIMESTAMP
                    )
                """)
                
                conn.execute("CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_users_has_deposited ON users(has_deposited)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_bonuses_user_id ON bonuses(user_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_bonuses_status ON bonuses(status)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_bonuses_expiry ON bonuses(expiry_date)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_promo_codes_code ON promo_codes(code)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_promo_codes_active ON promo_codes(is_active)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_used_promo_user ON used_promo_codes(user_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_invoices_user_id ON crypto_invoices(user_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_invoices_status ON crypto_invoices(status)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON game_sessions(user_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_game_type ON game_sessions(game_type)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_bonus ON game_sessions(is_bonus_bet)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_checks_code ON crypto_checks(check_code)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_checks_user ON crypto_checks(user_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_checks_status ON crypto_checks(status)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_withdrawals_user_id ON withdrawals(user_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_withdrawals_status ON withdrawals(status)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_withdrawals_created ON withdrawals(created_at)")
                
                conn.commit()
                logger.info("Database initialized successfully with withdrawal system")
                
        except sqlite3.Error as e:
            logger.error(f"Error initializing database: {e}")
            raise

    def ban_user(self, user_id: int, reason: str = "") -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE users 
                    SET status = 'banned' 
                    WHERE telegram_id = ?
                """, (user_id,))
                conn.commit()
                logger.info(f"User {user_id} banned. Reason: {reason}")
                return True
        except Exception as e:
            logger.error(f"Error banning user: {e}")
            return False

    def unban_user(self, user_id: int) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE users 
                    SET status = 'active' 
                    WHERE telegram_id = ?
                """, (user_id,))
                conn.commit()
                logger.info(f"User {user_id} unbanned")
                return True
        except Exception as e:
            logger.error(f"Error unbanning user: {e}")
            return False

    def is_user_banned(self, user_id: int) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT status FROM users 
                    WHERE telegram_id = ?
                """, (user_id,))
                user = cursor.fetchone()
                return user and user['status'] == 'banned'
        except Exception as e:
            logger.error(f"Error checking user ban status: {e}")
            return False
            
    def get_connection(self):
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys=ON")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def create_bonus(self, user_id: int, amount: float, wagering_requirement: float = None) -> Optional[str]:
        try:
            if wagering_requirement is None:
                wagering_requirement = Config.BONUS_WAGERING_REQUIREMENT
            required_turnover = amount * wagering_requirement
            expiry_date = datetime.now() + timedelta(days=Config.BONUS_EXPIRY_DAYS)
            bonus_id = str(uuid.uuid4())
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO bonuses 
                    (id, user_id, amount, wagering_requirement, required_turnover, expiry_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (bonus_id, user_id, amount, wagering_requirement, required_turnover, expiry_date))
                cursor.execute("""
                    UPDATE users 
                    SET bonus_balance = bonus_balance + ?
                    WHERE id = ?
                """, (amount, user_id))
                transaction_id = str(uuid.uuid4())
                cursor.execute("""
                    SELECT bonus_balance FROM users WHERE id = ?
                """, (user_id,))
                user_data = cursor.fetchone()
                bonus_balance_before = user_data['bonus_balance'] - amount if user_data else 0
                bonus_balance_after = bonus_balance_before + amount
                cursor.execute("""
                    INSERT INTO transactions 
                    (id, user_id, type, amount, before_balance, after_balance, is_bonus, description)
                    VALUES (?, ?, 'bonus', ?, ?, ?, 1, ?)
                """, (
                    transaction_id,
                    user_id,
                    amount,
                    bonus_balance_before,
                    bonus_balance_after,
                    f"Начисление бонуса {CurrencyConverter.format_currency(amount)}"
                ))
                conn.commit()
                logger.info(f"Bonus created: {bonus_id}, user: {user_id}, amount: {amount}, wager: x{wagering_requirement}")
                return bonus_id
        except Exception as e:
            logger.error(f"Error creating bonus: {e}")
            return None
    
    def update_bonus_turnover(self, user_id: int, bet_amount: float, is_bonus_bet: bool) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM bonuses 
                    WHERE user_id = ? AND status = 'active'
                    ORDER BY created_at
                """, (user_id,))
                bonuses = cursor.fetchall()
                if not bonuses:
                    if is_bonus_bet:
                        cursor.execute("""
                            UPDATE users 
                            SET bonus_turnover = bonus_turnover + ?
                            WHERE id = ?
                        """, (bet_amount, user_id))
                    else:
                        cursor.execute("""
                            UPDATE users 
                            SET turnover = turnover + ?
                            WHERE id = ?
                        """, (bet_amount, user_id))
                    conn.commit()
                    return True
                remaining_amount = bet_amount
                for bonus in bonuses:
                    if remaining_amount <= 0:
                        break
                    bonus_id = bonus['id']
                    current_turnover = bonus['current_turnover'] or 0
                    required_turnover = bonus['required_turnover']
                    needed_for_this_bonus = required_turnover - current_turnover
                    if needed_for_this_bonus > 0:
                        amount_for_this_bonus = min(remaining_amount, needed_for_this_bonus)
                        new_turnover = current_turnover + amount_for_this_bonus
                        cursor.execute("""
                            UPDATE bonuses 
                            SET current_turnover = ?
                            WHERE id = ?
                        """, (new_turnover, bonus_id))
                        if new_turnover >= required_turnover:
                            cursor.execute("""
                                UPDATE bonuses 
                                SET status = 'completed', completed_at = CURRENT_TIMESTAMP
                                WHERE id = ?
                            """, (bonus_id,))
                            logger.info(f"Bonus {bonus_id} completed")
                        remaining_amount -= amount_for_this_bonus
                if is_bonus_bet:
                    cursor.execute("""
                        UPDATE users 
                        SET bonus_turnover = bonus_turnover + ?
                        WHERE id = ?
                    """, (bet_amount, user_id))
                else:
                    cursor.execute("""
                        UPDATE users 
                        SET turnover = turnover + ?
                        WHERE id = ?
                    """, (bet_amount, user_id))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error updating bonus turnover: {e}")
            return False
    
    def convert_completed_bonuses(self, user_id: int) -> Dict[str, Any]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("BEGIN TRANSACTION")
                cursor.execute("""
                    SELECT * FROM bonuses 
                    WHERE user_id = ? AND status = 'completed' AND is_converted = 0
                """, (user_id,))
                completed_bonuses = cursor.fetchall()
                if not completed_bonuses:
                    cursor.execute("ROLLBACK")
                    return {'success': False, 'message': 'Нет выполненных бонусов для конвертации'}
                total_converted = 0.0
                converted_bonuses = []
                for bonus in completed_bonuses:
                    bonus_id = bonus['id']
                    bonus_amount = bonus['amount']
                    cursor.execute("""
                        UPDATE users 
                        SET bonus_balance = bonus_balance - ?,
                            balance = balance + ?
                        WHERE id = ?
                    """, (bonus_amount, bonus_amount, user_id))
                    cursor.execute("""
                        UPDATE bonuses 
                        SET is_converted = 1, converted_amount = ?, converted_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (bonus_amount, bonus_id))
                    transaction_id = str(uuid.uuid4())
                    cursor.execute("""
                        SELECT balance FROM users WHERE id = ?
                    """, (user_id,))
                    user_data = cursor.fetchone()
                    balance_before = user_data['balance'] - bonus_amount if user_data else 0
                    balance_after = balance_before + bonus_amount
                    cursor.execute("""
                        INSERT INTO transactions 
                        (id, user_id, type, amount, before_balance, after_balance, is_bonus, description)
                        VALUES (?, ?, 'bonus_conversion', ?, ?, ?, 0, ?)
                    """, (
                        transaction_id,
                        user_id,
                        bonus_amount,
                        balance_before,
                        balance_after,
                        f"Конвертация бонуса {CurrencyConverter.format_currency(bonus_amount)}"
                    ))
                    total_converted += bonus_amount
                    converted_bonuses.append(bonus_id)
                conn.commit()
                logger.info(f"Converted {len(converted_bonuses)} bonuses for user {user_id}: {total_converted} RUB")
                return {
                    'success': True,
                    'converted_amount': total_converted,
                    'converted_bonuses': converted_bonuses,
                    'message': f'Конвертировано {CurrencyConverter.format_currency(total_converted)}'
                }
        except Exception as e:
            logger.error(f"Error converting bonuses: {e}")
            if 'conn' in locals():
                try:
                    conn.rollback()
                except:
                    pass
            return {'success': False, 'message': f'Ошибка конвертации: {str(e)}'}
    
    def get_active_bonus(self, user_id: int) -> Optional[Dict]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM bonuses 
                    WHERE user_id = ? AND status = 'active'
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (user_id,))
                bonus = cursor.fetchone()
                return dict(bonus) if bonus else None
        except Exception as e:
            logger.error(f"Error getting active bonus: {e}")
            return None
    
    def get_user_bonuses(self, user_id: int, limit: int = 10) -> List[Dict]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM bonuses 
                    WHERE user_id = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (user_id, limit))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting user bonuses: {e}")
            return []
    
    def check_and_expire_bonuses(self):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("BEGIN TRANSACTION")
                cursor.execute("""
                    SELECT b.*, u.telegram_id, u.username 
                    FROM bonuses b
                    JOIN users u ON b.user_id = u.id
                    WHERE b.status = 'active' 
                    AND b.expiry_date < CURRENT_TIMESTAMP
                """)
                expired_bonuses = cursor.fetchall()
                expired_count = 0
                for bonus in expired_bonuses:
                    bonus_id = bonus['id']
                    user_id = bonus['user_id']
                    amount = bonus['amount']
                    cursor.execute("""
                        UPDATE users 
                        SET bonus_balance = bonus_balance - ?
                        WHERE id = ?
                    """, (amount, user_id))
                    cursor.execute("""
                        UPDATE bonuses 
                        SET status = 'expired'
                        WHERE id = ?
                    """, (bonus_id,))
                    expired_count += 1
                conn.commit()
                if expired_count > 0:
                    logger.info(f"Expired {expired_count} bonuses")
                return expired_count
        except Exception as e:
            logger.error(f"Error expiring bonuses: {e}")
            if 'conn' in locals():
                try:
                    conn.rollback()
                except:
                    pass
            return 0
    
    def create_promo_code(self, code: str, bonus_amount: float, wagering_requirement: float = None,
                        max_uses: int = 1, expiry_days: int = 30) -> bool:
        try:
            if wagering_requirement is None:
                wagering_requirement = Config.BONUS_WAGERING_REQUIREMENT
            expiry_date = datetime.now() + timedelta(days=expiry_days) if expiry_days > 0 else None
            promo_id = str(uuid.uuid4())
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT INTO promo_codes 
                    (id, code, bonus_amount, wagering_requirement, max_uses, expiry_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (promo_id, code, bonus_amount, wagering_requirement, max_uses, expiry_date))
                conn.commit()
                logger.info(f"Promo code created: {code}, amount: {bonus_amount}, uses: {max_uses}")
                return True
        except sqlite3.IntegrityError:
            logger.warning(f"Promo code {code} already exists")
            return False
        except Exception as e:
            logger.error(f"Error creating promo code: {e}")
            return False
    
    def use_promo_code(self, user_id: int, promo_code: str) -> Dict[str, Any]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("BEGIN TRANSACTION")
                cursor.execute("""
                    SELECT * FROM promo_codes 
                    WHERE code = ? AND is_active = 1
                """, (promo_code,))
                promo = cursor.fetchone()
                if not promo:
                    cursor.execute("ROLLBACK")
                    return {'success': False, 'message': 'Промокод не найден или не активен'}
                promo_id = promo['id']
                expiry_date = promo['expiry_date']
                max_uses = promo['max_uses']
                current_uses = promo['current_uses']
                if expiry_date:
                    try:
                        expiry_dt = datetime.strptime(expiry_date, '%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        try:
                            expiry_dt = datetime.strptime(expiry_date, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            cursor.execute("ROLLBACK")
                            return {'success': False, 'message': 'Неверный формат даты промокода'}
                    if expiry_dt < datetime.now():
                        cursor.execute("ROLLBACK")
                        return {'success': False, 'message': 'Промокод истек'}
                if current_uses >= max_uses:
                    cursor.execute("ROLLBACK")
                    return {'success': False, 'message': 'Промокод уже использован максимальное количество раз'}
                cursor.execute("""
                    SELECT * FROM used_promo_codes 
                    WHERE user_id = ? AND promo_code_id = ?
                """, (user_id, promo_id))
                if cursor.fetchone():
                    cursor.execute("ROLLBACK")
                    return {'success': False, 'message': 'Вы уже использовали этот промокод'}
                bonus_id = self.create_bonus(
                    user_id=user_id,
                    amount=promo['bonus_amount'],
                    wagering_requirement=promo['wagering_requirement']
                )
                if not bonus_id:
                    cursor.execute("ROLLBACK")
                    return {'success': False, 'message': 'Ошибка при создании бонуса'}
                cursor.execute("""
                    UPDATE promo_codes 
                    SET current_uses = current_uses + 1
                    WHERE id = ?
                """, (promo_id,))
                cursor.execute("""
                    INSERT INTO used_promo_codes (user_id, promo_code_id)
                    VALUES (?, ?)
                """, (user_id, promo_id))
                conn.commit()
                logger.info(f"Promo code {promo_code} used by user {user_id}, bonus created: {bonus_id}")
                return {
                    'success': True,
                    'bonus_id': bonus_id,
                    'amount': promo['bonus_amount'],
                    'wagering_requirement': promo['wagering_requirement']
                }
        except Exception as e:
            logger.error(f"Error using promo code: {e}", exc_info=True)
            if 'conn' in locals():
                try:
                    conn.rollback()
                except:
                    pass
            return {'success': False, 'message': f'Ошибка при активации промокода: {str(e)}'}
    
    def get_promo_code_info(self, promo_code: str) -> Optional[Dict]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM promo_codes 
                    WHERE code = ?
                """, (promo_code,))
                promo = cursor.fetchone()
                return dict(promo) if promo else None
        except Exception as e:
            logger.error(f"Error getting promo code info: {e}")
            return None
    
    def get_or_create_user(self, telegram_id: int, username: str = "", first_name: str = "", last_name: str = "") -> Dict:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM users WHERE telegram_id = ?",
                    (telegram_id,)
                )
                user = cursor.fetchone()
                if user:
                    cursor.execute(
                        """UPDATE users SET 
                           last_active = CURRENT_TIMESTAMP,
                           username = COALESCE(NULLIF(?, ''), username),
                           first_name = COALESCE(NULLIF(?, ''), first_name),
                           last_name = COALESCE(NULLIF(?, ''), last_name)
                           WHERE telegram_id = ?""",
                        (username, first_name, last_name, telegram_id)
                    )
                    conn.commit()
                    return dict(user)
                else:
                    cursor.execute("""
                        INSERT INTO users (telegram_id, username, first_name, last_name) 
                        VALUES (?, ?, ?, ?)
                    """, (telegram_id, username or "", first_name or "", last_name or ""))
                    user_id = cursor.lastrowid
                    conn.commit()
                    cursor.execute(
                        "SELECT * FROM users WHERE id = ?",
                        (user_id,)
                    )
                    new_user = cursor.fetchone()
                    return dict(new_user) if new_user else {}
        except sqlite3.Error as e:
            logger.error(f"Error in get_or_create_user: {e}")
            return {}
    
    def update_user_balance(self, user_id: int, amount: float, 
                          transaction_type: str, description: str = "",
                          metadata: Dict = None, is_bonus: bool = False) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("BEGIN TRANSACTION")
                
                cursor.execute("""
                    SELECT balance, bonus_balance FROM users 
                    WHERE id = ?
                """, (user_id,))
                
                user = cursor.fetchone()
                if not user:
                    cursor.execute("ROLLBACK")
                    logger.warning(f"User {user_id} not found")
                    return False
                
                old_balance = user['balance'] or 0.0
                old_bonus_balance = user['bonus_balance'] or 0.0
                
                if is_bonus:
                    new_bonus_balance = old_bonus_balance + amount
                    
                    if new_bonus_balance < 0:
                        cursor.execute("ROLLBACK")
                        logger.warning(f"Insufficient bonus balance for user {user_id}: {old_bonus_balance}, trying to subtract {amount}")
                        return False
                    
                    cursor.execute(
                        "UPDATE users SET bonus_balance = ? WHERE id = ?",
                        (new_bonus_balance, user_id)
                    )
                    
                    before_balance = old_bonus_balance
                    after_balance = new_bonus_balance
                    
                else:
                    new_balance = old_balance + amount
                    
                    if new_balance < 0 and transaction_type not in ['withdrawal_reserve', 'admin_correction']:
                        cursor.execute("ROLLBACK")
                        logger.warning(f"Insufficient balance for user {user_id}: {old_balance}, trying to subtract {amount}")
                        return False
                    
                    cursor.execute(
                        "UPDATE users SET balance = ? WHERE id = ?",
                        (new_balance, user_id)
                    )
                    
                    before_balance = old_balance
                    after_balance = new_balance
                
                if transaction_type == 'deposit' and not is_bonus:
                    cursor.execute(
                        """UPDATE users SET 
                           total_deposits = total_deposits + ?, 
                           last_deposit_at = CURRENT_TIMESTAMP,
                           has_deposited = 1
                           WHERE id = ?""",
                        (amount if amount > 0 else 0, user_id)
                    )
                elif transaction_type == 'withdrawal' and not is_bonus:
                    cursor.execute(
                        "UPDATE users SET total_withdrawals = total_withdrawals + ?, last_withdrawal_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (abs(amount), user_id)
                    )
                elif transaction_type == 'bet':
                    pass
                elif transaction_type == 'win' and not is_bonus:
                    cursor.execute(
                        "UPDATE users SET games_won = games_won + 1 WHERE id = ?",
                        (user_id,)
                    )
                
                transaction_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO transactions 
                    (id, user_id, type, amount, before_balance, after_balance, is_bonus, description, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    transaction_id, user_id, transaction_type, 
                    abs(amount), before_balance, after_balance, 
                    1 if is_bonus else 0,
                    description or f"Transaction type: {transaction_type}",
                    json.dumps(metadata or {})
                ))
                
                conn.commit()
                logger.info(f"Balance updated: user={user_id}, type={transaction_type}, amount={amount:.2f}, is_bonus={is_bonus}, new_balance={after_balance:.2f}")
                return True
                
        except sqlite3.Error as e:
            logger.error(f"Error updating balance: {e}")
            if 'conn' in locals():
                try:
                    conn.rollback()
                except:
                    pass
            return False
    
    def mark_user_has_deposited(self, user_id: int) -> bool:
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    UPDATE users 
                    SET has_deposited = 1
                    WHERE id = ?
                """, (user_id,))
                conn.commit()
                logger.info(f"User {user_id} marked as having deposited")
                return True
        except Exception as e:
            logger.error(f"Error marking user as deposited: {e}")
            return False
    
    def create_game_session(self, session_id: str, user_id: int, game_type: str,
                          bet_amount: float, user_choice: str = None, is_bonus_bet: bool = False) -> bool:
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT INTO game_sessions 
                    (id, user_id, game_type, bet_amount, user_choice, is_bonus_bet)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (session_id, user_id, game_type, bet_amount, user_choice, 1 if is_bonus_bet else 0))
                conn.commit()
                logger.info(f"Game session created: {session_id}, is_bonus={is_bonus_bet}")
                return True
        except sqlite3.Error as e:
            logger.error(f"Error creating game session: {e}")
            return False
    
    def update_game_result(self, session_id: str, bot_choice: str, 
                         result: str, win_amount: float, coefficient: float = 1.0):
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    UPDATE game_sessions 
                    SET bot_choice = ?, result = ?, win_amount = ?, coefficient = ?
                    WHERE id = ?
                """, (bot_choice, result, win_amount, coefficient, session_id))
                conn.commit()
                logger.info(f"Game result updated: {session_id}, result={result}, win={win_amount:.2f}")
        except sqlite3.Error as e:
            logger.error(f"Error updating game result: {e}")
    
    def create_crypto_invoice(self, invoice_id: str, user_id: int, asset: str, 
                            amount: float, amount_rub: float, pay_url: str):
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT INTO crypto_invoices 
                    (id, user_id, asset, amount, amount_rub, pay_url, status)
                    VALUES (?, ?, ?, ?, ?, ?, 'active')
                """, (invoice_id, user_id, asset, amount, amount_rub, pay_url))
                conn.commit()
                logger.info(f"Invoice created: {invoice_id} for user {user_id}")
        except sqlite3.Error as e:
            logger.error(f"Error creating invoice: {e}")
    
    def update_invoice_status(self, invoice_id: str, status: str):
        try:
            with self.get_connection() as conn:
                if status == 'paid':
                    conn.execute("""
                        UPDATE crypto_invoices 
                        SET status = ?, paid_at = CURRENT_TIMESTAMP 
                        WHERE id = ?
                    """, (status, invoice_id))
                else:
                    conn.execute("""
                        UPDATE crypto_invoices SET status = ? WHERE id = ?
                    """, (status, invoice_id))
                conn.commit()
                logger.info(f"Invoice {invoice_id} status updated to {status}")
        except sqlite3.Error as e:
            logger.error(f"Error updating invoice status: {e}")
    
    def get_invoice_by_id(self, invoice_id: str) -> Optional[Dict]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM crypto_invoices 
                    WHERE id = ?
                """, (invoice_id,))
                row = cursor.fetchone()
                return dict(row) if row else None
        except sqlite3.Error as e:
            logger.error(f"Error getting invoice by id: {e}")
            return None
    
    def save_crypto_check(self, check_code: str, user_id: int, estimated_amount: float = 0.0, currency: str = "USDT") -> bool:
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT INTO crypto_checks (check_code, user_id, estimated_amount, currency, status)
                    VALUES (?, ?, ?, ?, 'pending')
                    ON CONFLICT(check_code) DO UPDATE SET
                        user_id = excluded.user_id,
                        estimated_amount = excluded.estimated_amount,
                        currency = excluded.currency,
                        status = CASE WHEN status != 'credited' THEN 'pending' ELSE status END
                """, (check_code, user_id, estimated_amount, currency))
                conn.commit()
                logger.info(f"Check saved: {check_code}, user: {user_id}, estimated: {estimated_amount} {currency}")
                return True
        except Exception as e:
            logger.error(f"Error saving check: {e}")
            return False
    
    def update_check_with_real_amount(self, check_code: str, real_amount: float, status: str = 'activated'):
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    UPDATE crypto_checks 
                    SET real_amount = ?, status = ?, activated_at = CURRENT_TIMESTAMP
                    WHERE check_code = ?
                """, (real_amount, status, check_code))
                conn.commit()
                logger.info(f"Check {check_code} updated with real amount: {real_amount}, status: {status}")
                return True
        except Exception as e:
            logger.error(f"Error updating check with real amount: {e}")
            return False
    
    def update_check_status(self, check_code: str, status: str, 
                          activation_result: str = None, error: str = None):
        try:
            with self.get_connection() as conn:
                if status == 'activated':
                    conn.execute("""
                        UPDATE crypto_checks 
                        SET status = ?, activation_result = ?, activation_error = ?, activated_at = CURRENT_TIMESTAMP
                        WHERE check_code = ?
                    """, (status, activation_result, error, check_code))
                elif status == 'credited':
                    conn.execute("""
                        UPDATE crypto_checks 
                        SET status = ?, credited_at = CURRENT_TIMESTAMP
                        WHERE check_code = ?
                    """, (status, check_code))
                else:
                    conn.execute("""
                        UPDATE crypto_checks 
                        SET status = ?, activation_result = ?, activation_error = ?
                        WHERE check_code = ?
                    """, (status, activation_result, error, check_code))
                conn.commit()
                logger.info(f"Check {check_code} status updated to {status}")
                return True
        except Exception as e:
            logger.error(f"Error updating check status: {e}")
            return False
    
    def credit_check_amount(self, check_code: str, user_id: int, credited_amount: float) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("BEGIN TRANSACTION")
                cursor.execute("SELECT balance FROM users WHERE id = ?", (user_id,))
                user_data = cursor.fetchone()
                if not user_data:
                    logger.error(f"User {user_id} does not exist")
                    cursor.execute("ROLLBACK")
                    return False
                before_balance = user_data['balance'] or 0.0
                after_balance = before_balance + credited_amount
                cursor.execute("""
                    UPDATE crypto_checks 
                    SET status = 'credited', credited_amount = ?, credited_at = CURRENT_TIMESTAMP
                    WHERE check_code = ?
                """, (credited_amount, check_code))
                cursor.execute("""
                    UPDATE users 
                    SET balance = balance + ?,
                        total_deposits = total_deposits + ?,
                        last_deposit_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (credited_amount, credited_amount, user_id))
                transaction_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO transactions 
                    (id, user_id, type, amount, before_balance, after_balance, description, metadata)
                    VALUES (?, ?, 'deposit', ?, ?, ?, ?, ?)
                """, (
                    transaction_id,
                    user_id,
                    credited_amount,
                    before_balance,
                    after_balance,
                    f"Активация чека CryptoBot ({check_code[:10]}...)",
                    json.dumps({'check_code': check_code, 'credited_amount': credited_amount})
                ))
                conn.commit()
                logger.info(f"Check {check_code} credited: {credited_amount} to user {user_id}. Balance: {before_balance:.4f} -> {after_balance:.4f}")
                return True
        except Exception as e:
            logger.error(f"Error crediting check amount: {e}")
            if 'conn' in locals():
                try:
                    conn.rollback()
                except:
                    pass
            return False
    
    def get_check_by_code(self, check_code: str) -> Optional[Dict]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT cc.*, u.telegram_id, u.username 
                    FROM crypto_checks cc
                    JOIN users u ON cc.user_id = u.id
                    WHERE cc.check_code = ?
                """, (check_code,))
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting check by code: {e}")
            return None
    
    def get_pending_checks(self) -> List[Dict]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT cc.*, u.telegram_id, u.username 
                    FROM crypto_checks cc
                    JOIN users u ON cc.user_id = u.id
                    WHERE cc.status = 'pending'
                    ORDER BY cc.created_at ASC
                    LIMIT 10
                """)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting pending checks: {e}")
            return []
    
    def get_user_checks(self, user_id: int, limit: int = 10) -> List[Dict]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM crypto_checks 
                    WHERE user_id = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (user_id, limit))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting user checks: {e}")
            return []
    
    def create_withdrawal_request(self, user_id: int, amount: float) -> Optional[str]:
        try:
            withdrawal_id = str(uuid.uuid4())
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("BEGIN TRANSACTION")
                
                cursor.execute("SELECT balance FROM users WHERE id = ?", (user_id,))
                user_data = cursor.fetchone()
                
                if not user_data:
                    cursor.execute("ROLLBACK")
                    logger.error(f"User {user_id} does not exist")
                    return None
                
                balance = user_data['balance'] or 0.0
                
                if balance < amount:
                    cursor.execute("ROLLBACK")
                    logger.warning(f"Insufficient balance for user {user_id}: {balance}, trying to withdraw {amount}")
                    return None
                
                cursor.execute("""
                    INSERT INTO withdrawals 
                    (id, user_id, amount, status, created_at, updated_at)
                    VALUES (?, ?, ?, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, (withdrawal_id, user_id, amount))
                
                before_balance = balance
                after_balance = balance - amount
                
                cursor.execute("""
                    UPDATE users 
                    SET balance = ?
                    WHERE id = ?
                """, (after_balance, user_id))
                
                transaction_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO transactions 
                    (id, user_id, type, amount, before_balance, after_balance, description, metadata)
                    VALUES (?, ?, 'withdrawal_reserve', ?, ?, ?, ?, ?)
                """, (
                    transaction_id,
                    user_id,
                    -amount,
                    before_balance,
                    after_balance,
                    f"Заявка на вывод {amount:.2f} ₽",
                    json.dumps({'withdrawal_id': withdrawal_id})
                ))
                
                conn.commit()
                logger.info(f"Withdrawal request created: {withdrawal_id}, user: {user_id}, amount: {amount}")
                return withdrawal_id
        except Exception as e:
            logger.error(f"Error creating withdrawal request: {e}")
            if 'conn' in locals():
                try:
                    conn.rollback()
                except:
                    pass
            return None
    
    def get_withdrawal_request(self, withdrawal_id: str) -> Optional[Dict]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT w.*, u.telegram_id, u.username, u.first_name 
                    FROM withdrawals w
                    JOIN users u ON w.user_id = u.id
                    WHERE w.id = ?
                """, (withdrawal_id,))
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting withdrawal request: {e}")
            return None
    
    def update_withdrawal_status(self, withdrawal_id: str, status: str, admin_id: int = None, admin_message: str = None) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("BEGIN TRANSACTION")
                
                cursor.execute("""
                    SELECT * FROM withdrawals 
                    WHERE id = ?
                """, (withdrawal_id,))
                
                withdrawal = cursor.fetchone()
                if not withdrawal:
                    cursor.execute("ROLLBACK")
                    return False
                
                user_id = withdrawal['user_id']
                amount = withdrawal['amount']
                
                if status == 'approved':
                    cursor.execute("""
                        UPDATE withdrawals 
                        SET status = ?, admin_id = ?, approved_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (status, admin_id, withdrawal_id))
                    
                elif status == 'completed':
                    cursor.execute("""
                        UPDATE withdrawals 
                        SET status = ?, admin_id = ?, admin_message = ?, completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (status, admin_id, admin_message, withdrawal_id))
                    
                elif status == 'rejected':
                    cursor.execute("""
                        UPDATE withdrawals 
                        SET status = ?, admin_id = ?, admin_message = ?, rejected_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (status, admin_id, admin_message, withdrawal_id))
                    
                    cursor.execute("""
                        UPDATE users 
                        SET balance = balance + ?
                        WHERE id = ?
                    """, (amount, user_id))
                    
                    cursor.execute("SELECT balance FROM users WHERE id = ?", (user_id,))
                    user_data = cursor.fetchone()
                    new_balance = user_data['balance'] or 0.0
                    
                    transaction_id = str(uuid.uuid4())
                    cursor.execute("""
                        INSERT INTO transactions 
                        (id, user_id, type, amount, before_balance, after_balance, description, metadata)
                        VALUES (?, ?, 'refund', ?, ?, ?, ?, ?)
                    """, (
                        transaction_id,
                        user_id,
                        amount,
                        new_balance - amount,
                        new_balance,
                        f"Возврат средств после отклонения вывода",
                        json.dumps({'withdrawal_id': withdrawal_id})
                    ))
                
                else:
                    cursor.execute("""
                        UPDATE withdrawals 
                        SET status = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (status, withdrawal_id))
                
                conn.commit()
                logger.info(f"Withdrawal {withdrawal_id} status updated to {status}")
                return True
        except Exception as e:
            logger.error(f"Error updating withdrawal status: {e}")
            if 'conn' in locals():
                try:
                    conn.rollback()
                except:
                    pass
            return False
        
    def get_pending_withdrawals(self, limit: int = 10) -> List[Dict]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT w.*, u.telegram_id, u.username, u.first_name 
                    FROM withdrawals w
                    JOIN users u ON w.user_id = u.id
                    WHERE w.status = 'pending'
                    ORDER BY w.created_at ASC
                    LIMIT ?
                """, (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting pending withdrawals: {e}")
            return []
    
    def get_user_withdrawals(self, user_id: int, limit: int = 10) -> List[Dict]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM withdrawals 
                    WHERE user_id = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (user_id, limit))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting user withdrawals: {e}")
            return []
    
    def get_user_stats(self, user_id: int) -> Dict[str, Any]:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        u.*,
                        (SELECT COUNT(*) FROM game_sessions WHERE user_id = u.id) as total_games,
                        (SELECT COUNT(*) FROM game_sessions WHERE user_id = u.id AND result = 'win') as wins,
                        (SELECT COUNT(*) FROM game_sessions WHERE user_id = u.id AND result = 'draw') as draws,
                        (SELECT COALESCE(SUM(win_amount), 0) FROM game_sessions WHERE user_id = u.id) as total_wins
                    FROM users u
                    WHERE u.id = ?
                """, (user_id,))
                user = cursor.fetchone()
                if not user:
                    return {}
                created_at = datetime.strptime(user['created_at'], '%Y-%m-%d %H:%M:%S')
                account_age = (datetime.now() - created_at).days
                return {
                    'balance': user['balance'] or 0.0,
                    'bonus_balance': user['bonus_balance'] or 0.0,
                    'turnover': user['turnover'] or 0.0,
                    'bonus_turnover': user['bonus_turnover'] or 0.0,
                    'total_deposits': user['total_deposits'] or 0.0,
                    'total_withdrawals': user['total_withdrawals'] or 0.0,
                    'games_played': user['games_played'] or 0,
                    'games_won': user['games_won'] or 0,
                    'total_games': user['total_games'] or 0,
                    'wins': user['wins'] or 0,
                    'draws': user['draws'] or 0,
                    'total_wins': user['total_wins'] or 0.0,
                    'has_deposited': user['has_deposited'] or 0,
                    'account_age': account_age
                }
        except Exception as e:
            logger.error(f"Error getting user stats: {e}")
            return {}
    
    def set_user_balance(self, telegram_id: int, new_balance: float, is_bonus: bool = False) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"SELECT id, {'bonus_balance' if is_bonus else 'balance'} FROM users WHERE telegram_id = ?",
                    (telegram_id,)
                )
                user = cursor.fetchone()
                if not user:
                    return False
                old_balance = user['bonus_balance' if is_bonus else 'balance'] or 0.0
                difference = new_balance - old_balance
                if difference == 0:
                    return True
                transaction_type = 'deposit' if difference > 0 else 'withdrawal'
                cursor.execute(
                    f"UPDATE users SET {'bonus_balance' if is_bonus else 'balance'} = ? WHERE telegram_id = ?",
                    (new_balance, telegram_id)
                )
                transaction_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO transactions 
                    (id, user_id, type, amount, before_balance, after_balance, is_bonus, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    transaction_id, user['id'], transaction_type,
                    abs(difference), old_balance, new_balance,
                    1 if is_bonus else 0,
                    f"Корректировка {'бонусного ' if is_bonus else ''}баланса администратором"
                ))
                conn.commit()
                logger.info(f"{'Bonus ' if is_bonus else ''}Balance set: user={telegram_id}, old={old_balance:.2f}, new={new_balance:.2f}")
                return True
        except Exception as e:
            logger.error(f"Error setting {'bonus ' if is_bonus else ''}balance: {e}")
            return False

class CasinoBot:
    def __init__(self):
        self.bot = Bot(
            token=Config.BOT_TOKEN,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        self.storage = MemoryStorage()
        self.dp = Dispatcher(storage=self.storage)
        self.router = Router()
        self.dp.include_router(self.router)
        
        self.db = SQLiteDatabase()
        self.crypto_api = CryptoPayAPI(Config.CRYPTOBOT_API_TOKEN)
        self.real_game = RealEmojiGame()
        
        self.telethon_client = TelethonCryptoBot(
            api_id=Config.TELEGRAM_API_ID,
            api_hash=Config.TELEGRAM_API_HASH,
            phone=Config.TELEGRAM_PHONE
        )
        
        self.background_tasks = set()
        self._check_activation_task = None
        self._invoice_check_task = None
        self._bonus_check_task = None
        
        self.last_bets = {}
        self.admin_withdrawal_context = {}
        
        self.register_handlers()
        
    async def start_background_tasks(self):
        if not self._check_activation_task:
            self._check_activation_task = asyncio.create_task(
                self.check_pending_checks_loop()
            )
            self.background_tasks.add(self._check_activation_task)
            logger.info("Check activation task started")
        
        if not self._invoice_check_task:
            self._invoice_check_task = asyncio.create_task(
                self.check_pending_invoices_loop()
            )
            self.background_tasks.add(self._invoice_check_task)
            logger.info("Invoice check task started")
        
        if not self._bonus_check_task:
            self._bonus_check_task = asyncio.create_task(
                self.check_bonuses_loop()
            )
            self.background_tasks.add(self._bonus_check_task)
            logger.info("Bonus check task started")
        
        try:
            telethon_connected = await self.telethon_client.connect()
            if telethon_connected:
                logger.info("Telethon client connected successfully")
            else:
                logger.warning("Telethon client failed to connect.")
        except Exception as e:
            logger.error(f"Failed to connect Telethon client: {e}")
    
    async def check_bonuses_loop(self):
        logger.info("Starting bonuses check loop")
        while True:
            try:
                expired_count = self.db.check_and_expire_bonuses()
                if expired_count > 0:
                    logger.info(f"Expired {expired_count} bonuses")
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                logger.info("Bonus check loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in bonus check loop: {e}")
                await asyncio.sleep(300)
    
    async def check_pending_checks_loop(self):
        logger.info("Starting pending checks processing loop")
        while True:
            try:
                pending_checks = self.db.get_pending_checks()
                if pending_checks:
                    logger.info(f"Found {len(pending_checks)} pending checks")
                    for check in pending_checks:
                        await self.process_check_activation(check)
                        await asyncio.sleep(5)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                logger.info("Check activation loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in check processing loop: {e}")
                await asyncio.sleep(60)
    
    async def check_pending_invoices_loop(self):
        logger.info("Starting pending invoices check loop")
        while True:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                logger.info("Invoice check loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in invoice check loop: {e}")
                await asyncio.sleep(60)
    
    async def process_check_activation(self, check: Dict):
        try:
            check_code = check['check_code']
            user_id = check['user_id']
            telegram_id = check['telegram_id']
            estimated_amount = check.get('estimated_amount', 0) or 0
            logger.info(f"Processing check: {check_code} for user {telegram_id}")
            if check.get('status') == 'credited':
                logger.info(f"Check {check_code} already credited")
                return
            self.db.update_check_status(
                check_code=check_code,
                status='processing',
                activation_result='Starting activation'
            )
            real_amount = 0.0
            activation_success = False
            if self.telethon_client and self.telethon_client.is_connected:
                logger.info(f"Activating check {check_code} via Telethon...")
                activation_result = await self.telethon_client.activate_check_and_get_amount(check_code)
                if activation_result['success']:
                    real_amount_usdt = activation_result['amount']
                    currency = activation_result.get('currency', 'USDT')
                    response_type = activation_result.get('response_type', 'unknown')
                    real_amount = CurrencyConverter.usdt_to_rub(real_amount_usdt)
                    logger.info(f"Check {check_code} activated via Telethon: {real_amount_usdt} {currency} -> {real_amount} RUB (type: {response_type})")
                    activation_success = True
                    activation_message = f"Activated via Telethon: {real_amount_usdt} {currency} -> {real_amount} RUB"
                else:
                    error_msg = activation_result.get('error', 'Unknown error')
                    logger.warning(f"Telethon activation failed: {error_msg}")
                    if estimated_amount > 0:
                        real_amount = estimated_amount
                        activation_success = True
                        activation_message = f"Used estimated amount: {real_amount} RUB (Telethon failed: {error_msg})"
                        logger.info(f"Using estimated amount: {real_amount}")
                    else:
                        activation_message = f"Telethon failed: {error_msg}"
            else:
                logger.warning("Telethon not connected, using estimated amount")
                if estimated_amount > 0:
                    real_amount = estimated_amount
                    activation_success = True
                    activation_message = f"Used estimated amount: {real_amount} RUB (Telethon not connected)"
                else:
                    activation_message = "Telethon not connected and no estimated amount"
            if activation_success and real_amount > 0:
                self.db.update_check_with_real_amount(
                    check_code=check_code,
                    real_amount=real_amount,
                    status='activated'
                )
                self.db.mark_user_has_deposited(user_id)
                credit_success = self.db.credit_check_amount(
                    check_code=check_code,
                    user_id=user_id,
                    credited_amount=real_amount
                )
                if credit_success:
                    user_info = self.db.get_or_create_user(telegram_id=telegram_id)
                    new_balance = user_info.get('balance', 0) if user_info else 0
                    try:
                        if estimated_amount == 0:
                            message = (
                                f"✅ <b>Чек активирован!</b>\n\n"
                                f"💰 <b>Сумма:</b> {real_amount:.2f} ₽\n"
                                f"🆔 <b>Код чека:</b> {check_code[:10]}...\n\n"
                                f"💵 <b>Зачислено на баланс</b>\n"
                                f"💰 <b>Текущий баланс:</b> {new_balance:.2f} ₽\n\n"
                                f"🎁 <b>Теперь вам доступны бонусы!</b>\n\n"
                                f"🎮 <b>Можете начинать играть!</b>"
                            )
                        else:
                            message = (
                                f"✅ <b>Чек активирован!</b>\n\n"
                                f"💰 <b>Реальная сумма:</b> {real_amount:.2f} ₽\n"
                                f"📝 <b>Указанная сумма:</b> {estimated_amount} ₽\n"
                                f"🆔 <b>Код чека:</b> {check_code[:10]}...\n\n"
                                f"💵 <b>Зачислено на баланс</b>\n"
                                f"💰 <b>Текущий баланс:</b> {new_balance:.2f} ₽\n\n"
                                f"🎁 <b>Теперь вам доступны бонусы!</b>\n"
                                f"ℹ️ <i>Зачисляется реальная сумма из чека</i>"
                            )
                        await self.bot.send_message(
                            chat_id=telegram_id,
                            text=message,
                            parse_mode=ParseMode.HTML,
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                [InlineKeyboardButton(text="🎮 Играть", callback_data="play")],
                                [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
                                [InlineKeyboardButton(text="🎁 Бонусы", callback_data="bonus_info")]
                            ])
                        )
                        logger.info(f"User {telegram_id} notified about successful check activation")
                    except Exception as e:
                        logger.error(f"Error notifying user: {e}")
                    logger.info(f"Check {check_code} successfully credited: {real_amount} RUB")
                else:
                    logger.error(f"Failed to credit check {check_code}")
                    self.db.update_check_status(
                        check_code=check_code,
                        status='failed',
                        activation_result='Credit failed',
                        error='Database error'
                    )
                    try:
                        await self.bot.send_message(
                            chat_id=telegram_id,
                            text=(
                                f"❌ <b>Ошибка при зачислении средств</b>\n\n"
                                f"🆔 Код: {check_code[:10]}...\n"
                                f"💰 Сумма: {real_amount} ₽\n\n"
                                f"⚠️ <b>Пожалуйста, обратитесь в поддержку</b>"
                            ),
                            parse_mode=ParseMode.HTML
                        )
                    except Exception as e:
                        logger.error(f"Error notifying user about credit failure: {e}")
            else:
                logger.error(f"Check activation failed: {activation_message}")
                self.db.update_check_status(
                    check_code=check_code,
                    status='failed',
                    activation_result='Activation failed',
                    error=activation_message
                )
                try:
                    if real_amount == 0:
                        message = (
                            f"❌ <b>Не удалось активировать чек</b>\n\n"
                            f"🆔 Код: {check_code[:10]}...\n"
                            f"📝 Причина: {activation_message}\n\n"
                            f"⚠️ <b>Пожалуйста, обратитесь в поддержку</b>"
                        )
                    else:
                        message = (
                            f"⚠️ <b>Чек активирован, но сумма 0</b>\n\n"
                            f"🆔 Код: {check_code[:10]}...\n"
                            f"💰 Сумма чека: 0 ₽\n\n"
                            f"ℹ️ <i>Чек не содержит средств</i>"
                        )
                    await self.bot.send_message(
                        chat_id=telegram_id,
                        text=message,
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    logger.error(f"Error notifying user about failed check: {e}")
        except Exception as e:
            logger.error(f"Error processing check activation: {e}")
            self.db.update_check_status(
                check_code=check['check_code'],
                status='failed',
                activation_result='Processing error',
                error=str(e)[:200]
            )
    
    def _format_game_result_message(self, result_type: str, **kwargs) -> str:
        if result_type == 'win':
            template = Config.MESSAGES['game_result_win']
            return template.format(
                bet_info=f"<tg-emoji emoji-id='5260342697075416641'>🗓</tg-emoji> <b>Ваша ставка:</b> {kwargs.get('bet_name', '')}",
                coefficient_info=f"<tg-emoji emoji-id='5258105663359294787'>🔢</tg-emoji> <b>Коэффициент</b> {kwargs.get('coefficient', 0)}x",
                bet_amount_info=f"<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Ставка:</b> {kwargs.get('bet_amount', 0):.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>",
                balance_info=f"<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Ваш баланс:</b> {kwargs.get('balance', 0):.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>"
            )
        elif result_type == 'loss':
            template = Config.MESSAGES['game_result_loss']
            return template.format(
                bet_info=f"<tg-emoji emoji-id='5260342697075416641'>🗓</tg-emoji> <b>Ваша ставка:</b> {kwargs.get('bet_name', '')}",
                bet_amount_info=f"<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Ставка:</b> {kwargs.get('bet_amount', 0):.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>",
                balance_info=f"<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Ваш баланс:</b> {kwargs.get('balance', 0):.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>"
            )
        else:
            template = Config.MESSAGES['game_result_draw']
            return template.format(
                bet_info=f"<tg-emoji emoji-id='5260342697075416641'>🗓</tg-emoji> <b>Ваша ставка:</b> {kwargs.get('bet_name', '')}",
                bet_amount_info=f"<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Ставка:</b> {kwargs.get('bet_amount', 0):.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>",
                balance_info=f"<tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji> <b>Ваш баланс:</b> {kwargs.get('balance', 0):.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>"
            )
    
    def _get_choice_name(self, game_type: str, choice: str) -> str:
        names = {
            'dice': {
                'odd': 'Нечет',
                'even': 'Чет',
                '1': '1', '2': '2', '3': '3',
                '4': '4', '5': '5', '6': '6'
            },
            'darts': {
                'miss': 'Дартс мимо',
                'center': 'Дартс центр',
                'red': 'Дартс красное',
                'white': 'Дартс белое'
            },
            'rps': {
                'rock': 'Камень',
                'scissors': 'Ножницы',
                'paper': 'Бумага'
            },
            'basketball': {
                'goal': 'Баскетбол гол',
                'stuck': 'Баскетбол застрял',
                'miss': 'Баскетбол мимо',
                'clean': 'Баскетбол чистый'
            },
            'football': {
                'goal': 'Футбол гол',
                'miss': 'Футбол мимо'
            },
            'kb': {
                'red': 'Красное',
                'white': 'Белое'
            },
            'bowling': {
                'strike': 'Страйк',
                '2_pins': '2 кегли',
                '3_pins': '3 кегли',
                '4_pins': '4 кегли',
                '5_pins': '5 кеглей'
            },
            'slots': {
                '777': 'Джекпот',
                'double': 'Выигрыш'
            }
        }
        game_names = names.get(game_type, {})
        return game_names.get(choice, choice)
    
    def register_handlers(self):
        @self.router.message(CommandStart())
        async def cmd_start(message: Message):
            if self.db.is_user_banned(message.from_user.id):
                await message.answer(
                    "<tg-emoji emoji-id='5240241223632954241'>🚫</tg-emoji> <b>Вы были заблокированы</b>\n\n"
                    "<tg-emoji emoji-id='5397782960512444700'>📌</tg-emoji> Если вы считаете это ошибкой обратитесь к админу @casinomayami\n\n"
                    "<tg-emoji emoji-id='5440660757194744323'>‼️</tg-emoji>Просим вас не писать каких-либо отзывов и не начинать публичное обсуждение "
                    "до окончания разбирательства. Нам важно поддерживать порядок на площадке, "
                    "но, к сожалению, добиться этого без блокировок не представляется возможным.",
                    parse_mode=ParseMode.HTML
                )
                return
            try:
                user = self.db.get_or_create_user(
                    telegram_id=message.from_user.id,
                    username=message.from_user.username or "",
                    first_name=message.from_user.first_name or "",
                    last_name=message.from_user.last_name or ""
                )
                if not user:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при создании профиля")
                    return
                welcome_text = Config.MESSAGES['welcome'].format(
                    username=message.from_user.username or message.from_user.first_name,
                    support=Config.SUPPORT_LINK,
                    news=Config.NEWS_CHANNEL
                )
                await message.answer_photo(
                    photo=Config.PHOTOS["menu"],
                    caption=welcome_text,
                    reply_markup=self.get_main_keyboard(),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Error in /start: {e}", exc_info=True)
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Произошла ошибка. Попробуйте позже.")
        
        @self.router.message(Command("setbalance"))
        async def cmd_set_balance(message: Message):
            if message.from_user.id not in Config.ADMIN_IDS:
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> У вас нет прав для этой команды")
                return
            try:
                parts = message.text.split()
                if len(parts) != 4:
                    await message.answer(
                        "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> <b>Использование:</b>\n"
                        "<code>/setbalance ID_пользователя сумма тип(bonus/real)</code>\n\n"
                        "Пример: <code>/setbalance 123456789 50000.0 real</code>\n"
                        "Пример: <code>/setbalance 123456789 10000.0 bonus</code>",
                        parse_mode="HTML"
                    )
                    return
                user_id = int(parts[1])
                amount = float(parts[2])
                balance_type = parts[3].lower()
                if balance_type not in ['bonus', 'real']:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Тип баланса должен быть 'bonus' или 'real'")
                    return
                if amount < 0:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Баланс не может быть отрицательным")
                    return
                is_bonus = (balance_type == 'bonus')
                success = self.db.set_user_balance(user_id, amount, is_bonus)
                if success:
                    user = self.db.get_or_create_user(telegram_id=user_id)
                    balance_type_text = "бонусный" if is_bonus else "реальный"
                    await message.answer(
                        f"<tg-emoji emoji-id='5206607081334906820'>✅</tg-emoji> <b>{balance_type_text.capitalize()} баланс установлен!</b>\n\n"
                        f"👤 Пользователь: <code>{user_id}</code>\n"
                        f"💰 Новый баланс: <code>{amount:.2f} ₽</code>\n"
                        f"📝 Тип: {balance_type_text}\n"
                        f"👤 Имя: {user.get('first_name', 'не указано')}\n"
                        f"📅 Операция: Корректировка администратором",
                        parse_mode="HTML"
                    )
                else:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка: пользователь не найден")
            except ValueError:
                await message.answer(
                    "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Неверный формат. Используйте числа\n"
                    "Пример: <code>/setbalance 123456789 50000.0 real</code>",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Error in set_balance: {e}", exc_info=True)
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Произошла ошибка")

        @self.router.message(Command("sendcheck"))
        async def cmd_send_check(message: Message):
            if message.from_user.id not in Config.ADMIN_IDS:
                return
                
            try:
                args = message.text.split()
                if len(args) < 4:
                    await message.answer(
                        "📝 <b>Использование:</b>\n"
                        "<code>/sendcheck USER_ID WITHDRAWAL_ID сообщение</code>\n\n"
                        "Пример:\n"
                        "<code>/sendcheck 123456789 abc-def-ghijk Ваш чек готов!</code>",
                        parse_mode="HTML"
                    )
                    return
                
                target_user_id = int(args[1])
                withdrawal_id = args[2]
                admin_message = ' '.join(args[3:])
                
                # Отправляем сообщение пользователю
                await self.bot.send_message(
                    chat_id=target_user_id,
                    text=f"💰 <b>Чек по вашему выводу</b>\n\n{admin_message}"
                )
                
                # Обновляем статус
                self.db.update_withdrawal_status(
                    withdrawal_id=withdrawal_id,
                    status='completed',
                    admin_id=message.from_user.id,
                    admin_message=admin_message
                )
                
                await message.answer(f"✅ Сообщение отправлено пользователю {target_user_id}")
                
            except Exception as e:
                logger.error(f"Error in /sendcheck: {e}")
                await message.answer("❌ Ошибка при отправке сообщения")

        @self.router.message(Command("ban"))
        async def cmd_ban(message: Message):
            if message.from_user.id not in Config.ADMIN_IDS:
                await message.answer("❌ У вас нет прав для этой команды")
                return
            
            try:
                parts = message.text.split()
                if len(parts) < 2:
                    await message.answer(
                        "<tg-emoji emoji-id='5240241223632954241'>🚫</tg-emoji> <b>Использование:</b>\n"
                        "<code>/ban ID_пользователя [причина]</code>\n\n"
                        "Пример:\n"
                        "<code>/ban 123456789 Нарушение правил</code>",
                        parse_mode="HTML"
                    )
                    return
                
                target_id = int(parts[1])
                reason = " ".join(parts[2:]) if len(parts) > 2 else "Нарушение правил"
                
                user = self.db.get_or_create_user(telegram_id=target_id)
                if not user:
                    await message.answer(f"❌ Пользователь с ID {target_id} не найден")
                    return
                
                success = self.db.ban_user(target_id, reason)
                if success:
                    try:
                        await self.bot.send_message(
                            chat_id=target_id,
                            text=(
                                "<tg-emoji emoji-id='5240241223632954241'>🚫</tg-emoji> <b>Вы были заблокированы</b>\n\n"
                                "<tg-emoji emoji-id='5397782960512444700'>📌</tg-emoji> Если вы считаете это ошибкой обратитесь к админу @casinomayami\n\n"
                                "<tg-emoji emoji-id='5440660757194744323'>‼️</tg-emoji>Просим вас не писать каких-либо отзывов и не начинать публичное обсуждение "
                                "до окончания разбирательства. Нам важно поддерживать порядок на площадке, "
                                "но, к сожалению, добиться этого без блокировок не представляется возможным."
                            ),
                            parse_mode=ParseMode.HTML
                        )
                    except Exception as e:
                        logger.error(f"Error notifying banned user: {e}")
                    
                    await message.answer(
                        f"✅ <b>Пользователь заблокирован</b>\n\n"
                        f"👤 ID: <code>{target_id}</code>\n"
                        f"📝 Имя: {user.get('first_name', 'Неизвестно')}\n"
                        f"📛 Причина: {reason}\n"
                        f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
                        parse_mode="HTML"
                    )
                else:
                    await message.answer("❌ Ошибка при блокировке пользователя")
                    
            except ValueError:
                await message.answer("❌ Неверный ID пользователя. Используйте числа")
            except Exception as e:
                logger.error(f"Error in /ban: {e}")
                await message.answer("❌ Произошла ошибка")

        @self.router.message(Command("unban"))
        async def cmd_unban(message: Message):
            if message.from_user.id not in Config.ADMIN_IDS:
                await message.answer("❌ У вас нет прав для этой команды")
                return
            
            try:
                parts = message.text.split()
                if len(parts) != 2:
                    await message.answer(
                        "<tg-emoji emoji-id='5296369303661067030'>🔒</tg-emoji> <b>Использование:</b>\n"
                        "<code>/unban ID_пользователя</code>\n\n"
                        "Пример:\n"
                        "<code>/unban 123456789</code>",
                        parse_mode="HTML"
                    )
                    return
                
                target_id = int(parts[1])
                
                user = self.db.get_or_create_user(telegram_id=target_id)
                if not user:
                    await message.answer(f"❌ Пользователь с ID {target_id} не найден")
                    return
                
                success = self.db.unban_user(target_id)
                if success:
                    try:
                        await self.bot.send_message(
                            chat_id=target_id,
                            text=(
                                "<tg-emoji emoji-id='5296369303661067030'>🔒</tg-emoji> <b>Вы были разблокированы</b>\n\n"
                                "<tg-emoji emoji-id='5467538555158943525'>💭</tg-emoji> Не нарушайте больше правила чтобы таких неприятных ситуаций "
                                "больше не происходило."
                            ),
                            parse_mode=ParseMode.HTML
                        )
                    except Exception as e:
                        logger.error(f"Error notifying unbanned user: {e}")
                    
                    await message.answer(
                        f"✅ <b>Пользователь разблокирован</b>\n\n"
                        f"👤 ID: <code>{target_id}</code>\n"
                        f"📝 Имя: {user.get('first_name', 'Неизвестно')}\n"
                        f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
                        parse_mode="HTML"
                    )
                else:
                    await message.answer("❌ Ошибка при разблокировке пользователя")
                    
            except ValueError:
                await message.answer("❌ Неверный ID пользователя. Используйте числа")
            except Exception as e:
                logger.error(f"Error in /unban: {e}")
                await message.answer("❌ Произошла ошибка")
        
        @self.router.message(Command("stats"))
        async def cmd_stats(message: Message):
            if self.db.is_user_banned(user_id):
                await message.answer(
                    "<tg-emoji emoji-id='5240241223632954241'>🚫</tg-emoji> <b>Вы были заблокированы</b>\n\n"
                    "<tg-emoji emoji-id='5397782960512444700'>📌</tg-emoji> Если вы считаете это ошибкой обратитесь к админу @casinomayami\n\n"
                    "<tg-emoji emoji-id='5440660757194744323'>‼️</tg-emoji>Просим вас не писать каких-либо отзывов и не начинать публичное обсуждение "
                    "до окончания разбирательства. Нам важно поддерживать порядок на площадке, "
                    "но, к сожалению, добиться этого без блокировок не представляется возможным.",
                    parse_mode=ParseMode.HTML
                )
                return
            try:
                user = self.db.get_or_create_user(
                    telegram_id=message.from_user.id,
                    username=message.from_user.username or "",
                    first_name=message.from_user.first_name or ""
                )
                if not user:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден")
                    return
                stats = self.db.get_user_stats(user['id'])
                if not stats:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Не удалось получить статистику")
                    return
                win_rate = (stats['wins'] / stats['total_games'] * 100) if stats['total_games'] > 0 else 0
                text = (
                    f"📊 <b>Статистика {message.from_user.username or message.from_user.first_name}</b>\n\n"
                    f"💰 <b>Баланс:</b> {stats['balance']:.2f} ₽\n"
                    f"🎁 <b>Бонусный баланс:</b> {stats['bonus_balance']:.2f} ₽\n"
                    f"📈 <b>Оборот:</b> {stats['turnover']:.2f} ₽\n"
                    f"🎯 <b>Бонусный оборот:</b> {stats['bonus_turnover']:.2f} ₽\n\n"
                    f"🎮 <b>Игры:</b> {stats['games_played']}\n"
                    f"✅ <b>Победы:</b> {stats['wins']}\n"
                    f"📊 <b>Процент побед:</b> {win_rate:.1f}%\n\n"
                    f"📥 <b>Пополнений:</b> {stats['total_deposits']:.2f} ₽\n"
                    f"📤 <b>Выводов:</b> {stats['total_withdrawals']:.2f} ₽\n"
                    f"📅 <b>Аккаунту:</b> {stats['account_age']} дней\n"
                    f"🎁 <b>Депозит сделан:</b> {'✅' if stats['has_deposited'] else '❌'}"
                )
                await message.answer(text, parse_mode=ParseMode.HTML)
            except Exception as e:
                logger.error(f"Error in /stats: {e}")
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при получении статистики")
        
        @self.router.message(Command("bonus"))
        async def cmd_bonus_info(message: Message):
            if self.db.is_user_banned(user_id):
                await message.answer(
                    "<tg-emoji emoji-id='5240241223632954241'>🚫</tg-emoji> <b>Вы были заблокированы</b>\n\n"
                    "<tg-emoji emoji-id='5397782960512444700'>📌</tg-emoji> Если вы считаете это ошибкой обратитесь к админу @casinomayami\n\n"
                    "<tg-emoji emoji-id='5440660757194744323'>‼️</tg-emoji>Просим вас не писать каких-либо отзывов и не начинать публичное обсуждение "
                    "до окончания разбирательства. Нам важно поддерживать порядок на площадке, "
                    "но, к сожалению, добиться этого без блокировок не представляется возможным.",
                    parse_mode=ParseMode.HTML
                )
                return
            try:
                user = self.db.get_or_create_user(
                    telegram_id=message.from_user.id,
                    username=message.from_user.username or "",
                    first_name=message.from_user.first_name or ""
                )
                if not user:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден")
                    return
                active_bonus = self.db.get_active_bonus(user['id'])
                if not active_bonus:
                    if not user.get('has_deposited'):
                        await message.answer(
                            Config.MESSAGES.get('bonus_info_message', "").format(
                                wagering_requirement=Config.BONUS_WAGERING_REQUIREMENT,
                                expiry_days=Config.BONUS_EXPIRY_DAYS
                            ),
                            parse_mode=ParseMode.HTML
                        )
                    else:
                        await message.answer(
                            Config.MESSAGES['no_active_bonus'],
                            parse_mode=ParseMode.HTML
                        )
                    return
                amount = active_bonus['amount']
                wagering_requirement = active_bonus['wagering_requirement']
                required_turnover = active_bonus['required_turnover']
                current_turnover = active_bonus['current_turnover'] or 0
                progress = (current_turnover / required_turnover * 100) if required_turnover > 0 else 0
                expiry_date = datetime.strptime(active_bonus['expiry_date'], '%Y-%m-%d %H:%M:%S')
                expiry_str = expiry_date.strftime('%d.%m.%Y %H:%M')
                status_text = "Активен" if active_bonus['status'] == 'active' else active_bonus['status']
                message_text = Config.MESSAGES['bonus_activated'].format(
                    amount=amount,
                    multiplier=wagering_requirement,
                    required_turnover=required_turnover,
                    current_turnover=current_turnover,
                    progress=progress
                )
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🎮 Играть с бонусом", callback_data="play_with_bonus")],
                    [InlineKeyboardButton(text="💰 Конвертировать", callback_data="convert_bonus")],
                    [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")]
                ])
                await message.answer_photo(
                    photo=Config.PHOTOS["bonus"],
                    caption=message_text,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Error in /bonus: {e}")
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при получении информации о бонусе")
        
        @self.router.message(Command("promo"))
        async def cmd_use_promo(message: Message):
            try:
                parts = message.text.split()
                if len(parts) != 2:
                    await message.answer(
                        "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> <b>Использование:</b>\n"
                        "<code>/promo КОД_ПРОМОКОДА</code>\n\n"
                        "Пример: <code>/promo WELCOME2024</code>",
                        parse_mode="HTML"
                    )
                    return
                promo_code = parts[1].strip().upper()
                user = self.db.get_or_create_user(
                    telegram_id=message.from_user.id,
                    username=message.from_user.username or "",
                    first_name=message.from_user.first_name or ""
                )
                if not user:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден")
                    return
                if not user.get('has_deposited'):
                    await message.answer(
                        "🎁 <b>Промокоды доступны только после депозита!</b>\n\n"
                        "Сделайте депозит на любую сумму, чтобы активировать промокод.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="💰 Пополнить", callback_data="deposit")]
                        ])
                    )
                    return
                result = self.db.use_promo_code(user['id'], promo_code)
                if result['success']:
                    bonus_amount = result['amount']
                    wagering_requirement = result['wagering_requirement']
                    required_turnover = bonus_amount * wagering_requirement
                    await message.answer(
                        Config.MESSAGES['promo_code_success'].format(
                            amount=bonus_amount,
                            multiplier=wagering_requirement,
                            promo_code=promo_code
                        ),
                        parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="🎮 Играть с бонусом", callback_data="play_with_bonus")],
                            [InlineKeyboardButton(text="🎁 Инфо о бонусе", callback_data="bonus_info")]
                        ])
                    )
                else:
                    await message.answer(
                        Config.MESSAGES['promo_code_not_found'] if "не найден" in result['message'] else result['message'],
                        parse_mode=ParseMode.HTML
                    )
            except Exception as e:
                logger.error(f"Error in /promo: {e}")
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при активации промокода")
        
        @self.router.callback_query(F.data == "bonus_info")
        async def bonus_info_callback(callback: CallbackQuery):
            try:
                await callback.answer()
                await cmd_bonus_info(callback.message)
            except Exception as e:
                logger.error(f"Error in bonus_info_callback: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.callback_query(F.data == "convert_bonus")
        async def convert_bonus_callback(callback: CallbackQuery):
            try:
                await callback.answer()
                user = self.db.get_or_create_user(
                    telegram_id=callback.from_user.id,
                    username=callback.from_user.username or "",
                    first_name=callback.from_user.first_name or ""
                )
                if not user:
                    await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден", show_alert=True)
                    return
                result = self.db.convert_completed_bonuses(user['id'])
                if result['success']:
                    converted_amount = result['converted_amount']
                    updated_user = self.db.get_or_create_user(telegram_id=callback.from_user.id)
                    new_balance = updated_user.get('balance', 0.0)
                    await callback.message.answer(
                        Config.MESSAGES['bonus_converted'].format(
                            bonus_amount=converted_amount,
                            actual_turnover=converted_amount * Config.BONUS_WAGERING_REQUIREMENT,
                            converted_amount=converted_amount,
                            new_balance=new_balance
                        ),
                        parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="🎮 Играть", callback_data="play")],
                            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")]
                        ])
                    )
                else:
                    await callback.answer(result['message'], show_alert=True)
            except Exception as e:
                logger.error(f"Error in convert_bonus_callback: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при конвертации", show_alert=True)
        
        @self.router.callback_query(F.data == "play_with_bonus")
        async def play_with_bonus_callback(callback: CallbackQuery):
            try:
                await callback.answer()
                user = self.db.get_or_create_user(
                    telegram_id=callback.from_user.id,
                    username=callback.from_user.username or "",
                    first_name=callback.from_user.first_name or ""
                )
                if not user:
                    await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден", show_alert=True)
                    return
                bonus_balance = user.get('bonus_balance', 0.0)
                real_balance = user.get('balance', 0.0)
                if bonus_balance <= 0:
                    await callback.answer(
                        "У вас нет бонусного баланса для игры",
                        show_alert=True
                    )
                    return
                await callback.message.answer(
                    Config.MESSAGES['play_menu'],
                    reply_markup=self.get_games_keyboard(),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Error in play_with_bonus_callback: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.message(Command("givebonus"))
        async def cmd_give_bonus(message: Message):
            if message.from_user.id not in Config.ADMIN_IDS:
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> У вас нет прав для этой команды")
                return
            try:
                parts = message.text.split()
                if len(parts) != 3:
                    await message.answer(
                        "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> <b>Использование:</b>\n"
                        "<code>/givebonus ID_пользователя сумма</code>\n\n"
                        "Пример: <code>/givebonus 123456789 5000.0</code>",
                        parse_mode="HTML"
                    )
                    return
                user_id = int(parts[1])
                amount = float(parts[2])
                if amount <= 0:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Сумма должна быть положительной")
                    return
                user = self.db.get_or_create_user(telegram_id=user_id)
                if not user:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Пользователь не найден")
                    return
                bonus_id = self.db.create_bonus(
                    user_id=user['id'],
                    amount=amount,
                    wagering_requirement=Config.BONUS_WAGERING_REQUIREMENT
                )
                if bonus_id:
                    required_turnover = amount * Config.BONUS_WAGERING_REQUIREMENT
                    expiry_date = datetime.now() + timedelta(days=Config.BONUS_EXPIRY_DAYS)
                    await message.answer(
                        f"✅ <b>Бонус выдан!</b>\n\n"
                        f"👤 Пользователь: <code>{user_id}</code>\n"
                        f"💰 Сумма: <code>{amount:.2f} ₽</code>\n"
                        f"🎯 Вейджеринг: <code>x{Config.BONUS_WAGERING_REQUIREMENT}</code>\n"
                        f"📊 Требуемый оборот: <code>{required_turnover:.2f} ₽</code>\n"
                        f"⏳ Истекает: <code>{expiry_date.strftime('%d.%m.%Y %H:%M')}</code>\n\n"
                        f"🆔 ID бонуса: <code>{bonus_id}</code>",
                        parse_mode="HTML"
                    )
                    try:
                        await self.bot.send_message(
                            chat_id=user_id,
                            text=Config.MESSAGES['bonus_received'].format(
                                amount=amount,
                                multiplier=Config.BONUS_WAGERING_REQUIREMENT,
                                required_turnover=required_turnover,
                                expiry_date=expiry_date.strftime('%d.%m.%Y %H:%M')
                            ),
                            parse_mode=ParseMode.HTML,
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                [InlineKeyboardButton(text="🎮 Играть с бонусом", callback_data="play_with_bonus")],
                                [InlineKeyboardButton(text="🎁 Инфо о бонусе", callback_data="bonus_info")]
                            ])
                        )
                    except Exception as e:
                        logger.error(f"Error notifying user about bonus: {e}")
                else:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при создании бонуса")
            except ValueError:
                await message.answer(
                    "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Неверный формат. Используйте числа\n"
                    "Пример: <code>/givebonus 123456789 5000.0</code>",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Error in give_bonus: {e}", exc_info=True)
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Произошла ошибка")
        
        @self.router.message(Command("createpromo"))
        async def cmd_create_promo(message: Message):
            if message.from_user.id not in Config.ADMIN_IDS:
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> У вас нет прав для этой команды")
                return
            try:
                parts = message.text.split()
                if len(parts) < 3:
                    await message.answer(
                        "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> <b>Использование:</b>\n"
                        "<code>/createpromo КОД сумма [кол-во_использований=1] [дней_действия=30]</code>\n\n"
                        "Примеры:\n"
                        "<code>/createpromo WELCOME2024 5000.0</code>\n"
                        "<code>/createpromo BONUS100 10000.0 5 60</code>",
                        parse_mode="HTML"
                    )
                    return
                code = parts[1].strip().upper()
                amount = float(parts[2])
                max_uses = 1
                expiry_days = 30
                if len(parts) > 3:
                    max_uses = int(parts[3])
                if len(parts) > 4:
                    expiry_days = int(parts[4])
                if amount <= 0:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Сумма должна быть положительной")
                    return
                if max_uses <= 0:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Количество использований должно быть положительным")
                    return
                success = self.db.create_promo_code(
                    code=code,
                    bonus_amount=amount,
                    wagering_requirement=Config.BONUS_WAGERING_REQUIREMENT,
                    max_uses=max_uses,
                    expiry_days=expiry_days
                )
                if success:
                    expiry_date = datetime.now() + timedelta(days=expiry_days)
                    await message.answer(
                        f"✅ <b>Промокод создан!</b>\n\n"
                        f"🎁 Код: <code>{code}</code>\n"
                        f"💰 Сумма: <code>{amount:.2f} ₽</code>\n"
                        f"🎯 Вейджеринг: <code>x{Config.BONUS_WAGERING_REQUIREMENT}</code>\n"
                        f"👥 Макс. использований: <code>{max_uses}</code>\n"
                        f"⏳ Истекает: <code>{expiry_date.strftime('%d.%m.%Y %H:%M')}</code>\n\n"
                        f"📝 Для использования: <code>/promo {code}</code>",
                        parse_mode="HTML"
                    )
                else:
                    await message.answer(f"❌ Промокод <code>{code}</code> уже существует", parse_mode="HTML")
            except ValueError:
                await message.answer(
                    "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Неверный формат.",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Error in create_promo: {e}", exc_info=True)
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Произошла ошибка")
        
        @self.router.callback_query(F.data == "main_menu")
        async def main_menu(callback: CallbackQuery):
            try:
                await callback.answer()
                welcome_text = Config.MESSAGES['welcome'].format(
                    username=callback.from_user.username or callback.from_user.first_name,
                    support=Config.SUPPORT_LINK,
                    news=Config.NEWS_CHANNEL
                )
                await callback.message.answer_photo(
                    photo=Config.PHOTOS["menu"],
                    caption=welcome_text,
                    reply_markup=self.get_main_keyboard(),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Error in main_menu: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.callback_query(UserStates.waiting_bet_source, F.data.startswith("bet_source_bonus_"))
        async def choose_bonus_balance(call: CallbackQuery, state: FSMContext):
            _, _, _, game_type, user_choice = call.data.split("_", 4)
            await state.update_data(
                bet_source="bonus",
                is_bonus_bet=True,
                game_type=game_type,
                user_choice=user_choice
            )
            await call.answer("Выбран бонусный баланс 🎁")
            await call.message.edit_text(
                "🎁 <b>Выбран бонусный баланс</b>\n\n"
                "💰 Введите сумму ставки:",
                parse_mode=ParseMode.HTML
            )
            await state.set_state(UserStates.waiting_bet_amount)
        
        @self.router.callback_query(UserStates.waiting_bet_source, F.data.startswith("bet_source_real_"))
        async def choose_real_balance(call: CallbackQuery, state: FSMContext):
            _, _, _, game_type, user_choice = call.data.split("_", 4)
            await state.update_data(
                bet_source="real",
                is_bonus_bet=False,
                game_type=game_type,
                user_choice=user_choice
            )
            await call.answer("Выбран реальный баланс 💰")
            await call.message.answer(
                "💰 <b>Выбран реальный баланс</b>\n\n"
                "💰 Введите сумму ставки:",
                parse_mode=ParseMode.HTML
            )
            await state.set_state(UserStates.waiting_bet_amount)
        
        @self.router.callback_query(F.data == "play")
        async def play_menu(callback: CallbackQuery):
            if self.db.is_user_banned(user_id):
                await message.answer(
                    "<tg-emoji emoji-id='5240241223632954241'>🚫</tg-emoji> <b>Вы были заблокированы</b>\n\n"
                    "<tg-emoji emoji-id='5397782960512444700'>📌</tg-emoji> Если вы считаете это ошибкой обратитесь к админу @casinomayami\n\n"
                    "<tg-emoji emoji-id='5440660757194744323'>‼️</tg-emoji>Просим вас не писать каких-либо отзывов и не начинать публичное обсуждение "
                    "до окончания разбирательства. Нам важно поддерживать порядок на площадке, "
                    "но, к сожалению, добиться этого без блокировок не представляется возможным.",
                    parse_mode=ParseMode.HTML
                )
                return
            try:
                await callback.answer()
                await callback.message.answer(
                    text=Config.MESSAGES['play_menu'],
                    reply_markup=self.get_games_keyboard(),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Error in play_menu: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.callback_query(F.data == "profile")
        async def profile_menu(callback: CallbackQuery):
            if self.db.is_user_banned(user_id):
                await message.answer(
                    "<tg-emoji emoji-id='5240241223632954241'>🚫</tg-emoji> <b>Вы были заблокированы</b>\n\n"
                    "<tg-emoji emoji-id='5397782960512444700'>📌</tg-emoji> Если вы считаете это ошибкой обратитесь к админу @casinomayami\n\n"
                    "<tg-emoji emoji-id='5440660757194744323'>‼️</tg-emoji>Просим вас не писать каких-либо отзывов и не начинать публичное обсуждение "
                    "до окончания разбирательства. Нам важно поддерживать порядок на площадке, "
                    "но, к сожалению, добиться этого без блокировок не представляется возможным.",
                    parse_mode=ParseMode.HTML
                )
                return
            try:
                await callback.answer()
                user = self.db.get_or_create_user(
                    telegram_id=callback.from_user.id,
                    username=callback.from_user.username or "",
                    first_name=callback.from_user.first_name or ""
                )
                if not user:
                    await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден", show_alert=True)
                    return
                balance = user.get('balance', 0.0)
                bonus_balance = user.get('bonus_balance', 0.0)
                turnover = user.get('turnover', 0.0)
                remaining = max(0, 50000.0 - turnover)
                profile_text = Config.MESSAGES['profile'].format(
                    balance=balance,
                    bonus_balance=bonus_balance,
                    turnover=turnover,
                    remaining=remaining
                )
                await callback.message.answer_photo(
                    photo=Config.PHOTOS["profile"],
                    caption=profile_text,
                    reply_markup=self.get_profile_keyboard(),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Error in profile_menu: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.callback_query(F.data.startswith("game_"))
        async def select_game(callback: CallbackQuery):
            try:
                await callback.answer()
                game_type = callback.data.split("_")[1]
                game_texts = {
                    "dice": "🎲 <b>Кубик</b>\n\nВыберите ставку:",
                    "darts": "🎯 <b>Дартс</b>\n\nВыберите ставку:",
                    "rps": "🤜🏻 <b>Камень-Ножницы-Бумага</b>\n\nВыберите ставку:",
                    "bowling": "🎳 <b>Боулинг</b>\n\nВыберите ставку:",
                    "basketball": "🏀 <b>Баскетбол</b>\n\nВыберите ставку:",
                    "football": "⚽️ <b>Футбол</b>\n\nВыберите ставку:",
                    "slots": "🎰 <b>Слоты</b>\n\nНажмите 'Играть' чтобы крутить:",
                    "kb": "❤️🤍 <b>Красное/Белое</b>\n\nВыберите ставку:"
                }
                text = game_texts.get(game_type, "Выберите игру")
                keyboard_mapping = {
                    'dice': self.get_dice_keyboard,
                    'darts': self.get_darts_keyboard,
                    'rps': self.get_rps_keyboard,
                    'bowling': self.get_bowling_keyboard,
                    'basketball': self.get_basketball_keyboard,
                    'football': self.get_football_keyboard,
                    'slots': self.get_slots_keyboard,
                    'kb': self.get_kb_keyboard
                }
                keyboard_func = keyboard_mapping.get(game_type)
                if keyboard_func:
                    keyboard = keyboard_func()
                else:
                    keyboard = self.get_games_keyboard()
                await callback.message.answer(
                    text=text,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Error in select_game: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.callback_query(F.data.startswith("bet_"))
        async def place_bet(callback: CallbackQuery, state: FSMContext):
            try:
                await callback.answer()
                data = callback.data.split("_")
                if len(data) < 2:
                    await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка в данных")
                    return
                game_type = data[1]
                user_choice = data[2] if len(data) > 2 else None
                if game_type not in [gt.value for gt in GameType]:
                    await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Неизвестная игра")
                    return
                await state.update_data(
                    game_type=game_type,
                    user_choice=user_choice
                )
                user = self.db.get_or_create_user(
                    telegram_id=callback.from_user.id,
                    username=callback.from_user.username or "",
                    first_name=callback.from_user.first_name or ""
                )
                if not user:
                    await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден", show_alert=True)
                    return
                real_balance = user.get('balance', 0.0)
                bonus_balance = user.get('bonus_balance', 0.0)
                game_names = {
                    'dice': '🎲 Кубик',
                    'darts': '🎯 Дартс',
                    'rps': '🤜🏻 КНБ',
                    'bowling': '🎳 Боулинг',
                    'basketball': '🏀 Баскетбол',
                    'football': '⚽ Футбол',
                    'slots': '🎰 Слоты',
                    'kb': '❤️🤍 Красное/Белое'
                }
                game_name = game_names.get(game_type, game_type)
                if bonus_balance > 0 and user.get('has_deposited'):
                    await callback.message.answer_photo(
                        photo=Config.PHOTOS["bet"],
                        caption=Config.MESSAGES['use_bonus_balance'].format(
                            bonus_balance=bonus_balance,
                            real_balance=real_balance
                        ),
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [
                                InlineKeyboardButton(text="🎁 Бонусный", callback_data=f"bet_source_bonus_{game_type}_{user_choice}"),
                                InlineKeyboardButton(text="💰 Реальный", callback_data=f"bet_source_real_{game_type}_{user_choice}")
                            ],
                            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"game_{game_type}")]
                        ]),
                        parse_mode=ParseMode.HTML
                    )
                    await state.set_state(UserStates.waiting_bet_source)
                else:
                    await callback.message.answer_photo(
                        photo=Config.PHOTOS["bet"],
                        caption=f"🎮 <b>{game_name}</b>\n\nВведите сумму ставки (₽):",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"game_{game_type}")]
                        ]),
                        parse_mode=ParseMode.HTML
                    )
                    await state.set_state(UserStates.waiting_bet_amount)
                    await state.update_data(use_bonus=False)
            except Exception as e:
                logger.error(f"Error in place_bet: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.callback_query(F.data.startswith("bet_source_"))
        async def select_bet_source(callback: CallbackQuery, state: FSMContext):
            try:
                await callback.answer()
                data = callback.data.split("_")
                if len(data) < 5:
                    logger.error(f"Invalid bet_source data: {callback.data}")
                    await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка в данных", show_alert=True)
                    return
                source = data[2]
                game_type = data[3]
                user_choice = data[4]
                use_bonus = (source == 'bonus')
                logger.info(f"Bet source selected: user={callback.from_user.id}, game={game_type}, "
                           f"choice={user_choice}, use_bonus={use_bonus}")
                await state.update_data(
                    game_type=game_type,
                    user_choice=user_choice,
                    use_bonus=use_bonus
                )
                game_names = {
                    'dice': '🎲 Кубик',
                    'darts': '🎯 Дартс',
                    'rps': '🤜🏻 КНБ',
                    'bowling': '🎳 Боулинг',
                    'basketball': '🏀 Баскетбол',
                    'football': '⚽ Футбол',
                    'slots': '🎰 Слоты',
                    'kb': '❤️🤍 Красное/Белое'
                }
                game_name = game_names.get(game_type, game_type)
                source_text = "бонусного" if use_bonus else "реального"
                try:
                    await callback.message.delete()
                except:
                    pass
                await callback.message.answer_photo(
                    photo=Config.PHOTOS["bet"],
                    caption=f"🎮 <b>{game_name}</b>\n\nВведите сумму ставки из {source_text} баланса (₽):\n\n"
                           f"<i>Минимальная ставка: {Config.MIN_BET} ₽\n"
                           f"Максимальная ставка: {Config.MAX_BET} ₽</i>",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"game_{game_type}")]
                    ]),
                    parse_mode=ParseMode.HTML
                )
                await state.set_state(UserStates.waiting_bet_amount)
            except Exception as e:
                logger.error(f"Error in select_bet_source: {e}", exc_info=True)
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.message(UserStates.waiting_bet_amount)
        async def process_bet_amount(message: Message, state: FSMContext):
            try:
                user = self.db.get_or_create_user(
                    telegram_id=message.from_user.id,
                    username=message.from_user.username or "",
                    first_name=message.from_user.first_name or ""
                )
                if not user:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден")
                    await state.clear()
                    return
                try:
                    bet_amount = float(message.text)
                    if bet_amount < Config.MIN_BET:
                        await message.answer(
                            Config.MESSAGES['min_bet'].format(min=Config.MIN_BET)
                        )
                        return
                    if bet_amount > Config.MAX_BET:
                        await message.answer(
                            Config.MESSAGES['max_bet'].format(max=Config.MAX_BET)
                        )
                        return
                except ValueError:
                    await message.answer(Config.MESSAGES['invalid_amount'])
                    return
                user_data = await state.get_data()
                game_type = user_data.get('game_type')
                user_choice = user_data.get('user_choice')
                use_bonus = user_data.get('use_bonus', False)
                if not game_type:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Сначала выберите игру")
                    await state.clear()
                    return
                if use_bonus:
                    balance = user.get('bonus_balance', 0.0)
                    balance_type = "бонусного"
                else:
                    balance = user.get('balance', 0.0)
                    balance_type = "реального"
                if balance < bet_amount:
                    await message.answer(f"<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Недостаточно средств на {balance_type} балансе")
                    await state.clear()
                    return
                self.last_bets[message.from_user.id] = {
                    'game_type': game_type,
                    'bet_amount': bet_amount,
                    'user_choice': user_choice,
                    'use_bonus': use_bonus,
                    'timestamp': datetime.now()
                }
                success = self.db.update_user_balance(
                    user_id=user['id'],
                    amount=-bet_amount,
                    transaction_type='bet',
                    description=f"Ставка в {game_type}: {user_choice or 'без выбора'}",
                    is_bonus=use_bonus
                )
                if not success:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при списании средств")
                    await state.clear()
                    return
                self.db.update_bonus_turnover(user['id'], bet_amount, use_bonus)
                session_id = str(uuid.uuid4())
                self.db.create_game_session(
                    session_id=session_id,
                    user_id=user['id'],
                    game_type=game_type,
                    bet_amount=bet_amount,
                    user_choice=user_choice,
                    is_bonus_bet=use_bonus
                )
                result_text = ""
                win_amount = 0.0
                coefficient = 1.0
                bot_choice = ""
                if game_type in ['dice', 'darts', 'basketball', 'football']:
                    await message.answer("🎲 Бросаю...")
                    dice_result = await self.real_game.throw_real_dice(
                        self.bot, message.chat.id, game_type
                    )
                    real_value = dice_result['value']
                    game_result = self.real_game.check_game_result(
                        game_type, real_value, user_choice
                    )
                    result_text = game_result['result_text']
                    win = game_result['win']
                    coefficient = game_result['coefficient']
                    bot_choice = game_result['bot_choice']
                    win_amount = bet_amount * coefficient if win else 0
                    if coefficient == 1.0 and game_type in ['darts', 'basketball', 'football']:
                        win_amount = bet_amount
                elif game_type == 'bowling':
                    await message.answer("🎳 Бросаю шар...")
                    dice_result = await self.real_game.throw_real_dice(
                        self.bot, message.chat.id, game_type
                    )
                    real_value = dice_result['value']
                    game_result = self.real_game.check_game_result(
                        game_type, real_value, user_choice
                    )
                    result_text = game_result['result_text']
                    win = game_result['win']
                    coefficient = game_result['coefficient']
                    bot_choice = game_result['bot_choice']
                    win_amount = bet_amount * coefficient if win else 0
                    if user_choice is None and not win:
                        win_amount = 0
                elif game_type == 'slots':
                    await message.answer("🎰 Кручу барабаны...")
                    dice_result = await self.real_game.throw_real_dice(
                        self.bot, message.chat.id, game_type
                    )
                    real_value = dice_result['value']
                    game_result = self.real_game.check_game_result(
                        game_type, real_value, user_choice
                    )
                    result_text = game_result['result_text']
                    win = game_result['win']
                    coefficient = game_result['coefficient']
                    bot_choice = game_result['bot_choice']
                    win_amount = bet_amount * coefficient if win else 0
                elif game_type == 'rps':
                    await message.answer("🤜🏻 Играю...")
                    game_result = self.real_game.check_game_result(game_type, 0, user_choice)
                    result_text = game_result['result_text']
                    win = game_result['win']
                    coefficient = game_result['coefficient']
                    bot_choice = game_result['bot_choice']
                    win_amount = bet_amount * coefficient if win else 0
                    if coefficient == 1.0:
                        win_amount = bet_amount
                elif game_type == 'kb':
                    await message.answer("❤️🤍 Выбираю...")
                    game_result = self.real_game.check_game_result(game_type, 0, user_choice)
                    result_text = game_result['result_text']
                    win = game_result['win']
                    coefficient = game_result['coefficient']
                    bot_choice = game_result['bot_choice']
                    win_amount = bet_amount * coefficient if win else 0
                result = 'win' if win_amount > 0 else 'loss'
                if coefficient == 1.0 and win_amount == bet_amount:
                    result = 'draw'
                self.db.update_game_result(
                    session_id=session_id,
                    bot_choice=bot_choice,
                    result=result,
                    win_amount=win_amount,
                    coefficient=coefficient
                )
                if win_amount > 0:
                    if win_amount == bet_amount:
                        transaction_type = 'refund'
                        description = "Возврат ставки"
                    else:
                        transaction_type = 'win'
                        description = f"Выигрыш в {game_type}"
                    if use_bonus and win_amount > bet_amount:
                        net_win = win_amount - bet_amount
                        self.db.update_user_balance(
                            user_id=user['id'],
                            amount=net_win,
                            transaction_type=transaction_type,
                            description=description + " (из бонуса)",
                            is_bonus=False
                        )
                    else:
                        self.db.update_user_balance(
                            user_id=user['id'],
                            amount=win_amount,
                            transaction_type=transaction_type,
                            description=description,
                            is_bonus=use_bonus
                        )
                updated_user = self.db.get_or_create_user(
                    telegram_id=message.from_user.id,
                    username=message.from_user.username or "",
                    first_name=message.from_user.first_name or ""
                )
                new_balance = updated_user.get('balance', 0.0)
                new_bonus_balance = updated_user.get('bonus_balance', 0.0)
                bet_name = self._get_choice_name(game_type, user_choice)
                if result == 'win':
                    message_text = self._format_game_result_message(
                        'win',
                        bet_name=bet_name,
                        coefficient=coefficient,
                        bet_amount=bet_amount,
                        balance=new_bonus_balance if use_bonus else new_balance
                    )
                    if use_bonus:
                        message_text += f"\n\n<tg-emoji emoji-id='5409048419211682843'>🎁</tg-emoji> <b>Бонусный баланс:</b> {new_bonus_balance:.2f} <tg-emoji emoji-id='5231449120635370684'>💰</tg-emoji>"
                    photo = Config.PHOTOS["win"]
                elif result == 'draw':
                    message_text = self._format_game_result_message(
                        'draw',
                        bet_name=bet_name,
                        bet_amount=bet_amount,
                        balance=new_bonus_balance if use_bonus else new_balance
                    )
                    if use_bonus:
                        message_text += f"\n\n<tg-emoji emoji-id='5409048419211682843'>🎁</tg-emoji> <b>Бонусный баланс:</b> {new_bonus_balance:.2f} ₽"
                    photo = Config.PHOTOS["draw"]
                else:
                    message_text = self._format_game_result_message(
                        'loss',
                        bet_name=bet_name,
                        bet_amount=bet_amount,
                        balance=new_bonus_balance if use_bonus else new_balance
                    )
                    if use_bonus:
                        message_text += f"\n\n<tg-emoji emoji-id='5409048419211682843'>🎁</tg-emoji> <b>Бонусный баланс:</b> {new_bonus_balance:.2f} ₽"
                    photo = Config.PHOTOS["lose"]
                await message.answer_photo(
                    photo=photo,
                    caption=message_text,
                    reply_markup=self.get_play_again_keyboard(message.from_user.id, use_bonus),
                    parse_mode=ParseMode.HTML
                )
                await state.clear()
            except Exception as e:
                logger.error(f"Error in process_bet_amount: {e}", exc_info=True)
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Произошла ошибка при обработке ставки")
                await state.clear()
        
        @self.router.callback_query(F.data == "repeat_last_bet")
        async def repeat_last_bet(callback: CallbackQuery):
            try:
                await callback.answer()
                user_id = callback.from_user.id
                if user_id not in self.last_bets:
                    await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> У вас нет последней ставки", show_alert=True)
                    return
                last_bet = self.last_bets[user_id]
                if (datetime.now() - last_bet['timestamp']).seconds > 3600:
                    del self.last_bets[user_id]
                    await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Последняя ставка устарела", show_alert=True)
                    return
                user = self.db.get_or_create_user(
                    telegram_id=user_id,
                    username=callback.from_user.username or "",
                    first_name=callback.from_user.first_name or ""
                )
                if not user:
                    await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден", show_alert=True)
                    return
                game_type = last_bet['game_type']
                bet_amount = last_bet['bet_amount']
                user_choice = last_bet['user_choice']
                use_bonus = last_bet.get('use_bonus', False)
                if use_bonus:
                    balance = user.get('bonus_balance', 0.0)
                else:
                    balance = user.get('balance', 0.0)
                if balance < bet_amount:
                    await callback.answer(
                        Config.MESSAGES['insufficient_balance'], 
                        show_alert=True
                    )
                    return
                message_response = await callback.message.answer(
                    "🔄 <b>Повторная ставка</b>\n\n⏳ Обрабатываю...",
                    parse_mode="HTML"
                )
                success = self.db.update_user_balance(
                    user_id=user['id'],
                    amount=-bet_amount,
                    transaction_type='bet',
                    description=f"Повторная ставка в {game_type}: {user_choice or 'без выбора'}",
                    is_bonus=use_bonus
                )
                if not success:
                    await message_response.edit_text("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при списании средств")
                    return
                self.db.update_bonus_turnover(user['id'], bet_amount, use_bonus)
                session_id = str(uuid.uuid4())
                self.db.create_game_session(
                    session_id=session_id,
                    user_id=user['id'],
                    game_type=game_type,
                    bet_amount=bet_amount,
                    user_choice=user_choice,
                    is_bonus_bet=use_bonus
                )
                result_text = ""
                win_amount = 0.0
                coefficient = 1.0
                bot_choice = ""
                if game_type in ['dice', 'darts', 'basketball', 'football', 'bowling', 'slots']:
                    dice_result = await self.real_game.throw_real_dice(
                        self.bot, callback.message.chat.id, game_type
                    )
                    real_value = dice_result['value']
                    game_result = self.real_game.check_game_result(
                        game_type, real_value, user_choice
                    )
                    result_text = game_result['result_text']
                    win = game_result['win']
                    coefficient = game_result['coefficient']
                    bot_choice = game_result['bot_choice']
                    win_amount = bet_amount * coefficient if win else 0
                    if coefficient == 1.0 and game_type in ['darts', 'basketball', 'football']:
                        win_amount = bet_amount
                elif game_type == 'rps':
                    game_result = self.real_game.check_game_result(game_type, 0, user_choice)
                    result_text = game_result['result_text']
                    win = game_result['win']
                    coefficient = game_result['coefficient']
                    bot_choice = game_result['bot_choice']
                    win_amount = bet_amount * coefficient if win else 0
                    if coefficient == 1.0:
                        win_amount = bet_amount
                elif game_type == 'kb':
                    game_result = self.real_game.check_game_result(game_type, 0, user_choice)
                    result_text = game_result['result_text']
                    win = game_result['win']
                    coefficient = game_result['coefficient']
                    bot_choice = game_result['bot_choice']
                    win_amount = bet_amount * coefficient if win else 0
                result = 'win' if win_amount > 0 else 'loss'
                if coefficient == 1.0 and win_amount == bet_amount:
                    result = 'draw'
                self.db.update_game_result(
                    session_id=session_id,
                    bot_choice=bot_choice,
                    result=result,
                    win_amount=win_amount,
                    coefficient=coefficient
                )
                if win_amount > 0:
                    if win_amount == bet_amount:
                        transaction_type = 'refund'
                        description = "Возврат ставки (повтор)"
                    else:
                        transaction_type = 'win'
                        description = f"Выигрыш в {game_type} (повтор)"
                    if use_bonus and win_amount > bet_amount:
                        net_win = win_amount - bet_amount
                        self.db.update_user_balance(
                            user_id=user['id'],
                            amount=net_win,
                            transaction_type=transaction_type,
                            description=description + " (из бонуса)",
                            is_bonus=False
                        )
                    else:
                        self.db.update_user_balance(
                            user_id=user['id'],
                            amount=win_amount,
                            transaction_type=transaction_type,
                            description=description,
                            is_bonus=use_bonus
                        )
                self.last_bets[user_id]['timestamp'] = datetime.now()
                updated_user = self.db.get_or_create_user(
                    telegram_id=user_id,
                    username=callback.from_user.username or "",
                    first_name=callback.from_user.first_name or ""
                )
                new_balance = updated_user.get('balance', 0.0)
                new_bonus_balance = updated_user.get('bonus_balance', 0.0)
                bet_name = self._get_choice_name(game_type, user_choice)
                if result == 'win':
                    message_text = self._format_game_result_message(
                        'win',
                        bet_name=bet_name,
                        coefficient=coefficient,
                        bet_amount=bet_amount,
                        balance=new_bonus_balance if use_bonus else new_balance
                    )
                    if use_bonus:
                        message_text += f"\n\n<tg-emoji emoji-id='5409048419211682843'>🎁</tg-emoji> <b>Бонусный баланс:</b> {new_bonus_balance:.2f} ₽"
                    photo = Config.PHOTOS["win"]
                elif result == 'draw':
                    message_text = self._format_game_result_message(
                        'draw',
                        bet_name=bet_name,
                        bet_amount=bet_amount,
                        balance=new_bonus_balance if use_bonus else new_balance
                    )
                    if use_bonus:
                        message_text += f"\n\n<tg-emoji emoji-id='5409048419211682843'>🎁</tg-emoji> <b>Бонусный баланс:</b> {new_bonus_balance:.2f} ₽"
                    photo = Config.PHOTOS["draw"]
                else:
                    message_text = self._format_game_result_message(
                        'loss',
                        bet_name=bet_name,
                        bet_amount=bet_amount,
                        balance=new_bonus_balance if use_bonus else new_balance
                    )
                    if use_bonus:
                        message_text += f"\n\n<tg-emoji emoji-id='5409048419211682843'>🎁</tg-emoji> <b>Бонусный баланс:</b> {new_bonus_balance:.2f} ₽"
                    photo = Config.PHOTOS["lose"]
                await message_response.delete()
                await callback.message.answer_photo(
                    photo=photo,
                    caption=message_text,
                    reply_markup=self.get_play_again_keyboard(user_id, use_bonus),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Error in repeat_last_bet: {e}", exc_info=True)
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при повторении ставки", show_alert=True)
        
        @self.router.callback_query(F.data == "deposit")
        async def deposit_menu(callback: CallbackQuery):
            if self.db.is_user_banned(user_id):
                await message.answer(
                    "<tg-emoji emoji-id='5240241223632954241'>🚫</tg-emoji> <b>Вы были заблокированы</b>\n\n"
                    "<tg-emoji emoji-id='5397782960512444700'>📌</tg-emoji> Если вы считаете это ошибкой обратитесь к админу @casinomayami\n\n"
                    "<tg-emoji emoji-id='5440660757194744323'>‼️</tg-emoji>Просим вас не писать каких-либо отзывов и не начинать публичное обсуждение "
                    "до окончания разбирательства. Нам важно поддерживать порядок на площадке, "
                    "но, к сожалению, добиться этого без блокировок не представляется возможным.",
                    parse_mode=ParseMode.HTML
                )
                return
            try:
                await callback.answer()
                user = self.db.get_or_create_user(
                    telegram_id=callback.from_user.id,
                    username=callback.from_user.username or "",
                    first_name=callback.from_user.first_name or ""
                )
                if not user:
                    await callback.message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден")
                    return
                text = Config.MESSAGES['deposit_menu'].format(
                    balance=user.get('balance', 0.0),
                    bonus_balance=user.get('bonus_balance', 0.0),
                    min_deposit=Config.MIN_DEPOSIT,
                    max_deposit=Config.MAX_DEPOSIT
                )
                await callback.message.answer_photo(
                    photo=Config.PHOTOS["deposit"],
                    caption=text,
                    reply_markup=self.get_deposit_keyboard(),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Error in deposit_menu: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.callback_query(F.data.startswith("deposit_amount:"))
        async def deposit_amount(callback: CallbackQuery):
            try:
                await callback.answer()
                amount_str = callback.data.split(":")[1]
                amount = float(amount_str)
                await self.process_deposit(callback.message, amount)
            except ValueError:
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Неверная сумма", show_alert=True)
            except Exception as e:
                logger.error(f"Error in deposit_amount: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.callback_query(F.data == "deposit_custom")
        async def deposit_custom(callback: CallbackQuery, state: FSMContext):
            try:
                await callback.answer()
                await callback.message.answer(
                    text="💎 Введите сумму депозита в рублях:",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="◀️ Назад", callback_data="deposit")]
                    ]),
                    parse_mode=ParseMode.HTML
                )
                await state.set_state(UserStates.waiting_deposit_amount)
            except Exception as e:
                logger.error(f"Error in deposit_custom: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.message(UserStates.waiting_deposit_amount)
        async def process_custom_deposit(message: Message, state: FSMContext):
            try:
                user = self.db.get_or_create_user(
                    telegram_id=message.from_user.id,
                    username=message.from_user.username or "",
                    first_name=message.from_user.first_name or ""
                )
                try:
                    amount = float(message.text)
                    if amount < Config.MIN_DEPOSIT:
                        await message.answer(
                            Config.MESSAGES['min_deposit'].format(min=Config.MIN_DEPOSIT)
                        )
                        return
                    if amount > Config.MAX_DEPOSIT:
                        await message.answer(
                            Config.MESSAGES['max_deposit'].format(max=Config.MAX_DEPOSIT)
                        )
                        return
                except ValueError:
                    await message.answer(Config.MESSAGES['invalid_amount'])
                    return
                await self.process_deposit(message, amount)
                await state.clear()
            except Exception as e:
                logger.error(f"Error in process_custom_deposit: {e}", exc_info=True)
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при создании депозита")
                await state.clear()
        
        @self.router.callback_query(F.data == "withdraw")
        async def withdraw_menu(callback: CallbackQuery):
            if self.db.is_user_banned(user_id):
                await message.answer(
                    "<tg-emoji emoji-id='5240241223632954241'>🚫</tg-emoji> <b>Вы были заблокированы</b>\n\n"
                    "<tg-emoji emoji-id='5397782960512444700'>📌</tg-emoji> Если вы считаете это ошибкой обратитесь к админу @casinomayami\n\n"
                    "<tg-emoji emoji-id='5440660757194744323'>‼️</tg-emoji>Просим вас не писать каких-либо отзывов и не начинать публичное обсуждение "
                    "до окончания разбирательства. Нам важно поддерживать порядок на площадке, "
                    "но, к сожалению, добиться этого без блокировок не представляется возможным.",
                    parse_mode=ParseMode.HTML
                )
                return
            try:
                await callback.answer()
                user = self.db.get_or_create_user(
                    telegram_id=callback.from_user.id,
                    username=callback.from_user.username or "",
                    first_name=callback.from_user.first_name or ""
                )
                if not user:
                    await callback.message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден")
                    return
                text = Config.MESSAGES['withdraw_menu'].format(
                    balance=user.get('balance', 0.0),
                    bonus_balance=user.get('bonus_balance', 0.0),
                    min_withdraw=Config.MIN_WITHDRAWAL,
                    max_withdraw=Config.MAX_WITHDRAWAL
                )
                await callback.message.answer_photo(
                    photo=Config.PHOTOS["withdraw"],
                    caption=text,
                    reply_markup=self.get_withdraw_keyboard(),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Error in withdraw_menu: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.callback_query(F.data.startswith("withdraw_amount:"))
        async def withdraw_amount(callback: CallbackQuery):
            try:
                await callback.answer()
                amount_str = callback.data.split(":")[1]
                amount = float(amount_str)
                await self.process_withdrawal(callback.message, amount)
            except ValueError:
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Неверная сумма", show_alert=True)
            except Exception as e:
                logger.error(f"Error in withdraw_amount: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.callback_query(F.data == "withdraw_custom")
        async def withdraw_custom(callback: CallbackQuery, state: FSMContext):
            try:
                await callback.answer()
                await callback.message.answer(
                    text="📤 Введите сумму вывода в рублях:",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="◀️ Назад", callback_data="withdraw")]
                    ]),
                    parse_mode=ParseMode.HTML
                )
                await state.set_state(UserStates.waiting_withdraw_amount)
            except Exception as e:
                logger.error(f"Error in withdraw_custom: {e}")
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка", show_alert=True)
        
        @self.router.message(UserStates.waiting_withdraw_amount)
        async def process_custom_withdraw(message: Message, state: FSMContext):
            try:
                user = self.db.get_or_create_user(
                    telegram_id=message.from_user.id,
                    username=message.from_user.username or "",
                    first_name=message.from_user.first_name or ""
                )
                if not user:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден")
                    await state.clear()
                    return
                try:
                    amount = float(message.text)
                    if amount < Config.MIN_WITHDRAWAL:
                        await message.answer(
                            Config.MESSAGES['min_withdrawal'].format(min=Config.MIN_WITHDRAWAL)
                        )
                        return
                    if amount > Config.MAX_WITHDRAWAL:
                        await message.answer(
                            Config.MESSAGES['max_withdrawal'].format(max=Config.MAX_WITHDRAWAL)
                        )
                        return
                    balance = user.get('balance', 0.0)
                    if balance < amount:
                        await message.answer(Config.MESSAGES['insufficient_balance'])
                        return
                except ValueError:
                    await message.answer(Config.MESSAGES['invalid_amount'])
                    return
                await self.process_withdrawal(message, amount)
                await state.clear()
            except Exception as e:
                logger.error(f"Error in process_custom_withdraw: {e}", exc_info=True)
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при создании вывода")
                await state.clear()
        
        @self.router.callback_query(F.data.startswith("check_invoice:"))
        async def check_invoice(callback: CallbackQuery):
            try:
                await callback.answer()
                invoice_id = callback.data.split(":", 1)[1]
                invoice_db = self.db.get_invoice_by_id(invoice_id)
                if not invoice_db:
                    await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Инвойс не найден", show_alert=True)
                    return
                invoice_info = await self.crypto_api.get_invoice(invoice_id)
                if not invoice_info:
                    await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Не удалось получить статус", show_alert=True)
                    return
                status = invoice_info.get('status')
                if status == 'paid' and invoice_db['status'] != 'paid':
                    success = self.db.update_user_balance(
                        user_id=invoice_db['user_id'],
                        amount=invoice_db['amount_rub'],
                        transaction_type='deposit',
                        description=f"Крипто-депозит (инвойс {invoice_id})"
                    )
                    if success:
                        self.db.update_invoice_status(invoice_id, 'paid')
                        updated_user = self.db.get_or_create_user(telegram_id=callback.from_user.id)
                        new_balance = updated_user.get('balance', 0.0)
                        await callback.message.answer(
                            Config.MESSAGES['deposit_success'].format(
                                amount=invoice_db['amount_rub'],
                                new_balance=new_balance
                            ),
                            parse_mode=ParseMode.HTML
                        )
                        await callback.answer("<tg-emoji emoji-id='5206607081334906820'>✅</tg-emoji> Оплата подтверждена!", show_alert=True)
                    else:
                        await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при зачислении средств", show_alert=True)
                else:
                    self.db.update_invoice_status(invoice_id, status)
                    status_texts = {
                        'active': '⏳ Ожидает оплаты',
                        'expired': '❌ Инвойс просрочен',
                        'cancelled': '❌ Инвойс отменён'
                    }
                    await callback.answer(
                        status_texts.get(status, f"⚠️ Статус: {status}"),
                        show_alert=True
                    )
            except Exception as e:
                logger.error(f"Error in check_invoice: {e}", exc_info=True)
                await callback.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при проверке оплаты", show_alert=True)
        
        @self.router.message(F.content_type.in_({'text'}))
        async def handle_all_messages(message: Message):
            try:
                logger.info(f"Получено сообщение от пользователя {message.from_user.id}")
                if hasattr(message, 'reply_markup') and message.reply_markup:
                    if hasattr(message.reply_markup, 'inline_keyboard') and message.reply_markup.inline_keyboard:
                        logger.info(f"Сообщение содержит инлайн-кнопки, количество: {len(message.reply_markup.inline_keyboard)}")
                        check_codes = []
                        for row in message.reply_markup.inline_keyboard:
                            for button in row:
                                if hasattr(button, 'url') and button.url:
                                    url = button.url
                                    logger.info(f"Найдена кнопка с URL: {url[:100]}")
                                    check_code = CryptoReceiptParser.extract_check_code_from_url(url)
                                    if check_code:
                                        logger.info(f"Найден код чека в URL кнопки: {check_code}")
                                        if check_code not in check_codes:
                                            check_codes.append(check_code)
                        if check_codes:
                            user = self.db.get_or_create_user(
                                telegram_id=message.from_user.id,
                                username=message.from_user.username or "",
                                first_name=message.from_user.first_name or ""
                            )
                            if not user:
                                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден")
                                return
                            processed_codes = []
                            for check_code in check_codes:
                                existing_check = self.db.get_check_by_code(check_code)
                                if existing_check and existing_check['status'] in ['credited', 'activated', 'processing']:
                                    logger.info(f"Чек {check_code} уже в обработке")
                                    continue
                                check_saved = self.db.save_crypto_check(
                                    check_code=check_code,
                                    user_id=user['id'],
                                    estimated_amount=0.0,
                                    currency="USDT"
                                )
                                if check_saved:
                                    processed_codes.append(check_code)
                                    logger.info(f"Чек {check_code} сохранен для обработки")
                            if processed_codes:
                                codes_text = "\n".join([f"• {code[:10]}..." for code in processed_codes])
                                if len(processed_codes) == 1:
                                    response_text = (
                                        f"✅ <b>Обнаружен чек в кнопке!</b>\n\n"
                                        f"🆔 <b>Код:</b> {processed_codes[0][:10]}...\n\n"
                                        f"⏳ <b>Активирую чек...</b>\n"
                                        f"Сумма будет определена и зачислена в течение 1-2 минут."
                                    )
                                else:
                                    response_text = (
                                        f"✅ <b>Обнаружены чеки в кнопках!</b>\n\n"
                                        f"📋 <b>Коды:</b>\n{codes_text}\n\n"
                                        f"📊 <b>Количество:</b> {len(processed_codes)}\n\n"
                                        f"⏳ <b>Активирую чеки...</b>\n"
                                        f"Суммы будут определены и зачислены в течение 1-2 минут."
                                    )
                                await message.answer(response_text, parse_mode=ParseMode.HTML)
                                for check_code in processed_codes:
                                    check_info = self.db.get_check_by_code(check_code)
                                    if check_info:
                                        task = asyncio.create_task(self.process_check_activation(check_info))
                                        self.background_tasks.add(task)
                                        task.add_done_callback(self.background_tasks.discard)
                                return
                if message.text:
                    logger.info(f"Проверяем текст сообщения на наличие чеков: {message.text[:100]}")
                    if message.text.startswith('/'):
                        return
                    parser = CryptoReceiptParser()
                    check_info = parser.parse_check_info_from_user_message(message.text)
                    if check_info:
                        check_code = check_info['check_code']
                        estimated_amount = check_info.get('estimated_amount', 0.0)
                        currency = check_info.get('currency', 'USDT')
                        user = self.db.get_or_create_user(
                            telegram_id=message.from_user.id,
                            username=message.from_user.username or "",
                            first_name=message.from_user.first_name or ""
                        )
                        if not user:
                            await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден")
                            return
                        existing_check = self.db.get_check_by_code(check_code)
                        if existing_check and existing_check['status'] in ['credited', 'activated', 'processing']:
                            await message.answer(
                                "✅ <b>Этот чек уже в обработке</b>\n\n"
                                "⏳ Чек уже находится в очереди на активацию.\n"
                                "Средства будут зачислены в ближайшее время.",
                                parse_mode=ParseMode.HTML
                            )
                            return
                        check_saved = self.db.save_crypto_check(
                            check_code=check_code,
                            user_id=user['id'],
                            estimated_amount=estimated_amount,
                            currency=currency
                        )
                        if check_saved:
                            if estimated_amount > 0:
                                response_text = Config.MESSAGES['check_detected'].format(
                                    amount=estimated_amount,
                                    currency=currency,
                                    check_code=check_code[:10] + "..."
                                )
                            else:
                                response_text = (
                                    f"✅ <b>Чек обнаружен!</b>\n\n"
                                    f"🆔 <b>Код:</b> {check_code[:10]}...\n"
                                    f"💰 <b>Сумма:</b> будет определена после активации\n\n"
                                    f"⏳ <b>Активирую чек...</b>\n"
                                    f"Сумма будет определена и зачислена в течение 1-2 минут."
                                )
                            await message.answer(response_text, parse_mode=ParseMode.HTML)
                            check_info_db = self.db.get_check_by_code(check_code)
                            if check_info_db:
                                task = asyncio.create_task(self.process_check_activation(check_info_db))
                                self.background_tasks.add(task)
                                task.add_done_callback(self.background_tasks.discard)
                        return
                    keywords = ['чек', 'check', 'CryptoBot', '/start']
                    if any(keyword in message.text.lower() for keyword in ['чек', 'check']):
                        possible_codes = re.findall(r'([A-Za-z0-9_\-]{10,})', message.text)
                        if possible_codes:
                            for code in possible_codes:
                                if len(code) >= 10 and re.match(r'^[A-Za-z]', code):
                                    logger.info(f"Найден возможный код чека: {code}")
                                    await message.answer(
                                        f"🔍 <b>Найден возможный код чека:</b> {code[:10]}...\n\n"
                                        f"Если это действительно чек CryptoBot, активируйте его командой:\n"
                                        f"<code>/activate {code}</code>",
                                        parse_mode=ParseMode.HTML
                                    )
                                    break
            except Exception as e:
                logger.error(f"Ошибка при обработке сообщения: {e}", exc_info=True)
        
        @self.router.message(Command("mychecks"))
        async def cmd_my_checks(message: Message):
            try:
                user = self.db.get_or_create_user(
                    telegram_id=message.from_user.id,
                    username=message.from_user.username or "",
                    first_name=message.from_user.first_name or ""
                )
                if not user:
                    await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден")
                    return
                checks = self.db.get_user_checks(user['id'])
                if not checks:
                    await message.answer(
                        "<tg-emoji emoji-id='5294087731134082941'>📝</tg-emoji> <b>Мои чеки</b>\n\n"
                        "У вас еще нет обработанных чеков.\n"
                        "Перешлите мне чек от @CryptoBot, и я его активирую!",
                        parse_mode=ParseMode.HTML
                    )
                    return
                text = "<tg-emoji emoji-id='5294087731134082941'>📝</tg-emoji> <b>Мои чеки</b>\n\n"
                for check in checks:
                    status_emoji = {
                        'pending': '⏳',
                        'activated': '✅',
                        'credited': '💰',
                        'failed': '❌'
                    }.get(check['status'], '❓')
                    date_str = datetime.strptime(check['created_at'], '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')
                    if check['status'] == 'credited':
                        amount_text = f"{check['credited_amount']} {check['currency']}"
                    elif check['real_amount'] > 0:
                        amount_text = f"{check['real_amount']} {check['currency']}"
                    else:
                        amount_text = f"{check['estimated_amount']} {check['currency']} (ожидание)"
                    text += (
                        f"{status_emoji} <code>{check['check_code'][:8]}...</code>\n"
                        f"   💰 {amount_text}\n"
                        f"   📅 {date_str}\n"
                        f"   📊 {check['status']}\n\n"
                    )
                text += f"\n<tg-emoji emoji-id='5445355530111437729'>📤</tg-emoji> Всего чеков: {len(checks)}"
                await message.answer(text, parse_mode=ParseMode.HTML)
            except Exception as e:
                logger.error(f"Error in /mychecks: {e}")
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при получении информации о чеках")
        
        @self.router.message(Command("allchecks"))
        async def cmd_all_checks(message: Message):
            if message.from_user.id not in Config.ADMIN_IDS:
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> У вас нет прав для этой команды")
                return
            try:
                with self.db.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT cc.*, u.telegram_id, u.username 
                        FROM crypto_checks cc
                        JOIN users u ON cc.user_id = u.id
                        ORDER BY cc.created_at DESC
                        LIMIT 20
                    """)
                    checks = cursor.fetchall()
                    if not checks:
                        await message.answer("❌ Нет чеков в базе данных")
                        return
                    text = "📊 <b>Все чеки (последние 20)</b>\n\n"
                    for check in checks:
                        status_emoji = {
                            'pending': '⏳',
                            'activated': '✅',
                            'credited': '💰',
                            'failed': '❌'
                        }.get(check['status'], '❓')
                        date_str = datetime.strptime(check['created_at'], '%Y-%m-%d %H:%M:%S').strftime('%d.%m %H:%M')
                        amount = check['real_amount'] or check['estimated_amount'] or 0
                        text += (
                            f"{status_emoji} <code>{check['check_code'][:10]}...</code>\n"
                            f"   👤 @{check['username'] or check['telegram_id']}\n"
                            f"   💰 {amount} {check['currency']}\n"
                            f"   📅 {date_str} | {check['status']}\n\n"
                        )
                    await message.answer(text, parse_mode=ParseMode.HTML)
            except Exception as e:
                logger.error(f"Error in /allchecks: {e}")
                await message.answer("❌ Ошибка при получении информации о чеках")
        
        @self.router.message(Command("activate"))
        async def cmd_activate_check(message: Message):
            if message.from_user.id not in Config.ADMIN_IDS:
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> У вас нет прав для этой команды")
                return
            try:
                parts = message.text.split()
                if len(parts) != 2:
                    await message.answer(
                        "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> <b>Использование:</b>\n"
                        "<code>/activate код_чека</code>\n\n"
                        "Пример: <code>/activate CQytSkuCbbp7</code>",
                        parse_mode="HTML"
                    )
                    return
                check_code = parts[1]
                check_info = self.db.get_check_by_code(check_code)
                if not check_info:
                    await message.answer(f"❌ Чек с кодом <code>{check_code}</code> не найден", parse_mode="HTML")
                    return
                await message.answer(f"⏳ Активирую чек <code>{check_code}</code>...", parse_mode="HTML")
                await self.process_check_activation(check_info)
                await message.answer(f"✅ Чек <code>{check_code}</code> обработан", parse_mode="HTML")
            except Exception as e:
                logger.error(f"Error in /activate: {e}")
                await message.answer("❌ Ошибка при активации чека")
        
        @self.router.message(Command("demo"))
        async def cmd_demo(message: Message):
            await message.answer("🎮 <b>Демо реальных игр Telegram</b>\n\nОтправляю все типы эмодзи...")
            games = [
                ("🎲 Кубик", "dice"),
                ("🎯 Дартс", "darts"),
                ("🏀 Баскетбол", "basketball"),
                ("⚽ Футбол", "football"),
                ("🎳 Боулинг", "bowling"),
                ("🎰 Слоты", "slots")
            ]
            for game_name, game_type in games:
                try:
                    dice_result = await self.real_game.throw_real_dice(
                        self.bot, message.chat.id, game_type
                    )
                    await message.answer(
                        f"{game_name}\n"
                        f"Значение: {dice_result['value']}\n"
                        f"Эмодзи: {dice_result['emoji']}"
                    )
                    await asyncio.sleep(1.5)
                except Exception as e:
                    logger.error(f"Error in demo for {game_type}: {e}")
                    await message.answer(f"❌ Ошибка при демо {game_name}")
            await message.answer("✅ Демо завершено! Все игры используют реальные случайные значения от Telegram.")
        
        @self.router.message()
        async def handle_other_messages(message: Message):
            if message.text and message.text.startswith('/'):
                await message.answer(
                    "<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Неизвестная команда. Используйте /start"
                )
        
        @self.router.callback_query(F.data.startswith("withdraw_"))
        async def handle_withdrawal_callback(callback: CallbackQuery, state: FSMContext):
            try:
                data = callback.data.split("_")
                if len(data) < 3:
                    await callback.answer("❌ Ошибка в данных", show_alert=True)
                    return
                
                action = data[1]
                withdrawal_id = data[2]
                
                if action == "approve":
                    await callback.answer()
                    withdrawal = self.db.get_withdrawal_request(withdrawal_id)
                    if not withdrawal:
                        await callback.message.answer("❌ Заявка на вывод не найдена")
                        return
                    
                    # СОХРАНЯЕМ ДАННЫЕ СРАЗУ
                    self.admin_withdrawal_context[callback.from_user.id] = {
                        'withdrawal_id': withdrawal_id,
                        'user_id': withdrawal['telegram_id'],
                        'amount': withdrawal['amount'],
                        'username': withdrawal['username'] or withdrawal['first_name']
                    }
                    
                    # УСТАНАВЛИВАЕМ СОСТОЯНИЕ
                    await state.set_state(AdminStates.waiting_admin_check)
                    
                    success = self.db.update_withdrawal_status(
                        withdrawal_id=withdrawal_id,
                        status='approved',
                        admin_id=callback.from_user.id
                    )
                    
                    if success:
                        await callback.message.edit_text(
                            f"✅ <b>Заявка на вывод одобрена!</b>\n\n"
                            f"👤 Пользователь: {withdrawal['username'] or withdrawal['first_name']} (ID: {withdrawal['telegram_id']})\n"
                            f"💰 Сумма: {withdrawal['amount']:.2f} ₽\n"
                            f"📅 Дата: {withdrawal['created_at']}\n"
                            f"🆔 ID вывода: <code>{withdrawal_id}</code>\n\n"
                            f"⚠️ <b>Теперь отправьте чек пользователю</b>\n"
                            f"<code>/sendcheck 123456789 ...айди-вывода... ...ссылка начек...</code>",
                            parse_mode=ParseMode.HTML
                        )
                        
                        try:
                            await self.bot.send_message(
                                chat_id=withdrawal['telegram_id'],
                                text=Config.MESSAGES['withdrawal_approved'].format(
                                    amount=withdrawal['amount']
                                ),
                                parse_mode=ParseMode.HTML
                            )
                        except Exception as e:
                            logger.error(f"Error notifying user: {e}")
                    
                    else:
                        await callback.message.answer("❌ Ошибка при одобрении заявки")
                
                elif action == "reject":
                    await callback.answer()
                    withdrawal = self.db.get_withdrawal_request(withdrawal_id)
                    if not withdrawal:
                        await callback.message.answer("❌ Заявка на вывод не найдена")
                        return
                    
                    success = self.db.update_withdrawal_status(
                        withdrawal_id=withdrawal_id,
                        status='rejected',
                        admin_id=callback.from_user.id,
                        admin_message="Отклонено администратором"
                    )
                    
                    if success:
                        await callback.message.edit_text(
                            f"❌ <b>Заявка на вывод отклонена</b>\n\n"
                            f"👤 Пользователь: {withdrawal['username'] or withdrawal['first_name']}\n"
                            f"💰 Сумма: {withdrawal['amount']:.2f} ₽\n"
                            f"📅 Дата: {withdrawal['created_at']}",
                            parse_mode=ParseMode.HTML
                        )
                        
                        try:
                            user_info = self.db.get_or_create_user(telegram_id=withdrawal['telegram_id'])
                            await self.bot.send_message(
                                chat_id=withdrawal['telegram_id'],
                                text=Config.MESSAGES['withdrawal_rejected'].format(
                                    amount=withdrawal['amount'],
                                    balance=user_info.get('balance', 0.0)
                                ),
                                parse_mode=ParseMode.HTML
                            )
                        except Exception as e:
                            logger.error(f"Error notifying user: {e}")
                    
                    else:
                        await callback.message.answer("❌ Ошибка при отклонении заявки")
                
                elif action == "complete":
                    await callback.answer()
                    withdrawal = self.db.get_withdrawal_request(withdrawal_id)
                    if not withdrawal:
                        await callback.message.answer("❌ Заявка на вывод не найдена")
                        return
                    
                    if callback.from_user.id not in self.admin_withdrawal_context:
                        await callback.message.answer("❌ Нет активной заявки для завершения")
                        return
                    
                    context = self.admin_withdrawal_context[callback.from_user.id]
                    if context['withdrawal_id'] != withdrawal_id:
                        await callback.message.answer("❌ Неверная заявка")
                        return
                    
                    success = self.db.update_withdrawal_status(
                        withdrawal_id=withdrawal_id,
                        status='completed',
                        admin_id=callback.from_user.id,
                        admin_message="Выполнено"
                    )
                    
                    if success:
                        del self.admin_withdrawal_context[callback.from_user.id]
                        await callback.message.edit_text(
                            f"✅ <b>Вывод средств завершен!</b>\n\n"
                            f"👤 Пользователь: {withdrawal['username'] or withdrawal['first_name']}\n"
                            f"💰 Сумма: {withdrawal['amount']:.2f} ₽\n"
                            f"📅 Дата: {withdrawal['created_at']}",
                            parse_mode=ParseMode.HTML
                        )
                        
                        try:
                            user_info = self.db.get_or_create_user(telegram_id=withdrawal['telegram_id'])
                            await self.bot.send_message(
                                chat_id=withdrawal['telegram_id'],
                                text=Config.MESSAGES['withdrawal_completed'].format(
                                    amount=withdrawal['amount'],
                                    balance=user_info.get('balance', 0.0)
                                ),
                                parse_mode=ParseMode.HTML
                            )
                        except Exception as e:
                            logger.error(f"Error notifying user: {e}")
                    
                    else:
                        await callback.message.answer("❌ Ошибка при завершении вывода")
            
            except Exception as e:
                logger.error(f"Error in handle_withdrawal_callback: {e}", exc_info=True)
                await callback.answer("❌ Произошла ошибка", show_alert=True)
        
        @self.router.message(AdminStates.waiting_admin_check)
        async def handle_admin_check_message(message: Message, state: FSMContext):
            try:
                user_id = message.from_user.id
                
                if user_id not in self.admin_withdrawal_context:
                    await message.answer("❌ Нет активной заявки на вывод")
                    await state.clear()
                    return
                
                context = self.admin_withdrawal_context[user_id]
                withdrawal_id = context['withdrawal_id']
                target_user_id = context['user_id']
                amount = context['amount']
                
                # 1️⃣ Отправляем сообщение пользователю
                try:
                    await self.bot.send_message(
                        chat_id=target_user_id,
                        text=f"💰 <b>Чек по вашему выводу</b>\n\n"
                             f"Сумма: {amount:.2f} ₽\n\n"
                             f"<i>Сообщение от администратора:</i>\n"
                             f"{message.text}"
                    )
                except Exception as e:
                    logger.error(f"Error sending message to user: {e}")
                    await message.answer(f"❌ Не удалось отправить сообщение пользователю: {e}")
                    return
                
                # 2️⃣ Обновляем статус заявки
                success = self.db.update_withdrawal_status(
                    withdrawal_id=withdrawal_id,
                    status='completed',
                    admin_id=user_id,
                    admin_message=message.text
                )
                
                if not success:
                    await message.answer("⚠️ Сообщение отправлено, но статус заявки не обновился")
                    await state.clear()
                    return
                
                # 3️⃣ Получаем баланс пользователя и отправляем финальное уведомление
                user_info = self.db.get_or_create_user(telegram_id=target_user_id)
                balance = user_info.get("balance", 0.0)
                
                await self.bot.send_message(
                    chat_id=target_user_id,
                    text=Config.MESSAGES['withdrawal_completed'].format(
                        amount=amount,
                        balance=balance
                    ),
                    parse_mode=ParseMode.HTML
                )
                
                # 4️⃣ Уведомляем админа
                await message.answer("✅ Чек отправлен пользователю, вывод завершён")
                
                # 5️⃣ Удаляем контекст и очищаем состояние
                del self.admin_withdrawal_context[user_id]
                await state.clear()

            except Exception as e:
                logger.error(f"Error in handle_admin_check_message: {e}", exc_info=True)
                await message.answer("❌ Ошибка при отправке сообщения пользователю")
                await state.clear()

    
    async def process_deposit(self, message, amount: float):
        try:
            user = self.db.get_or_create_user(
                telegram_id=message.from_user.id,
                username=message.from_user.username or "",
                first_name=message.from_user.first_name or ""
            )
            if not user:
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден")
                return
            amount_usdt = CurrencyConverter.rub_to_usdt(amount)
            invoice = await self.crypto_api.create_invoice(
                asset="USDT",
                amount=amount_usdt,
                description=f"Депозит для пользователя {user['id']}"
            )
            if not invoice:
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Не удалось создать счет. Попробуйте позже.")
                return
            self.db.create_crypto_invoice(
                invoice_id=invoice['invoice_id'],
                user_id=user['id'],
                asset="USDT",
                amount=amount_usdt,
                amount_rub=amount,
                pay_url=invoice['pay_url']
            )
            text = f"""
💎 <b>Счет на оплату</b>

Сумма: {amount} ₽ ({amount_usdt:.2f} USDT)
Срок действия: 1 час
ID инвойса: {invoice['invoice_id']}

Для оплаты нажмите кнопку ниже ⬇️

После оплаты нажмите кнопку "🔄 Проверить" для мгновенного зачисления.
            """
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="💳 Оплатить в Crypto Bot", 
                    url=invoice['pay_url']
                )],
                [
                    InlineKeyboardButton(
                        text="🔄 Проверить оплату", 
                        callback_data=f"check_invoice:{invoice['invoice_id']}"
                    ),
                    InlineKeyboardButton(
                        text="❌ Отмена", 
                        callback_data="deposit"
                    )
                ]
            ])
            await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Error in process_deposit: {e}", exc_info=True)
            await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при создании депозита")
    
    async def process_withdrawal(self, message, amount: float):
        try:
            user = self.db.get_or_create_user(
                telegram_id=message.from_user.id,
                username=message.from_user.username or "",
                first_name=message.from_user.first_name or ""
            )
            if not user:
                await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Профиль не найден")
                return
            
            withdrawal_id = self.db.create_withdrawal_request(user['id'], amount)
            
            if not withdrawal_id:
                await message.answer(Config.MESSAGES['insufficient_balance'])
                return
            
            text = Config.MESSAGES['withdrawal_request_created'].format(amount=amount)
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
                [InlineKeyboardButton(text="🎮 Играть", callback_data="play")]
            ])
            
            await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
            
            for admin_id in Config.ADMIN_IDS:
                try:
                    date_str = datetime.now().strftime('%d.%m.%Y %H:%M')
                    admin_text = Config.MESSAGES['withdrawal_admin_notification'].format(
                        username=user.get('username', user.get('first_name', 'Пользователь')),
                        user_id=user['telegram_id'],
                        amount=amount,
                        date=date_str
                    )
                    
                    admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [
                            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"withdraw_approve_{withdrawal_id}"),
                            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"withdraw_reject_{withdrawal_id}")
                        ]
                    ])
                    
                    await self.bot.send_message(
                        chat_id=admin_id,
                        text=admin_text,
                        reply_markup=admin_keyboard,
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    logger.error(f"Error notifying admin {admin_id}: {e}")
                
        except Exception as e:
            logger.error(f"Error in process_withdrawal: {e}", exc_info=True)
            await message.answer("<tg-emoji emoji-id='5465665476971471368'>❌</tg-emoji> Ошибка при создании вывода")
    
    def get_main_keyboard(self):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎮 Играть", callback_data="play")],
            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
            [InlineKeyboardButton(text="🎁 Бонусы", callback_data="bonus_info")],
            [
                InlineKeyboardButton(text="👨‍💻 Поддержка", url=Config.SUPPORT_LINK),
                InlineKeyboardButton(text="📢 Новости", url=Config.NEWS_CHANNEL)
            ]
        ])
    
    def get_profile_keyboard(self):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💰 Пополнить", callback_data="deposit")],
            [InlineKeyboardButton(text="📤 Вывод", callback_data="withdraw")],
            [InlineKeyboardButton(text="🎁 Бонусы", callback_data="bonus_info")],
            [InlineKeyboardButton(text="🎮 Играть", callback_data="play")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu")]
        ])
    
    def get_games_keyboard(self):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Кубик", callback_data="game_dice")],
            [InlineKeyboardButton(text="🎯 Дартс", callback_data="game_darts")],
            [InlineKeyboardButton(text="🤜🏻 КНБ", callback_data="game_rps")],
            [InlineKeyboardButton(text="🎳 Боулинг", callback_data="game_bowling")],
            [InlineKeyboardButton(text="🏀 Баскетбол", callback_data="game_basketball")],
            [InlineKeyboardButton(text="⚽️ Футбол", callback_data="game_football")],
            [InlineKeyboardButton(text="🎰 Слоты", callback_data="game_slots")],
            [InlineKeyboardButton(text="❤️🤍 КБ", callback_data="game_kb")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu")]
        ])
    
    def get_dice_keyboard(self):
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Нечет | 2x", callback_data="bet_dice_odd"),
                InlineKeyboardButton(text="Чет | 2x", callback_data="bet_dice_even")
            ],
            [
                InlineKeyboardButton(text="1 | 3x", callback_data="bet_dice_1"),
                InlineKeyboardButton(text="2 | 3x", callback_data="bet_dice_2"),
                InlineKeyboardButton(text="3 | 3x", callback_data="bet_dice_3")
            ],
            [
                InlineKeyboardButton(text="4 | 3x", callback_data="bet_dice_4"),
                InlineKeyboardButton(text="5 | 3x", callback_data="bet_dice_5"),
                InlineKeyboardButton(text="6 | 3x", callback_data="bet_dice_6")
            ],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="play")]
        ])
    
    def get_darts_keyboard(self):
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Дартс мимо | 2,5x", callback_data="bet_darts_miss"),
                InlineKeyboardButton(text="Дартс центр | 2,5x", callback_data="bet_darts_center")
            ],
            [
                InlineKeyboardButton(text="Дартс красное | 2x", callback_data="bet_darts_red"),
                InlineKeyboardButton(text="Дартс белое | 2x", callback_data="bet_darts_white")
            ],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="play")]
        ])
    
    def get_rps_keyboard(self):
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Камень | 3x", callback_data="bet_rps_rock"),
                InlineKeyboardButton(text="Ножницы | 3x", callback_data="bet_rps_scissors"),
                InlineKeyboardButton(text="Бумага | 3x", callback_data="bet_rps_paper")
            ],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="play")]
        ])
    
    def get_bowling_keyboard(self):
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🎳 Все кегли | 2.5x", callback_data="bet_bowling_strike"),
                InlineKeyboardButton(text="🎳 Мимо | 2.5x", callback_data="bet_bowling_miss")
            ],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="play")]
        ])
    
    def get_basketball_keyboard(self):
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Баскетбол гол | 2x", callback_data="bet_basketball_goal"),
                InlineKeyboardButton(text="Баскетбол застрял | 2x", callback_data="bet_basketball_stuck")
            ],
            [
                InlineKeyboardButton(text="Баскетбол мимо | 2x", callback_data="bet_basketball_miss"),
                InlineKeyboardButton(text="Баскетбол чистый | 3x", callback_data="bet_basketball_clean")
            ],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="play")]
        ])
    
    def get_football_keyboard(self):
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Футбол гол | 1,5x", callback_data="bet_football_goal"),
                InlineKeyboardButton(text="Футбол мимо | 1,5x", callback_data="bet_football_miss")
            ],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="play")]
        ])
    
    def get_slots_keyboard(self):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎰 Крутить барабаны!", callback_data="bet_slots")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="play")]
        ])
    
    def get_kb_keyboard(self):
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="❤️ Красное | 1,5x", callback_data="bet_kb_red"),
                InlineKeyboardButton(text="🤍 Белое | 1,5x", callback_data="bet_kb_white")
            ],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="play")]
        ])
    
    def get_deposit_keyboard(self):
        amounts = [1000, 5000, 10000, 50000, 100000]
        buttons = []
        for i in range(0, len(amounts), 2):
            row = []
            for j in range(2):
                if i + j < len(amounts):
                    row.append(InlineKeyboardButton(
                        text=f"{amounts[i+j]} ₽", 
                        callback_data=f"deposit_amount:{amounts[i+j]}"
                    ))
            if row:
                buttons.append(row)
        buttons.append([InlineKeyboardButton(text="💰 Другая сумма", callback_data="deposit_custom")])
        buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="profile")])
        return InlineKeyboardMarkup(inline_keyboard=buttons)
    
    def get_withdraw_keyboard(self):
        amounts = [5000, 10000, 50000, 100000, 500000]
        buttons = []
        for i in range(0, len(amounts), 2):
            row = []
            for j in range(2):
                if i + j < len(amounts):
                    row.append(InlineKeyboardButton(
                        text=f"{amounts[i+j]} ₽", 
                        callback_data=f"withdraw_amount:{amounts[i+j]}"
                    ))
            if row:
                buttons.append(row)
        buttons.append([InlineKeyboardButton(text="💰 Другая сумма", callback_data="withdraw_custom")])
        buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="profile")])
        return InlineKeyboardMarkup(inline_keyboard=buttons)
    
    def get_play_again_keyboard(self, user_id: int = None, use_bonus: bool = False):
        buttons = []
        if user_id and user_id in self.last_bets:
            last_bet = self.last_bets[user_id]
            if (datetime.now() - last_bet['timestamp']).seconds < 3600:
                buttons.append([
                    InlineKeyboardButton(
                        text="🔁 Повторить ставку", 
                        callback_data="repeat_last_bet"
                    )
                ])
        if use_bonus:
            buttons.append([InlineKeyboardButton(text="🎮 Играть с бонусом", callback_data="play_with_bonus")])
        else:
            buttons.append([InlineKeyboardButton(text="🎮 Играть еще", callback_data="play")])
        buttons.extend([
            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
            [InlineKeyboardButton(text="🎁 Бонусы", callback_data="bonus_info")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
        ])
        return InlineKeyboardMarkup(inline_keyboard=buttons)
    
    async def start(self):
        try:
            await self.start_background_tasks()
            logger.info("✅ Casino Bot запущен!")
            logger.info(f"👤 ID администратора: {Config.ADMIN_IDS}")
            logger.info("✅ Парсер чеков CryptoBot активирован")
            logger.info("✅ Telethon клиент подключен")
            logger.info(f"✅ Бонусная система активирована")
            logger.info(f"🎯 Вейджеринг бонусов: x{Config.BONUS_WAGERING_REQUIREMENT}")
            logger.info(f"💰 Минимальный депозит для бонусов: {Config.MIN_DEPOSIT_FOR_BONUS} ₽")
            logger.info(f"🎮 Доступные игры: 8 (все с реальными бросками)")
            logger.info(f"💾 База данных: {Config.DB_PATH}")
            logger.info(f"💰 Курс обмена: 1 USDT = {Config.EXCHANGE_RATE} ₽")
            await self.dp.start_polling(
                self.bot, 
                allowed_updates=self.dp.resolve_used_update_types(),
                skip_updates=True
            )
        except Exception as e:
            logger.error(f"Failed to start bot: {e}", exc_info=True)
            raise
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        logger.info("Cleaning up resources...")
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self.background_tasks.clear()
        await self.telethon_client.disconnect()
        await self.crypto_api.close()
        logger.info("Cleanup completed")

async def main():
    try:
        bot = CasinoBot()
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        logger.info("Bot shutdown complete")

if __name__ == "__main__":
    if Config.TELEGRAM_API_ID == 123456:
        logger.warning("⚠️  TELEGRAM_API_ID не настроен!")
    if Config.TELEGRAM_API_HASH == "ваш_api_hash":
        logger.warning("⚠️  TELEGRAM_API_HASH не настроен!")
    if Config.TELEGRAM_PHONE == "+79991234567":
        logger.warning("⚠️  TELEGRAM_PHONE не настроен!")
    asyncio.run(main())