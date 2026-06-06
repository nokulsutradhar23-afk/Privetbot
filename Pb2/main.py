import requests , os  , sys , jwt , pickle , json , binascii , time , urllib3 , base64 , datetime , re , socket , threading , ssl , pytz , aiohttp
import math
from protobuf_decoder.protobuf_decoder import Parser
from KX import * ; from autoup import *
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from Pb2 import DEcwHisPErMsG_pb2 , MajoRLoGinrEq_pb2 ,MajoRLoGinrEs_pb2 , PorTs_pb2 , sQ_pb2 , kyro_title_pb2 , clan_pb2
from cfonts import render, say
import asyncio
import multiprocessing
import signal
import sys
import traceback
import google.protobuf.json_format as json_format
import random
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from collections import OrderedDict
from asyncio import Lock


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  

online_writer = None
whisper_writer = None
SHOW_RAW_DATA = False   
DECRYPT_DATA = False
import re
from bs4 import BeautifulSoup
import dev_generator_pb2
import devxt_count_pb2
import threading
import asyncio
import queue
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import binascii
spammer_uid = None
spam_chat_id = None
spam_uid = None
Spy = False
Chat_Leave = False
fast_spam_running = False
fast_spam_task = None
custom_spam_running = False
custom_spam_task = None
spam_request_running = False
spam_request_task = None
evo_fast_spam_running = False
evo_fast_spam_task = None
evo_custom_spam_running = False
evo_custom_spam_task = None
emote_hijack = False
lag_running = False
lag_task = None
status_response_cache = {}  
pending_status_requests = {}
room_info_cache = {}
last_status_packet = None
insquad = None 
joining_team = False 
online_writer = None 
whisper_writer = None 
last_bot_status_check = 0
bot_status_cache_time = 120  
cached_bot_status = None
last_status_packet = None
auto_start_running = False
auto_start_teamcode = None
stop_auto = False
auto_start_task = None
room_invite_spam_running = False
room_invite_spam_task = None
fast_start_spam_duration = 8
fast_wait_after_match = 3
fast_start_spam_delay = 0.3  
PACKET_DELAY_ULTRA_FAST = 0.5
SPAM_REQUESTS = 100
BADGE_REQUESTS = 5    
last_0500_info = {}   # will contain {'idT': ..., 'squad_code': ...}
    
BOT_NAME = "NAYAN 1M"
region = 'ind'
REGION_BASE_URLS = {
    "IND": "https://client.ind.freefiremobile.com/",
    "ID": "https://clientbp.ggpolarbear.com/",
    "BR": "https://client.us.freefiremobile.com/",
    "ME": "https://clientbp.common.ggbluefox.com/",
    "VN": "https://clientbp.ggpolarbear.com/",
    "TH": "https://clientbp.common.ggbluefox.com/",
    "CIS": "https://clientbp.ggpolarbear.com/",
    "BD": "https://clientbp.ggpolarbear.com/",
    "PK": "https://clientbp.ggpolarbear.com/",
    "SG": "https://clientbp.ggpolarbear.com/",
    "SAC": "https://client.us.freefiremobile.com/",
    "TW": "https://clientbp.ggpolarbear.com/"
}


def get_base_url(region: str) -> str:
    return REGION_BASE_URLS.get(region.upper(), "https://client.ind.freefiremobile.com/")
CURRENT_BOT_UID = None

OWNER_UID = "6861053275"
equip_emote_id = 909000001

WHITELISTED_UIDS = {
    OWNER_UID,
}
WHITELIST_ONLY = False
BLOCKED_NAMES = ["nayan1m", "ongkar", "classy", "ONGKAR", "NAYAN", "CLASSY","raj", "raj here", "RAJ HERE", "DEVANSH", "RAJ DEVANSH", "NAYAN 1M", "n@yan", "na@yan", "nayan", "CLASSY","raj", "raj here", "RAJ HERE", "DEVANSH", "RAJ DEVANSH"] 
cache = {}
cache_lock = asyncio.Lock() 
player_cache = {}
friend_cache = {}
friend_cache_lock = Lock()
GEMINI_API_KEY = "AIzaSyC2lTKj3mEnuv4dq-uNqFS4nHWMCzIw6sw"
GEMINI_MODEL = "gemini-1.5-flash" 
chat_histories = {}
chat_history_lock = asyncio.Lock()
MAX_HISTORY = 1000  
LOGIN_METHOD = None 
LOGIN_TYPE = None
evo_cycle_tasks = {}  
manager = multiprocessing.Manager()
status_response_cache = manager.dict()            
ttt_games = {}

# ========== Load config from bot_config.json (if exists) ========
CONFIG_FILE = "bot_config.json"
if os.path.exists(CONFIG_FILE):
    try:
        with open(CONFIG_FILE, "r") as f:
            config = json.load(f)
        LOGIN_METHOD = config.get("login_method", "accs")      # "accs" or "direct"
        LOGIN_TYPE = config.get("login_type", 1)               # 1 = mobile, 2 = pc
    except:
        pass
# ========================================================

class RestartBot(Exception):
    pass

async def stop_all_spam_tasks():
    global fast_spam_running, fast_spam_task, custom_spam_running, custom_spam_task
    global spam_request_running, spam_request_task, evo_fast_spam_running, evo_fast_spam_task
    global evo_custom_spam_running, evo_custom_spam_task
    global room_invite_spam_running, room_invite_spam_task, lag_running, lag_task
    global evo_cycle_tasks, msg_spam_running
    global auto_start_running, auto_start_task, stop_auto

    fast_spam_running = False
    custom_spam_running = False
    spam_request_running = False
    evo_fast_spam_running = False
    evo_custom_spam_running = False
    room_invite_spam_running = False
    lag_running = False
    msg_spam_running = False
    auto_start_running = False
    stop_auto = True

    tasks_to_cancel = []
    if fast_spam_task and not fast_spam_task.done():
        tasks_to_cancel.append(fast_spam_task)
    if custom_spam_task and not custom_spam_task.done():
        tasks_to_cancel.append(custom_spam_task)
    if spam_request_task and not spam_request_task.done():
        tasks_to_cancel.append(spam_request_task)
    if evo_fast_spam_task and not evo_fast_spam_task.done():
        tasks_to_cancel.append(evo_fast_spam_task)
    if evo_custom_spam_task and not evo_custom_spam_task.done():
        tasks_to_cancel.append(evo_custom_spam_task)
    if room_invite_spam_task and not room_invite_spam_task.done():
        tasks_to_cancel.append(room_invite_spam_task)
    if lag_task and not lag_task.done():
        tasks_to_cancel.append(lag_task)
    if auto_start_task and not auto_start_task.done():
        tasks_to_cancel.append(auto_start_task)

    for task in evo_cycle_tasks.values():
        if not task.done():
            tasks_to_cancel.append(task)
    evo_cycle_tasks.clear()

    for task in tasks_to_cancel:
        task.cancel()
    if tasks_to_cancel:
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        
evo_emotes = {
    "1": "909000063",   
    "2": "909000068",   
    "3": "909000075",   
    "4": "909040010",   
    "5": "909000081",   
    "6": "909039011",   
    "7": "909000085",   
    "8": "909000090",   
    "9": "909000098",   
    "10": "909035007",  
    "11": "909042008",  
    "12": "909041005",  
    "13": "909033001",  
    "14": "909038010",  
    "15": "909038012",  
    "16": "909045001",  
    "17": "909049010",  
    "18": "909051003"  
}
EMOTE_MAP = {
    1: 909000063,
    2: 909000081,
    3: 909000075,
    4: 909000085,
    5: 909000134,
    6: 909000098,
    7: 909035007,
    8: 909051012,
    9: 909000141,
    10: 909034008,
    11: 909051015,
    12: 909041002,
    13: 909039004,
    14: 909042008,
    15: 909051014,
    16: 909039012,
    17: 909040010,
    18: 909035010,
    19: 909041005,
    20: 909051003,
    21: 909034001
}
BADGE_VALUES = {
    "s1": 1048576,    
    "s2": 32768,      
    "s3": 2048,      
    "s4": 64,         
    "s5": 262144     
}

def titles():
    titles_list = [
        905090075, 904990072, 904990069, 905190079
    ]
    return titles_list  
   
def _fmt_ts(ts):
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    
def load_token():
    try:
        with open("token.json", "r") as f:
            data = json.load(f)
            return data.get("token")
    except Exception as e:
        print("Token load error:", e)
        return None
           
def get_random_sticker():

    sticker_packs = [
        ("1200000001", 1, 24),
        ("1200000002", 1, 15),
        ("1200000004", 1, 13),
    ]

    pack_id, start, end = random.choice(sticker_packs)
    sticker_no = random.randint(start, end)

    return f"[1={pack_id}-{sticker_no}]"               

def create_credentials_template():
    filename = "accs.txt"
   
    if os.path.exists(filename):
        print(f"📁 {filename} already exists")
        return True
    
    json_template = {
        "4870750605": "A9A27888F2CAFED604B02214FD54E0CC2CFBD1AC7B0973836490DDB61FD1668A",
    }
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(json_template, f, indent=2)
        
        print(f"📝 Created {filename} in JSON format")
        print("✅ RECOMMENDED JSON FORMAT:")
        print("   {\"UID\": \"PASSWORD\", \"UID2\": \"PASSWORD2\"}")
        print("\n✏️ Edit the file with your actual credentials")
        return True
        
    except Exception as e:
        print(f"❌ Could not create JSON template: {e}")
        
        template = """# DEVIL Free Fire Bot Credentials
# Choose ONE format (JSON recommended):

# FORMAT 1: JSON (RECOMMENDED)
{"4870750605": "A9A27888F2CAFED604B02214FD54E0CC2CFBD1AC7B0973836490DDB61FD1668A"}

# FORMAT 2: Comma-separated
# uid=4870750605,password=A9A27888F2CAFED604B02214FD54E0CC2CFBD1AC7B0973836490DDB61FD1668A

# FORMAT 3: Line-separated  
# uid: 4870750605
# password: A9A27888F2CAFED604B02214FD54E0CC2CFBD1AC7B0973836490DDB61FD1668A
"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(template)
            print(f"📝 Created {filename} with all format examples")
            return True
        except Exception as e2:
            print(f"❌ Failed to create any template: {e2}")
            return False
    
def load_all_accounts_from_file(filename="accs.txt"):
    try:
        if not os.path.exists(filename):
            return []
        
        with open(filename, 'r', encoding='utf-8') as f:
            content = json.load(f)
        
        accounts = []
        for uid, password in content.items():
            if uid and password:
                accounts.append((uid, password))
        
        print(f"📦 Loaded {len(accounts)} accounts from {filename}")
        return accounts
        
    except Exception as e:
        print(f"❌ Error loading all accounts: {e}")
        return []

def get_random_account(filename="accs.txt"):
    accounts = load_all_accounts_from_file(filename)
    if accounts:
        return random.choice(accounts)
    return None, None    
    
async def build_get_backpack_proto(character_id: int) -> bytes:
    def encode_varint(value: int) -> bytes:
        result = []
        while True:
            byte = value & 0x7F
            value >>= 7
            if value:
                result.append(byte | 0x80)
            else:
                result.append(byte)
                break
        return bytes(result)
    return bytes([0x08]) + encode_varint(character_id)
    
def _encode_varint(value: int) -> bytes:
    out = []
    while True:
        b = value & 0x7F
        value >>= 7
        if value:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)

def _encode_length_delimited(field_num: int, data: bytes | str) -> bytes:
    if isinstance(data, str):
        data = data.encode('utf-8')
    tag = (field_num << 3) | 2
    return _encode_varint(tag) + _encode_varint(len(data)) + data

def _encode_varint_field(field_num: int, value: int) -> bytes:
    tag = (field_num << 3) | 0
    return _encode_varint(tag) + _encode_varint(value)
        
def encrypt_id(num: int) -> str:
    x = float(num)
    x = x / 128
    if x > 128:
        x = x / 128
        if x > 128:
            x = x / 128
            if x > 128:
                x = x / 128
                strx = int(x)
                y = (x - strx) * 128
                stry = int(y)
                z = (y - stry) * 128
                strz = int(z)
                n = (z - strz) * 128
                strn = int(n)
                m = (n - strn) * 128
                return dec[int(m)] + dec[int(n)] + dec[int(z)] + dec[int(y)] + x_list[int(x)]
            else:
                strx = int(x)
                y = (x - strx) * 128
                stry = int(y)
                z = (y - stry) * 128
                strz = int(z)
                n = (z - strz) * 128
                strn = int(n)
                return dec[int(n)] + dec[int(z)] + dec[int(y)] + x_list[int(x)]
    return None
    
da = 'f2212101'
dec = ['80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '8a', '8b', '8c', '8d', '8e', '8f', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99', '9a', '9b', '9c', '9d', '9e', '9f', 'a0', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'aa', 'ab', 'ac', 'ad', 'ae', 'af', 'b0', 'b1', 'b2', 'b3', 'b4', 'b5', 'b6', 'b7', 'b8', 'b9', 'ba', 'bb', 'bc', 'bd', 'be', 'bf', 'c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'ca', 'cb', 'cc', 'cd', 'ce', 'cf', 'd0', 'd1', 'd2', 'd3', 'd4', 'd5', 'd6', 'd7', 'd8', 'd9', 'da', 'db', 'dc', 'dd', 'de', 'df', 'e0', 'e1', 'e2', 'e3', 'e4', 'e5', 'e6', 'e7', 'e8', 'e9', 'ea', 'eb', 'ec', 'ed', 'ee', 'ef', 'f0', 'f1', 'f2', 'f3', 'f4', 'f5', 'f6', 'f7', 'f8', 'f9', 'fa', 'fb', 'fc', 'fd', 'fe', 'ff']
x_list = ['1','01', '02', '03', '04', '05', '06', '07', '08', '09', '0a', '0b', '0c', '0d', '0e', '0f', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '1a', '1b', '1c', '1d', '1e', '1f', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '2a', '2b', '2c', '2d', '2e', '2f', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '3a', '3b', '3c', '3d', '3e', '3f', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '4a', '4b', '4c', '4d', '4e', '4f', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '5a', '5b', '5c', '5d', '5e', '5f', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '6a', '6b', '6c', '6d', '6e', '6f', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '7a', '7b', '7c', '7d', '7e', '7f']

def Decrypt_ID(da):
    if da != None and len(da) == 10:
        w = 128
        xxx = len(da)/2 - 1
        xxx = str(xxx)[:1]
        for i in range(int(xxx)-1):
            w = w * 128
        x1 = da[:2]
        x2 = da[2:4]
        x3 = da[4:6]
        x4 = da[6:8]
        x5 = da[8:10]
        return str(w * x_list.index(x5) + (dec.index(x2) * 128) + dec.index(x1) + (dec.index(x3) * 128 * 128) + (dec.index(x4) * 128 * 128 * 128))

    if da != None and len(da) == 8:
        w = 128
        xxx = len(da)/2 - 1
        xxx = str(xxx)[:1]
        for i in range(int(xxx)-1):
            w = w * 128
        x1 = da[:2]
        x2 = da[2:4]
        x3 = da[4:6]
        x4 = da[6:8]
        return str(w * x_list.index(x4) + (dec.index(x2) * 128) + dec.index(x1) + (dec.index(x3) * 128 * 128))
    
    return None

def Encrypt_ID(x):
    x = int(x)
    x = x / 128 
    if x > 128:
        x = x / 128
        if x > 128:
            x = x / 128
            if x > 128:
                x = x / 128
                strx = int(x)
                y = (x - int(strx)) * 128
                stry = str(int(y))
                z = (y - int(stry)) * 128
                strz = str(int(z))
                n = (z - int(strz)) * 128
                strn = str(int(n))
                m = (n - int(strn)) * 128
                return dec[int(m)] + dec[int(n)] + dec[int(z)] + dec[int(y)] + x_list[int(x)]
            else:
                strx = int(x)
                y = (x - int(strx)) * 128
                stry = str(int(y))
                z = (y - int(stry)) * 128
                strz = str(int(z))
                n = (z - int(strz)) * 128
                strn = str(int(n))
                return dec[int(n)] + dec[int(z)] + dec[int(y)] + x_list[int(x)]

def decrypt_api(cipher_text):
    key = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
    iv = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plain_text = unpad(cipher.decrypt(bytes.fromhex(cipher_text)), AES.block_size)
    return plain_text.hex()

def encrypt_api(plain_text):
    plain_text = bytes.fromhex(plain_text)
    key = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
    iv = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])
    cipher = AES.new(key, AES.MODE_CBC, iv)
    cipher_text = cipher.encrypt(pad(plain_text, AES.block_size))
    return cipher_text.hex()
   
def encrypt_message(plaintext_bytes):
    key = b'Yg&tc%DEuh6%Zc^8'
    iv = b'6oyZDr22E3ychjM%'
    cipher = AES.new(key, AES.MODE_CBC, iv)
    padded = pad(plaintext_bytes, AES.block_size)
    encrypted = cipher.encrypt(padded)
    return binascii.hexlify(encrypted).decode('utf-8')        

def create_uid_protobuf(uid):
    msg = dev_generator_pb2.dev_generator()
    msg.saturn_ = int(uid)
    msg.garena = 1
    return msg.SerializeToString()

def enc(uid):
    pb = create_uid_protobuf(uid)
    return encrypt_message(pb)

def decode_player_info(binary):
    info = devxt_count_pb2.xt()
    info.ParseFromString(binary)
    return info    

def load_jwt_token():
    try:
        with open("token.json", "r") as f:
            data = json.load(f)
        token = data.get("token")
        if token:
            print(f"✅ Loaded token: {token[:20]}...")
            return token
        else:
            print("❌ No token found in token.json")
            return None
    except Exception as e:
        print(f"❌ Error loading token: {e}")
        return None
        
async def get_player_info_async(uid, token, region):
    base_url = get_base_url(region)
    url = base_url + "GetPlayerPersonalShow"

    encrypted_uid = enc(uid)
    edata = bytes.fromhex(encrypted_uid)

    headers = {
        'User-Agent': "Dalvik/2.1.0",
        'Connection': "Keep-Alive",
        'Accept-Encoding': "gzip",
        'Authorization': f"Bearer {token}",
        'Content-Type': "application/x-www-form-urlencoded",
        'X-Unity-Version': "2018.4.11f1",
        'X-GA': "v1 1",
        'ReleaseVersion': ob,
    }

    try:
        async with cache_lock:
            if uid in player_cache:
                return player_cache[uid]

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=edata, headers=headers, ssl=False) as response:
                if response.status != 200:
                    return "Unknown"
                content = await response.read()
                info = decode_player_info(content)
                data = json.loads(json_format.MessageToJson(info))
                account = data.get("AccountInfo", {})
                player_name = account.get("PlayerNickname", "Unknown")

        async with cache_lock:
            player_cache[uid] = player_name
        return player_name

    except Exception as e:
        print("❌ Name fetch error:", e)
        return "Unknown"
        
async def get_player_bio(uid: str, token: str, region: str) -> str | None:
    try:
        base_url = get_base_url(region)
        url = base_url + "GetPlayerPersonalShow"

        encoded_uid = await EnC_Uid(str(uid), Tp='Uid')
        plain_hex = f"08{encoded_uid}1007"
        encrypted_hex = await EnC_AEs(plain_hex)
        data = bytes.fromhex(encrypted_hex)

        headers = {
            'X-Unity-Version': '2018.4.11f1',
            'ReleaseVersion': ob,
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-GA': 'v1 1',
            'Authorization': f'Bearer {token}',
            'User-Agent': 'Dalvik/2.1.0',
            'Accept-Encoding': 'gzip'
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=data, ssl=False) as response:
                if response.status not in (200, 201):
                    print(f"Bio fetch failed HTTP {response.status}")
                    return None
                content = await response.read()
                packet_hex = binascii.hexlify(content).decode('utf-8')
                json_data = await DeCode_PackEt(packet_hex)
                KaLLu_data = json.loads(json_data)
                bio = KaLLu_data.get("9", {}).get("data", {}).get("9", {}).get("data")
                return bio if bio else None
    except Exception as e:
        print(f"Error in get_player_bio: {e}")
        return None
        
async def GeT_PLayer_InFo(uid, token, region):
    messages = [] 
    try:
        base_url = get_base_url(region)
        url = base_url + "GetPlayerPersonalShow"

        encoded_uid = await EnC_Uid(str(uid), Tp='Uid')
        plain_hex = f"08{encoded_uid}1007"
        encrypted_hex = await EnC_AEs(plain_hex)
        data = bytes.fromhex(encrypted_hex)

        headers = {
            'X-Unity-Version': '2018.4.11f1',
            'ReleaseVersion': ob,
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-GA': 'v1 1',
            'Authorization': f'Bearer {token}',
            'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 7.1.2; ASUS_Z01QD Build/QKQ1.190825.002)',
            'Accept-Encoding': 'gzip'
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=data, ssl=False) as response:
                if response.status not in (200, 201):
                    return [f"\n[b][c][FFD700]Failed to get player info (HTTP {response.status})!\n"]
                content = await response.read()
                packet_hex = binascii.hexlify(content).decode('utf-8')
                json_data = await DeCode_PackEt(packet_hex)
                KaLLu_data = json.loads(json_data)

                data_field = KaLLu_data.get("1", {}).get("data", {})
                player_info = {
                    'nickname': data_field.get("3", {}).get("data", "N/A"),
                    'accountId': data_field.get("1", {}).get("data", "N/A"),
                    'level': data_field.get("6", {}).get("data", "N/A"),
                    'liked': data_field.get("21", {}).get("data", "N/A"),
                    'socialHighlight': KaLLu_data.get("9", {}).get("data", {}).get("9", {}).get("data", "N/A"),
                    'createAt': data_field.get("44", {}).get("data"),
                    'lastLoginAt': data_field.get("24", {}).get("data")
                }

                player_msg = (
                    f"[C][B]-┌ [FFD700]📱 Player Info:\n"
                    f"[FFFFFF]-├─ Name: [00FF00]{fixnum(player_info['nickname'])}\n"
                    f"[FFFFFF]-├─ UID: [00FF00]{fixnum(player_info['accountId'])}\n"
                    f"[FFFFFF]-├─ Level: [00FF00]{fixnum(str(player_info['level']))}\n"
                    f"[FFFFFF]-├─ Likes: [00FF00]{fixnum(str(player_info['liked']))}\n"
                    f"[FFFFFF]-├─ Bio: [00FF00]{player_info['socialHighlight']}\n"
                    f"[FFFFFF]-├─ Created: [00FF00]{fixnum(_fmt_ts(player_info['createAt']))}\n"
                    f"[FFFFFF]-└─ Last Login: [00FF00]{fixnum(_fmt_ts(player_info['lastLoginAt']))}"
                )
                messages.append(player_msg)

                clan_info = {}
                try:
                    clan_data = KaLLu_data.get("6", {}).get("data", {})
                    clan_info['clanId'] = clan_data.get("1", {}).get("data")
                    clan_info['clanName'] = clan_data.get("2", {}).get("data", "N/A")
                    clan_info['clanLeader'] = clan_data.get("3", {}).get("data")
                    clan_info['clanLevel'] = clan_data.get("4", {}).get("data", "N/A")
                    clan_info['clanMembers'] = clan_data.get("6", {}).get("data", "N/A")
                    leader_data = KaLLu_data.get("7", {}).get("data", {})
                    clan_info['leaderName'] = leader_data.get("3", {}).get("data", "N/A")
                except:
                    clan_info = None

                if clan_info and clan_info.get('clanId'):
                    clan_msg = (
                        f"\n[C][B]-┌ [FFD700]🏰 Clan Info:\n"
                        f"[FFFFFF]-├─ Name: [00FF00]{clan_info['clanName']}\n"
                        f"[FFFFFF]-├─ UID: [00FF00]{fixnum(clan_info['clanId'])}\n"
                        f"[FFFFFF]-├─ Level: [00FF00]{fixnum(str(clan_info['clanLevel']))}\n"
                        f"[FFFFFF]-├─ Members: [00FF00]{fixnum(str(clan_info['clanMembers']))}\n"
                        f"[FFFFFF]-├─ Leader: [00FF00]{clan_info['leaderName']}\n"
                        f"[FFFFFF]-└─ Leader UID: [00FF00]{fixnum(clan_info['clanLeader'])}"
                    )
                else:
                    clan_msg = f"\n[C][B]-┌ [FFD700]🏰 Clan Info:\n[FFFFFF]-└─ [00FF00]No Clan"

                messages.append(clan_msg)
                return messages

    except Exception as e:
        return [f"\n[b][c][FFD700]Error: {str(e)}\n"]
        
async def get_friend_list(token, region):
    base_url = get_base_url(region)
    url = base_url + "GetFriendRequestList"

    payload = b'\x08\x00'
    cipher = AES.new(BIO_ENCRYPTION_KEY, AES.MODE_CBC, BIO_ENCRYPTION_IV)
    padded = payload + b'\x00' * (16 - len(payload) % 16)
    encrypted_payload = cipher.encrypt(padded)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "UnityPlayer/2022.3.47f1",
        "ReleaseVersion": ob,
        "X-Unity-Version": "2022.3.47f1",
        "X-GA": "v1 1",
    }

    cache_key = f"{region}_{token}"
    async with friend_cache_lock:
        if cache_key in friend_cache:
            return friend_cache[cache_key]

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=encrypted_payload, headers=headers, ssl=False) as resp:
                if resp.status != 200:
                    return None
                content = await resp.read()

        def parse_varint(data, offset):
            value = 0
            shift = 0
            i = offset
            while i < len(data):
                byte = data[i]
                value |= (byte & 0x7F) << shift
                shift += 7
                i += 1
                if not (byte & 0x80):
                    break
            return value, i - offset

        def extract_all_uids(data):
            uids = []
            i = 0
            while i < len(data) - 10:
                if data[i] in (0x08, 0x10):
                    val, n = parse_varint(data, i + 1)
                    if 10000000000 < val < 20000000000:
                        uids.append(str(val))
                    i += 1 + n
                else:
                    i += 1
            return uids

        def extract_all_utf8_strings(data):
            strings = []
            current = []
            for b in data:
                if 32 <= b < 127 or b >= 128:
                    current.append(b)
                else:
                    if len(current) >= 3:
                        try:
                            s = bytes(current).decode('utf-8')
                            if s.strip() and len(s.strip()) > 1:
                                strings.append(s)
                        except:
                            pass
                    current = []
            if len(current) >= 3:
                try:
                    s = bytes(current).decode('utf-8')
                    if s.strip() and len(s.strip()) > 1:
                        strings.append(s)
                except:
                    pass
            seen = set()
            unique = []
            for s in strings:
                if s not in seen and s not in ['IND', 'OB53', '80p', '0', '1']:
                    seen.add(s)
                    unique.append(s)
            return unique

        all_uids = extract_all_uids(content)
        all_names = extract_all_utf8_strings(content)

        your_uid = all_uids[0] if all_uids else None
        your_name = all_names[0] if all_names else None
        friend_uids = all_uids[1:] if len(all_uids) > 1 else []
        friend_names = all_names[1:] if len(all_names) > 1 else []

        friends = []
        for i, uid in enumerate(friend_uids):
            name = friend_names[i] if i < len(friend_names) else "Unknown"
            friends.append({"uid": uid, "name": name})

        from collections import OrderedDict
        result = OrderedDict([
            ("Region", region),
            ("YourUID", your_uid),
            ("YourName", your_name),
            ("Total", len(friends)),
            ("Friends", friends)
        ])

        async with friend_cache_lock:
            friend_cache[cache_key] = result
        return result

    except Exception as e:
        print(f"❌ Friend list fetch error: {e}")
        return None
        
async def fetch_clan_members(clan_id: int, token: str, region: str) -> list[str]:
    try:
        def varint(n: int) -> bytes:
            result = bytearray()
            while True:
                b = n & 0x7F
                n >>= 7
                if n:
                    b |= 0x80
                result.append(b)
                if not n:
                    break
            return bytes(result)

        req_bytes = varint(0x08) + varint(clan_id)

        cipher = AES.new(BIO_ENCRYPTION_KEY, AES.MODE_CBC, BIO_ENCRYPTION_IV)
        encrypted = cipher.encrypt(pad(req_bytes, 16))

        base_url = get_base_url(region)
        url = f"{base_url.rstrip('/')}/GetClanMembers"
        
        from urllib.parse import urlparse
        parsed_url = urlparse(base_url)
        dynamic_host = parsed_url.netloc  

        headers = {
            "Authorization": f"Bearer {token}",
            "X-Unity-Version": "2022.3.47f1",
            "ReleaseVersion": ob,
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "UnityPlayer/2022.3.47f1 (UnityWebRequest)",
            "Host": dynamic_host,  
            "Accept-Encoding": "gzip, deflate",
            "X-GA": "v1 1",
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=encrypted, ssl=False) as resp:
                if resp.status != 200:
                    return [f"[B][C][FF0000]❌ API error (HTTP {resp.status})"]

                content = await resp.read()
                if content[:2] == b'\x1f\x8b':
                    import gzip
                    content = gzip.decompress(content)

                response_proto = clan_pb2.GetClanMembersResponse()
                response_proto.ParseFromString(content)

                leader = None
                acting = None
                officers = []
                members = []
                for entry in response_proto.entries:
                    role = entry.role
                    data = {
                        "uid": str(entry.info.uid),
                        "name": entry.info.name,
                        "total": entry.total_glory,
                        "weekly": entry.weekly_glory
                    }
                    if role == 3:
                        leader = data
                    elif role == 4:
                        acting = data
                    elif role == 2:
                        officers.append(data)
                    else:
                        members.append(data)

                total = len(response_proto.entries)
                messages = []

                header = (
                    f"[C][B][00FF00]👥 Clan ID: [FFFFFF]{fixnum(str(clan_id))}\n"
                    f"[C][B][00FF00]📊 Total Members: [FFFFFF]{fixnum(str(total))}"
                )
                messages.append(header)

                if leader:
                    msg = (
                        f"[C][B][00FF00]-┌ 👑 LEADER\n"
                        f"[FFFFFF]-├─ Name: [00FF00]{leader['name']}\n"
                        f"[FFFFFF]-├─ UID: [00FF00]{fixnum(leader['uid'])}\n"
                        f"[FFFFFF]-├─ Total Glory: [00FF00]{leader['total']}\n"
                        f"[FFFFFF]-└─ Weekly Glory: [00FF00]{leader['weekly']}"
                    )
                    messages.append(msg)

                if acting:
                    msg = (
                        f"[C][B][FFA500]-┌ 🎖️ ACTING LEADER\n"
                        f"[FFFFFF]-├─ Name: [00FF00]{acting['name']}\n"
                        f"[FFFFFF]-├─ UID: [00FF00]{fixnum(acting['uid'])}\n"
                        f"[FFFFFF]-├─ Total Glory: [00FF00]{acting['total']}\n"
                        f"[FFFFFF]-└─ Weekly Glory: [00FF00]{acting['weekly']}"
                    )
                    messages.append(msg)

                for idx, off in enumerate(officers, 1):
                    msg = (
                        f"[C][B][00FFFF]-┌ ⚔️ OFFICER #{idx}\n"
                        f"[FFFFFF]-├─ Name: [00FF00]{off['name']}\n"
                        f"[FFFFFF]-├─ UID: [00FF00]{fixnum(off['uid'])}\n"
                        f"[FFFFFF]-├─ Total Glory: [00FF00]{off['total']}\n"
                        f"[FFFFFF]-└─ Weekly Glory: [00FF00]{off['weekly']}"
                    )
                    messages.append(msg)

                for idx, mem in enumerate(members, 1):
                    msg = (
                        f"[C][B][AAAAAA]-┌ 👥 MEMBER #{idx}\n"
                        f"[FFFFFF]-├─ Name: [00FF00]{mem['name']}\n"
                        f"[FFFFFF]-├─ UID: [00FF00]{fixnum(mem['uid'])}\n"
                        f"[FFFFFF]-├─ Total Glory: [00FF00]{mem['total']}\n"
                        f"[FFFFFF]-└─ Weekly Glory: [00FF00]{mem['weekly']}"
                    )
                    messages.append(msg)

                return messages

    except Exception as e:
        return [f"[B][C][FF0000]❌ Error fetching clan members: {str(e)}"]
        
def DeLet_Uid(id, token, region):
    try:
        print(f'🗑️ Removing friend: {id}')
        base_url = get_base_url(region)
        url = base_url + "RemoveFriend"
        
        from urllib.parse import urlparse
        parsed_url = urlparse(base_url)
        dynamic_host = parsed_url.netloc  
        
        headers = {
            'X-Unity-Version': '2018.4.11f1',
            'ReleaseVersion': ob,
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-GA': 'v1 1',
            'Authorization': f'Bearer {token}',
            'Content-Length': '16',
            'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 7.1.2; ASUS_Z01QD Build/QKQ1.190825.002)',
            'Host': dynamic_host,  
            'Connection': 'Keep-Alive',
            'Accept-Encoding': 'gzip'
        }
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        encrypted_uid = loop.run_until_complete(EnC_Uid(id, Tp='Uid'))
        data_string = f"08a7c4839f1e10{encrypted_uid}"
        encrypted_data = loop.run_until_complete(EnC_AEs(data_string))
        loop.close()
        data = bytes.fromhex(encrypted_data)
        
        ResPonse = requests.post(url, headers=headers, data=data, verify=False)
        
        if ResPonse.status_code == 400 and 'BR_FRIEND_NOT_SAME_REGION' in ResPonse.text:
            return f"[B][C][FFFF00]⚠️ UID {fixnum(id)} Not In Same Region!"
        elif ResPonse.status_code == 200:
            return f"[B][C][00FFFF]┌──────────┐                [B][C][00FF00]✅ SUCCESS ✅ [B][C][00FFFF]└──────────┘ \n[C][B][FFFFFF]🆔 UID: {fixnum(id)}                   \n[C][B][00FF00]✅ Friend removed successfully!" 
        else:
            return f"[B][C][FF0000]❌ Error removing friend! Status Code: {ResPonse.status_code}"
    except Exception as e:
        print(f"Error in DeLet_Uid: {e}")
        return f"[B][C][FF0000]❌ Error: {str(e)}"
        
async def RequestAddingFriend_Uid_async(target_uid: str, token: str, region: str) -> str:
    try:
        print(f'📨 Sending friend request to: {target_uid}')
        base_url = get_base_url(region)
        url = base_url + "RequestAddingFriend"
        
        from urllib.parse import urlparse
        parsed_url = urlparse(base_url)
        dynamic_host = parsed_url.netloc
        
        headers = {
            'X-Unity-Version': '2018.4.11f1',
            'ReleaseVersion': ob,
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-GA': 'v1 1',
            'Authorization': f'Bearer {token}',
            'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 7.1.2; ASUS_Z01QD Build/QKQ1.190825.002)',
            'Host': dynamic_host,  
            'Connection': 'Keep-Alive',
            'Accept-Encoding': 'gzip'
        }

        encrypted_uid = await EnC_Uid(target_uid, Tp='Uid')
        data_string = f"08a7c4839f1e10{encrypted_uid}"
        encrypted_data = await EnC_AEs(data_string)
        data = bytes.fromhex(encrypted_data)

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=data, ssl=False) as resp:
                if resp.status == 200:
                    return f"[B][C][00FFFF]┌──────────┐                [B][C][00FF00]✅ SUCCESS ✅ [B][C][00FFFF]└──────────┘                   \n[C][B][FFFFFF]🆔 UID: {fixnum(target_uid)}                   \n[C][B][00FF00]✅ Friend Request Sent successfully!" 
                elif resp.status == 400:
                    text = await resp.text()
                    if "ALREADY_FRIENDS" in text:
                        return f"[B][C][FFFF00]⚠️ UID {fixnum(target_uid)} is already your friend!"
                    elif "USER_NOT_FOUND" in text:
                        return f"[B][C][FF0000]❌ UID {fixnum(target_uid)} does not exist!"
                    else:
                        return f"[B][C][FF0000]❌ Error sending request (400)."
                else:
                    return f"[B][C][FF0000]❌ Error sending request! Status Code: {resp.status}"
    except Exception as e:
        print(f"Error in RequestAddingFriend_Uid_async: {e}")
        return f"[B][C][FF0000]❌ Error: {str(e)}"
        
async def RequestJoinClan(clan_id: str, token: str, region: str) -> str:
    try:
        encrypted_clan_hex = enc(clan_id)   
        data = bytes.fromhex(encrypted_clan_hex)

        base_url = get_base_url(region)
        url = base_url + "RequestJoinClan"
        
        from urllib.parse import urlparse
        parsed_url = urlparse(base_url)
        dynamic_host = parsed_url.netloc
        
        headers = {
            'X-Unity-Version': '2018.4.11f1',
            'ReleaseVersion': ob,
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-GA': 'v1 1',
            'Authorization': f'Bearer {token}',
            'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 7.1.2; ASUS_Z01QD Build/QKQ1.190825.002)',
            'Host': dynamic_host,
            'Connection': 'Keep-Alive',
            'Accept-Encoding': 'gzip'
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=data, ssl=False) as resp:
                if resp.status == 200:
                    return f"[B][C][00FFFF]┌──────────┐                [B][C][00FF00]✅ SUCCESS ✅ [B][C][00FFFF]└──────────┘                   \n[C][B][FFFFFF]🆔 UID: {fixnum(clan_id)}                   \n[C][B][00FF00]✅ Clan join request sent successfully!" 
                elif resp.status == 400:
                    text = await resp.text()
                    return f"[B][C][FF0000]❌ Error (400): {text[:100]}"
                else:
                    return f"[B][C][FF0000]❌ HTTP {resp.status}"
    except Exception as e:
        return f"[B][C][FF0000]❌ Error: {str(e)}"
        
async def QuitClan(clan_id: str, token: str, region: str) -> str:
    try:
        encrypted_clan_hex = enc(clan_id)  
        data = bytes.fromhex(encrypted_clan_hex)

        base_url = get_base_url(region)
        url = base_url + "QuitClan"
        
        from urllib.parse import urlparse
        parsed_url = urlparse(base_url)
        dynamic_host = parsed_url.netloc
        
        headers = {
            'X-Unity-Version': '2018.4.11f1',
            'ReleaseVersion': ob,
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-GA': 'v1 1',
            'Authorization': f'Bearer {token}',
            'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 7.1.2; ASUS_Z01QD Build/QKQ1.190825.002)',
            'Host': dynamic_host,
            'Connection': 'Keep-Alive',
            'Accept-Encoding': 'gzip'
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=data, ssl=False) as resp:
                if resp.status == 200:
                    return f"[B][C][00FFFF]┌──────────┐                [B][C][00FF00]✅ SUCCESS ✅ [B][C][00FFFF]└──────────┘                   \n[C][B][FFFFFF]🆔 UID: {fixnum(clan_id)}                   \n[C][B][00FF00]✅ Clan left successfully!" 
                elif resp.status == 400:
                    text = await resp.text()
                    return f"[B][C][FF0000]❌ Error (400): {text[:100]}"
                else:
                    return f"[B][C][FF0000]❌ HTTP {resp.status}"
    except Exception as e:
        return f"[B][C][FF0000]❌ Error: {str(e)}"
        
async def change_clothes(avatar_id: int, skill_ids: list, token: str, region: str) -> str:
    try:
        encrypted_skills_hex = ''.join(encrypt_id(sid) for sid in skill_ids)
        field2_bytes = bytes.fromhex(encrypted_skills_hex)

        field1 = bytes([0x08]) + _encode_varint(avatar_id)
        field2 = bytes([0x12, len(field2_bytes)]) + field2_bytes
        field3 = bytes([0x18]) + _encode_varint(50)
        protobuf = field1 + field2 + field3
        protobuf_hex = protobuf.hex()

        encrypted_payload_hex = encrypt_api(protobuf_hex)
        encrypted_payload = bytes.fromhex(encrypted_payload_hex)

        base_url = get_base_url(region)
        url = base_url + "ChangeClothes"
        headers = {
            "Accept-Encoding": "gzip",
            "Authorization": f"Bearer {token}",
            "Connection": "Keep-Alive",
            "Content-Type": "application/x-www-form-urlencoded",
            "Expect": "100-continue",
            "ReleaseVersion": ob,
            "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; G011A Build/PI)",
            "X-GA": "v1 1",
            "X-Unity-Version": "2018.4.11f1",
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=encrypted_payload, headers=headers, ssl=False) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    if resp.headers.get('content-encoding') == 'gzip':
                        import io, gzip
                        buf = io.BytesIO(content)
                        with gzip.GzipFile(fileobj=buf) as f:
                            content = f.read()
                    try:
                        text = content.decode('utf-8')
                        return f"[B][C][00FF00]✅ Outfit changed successfully!\n{text[:200]}"
                    except:
                        return "[B][C][00FF00]✅ Outfit changed successfully! (binary response)"
                else:
                    text = await resp.text()
                    return f"[B][C][FF0000]❌ Failed. Status {resp.status}\n{text[:200]}"
    except Exception as e:
        return f"[B][C][FF0000]❌ Error: {str(e)}"
                
def start_autooo(self):    
    try:
        fields = {
            1: 9,
            2: {
                1: 12480598706,
            },
        }
        packet = create_protobuf_packet(fields).hex()
        header_length = len(encrypt_packet(packet, self.key, self.iv)) // 2
        header_length_final = dec_to_hex(header_length)
        if len(header_length_final) == 2:
            final_packet = "0515000000" + header_length_final + self.nmnmmmmn(packet)
        elif len(header_length_final) == 3:
            final_packet = "051500000" + header_length_final + self.nmnmmmmn(packet)
        elif len(header_length_final) == 4:
            final_packet = "05150000" + header_length_final + self.nmnmmmmn(packet)
        elif len(header_length_final) == 5:
            final_packet = "0515000" + header_length_final + self.nmnmmmmn(packet)
        return bytes.fromhex(final_packet)
    except exception as e:
        print(e)

def load_credentials_from_file(filename="accs.txt"):
    try:
        if not os.path.exists(filename):
            print(f"❌ {filename} not found!")
            create_credentials_template()  
            return None, None
        
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        
        print(f"📁 Reading {filename}...")
        
        try:
            json_data = json.loads(content)
            
            if isinstance(json_data, dict):
                print("✅ Detected JSON format")
                
                for uid, password in json_data.items():
                    if uid and password:
                        print(f"✅ Loaded from JSON: UID={uid}, Password={'*' * len(password)}")
                        return uid, password
                
                print("❌ No valid entries in JSON")
                return None, None
            
            elif isinstance(json_data, list):
                print("✅ Detected JSON array format")
                if json_data and len(json_data) >= 2:
                    return str(json_data[0]), str(json_data[1])
                
        except json.JSONDecodeError:
            print("⚠️ Not JSON format, trying old formats...")
            return load_credentials_old_format(content)
        except Exception as e:
            print(f"❌ JSON parsing error: {e}")
            return load_credentials_old_format(content)
        
    except Exception as e:
        print(f"❌ Error loading credentials: {e}")
        return None, None

def load_credentials_from_file(filename="accs.txt"):
    try:
        if not os.path.exists(filename):
            print(f"❌ {filename} not found!")
            create_credentials_template()
            return None, None
        
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        
        print(f"📁 Reading {filename}...")
        
        try:
            json_data = json.loads(content)
            
            if isinstance(json_data, dict):
                print("✅ Detected JSON format")
                
                for uid, password in json_data.items():
                    if uid and password:
                        print(f"✅ Loaded from JSON: UID={uid}, Password={'*' * len(password)}")
                        return uid, password
                
                print("❌ No valid entries in JSON")
                return None, None
            
            elif isinstance(json_data, list):
                print("✅ Detected JSON array format")
                if json_data and len(json_data) >= 2:
                    return str(json_data[0]), str(json_data[1])
                
        except json.JSONDecodeError:
            print("⚠️ Not JSON format, trying old formats...")
            return load_credentials_old_format(content)
        except Exception as e:
            print(f"❌ JSON parsing error: {e}")
            return load_credentials_old_format(content)
        
    except Exception as e:
        print(f"❌ Error loading credentials: {e}")
        return None, None

def load_credentials_old_format(content):
    uid = None
    password = None
    
    import re
    
    comma_pattern = r'uid\s*[=:]\s*(\d+)\s*,\s*password\s*[=:]\s*([^\s,]+)'
    match = re.search(comma_pattern, content, re.IGNORECASE)
    
    if match:
        uid = match.group(1)
        password = match.group(2)
        print(f"✅ Loaded from comma format")
        return uid, password
    
    uid_match = re.search(r'(?:uid\s*[=:]\s*)(\d+)', content, re.IGNORECASE)
    if uid_match:
        uid = uid_match.group(1)
    
    pass_match = re.search(r'(?:password\s*[=:]\s*)([^\s\n\r]+)', content, re.IGNORECASE)
    if pass_match:
        password = pass_match.group(1)
    
    if uid and password:
        print(f"✅ Loaded from line format")
        return uid, password
    
    print("❌ Could not parse credentials from file")
    print("📝 Expected formats:")
    print("   JSON: {\"4870750605\": \"A9A27888F2CAFED604B02214FD54E0CC2CFBD1AC7B0973836490DDB61FD1668A\"}")
    print("   Old: uid=4870750605,password=A9A27888F2CAFED604B02214FD54E0CC2CFBD1AC7B0973836490DDB61FD1668A")
    return None, None

def dec_to_hex(decimal):
    hex_str = hex(decimal)[2:]
    return hex_str.upper() if len(hex_str) % 2 == 0 else '0' + hex_str.upper()



async def encrypt_packet(packet_hex, key, iv):
    cipher = AES.new(key, AES.MODE_CBC, iv)
    packet_bytes = bytes.fromhex(packet_hex)
    padded_packet = pad(packet_bytes, AES.block_size)
    encrypted = cipher.encrypt(padded_packet)
    return encrypted.hex()

async def nmnmmmmn(packet_hex, key, iv):
    return await encrypt_packet(packet_hex, key, iv)
    

def generate_random_hex_color():
    return ''.join([random.choice('0123456789ABCDEF') for _ in range(6)])

def bunner_():
    return random.randint(100000000, 999999999)

def Encrypt(number):
    number = int(number)
    encoded_bytes = []
    
    while True:
        byte = number & 0x7F
        number >>= 7
        if number:
            byte |= 0x80
        encoded_bytes.append(byte)
        if not number:
            break
    
    return bytes(encoded_bytes).hex()
        
async def animation_packet(bundle_id, key, iv):
    fields = {
        1: 88,
        2: {
            1: {
                1: int(bundle_id),
            }
        }
    }

    proto_bytes = await CrEaTe_ProTo(fields)
    packet_hex = proto_bytes.hex()

    encrypted_packet = await encrypt_packet(packet_hex, key, iv)

    packet_length = len(encrypted_packet) // 2
    packet_length_hex = await base_to_hex(packet_length)

    if len(packet_length_hex) == 2:
        header = "0515000000"
    elif len(packet_length_hex) == 3:
        header = "051500000"
    elif len(packet_length_hex) == 4:
        header = "05150000"
    elif len(packet_length_hex) == 5:
        header = "0515000"
    else:
        header = "0515000000"

    final_packet = header + packet_length_hex + encrypted_packet

    return bytes.fromhex(final_packet)
                      
async def Look_Changer(bundle_id, key, iv, look_type=1, region="ind"):

    fields = {
        1: 88,
        2: {
            1: {
                1: bundle_id,
                2: look_type   
            },
            2: 2
        }
    }

    packet = await CrEaTe_ProTo(fields)
    packet_hex = packet.hex()

    encrypted = await encrypt_packet(packet_hex, key, iv)

    header_length = len(encrypted) // 2
    header_length_hex = await DecodE_HeX(header_length)

    if region.lower() == "ind":
        packet_type = "0514"
    elif region.lower() == "bd":
        packet_type = "0519"
    else:
        packet_type = "0515"

    if len(header_length_hex) == 2:
        final_header = f"{packet_type}000000"
    elif len(header_length_hex) == 3:
        final_header = f"{packet_type}00000"
    elif len(header_length_hex) == 4:
        final_header = f"{packet_type}0000"
    elif len(header_length_hex) == 5:
        final_header = f"{packet_type}000"
    else:
        final_header = f"{packet_type}000000"

    final_packet_hex = final_header + header_length_hex + encrypted
    return bytes.fromhex(final_packet_hex)
        
def render_ttt_board(board):
    cells = []

    for i, cell in enumerate(board):
        value = cell if cell in ["X", "O"] else str(i + 1)
        cells.append(f"   {value}   ")

    return "\n".join([
        "┌───────┐",
        f"│{cells[0]}│{cells[1]}│{cells[2]}  │",
        "├───────┤",
        f"│{cells[3]}│{cells[4]}│{cells[5]} │",
        "├───────┤",
        f"│{cells[6]}│{cells[7]}│{cells[8]} │",
        "└───────┘"
    ])

def check_ttt_winner(board):
    """Return 'X' or 'O' if someone won, else None."""
    wins = [
        [0,1,2], [3,4,5], [6,7,8],
        [0,3,6], [1,4,7], [2,5,8],
        [0,4,8], [2,4,6]
    ]
    for a,b,c in wins:
        if board[a] == board[b] == board[c] != ' ':
            return board[a]
    return None
    
async def bot_make_move(chat_id, key, iv, chat_type, uid, chat_id_param, region):
    await asyncio.sleep(1)  

    if chat_id not in ttt_games:
        return  

    game = ttt_games[chat_id]

    if game['turn'] != "BOT":
        return
    if check_ttt_winner(game['board']) or game['moves'] == 9:
        return

    board = game['board']
    bot_marker = 'O' if game['players'][0] == str(uid) else 'X'
    opponent_marker = 'X' if bot_marker == 'O' else 'O'

    move = None

    for i in range(9):
        if board[i] == ' ':
            board[i] = bot_marker
            if check_ttt_winner(board) == bot_marker:
                move = i
                board[i] = ' '
                break
            board[i] = ' '

    if move is None:
        for i in range(9):
            if board[i] == ' ':
                board[i] = opponent_marker
                if check_ttt_winner(board) == opponent_marker:
                    move = i
                    board[i] = ' '
                    break
                board[i] = ' '

    if move is None and board[4] == ' ':
        move = 4

    if move is None:
        empty = [i for i, cell in enumerate(board) if cell == ' ']
        if empty:
            move = random.choice(empty)

    if move is None:
        return  

    board[move] = bot_marker
    game['moves'] += 1
    game['turn'] = game['players'][0]  

    winner = check_ttt_winner(board)
    board_msg = render_ttt_board(board)

    if winner:
        token = load_token()
        p1_name = await get_player_info_async(game['players'][0], token, region) if token else game['players'][0]
        p2_name = "Bot"
        winner_name = p1_name if winner == 'X' else p2_name
        over_msg = f"""[B][C][00FF00]🏆 GAME OVER!

[FFFFFF]Winner: {winner_name} ({winner})! 🎉

[00FFFF]{board_msg}"""
        await safe_send_message(chat_type, over_msg, uid, chat_id_param, key, iv)
        del ttt_games[chat_id]
        return
    elif game['moves'] == 9:
        over_msg = f"""[B][C][FFFF00]🤝 GAME OVER - DRAW!

[FFFFFF]It's a tie! Well played both.

[00FFFF]{board_msg}"""
        await safe_send_message(chat_type, over_msg, uid, chat_id_param, key, iv)
        del ttt_games[chat_id]
        return

    token = load_token()
    next_uid = game['turn']
    next_name = await get_player_info_async(next_uid, token, region) if token else next_uid
    next_marker = 'X' if next_uid == game['players'][0] else 'O'

    turn_msg = f"""[B][C][00FF00]🎮 BOT MOVED!

[FFFFFF]Turn: {next_name} ({next_marker})

[00FFFF]{board_msg}"""
    await safe_send_message(chat_type, turn_msg, uid, chat_id_param, key, iv)
                               
async def check_player_status(target_uid, key, iv, max_wait=3):
    try:
        if target_uid in status_response_cache:
            del status_response_cache[target_uid]
        
        status_packet = await createpacketinfo(target_uid, key, iv)
        if not status_packet:
            return None, "Failed to create packet"
        
        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', status_packet)
        print(f"📤 Sent status request for {target_uid}")
        
        start_time = time.time()
        while time.time() - start_time < max_wait:
            if target_uid in status_response_cache:
                cache_data = status_response_cache[target_uid]
                return cache_data, "Success"
            
            await asyncio.sleep(0.1)
        
        return None, f"No response after {max_wait} seconds"
        
    except Exception as e:
        return None, f"Error: {str(e)}"

async def createpacketinfo(idddd, key, iv):
    try:
        ida = Encrypt(idddd)
        packet = f"080112090A05{ida}1005"
        header_lenth = len(await encrypt_packet(packet, key, iv)) // 2
        header_lenth_final = dec_to_hex(header_lenth)
        
        if len(header_lenth_final) == 2:
            final_packet = "0F15000000" + header_lenth_final + await nmnmmmmn(packet, key, iv)
        elif len(header_lenth_final) == 3:
            final_packet = "0F1500000" + header_lenth_final + await nmnmmmmn(packet, key, iv)
        elif len(header_lenth_final) == 4:
            final_packet = "0F150000" + header_lenth_final + await nmnmmmmn(packet, key, iv)
        elif len(header_lenth_final) == 5:
            final_packet = "0F15000" + header_lenth_final + await nmnmmmmn(packet, key, iv)
        else:
            final_packet = "0F1500000" + header_lenth_final + await nmnmmmmn(packet, key, iv)
            
        return bytes.fromhex(final_packet)
        
    except Exception as e:
        print(f"Error creating packet info: {e}")
        return None

def fix_num(number):
    fixed = ""
    count = 0
    num_str = str(number)
    
    for char in num_str:
        if char.isdigit():
            count += 1
        fixed += char
        if count == 3:
            fixed += "[c]"
            count = 0
    return fixed

def get_available_room(input_text):
    try:
        from protobuf_decoder.protobuf_decoder import Parser
        parsed_results = Parser().parse(input_text)
        parsed_results_objects = parsed_results
        parsed_results_dict = parse_results(parsed_results_objects)
        json_data = json.dumps(parsed_results_dict)
        return json_data
    except Exception as e:
        print(f"error {e}")
        return None

def parse_results(parsed_results):
    result_dict = {}
    for result in parsed_results:
        field_data = {}
        field_data["wire_type"] = result.wire_type
        if result.wire_type == "varint":
            field_data["data"] = result.data
        if result.wire_type == "string":
            field_data["data"] = result.data
        if result.wire_type == "bytes":
            field_data["data"] = result.data
        elif result.wire_type == "length_delimited":
            field_data["data"] = parse_results(result.data.results)
        result_dict[result.field] = field_data
    return result_dict  

def get_player_status(packet):
    json_result = get_available_room(packet)
    if not json_result:
        return "OFFLINE"
    
    parsed_data = json.loads(json_result)
    
    if "5" not in parsed_data or "data" not in parsed_data["5"]:
        return "OFFLINE"
    
    json_data = parsed_data["5"]["data"]
    
    if "1" not in json_data or "data" not in json_data["1"]:
        return "OFFLINE"
    
    data = json_data["1"]["data"]
    
    if "3" not in data:
        return "OFFLINE"
    
    status_data = data["3"]
    
    if "data" not in status_data:
        return "OFFLINE"
    
    status = status_data["data"]
    
    if status == 1:
        return "SOLO"
    if status == 2:
        if "9" in data and "data" in data["9"]:
            group_count = data["9"]["data"]
            countmax1 = data["10"]["data"]
            countmax = countmax1 + 1
            return f"INSQUAD ({group_count}/{countmax})"
        return "INSQUAD"
    if status in [3, 5]:
        return "INGAME"
    if status == 4:
        return "IN ROOM"
    if status in [6, 7]:
        return "IN SOCIAL ISLAND MODE"
    
    return "NOTFOUND"

def get_idroom_by_idplayer(packet):
    try:
        json_result = get_available_room(packet)
        parsed_data = json.loads(json_result)
        json_data = parsed_data["5"]["data"]
        data = json_data["1"]["data"]
        idroom = data['15']["data"]
        return idroom
    except Exception as e:
        print(f"Error extracting room ID: {e}")
        return None



def get_leader(packet):
    try:
        json_result = get_available_room(packet)
        parsed_data = json.loads(json_result)
        json_data = parsed_data["5"]["data"]
        data = json_data["1"]["data"]
        leader = data['8']["data"]
        return leader
    except Exception as e:
        print(f"Error extracting leader: {e}")
        return None

status_queue = asyncio.Queue()
cache_dict = {}

async def handle_status_response(hex_data):
    try:
        await status_queue.put({
            'player_id': player_id,
            'data': cache_entry
        })
        
        print(f"📤 Queued status for {player_id}")
        
    except Exception as e:
        print(f"❌ Queue error: {e}")

async def cache_consumer():
    while True:
        try:
            item = await status_queue.get()
            player_id = item['player_id']
            cache_dict[player_id] = item['data']
            print(f"📥 Cache updated for {player_id}")
            status_queue.task_done()
        except Exception as e:
            print(f"❌ Consumer error: {e}")
        await asyncio.sleep(0.1)



async def StarTinG():
    consumer_task = asyncio.create_task(cache_consumer())

    while True:
        try:
            await MaiiiinE()

        except KeyboardInterrupt:
            consumer_task.cancel()
            break

        except Exception as e:
            print(f"Error: {e} => Restarting bot...")
            await stop_all_spam_tasks()
            if consumer_task:
                consumer_task.cancel()
                try:
                    await consumer_task
                except asyncio.CancelledError:
                    pass
            consumer_task = asyncio.create_task(cache_consumer())
            continue

import pickle
import os
import time

CACHE_FILE = 'status_cache.pkl'
CACHE_TIMEOUT = 30  

def save_to_cache(player_id, data):
    try:
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'rb') as f:
                    cache = pickle.load(f)
            except:
                cache = {}
        else:
            cache = {}
        
        data['saved_at'] = time.time()
        
        cache[str(player_id)] = data
        
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(cache, f)
        
        print(f"💾 Saved to file cache: {player_id}")
        return True
    except Exception as e:
        print(f"❌ Cache save error: {e}")
        import traceback
        traceback.print_exc()
        return False

def load_from_cache(player_id):
    try:
        if not os.path.exists(CACHE_FILE):
            return None
        
        with open(CACHE_FILE, 'rb') as f:
            cache = pickle.load(f)
        
        player_key = str(player_id)
        if player_key in cache:
            data = cache[player_key]
            
            if 'saved_at' in data:
                if time.time() - data['saved_at'] > CACHE_TIMEOUT:
                    print(f"⏰ Cache expired for {player_id}")
                    del cache[player_key]
                    with open(CACHE_FILE, 'wb') as f:
                        pickle.dump(cache, f)
                    return None
            
            print(f"📥 Loaded from cache: {player_id}")
            return data
        
        return None
    except Exception as e:
        print(f"❌ Cache load error: {e}")
        return None

def clear_cache_entry(player_id):
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'rb') as f:
                cache = pickle.load(f)
            
            player_key = str(player_id)
            if player_key in cache:
                del cache[player_key]
                
            with open(CACHE_FILE, 'wb') as f:
                pickle.dump(cache, f)
            print(f"🗑️ Cleared cache for {player_id}")
    except Exception as e:
        print(f"❌ Clear cache error: {e}")

def debug_file_cache():
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'rb') as f:
                cache = pickle.load(f)
            print(f"\n📁 FILE CACHE DEBUG:")
            print(f"Size: {len(cache)} entries")
            for uid, data in cache.items():
                age = time.time() - data.get('saved_at', 0)
                status = data.get('status', 'NO STATUS')
                print(f"  {uid}: {status} (age: {age:.1f}s)")
            print("---\n")
            return cache
        else:
            print("📁 No cache file exists")
            return {}
    except Exception as e:
        print(f"❌ Cache debug error: {e}")
        return {}

def load_from_cache(player_id):
    try:
        if not os.path.exists(CACHE_FILE):
            return None
        
        with open(CACHE_FILE, 'rb') as f:
            cache = pickle.load(f)
        
        if player_id in cache:
            return cache[player_id]
        return None
    except Exception as e:
        print(f"❌ Cache load error: {e}")
        return None

def clear_cache_entry(player_id):
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'rb') as f:
                cache = pickle.load(f)
            
            if player_id in cache:
                del cache[player_id]
                
            with open(CACHE_FILE, 'wb') as f:
                pickle.dump(cache, f)
    except:
        pass

        
    
    async def send_join_from_account(self, target_uid, account_uid, password, key, iv, region):
        try:
            open_id, access_token = await self.get_account_token(account_uid, password)
            if not open_id or not access_token:
                return False
            
            join_packet = await self.create_account_join_packet(target_uid, account_uid, open_id, access_token, key, iv, region)
            if join_packet:
                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_packet)
                return True
            return False
            
        except Exception as e:
            print(f"❌ Error sending join from {account_uid}: {e}")
            return False
                    
            
async def join_custom_room(room_id, room_password, key, iv, region):
    fields = {
        1: 61,  
        2: {
            1: int(room_id),
            2: {
                1: int(room_id),  
                2: int(time.time()),  
                3: "BOT",  
                5: 12,  
                6: 9999999,  
                7: 1,  
                8: {
                    2: 1,
                    3: 1,
                },
                9: 3,
            },
            3: str(room_password),  
        }
    }
    
    if region.lower() == "ind":
        packet_type = '0514'
    elif region.lower() == "bd":
        packet_type = "0519"
    else:
        packet_type = "0515"
        
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), packet_type, key, iv)
    
async def leave_squad(key, iv, region):
    fields = {
        1: 7,
        2: {
            1: 12480598706  
        }
    }
    
    packet = (await CrEaTe_ProTo(fields)).hex()
    
    if region.lower() == "ind":
        packet_type = '0514'
    elif region.lower() == "bd":
        packet_type = "0519"
    else:
        packet_type = "0515"
        
    return await GeneRaTePk(packet, packet_type, key, iv)    
    
async def request_join_with_badge(target_uid, badge_value, key, iv, region="IND"):
    try:
        avatar_id = int(get_random_avatar())
        
        fields = {
            1: 33,  
            2: {
                1: int(target_uid),        
                2: region.upper(),        
                3: 1,                     
                4: 1,                     
                5: bytes([1, 7, 9, 10, 11, 18, 25, 26, 32]),  
                6: "[C][B][FF0000]DEVIL YT!!",  
                7: 330,                   
                8: 1000,                  
                10: region.upper(),       
                11: bytes([              
                    49, 97, 99, 52, 98, 56, 48, 101, 99, 102, 48, 52, 55, 56,
                    97, 52, 52, 50, 48, 51, 98, 102, 56, 102, 97, 99, 54, 49,
                    50, 48, 102, 53
                ]),
                12: 1,                  
                13: int(target_uid),
                14: {
                    1: 2203434355,
                    2: 8,
                    3: b"\x10\x15\x08\x0A\x0B\x13\x0C\x0F\x11\x04\x07\x02\x03\x0D\x0E\x12\x01\x05\x06"
                },
                16: 1,
                17: 1,
                18: 312,
                19: 46,
                23: bytes([16, 1, 24, 1]), 
                24: avatar_id,            
                26: {},
                27: {
                    1: 11,
                    2: 12999994075,      
                    3: 9999              
                },
                28: {},                   
                31: {
                    1: 1,
                    2: int(badge_value)
                },
                32: int(badge_value),     
                34: {
                    1: int(target_uid),
                    2: 8,
                    3: b"\x0F\x06\x15\x08\x0A\x0B\x13\x0C\x11\x04\x0E\x14\x07\x02\x01\x05\x10\x03\x0D\x12"
                }
            },
            10: "en",
            13: {
                2: 1,
                3: 1
            }
        }
        
        proto_bytes = await CrEaTe_ProTo(fields)
        packet_hex = proto_bytes.hex()
        
        if region.lower() == "ind":
            packet_type = '0514'
        elif region.lower() == "bd":
            packet_type = "0519"
        else:
            packet_type = "0515"
            
        final_packet = await GeneRaTePk(packet_hex, packet_type, key, iv)
        
        print(f"✅ Created badge packet with value {badge_value} for UID {target_uid}")
        return final_packet
        
    except Exception as e:
        print(f"❌ Error creating badge packet: {e}")
        import traceback
        traceback.print_exc()
        return None
    
async def start_auto_packet(key, iv, region):
    fields = {
        1: 9,
        2: {
            1: 12480598706,
        },
    }
    
    if region.lower() == "ind":
        packet_type = '0514'
    elif region.lower() == "bd":
        packet_type = "0519"
    else:
        packet_type = "0515"
        
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), packet_type, key, iv)

async def leave_squad_packet(key, iv, region):
    fields = {
        1: 7,
        2: {
            1: 12480598706,
        },
    }
    
    if region.lower() == "ind":
        packet_type = '0514'
    elif region.lower() == "bd":
        packet_type = "0519"
    else:
        packet_type = "0515"
        
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), packet_type, key, iv)

async def join_teamcode_packet(team_code, key, iv, region):
    fields = {
        1: 4,
        2: {
            4: bytes.fromhex("01090a0b121920"),
            5: str(team_code),
            6: 6,
            8: 1,
            9: {
                2: 800,
                6: 11,
                8: "1.111.1",
                9: 5,
                10: 1
            }
        }
    }
    
    if region.lower() == "ind":
        packet_type = '0514'
    elif region.lower() == "bd":
        packet_type = "0519"
    else:
        packet_type = "0515"
        
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), packet_type, key, iv)
    
async def fast_auto_start_loop(team_code, uid, chat_id, chat_type, key, iv, region):
    global auto_start_running, stop_auto
    print(f"[FAST AUTO] Fast level up started for team {team_code}")
    
    while not stop_auto:
        try:
            status_msg = f"[B][C][FFA500]⚡ FAST AUTO START BOT\n🎯 Team: {team_code}\n⚡ Joining team..."
            await safe_send_message(chat_type, status_msg, uid, chat_id, key, iv)
            
            join_packet = await join_teamcode_packet(team_code, key, iv, region)
            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_packet)
            await asyncio.sleep(1)  
            
            start_msg = f"[B][C][00FF00]✅ Joined team {team_code}\n🎯 Fast match for {fast_start_spam_duration} seconds..."
            await safe_send_message(chat_type, start_msg, uid, chat_id, key, iv)
            
            start_packet = await start_auto_packet(key, iv, region)
            end_time = time.time() + fast_start_spam_duration
            spam_count = 0
            
            while time.time() < end_time and not stop_auto:
                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', start_packet)
                spam_count += 1
                await asyncio.sleep(fast_start_spam_delay)
            
            if stop_auto:
                break
            
            wait_msg = f"[B][C][FFFF00]⏳ Match started! Waiting {fast_wait_after_match} seconds..."
            await safe_send_message(chat_type, wait_msg, uid, chat_id, key, iv)
            
            waited = 0
            while waited < fast_wait_after_match and not stop_auto:
                await asyncio.sleep(1)
                waited += 1
            
            if stop_auto:
                break
            
            leave_msg = f"[B][C][FF0000]🔄 Leaving team {team_code} to restart fast cycle..."
            await safe_send_message(chat_type, leave_msg, uid, chat_id, key, iv)
            
            leave_packet = await leave_squad_packet(key, iv, region)
            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', leave_packet)
            await asyncio.sleep(1)
            
        except Exception as e:
            print(f"[FAST AUTO] Error: {e}")
            error_msg = f"[B][C][FF0000]❌ Fast auto error: {str(e)}\n"
            await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
            break
    
    auto_start_running = False
    stop_auto = False
    print(f"[FAST AUTO] Fast level up loop stopped for team {team_code}")
    
async def reset_bot_state(key, iv, region):
    try:
        leave_packet = await leave_squad(key, iv, region)
        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', leave_packet)
        await asyncio.sleep(0.5)
        
        print("✅ Bot state reset - left squad")
        return True
        
    except Exception as e:
        print(f"❌ Error resetting bot: {e}")
        return False                
    
async def create_custom_room(room_name, room_password, max_players, key, iv, region):
    fields = {
        1: 3,  
        2: {
            1: room_name,
            2: room_password,
            3: max_players,  
            4: 1,
            5: 1,  
            6: "en",  
            7: {   
                1: "BotHost",
                2: int(get_random_avatar()),
                3: 330,
                4: 1048576,
                5: "BOTCLAN"
            }
        }
    }
    
    if region.lower() == "ind":
        packet_type = '0514'
    elif region.lower() == "bd":
        packet_type = "0519"
    else:
        packet_type = "0515"
        
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), packet_type, key, iv)              

async def room_invite_spam_loop(target_uid, count, key, iv):
    global room_invite_spam_running
    sent = 0
    while room_invite_spam_running and sent < count:
        packet = await room_invite(key, iv, target_uid)
        if packet:
            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', packet)
            sent += 1
            await asyncio.sleep(0.1)      
    return sent

async def handle_room_spam_completion(spam_task, target_uid, count, sender_uid, chat_id, chat_type, key, iv):
    try:
        sent = await spam_task
        formatted_uid = fixnum(target_uid)
        msg = f"[B][C][00FF00]✅ Room spam completed!\n🎯 Target: {formatted_uid}\n✅ Sent: {sent}/{count}\n"
        if sent < count:
            msg = f"[B][C][FFFF00]⚠️ Room spam finished early (sent {sent}/{count})"
        await safe_send_message(chat_type, msg, sender_uid, chat_id, key, iv)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        await safe_send_message(chat_type, f"[B][C][FF0000]❌ Room spam error: {e}", sender_uid, chat_id, key, iv)

async def handle_badge_command(cmd, inPuTMsG, uid, chat_id, key, iv, region, chat_type):
    parts = inPuTMsG.strip().split()
    if len(parts) < 2:
        error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED \n[B][C][FFFFFF]Invalid command format.        \n[B][C][AAAAAA]Correct Usage: /{cmd} <uid> \n[B][C][AAAAAA]Example: /{cmd} 123[C]456[C]789" 
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        return
    
    target_uid = parts[1]
    badge_value = BADGE_VALUES.get(cmd, 1048576)
    
    if not target_uid.isdigit():
        error_msg = f"[B][C][FF0000]❌ Please write a valid player ID!\n"
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        return
    
    initial_msg = f"""[B][C][00FFFF]┌──────────┐                [B][C][FFFF00]⏳ PROCESSING ⏳ [B][C][00FFFF]└──────────┘
[C][B][FFFFFF]🆔 UID: {fixnum(target_uid)}
[C][B][FFFFFF]🎖️ COMMAND: {cmd} ({badge_value})
[C][B][FFFF00]⏳ Preparing request..."""
    await safe_send_message(chat_type, initial_msg, uid, chat_id, key, iv)
    
    try:
        badge_packet = await request_join_with_badge(target_uid, badge_value, key, iv, region)
        
        if badge_packet:
            for i in range(5):
                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', badge_packet)
                print(f"✅ Sent /{cmd} badge #{i+1} with value {badge_value}")
                await asyncio.sleep(0.2)  
            
            success_msg = f"[B][C][00FF00]✅ Successfully Sent {cmd} Badge!\n🎯 Target: {target_uid}\n🏷️ Badge Value: {badge_value}\n📤 Packets Sent: 5\n"
        else:
            success_msg = f"[B][C][FF0000]❌ Failed to create badge packet!\n"
        
        await safe_send_message(chat_type, success_msg, uid, chat_id, key, iv)
        
    except Exception as e:
        error_msg = f"[B][C][FF0000]❌ Error in /{cmd}: {str(e)}\n"
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)




    
    
    
async def auto_dual_emote(uid, key, iv, region):
    try:
        Emote_id = 909050008
        
        global CURRENT_BOT_UID
        bot_uid = int(CURRENT_BOT_UID)
        
        emote_to_sender = await Emote_k(int(uid), Emote_id, key, iv, region)
        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', emote_to_sender)
        
        await asyncio.sleep(0.5)
        
        emote_to_bot = await Emote_k(int(bot_uid), Emote_id, key, iv, region)
        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', emote_to_bot)
        
        print(f"🤖 Bot performed dual Aura Boder emote with sender {uid} and bot {bot_uid}!")
        
    except Exception as e:
        print(f"Error sending dual Aura Boder emote: {e}")    
        
async def equip_random_bundle(key, iv, region):
    bundle_ids = {
        "rampage": 914000002,
        "cannibal": 914000003,
        "devil": 914038001,
        "scorpio": 914039001,
        "frostfire": 914042001,
        "paradox": 914044001,
        "naruto": 914047001,
        "aurora": 914047002,
        "midnight": 914048001,
        "itachi": 914050001,
        "dreamspace": 914051001
    }
    bundle_name = random.choice(list(bundle_ids.keys()))
    bundle_id = bundle_ids[bundle_name]
    look_type = random.choice([1, 1])                     
    packet = await Look_Changer(bundle_id, key, iv, look_type, region)
    if packet and online_writer:
        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', packet)
        print(f"✅ Equipped random bundle: {bundle_name} (ID: {bundle_id}) with look type {look_type}")            
        
        
async def Room_Spam(Uid, Rm, Nm, K, V):
    fields = {
        1: 78,
        2: {
            1: int(Rm),  
            2: "[C][B][FF0000]DEVIL YT!!",  
            3: {
                2: 1,
                3: 1
            },
            4: 330,      
            5: 6000,     
            6: 201,      
            10: int(get_random_avatar()),  
            11: int(Uid), 
            12: 1,       
            15: {
                1: 1,
                2: 32768
            },
            16: 32768,    
            18: {
                1: 11481904755,  
                2: 8,
                3: "\u0010\u0015\b\n\u000b\u0013\f\u000f\u0011\u0004\u0007\u0002\u0003\r\u000e\u0012\u0001\u0005\u0006"
            },
            
            31: {
                1: 1,
                2: 32768
            },
            32: 32768,    
            34: {
                1: int(Uid),   
                2: 8,
                3: bytes([15,6,21,8,10,11,19,12,17,4,14,20,7,2,1,5,16,3,13,18])
            }
        }
    }
    
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), '0e15', K, V)
    
async def evo_cycle_spam(uids, key, iv, region):
    try:
        cycle_count = 0
        emote_numbers = list(evo_emotes.keys())  

        while True:
            cycle_count += 1
            print(f"Starting evolution emote cycle #{cycle_count}")

            uid_emote_mapping = {}
            for uid in uids:
                shuffled = emote_numbers.copy()
                random.shuffle(shuffled)
                uid_emote_mapping[uid] = shuffled

            for idx in range(len(emote_numbers)):
                if asyncio.current_task().cancelled():
                    return

                for uid in uids:
                    try:
                        uid_int = int(uid)
                        emote_number = uid_emote_mapping[uid][idx]
                        emote_id = evo_emotes[emote_number]
                        H = await Emote_k(uid_int, int(emote_id), key, iv, region)
                        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', H)
                        print(f"Sent emote {emote_number} to UID: {uid}")
                    except Exception as e:
                        print(f"Error sending evo emote to {uid}: {e}")

                for i in range(5):
                    if asyncio.current_task().cancelled():
                        return
                    await asyncio.sleep(1)

            for i in range(2):
                if asyncio.current_task().cancelled():
                    return
                await asyncio.sleep(1)

    except asyncio.CancelledError:
        print("Evolution emote cycle cancelled")
        return
    finally:
        print("Evolution emote cycle stopped")
        
    
async def get_colorful_message(message_text, message_number):
    color_palette = ["FF0000", "00FF00", "0000FF", "FFFF00", "FF00FF", 
                     "00FFFF", "FFA500", "FF1493", "00FF7F", "7B68EE",
                     "FFD700", "00CED1", "FF69B4", "32CD32", "9370DB",
                     "FF4500", "1E90FF", "ADFF2F", "FF6347", "8A2BE2"]
    
    color_index = (message_number - 1) % len(color_palette)
    return f"[C][B][{color_palette[color_index]}]{message_text}"    

def get_random_avatar():
	avatar_list = [
         '902050001', '902050002', '902050003', '902039016', '902050004', 
        '902047011', '902047010', '902049015', '902050006', '902049020'
    ]
	random_avatar = random.choice(avatar_list)
	return  random_avatar 


async def handle_msg_spam_completion(spam_task, message_text, times, sender_uid, chat_id, chat_type, key, iv):
    try:
        actual_times = await spam_task
        
        if actual_times >= times:
            completion_msg = f"[B][C][00FF00]✅ MESSAGE SPAM COMPLETED!\n"
            completion_msg += f"[FFFFFF]📝 Message: {message_text}\n"
            completion_msg += f"[FFFFFF]📊 Requested: {times} times\n"
            completion_msg += f"[FFFFFF]✅ Sent: {actual_times} times\n"
            completion_msg += f"[00FF00]✓ Success rate: 100%\n"
            completion_msg += f"[FFFFFF]💬 Check squad chat to see messages!\n"
        elif actual_times > 0:
            completion_msg = f"[B][C][FFFF00]⚠️ MESSAGE SPAM PARTIALLY COMPLETED!\n"
            completion_msg += f"[FFFFFF]📝 Message: {message_text}\n"
            completion_msg += f"[FFFFFF]📊 Requested: {times} times\n"
            completion_msg += f"[FFFFFF]⚠️ Sent: {actual_times} times\n"
            completion_msg += f"[FFFF00]↯ Success rate: {(actual_times/times)*100:.1f}%\n"
            completion_msg += f"[FFFFFF]💬 Check squad chat to see messages!\n"
        else:
            completion_msg = f"[B][C][FF0000]❌ MESSAGE SPAM FAILED!\n"
            completion_msg += f"[FFFFFF]📝 Message: {message_text}\n"
            completion_msg += f"[FFFFFF]📊 Requested: {times} times\n"
            completion_msg += f"[FFFFFF]❌ Sent: 0 times\n"
            completion_msg += f"[FF0000]✗ Failed to send any messages\n"
            completion_msg += f"[FFFFFF]🔧 Possible issues:\n"
            completion_msg += f"[FFFFFF]1. Bot not in a squad\n"
            completion_msg += f"[FFFFFF]2. Invalid chat_id\n"
            completion_msg += f"[FFFFFF]3. Connection error\n"
        
        await safe_send_message(chat_type, completion_msg, sender_uid, chat_id, key, iv)
        
    except asyncio.CancelledError:
        print("Message spam was cancelled by user")
        cancel_msg = f"[B][C][00FF00]🛑 MESSAGE SPAM CANCELLED!\n[FFFFFF]Message spam was stopped by user command.\n"
        await safe_send_message(chat_type, cancel_msg, sender_uid, chat_id, key, iv)
    except Exception as e:
        error_msg = f"[B][C][FF0000]❌ ERROR in message spam completion: {str(e)}\n"
        await safe_send_message(chat_type, error_msg, sender_uid, chat_id, key, iv)

async def lag_team_loop(team_code, key, iv, region):
    global lag_running
    count = 0
    
    while lag_running:
        try:
            join_packet = await GenJoinSquadsPacket(team_code, key, iv)
            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_packet)
            
            await asyncio.sleep(0.2)  
            
            leave_packet = await ExiT(None, key, iv)
            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', leave_packet)
            
            count += 1
            print(f"Lag cycle #{count} completed for team: {team_code}")
            
            await asyncio.sleep(0.01)
            
        except Exception as e:
            print(f"Error in lag loop: {e}")
            await asyncio.sleep(0.1)
 
####################################
#BAN CHECK
def get_ban_status(uid):
    try:
        url = f"https://bancheck-api-aditya-ffm.vercel.app/bancheck?uid={uid}"
        res = requests.get(url, timeout=10)

        if res.status_code != 200:
            return "[C][B][FF0000]❌ Failed to check ban status (API Error)."

        data = res.json()

        message = (
            f"[C][B]-┌ [FFD700]Ban Check Result:\n"
            f"[FFFFFF]-├─ Name: {data.get('nickname', 'N/A')}\n"
            f"- ├─ UID: {str(data.get('uid', 'N/A'))[:5]}[C]{str(data.get('uid', 'N/A'))[5:]}\n"
            f"- ├─ Region: {data.get('region', 'N/A')}\n"
            f"- ├─ Level: {data.get('level', 'N/A')}\n"
            f"- ├─ Likes: {data.get('likes', 'N/A')}\n"
            f"- ├─ Status: {data.get('ban_status', 'N/A')}\n"
            f"- └─ Since: {data.get('banned_since', 'N/A')}"
        )

        return message

    except Exception as e:
        return f"[C][B][FF0000]❌ Error occurred: {e}"
# CHAT WITH AI
async def talk_with_gemini(question: str, history: list, sender_name: str) -> tuple[str, list]:
    """
    Sends a question to Gemini API with conversation history.
    Uses a friendly Hinglish style with sender's name.
    Falls back to Pollinations AI if Gemini fails.
    """
    messages = history.copy()
    messages.append({"role": "user", "content": question})

    # New system prompt with sender name and friendly style
    system_prompt = f"""You are NAYAN AI (also called NAYAN bhai). Talk in a friendly, natural Hinglish style. Use these words casually: bro, yaar, bhai, dear, dost, friend. Don't overdo – just where it feels naturally.

Rules:
- Address the user ({sender_name}) sometimes as "bro", "yaar", "bhai", "dear", or just name.
- Refer to yourself as "NAYAN bhai" or "main" – no extra tags like "ki taraf se".
- For "what is your name?": "Main NAYAN bhai hoon, bro." or "I'm NAYAN AI, dear."
- For "kya kar rahe ho?": "Kuch nahi yaar, tera intezaar kar raha hun."
- For "mera naam kya hai?": "Your name is {sender_name}, bro."
- For general answers: short, clear, use "bro/yaar/bhai" once naturally.
- Examples: "Free fire mein best gun M1014 hai, {sender_name} bro."
- "Bhai, capital of India New Delhi hai."
- "Dear {sender_name}, 2+2 = 4."
- Never be rude. Max 3 lines. Same language as question."""

    # Build contents for Gemini API
    contents = []
    for msg in messages:
        role = "user" if msg["role"] == "user" else "model"
        contents.append({
            "role": role,
            "parts": [{"text": msg["content"]}]
        })

    payload = {
        "contents": contents,
        "system_instruction": {
            "parts": [{"text": system_prompt}]
        }
    }

    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    candidates = data.get("candidates", [])
                    if candidates:
                        reply = candidates[0]["content"]["parts"][0]["text"].strip()
                        reply = "\n".join(reply.split("\n")[:10])  # keep max 10 lines
                    else:
                        reply = "🤔 Hmm, I couldn't generate a response. Try again?"
                else:
                    error_text = await resp.text()
                    print(f"Gemini API error {resp.status}: {error_text}")
                    reply = None
    except Exception as e:
        print(f"Gemini exception: {e}")
        reply = None

    # Fallback to Pollinations AI if Gemini fails
    if reply is None:
        try:
            polli_messages = [{"role": "system", "content": system_prompt}] + messages
            payload_polli = {
                "model": "openai",
                "messages": polli_messages,
                "temperature": 0.8,
                "max_tokens": 500
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://text.pollinations.ai/openai",
                    json=payload_polli,
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        reply = result["choices"][0]["message"]["content"].strip()
                        reply = "\n".join(reply.split("\n")[:10])
                    else:
                        reply = "⚠️ AI services are busy. Please try later."
        except Exception as e:
            print(f"Pollinations fallback error: {e}")
            reply = "😵 Oops! AI is unavailable right now."

    # Update history
    messages.append({"role": "assistant", "content": reply})

    # Trim history to MAX_HISTORY * 2 (default MAX_HISTORY = 10)
    if len(messages) > MAX_HISTORY * 2:
        messages = messages[-(MAX_HISTORY * 2):]

    return reply, messages

async def handle_friend_list_command(inPuTMsG, uid, chat_id, key, iv, region, chat_type):
    print('Processing /friend_list command')

    if str(uid) != OWNER_UID:
        error_msg = "[B][C][FFA500]┌──────────┐                [B][C][FF0000]⛔ ACCESS DENIED ⛔ [B][C][FFA500]└──────────┘     [B][C][FFFFFF]⚠️ Only Admins Can Use This Command!" 
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        return

    token = load_token()
    if not token:
        error_msg = "[B][C][FF0000]❌ No token available! Please check token.json"
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        return

    status_msg = f"[B][C][FFFF00]📞 Fetching friend list from Indian server..."
    await safe_send_message(chat_type, status_msg, uid, chat_id, key, iv)

    try:
        friend_data = await get_friend_list(token, region)

        if not friend_data:
            error_msg = "[B][C][FF0000]❌ Failed to fetch friend list (API error or empty response)."
            await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
            return

        your_name = friend_data.get("YourName", "Unknown")
        your_uid = friend_data.get("YourUID", "Unknown")
        total = friend_data.get("Total", 0)
        friends = friend_data.get("Friends", [])

        bot_info_msg = f"""[C][B][00FF00]👤 Account: [FFFFFF]{your_name}
[C][B][00FF00]🆔 Bot UID: [FFFFFF]{fixnum(str(your_uid))}
[C][B][00FF00]👥 Total Friends: [FFFFFF]{fixnum(str(total))}"""
        await safe_send_message(chat_type, bot_info_msg, uid, chat_id, key, iv)
        await asyncio.sleep(0.3)

        if friends:
            for idx, friend in enumerate(friends, 1):
                friend_name = friend.get("name", "Unknown")
                friend_uid = friend.get("uid", "Unknown")
                info_msg = f"""
[C][B][FFFFFF]Name: [00FF00]{friend_name}
[C][B][FFFFFF]UID: [00FF00]{fixnum(friend_uid)}"""
                await safe_send_message(chat_type, info_msg, uid, chat_id, key, iv)
                await asyncio.sleep(0.3)
        else:
            await safe_send_message(chat_type, "\n[C][B][FFFF00]Friend list is empty.\n", uid, chat_id, key, iv)

    except Exception as e:
        error_msg = f"[B][C][FF0000]❌ Error: {str(e)[:50]}"
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        
async def handle_remove_command(inPuTMsG, uid, chat_id, key, iv, region, chat_type):
    
    print('Processing /remove command')
    
    if str(uid) != OWNER_UID:
        error_msg = "[B][C][FFA500]┌──────────┐                [B][C][FF0000]⛔ ACCESS DENIED ⛔ [B][C][FFA500]└──────────┘     [B][C][FFFFFF]⚠️ Only Admins Can Use This Command!" 
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        return
    
    parts = inPuTMsG.strip().split()
    
    if len(parts) != 2:
        error_msg = f"""[B][C][FF0000]⛔ COMMAND REJECTED
\n[B][C][FFFFFF]Invalid command format.
\n[B][C][AAAAAA]Correct Usage: /command <uid>
\n[B][C][AAAAAA]Example: /command 123[C]456[C]789
"""
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        return
    
    target_uid = parts[1]
    
    target_uid_clean = target_uid.replace('[C]', '')
    
    if not target_uid_clean.isdigit() or len(target_uid_clean) < 8:
        error_msg = f"[B][C][FF0000]❌ Invalid UID! Must be 8+ digits\n"
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        return
    
    token = load_token()
    if not token:
        error_msg = f"[B][C][FF0000]❌ No token available! Please check token.json\n"
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        return
    
    styled_uid = fixnum(target_uid_clean)
    
    processing_message = f"""[B][C][00FFFF]┌──────────┐                [B][C][FFFF00]⏳ PROCESSING ⏳ [B][C][00FFFF]└──────────┘
[C][B][FFFFFF]🆔 UID: {styled_uid}
[C][B][FFFF00]⏳ Removing from friend list..."""
    
    await safe_send_message(chat_type, processing_message, uid, chat_id, key, iv)
    
    try:
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            remove_result = await loop.run_in_executor(executor, DeLet_Uid, target_uid_clean, token, region)
        final_message = f"""{remove_result}"""
        
        await safe_send_message(chat_type, final_message, uid, chat_id, key, iv)
        
    except Exception as e:
        error_msg = f"""[C][B][FF0000]❌ Error: {str(e)[:100]}"""
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        print(f"❌ Remove command error: {e}")
        import traceback
        traceback.print_exc()
####################################                       
        	
#SEND 100 LIKES
def send_like(target_uid):
    try:
        url = "https://sulav-like--sulavgaming10.replit.app/api/like"
        params = {
            "uid": target_uid,
            "server_name": "ind"  
        }

        res = requests.get(url, params=params, timeout=10)

        if res.status_code != 200:
            return "[C][B][FF0000]Failed to send like (API error)."

        data = res.json()

        if data.get("status") != 1:
            return "[C][B][FF0000]Like failed. Try again later."

        likes_given = int(data.get("LikesGivenByAPI", 0))
        raw_uid = str(data.get("UID", target_uid))
        styled_uid = fixnum(raw_uid)  

        if likes_given <= 0:
            return (
                f"[C][B][FFA500]Max likes already sent today.\n"
                f"[C][B][FFFFFF]🆔 UID: {styled_uid}"
            )

        return (
            f"[C][B]-┌ [FFD700]Like Sent Successfully:\n"
            f"[FFFFFF]-├─ Name: {data.get('PlayerNickname', 'N/A')}\n"
            f"- ├─ UID: {styled_uid}\n"
            f"- ├─ Likes Before: {data.get('LikesbeforeCommand', 'N/A')}\n"
            f"- ├─ Likes Given: {data.get('LikesGivenByAPI', '0')}\n"
            f"- └─ Likes After: {data.get('LikesafterCommand', 'N/A')}"
        )

    except Exception as e:
        return f"[C][B][FF0000]Error: {e}"
####################################
#CHECK ACCOUNT IS BANNED
login_url , ob , version = AuToUpDaTE()

Hr = {
    'User-Agent': Uaa(),
    'Connection': "Keep-Alive",
    'Accept-Encoding': "gzip",
    'Content-Type': "application/x-www-form-urlencoded",
    'Expect': "100-continue",
    'X-Unity-Version': "2018.4.11f1",
    'X-GA': "v1 1",
    'ReleaseVersion': ob}

# ---- Random Colores ----
def get_random_color():
    colors = [
        "[FF0000]", "[00FF00]", "[0000FF]", "[FFFF00]", "[FF00FF]", "[00FFFF]", "[FFFFFF]", "[FFA500]",
        "[A52A2A]", "[800080]", "[000000]", "[808080]", "[C0C0C0]", "[FFC0CB]", "[FFD700]", "[ADD8E6]",
        "[90EE90]", "[D2691E]", "[DC143C]", "[00CED1]", "[9400D3]", "[F08080]", "[20B2AA]", "[FF1493]",
        "[7CFC00]", "[B22222]", "[FF4500]", "[DAA520]", "[00BFFF]", "[00FF7F]", "[4682B4]", "[6495ED]",
        "[5F9EA0]", "[DDA0DD]", "[E6E6FA]", "[B0C4DE]", "[556B2F]", "[8FBC8F]", "[2E8B57]", "[3CB371]",
        "[6B8E23]", "[808000]", "[B8860B]", "[CD5C5C]", "[8B0000]", "[FF6347]", "[FF8C00]", "[BDB76B]",
        "[9932CC]", "[8A2BE2]", "[4B0082]", "[6A5ACD]", "[7B68EE]", "[4169E1]", "[1E90FF]", "[191970]",
        "[00008B]", "[000080]", "[008080]", "[008B8B]", "[B0E0E6]", "[AFEEEE]", "[E0FFFF]", "[F5F5DC]",
        "[FAEBD7]"
    ]
    return random.choice(colors)
    
def get_random_evo_emote():
    evo_emotes = [
        909000063,  # AK
        909000068,  # SCAR  
        909000075,  # 1st MP40
        909040010,  # 2nd MP40
        909000081,  # 1st M1014
        909039011,  # 2nd M1014
        909000085,  # XM8
        909000090,  # Famas
        909000098,  # UMP
        909035007,  # M1887
        909042008,  # Woodpecker
        909041005,  # Groza
        909033001,  # M4A1
        909038010,  # Thompson
        909038012,  # G18
        909045001,  # Parafal
        909049010,  # P90
        909051003   # M60
    ]
    return random.choice(evo_emotes)
    
async def extract_uid_from_emote_packet(data_hex, key, iv):
    try:
        packet = await DeCode_PackEt(data_hex[10:])
        packet_json = json.loads(packet)
        
        print(f"📦 Analyzing packet structure: {json.dumps(packet_json, indent=2)[:200]}...")
        
        if packet_json.get('1') == 21:
            if ('2' in packet_json and 'data' in packet_json['2'] and
                '5' in packet_json['2']['data'] and 'data' in packet_json['2']['data']['5']):
                
                nested = packet_json['2']['data']['5']['data']
                if '1' in nested:
                    uid = nested['1']['data']
                    print(f"✅ Extracted UID from pattern 21: {uid}")
                    return uid
        
        elif packet_json.get('1') == 26:
            if ('2' in packet_json and 'data' in packet_json['2'] and
                '1' in packet_json['2']['data']):
                
                uid = packet_json['2']['data']['1']['data']
                print(f"✅ Extracted UID from pattern 26: {uid}")
                return uid
        
        for path in ['2/1', '5/1', '2/data/1', '5/data/1']:
            try:
                uid = get_nested_value(packet_json, path)
                if uid and str(uid).isdigit() and len(str(uid)) > 6:
                    print(f"✅ Extracted UID from path {path}: {uid}")
                    return uid
            except:
                pass
        
        print(f"❌ Could not extract UID from packet")
        return None
        
    except Exception as e:
        print(f"❌ UID extraction error: {e}")
        return None

def get_nested_value(data, path):
    keys = path.split('/')
    current = data
    
    for key in keys:
        if key.isdigit():
            key = str(key)  
        
        if key in current and 'data' in current[key]:
            current = current[key]['data']
        else:
            return None
    
    return current

async def ultra_quick_emote_attack(team_code, emote_id, target_uid, key, iv, region):
    try:
        join_packet = await GenJoinSquadsPacket(team_code, key, iv)
        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_packet)
        print(f"🤖 Joined team: {team_code}")
        
        await asyncio.sleep(1.5)  
        emote_packet = await Emote_k(int(target_uid), int(emote_id), key, iv, region)
        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', emote_packet)
        print(f"🎭 Performed emote {emote_id} to UID {target_uid}")
        
        await asyncio.sleep(0.5)
        
        leave_packet = await ExiT(None, key, iv)
        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', leave_packet)
        print(f"🚪 Left team: {team_code}")
        
        return True, f"Quick emote attack completed! Sent emote to UID {target_uid}"
        
    except Exception as e:
        return False, f"Quick emote attack failed: {str(e)}"
        
        
async def encrypted_proto(encoded_hex):
    key = b'Yg&tc%DEuh6%Zc^8'
    iv = b'6oyZDr22E3ychjM%'
    cipher = AES.new(key, AES.MODE_CBC, iv)
    padded_message = pad(encoded_hex, AES.block_size)
    encrypted_payload = cipher.encrypt(padded_message)
    return encrypted_payload
    
async def GeNeRaTeAccEss(uid , password):
    url = "https://100067.connect.garena.com/oauth/guest/token/grant"
    headers = {
        "Host": "100067.connect.garena.com",
        "User-Agent": (await Ua()),
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close"}
    data = {
        "uid": uid,
        "password": password,
        "response_type": "token",
        "client_type": "2",
        "client_secret": "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3",
        "client_id": "100067"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=Hr, data=data) as response:
            if response.status != 200: return await response.read()
            data = await response.json()
            open_id = data.get("open_id")
            access_token = data.get("access_token")
            return (open_id, access_token) if open_id and access_token else (None, None)
            
async def get_token_inspect_data(access_token):
    """Inspect access token to get open_id and platform info"""
    try:
        url = f"https://100067.connect.garena.com/oauth/token/inspect?token={access_token}"
        headers = {
            "User-Agent": "GarenaMSDK/4.0.19P4 (Vivo Y15c; Android 12; en;IN;)",
            "Connection": "close"
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, ssl=False) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'open_id' in data and 'platform' in data:
                        return data
    except Exception as e:
        print(f"Error inspecting token: {e}")
    return None

def build_major_login_packet(access_token: str, open_id: str, region: str = "IND", lang_code: str = "en",
                             open_id_type_val="4", login_open_id_type_val=4,
                             origin_platform_type_val=4, primary_platform_type_val=4) -> bytes:
    from datetime import datetime
    import requests

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        ip = requests.get('https://api.ipify.org', timeout=5).text
    except:
        ip = "0.0.0.0"

    packet = b''
    packet += _encode_length_delimited(3, now_str)
    packet += _encode_length_delimited(4, "free fire")
    packet += _encode_length_delimited(7, version)
    packet += _encode_length_delimited(20, ip)
    packet += _encode_length_delimited(21, lang_code)
    packet += _encode_length_delimited(22, open_id)
    packet += _encode_length_delimited(23, open_id_type_val)
    packet += _encode_length_delimited(26, region.upper())
    packet += _encode_length_delimited(29, access_token)
    packet += _encode_varint_field(76, 1)
    packet += _encode_varint_field(78, 3)
    packet += _encode_varint_field(79, 2)
    packet += _encode_varint_field(88, login_open_id_type_val)
    packet += _encode_varint_field(97, 1)
    packet += _encode_varint_field(98, 1)
    packet += _encode_length_delimited(99, str(origin_platform_type_val))
    packet += _encode_length_delimited(100, str(primary_platform_type_val))
    return packet


async def EncRypTMajoRLoGin_mobile(open_id: str, access_token: str, region: str = "IND", lang_code: str = "en",
                                   open_id_type_val="4", login_open_id_type_val=4,
                                   origin_platform_type_val=4, primary_platform_type_val=4) -> bytes:
    plain_packet = build_major_login_packet(access_token, open_id, region, lang_code,
                                            open_id_type_val, login_open_id_type_val,
                                            origin_platform_type_val, primary_platform_type_val)
    key = b'Yg&tc%DEuh6%Zc^8'
    iv = b'6oyZDr22E3ychjM%'
    cipher = AES.new(key, AES.MODE_CBC, iv)
    pad_len = 16 - (len(plain_packet) % 16)
    if pad_len == 0:
        pad_len = 16
    plain_padded = plain_packet + bytes([pad_len]) * pad_len
    return cipher.encrypt(plain_padded)


async def EncRypTMajoRLoGin_pc(open_id, access_token,
                               open_id_type="4", login_open_id_type=4,
                               origin_platform_type="4", primary_platform_type="4"):
    major_login = MajoRLoGinrEq_pb2.MajorLogin()
    major_login.event_time = str(datetime.now())[:-7]
    major_login.game_name = "free fire"
    major_login.platform_id = 1
    major_login.client_version = version
    major_login.system_software = "Android OS 9 / API-28 (PQ3B.190801.10101846/G9650ZHU2ARC6)"
    major_login.system_hardware = "Handheld"
    major_login.telecom_operator = "Verizon"
    major_login.network_type = "WIFI"
    major_login.screen_width = 1920
    major_login.screen_height = 1080
    major_login.screen_dpi = "280"
    major_login.processor_details = "ARM64 FP ASIMD AES VMH | 2865 | 4"
    major_login.memory = 3003
    major_login.gpu_renderer = "Adreno (TM) 640"
    major_login.gpu_version = "OpenGL ES 3.1 v1.46"
    major_login.unique_device_id = "Google|34a7dcdf-a7d5-4cb6-8d7e-3b0e448a0c57"
    major_login.client_ip = "223.191.51.89"
    major_login.language = "en"
    major_login.open_id = open_id
    major_login.open_id_type = open_id_type
    major_login.device_type = "Handheld"
    memory_available = major_login.memory_available
    memory_available.version = 55
    memory_available.hidden_value = 81
    major_login.access_token = access_token
    major_login.platform_sdk_id = 1
    major_login.network_operator_a = "Verizon"
    major_login.network_type_a = "WIFI"
    major_login.client_using_version = "7428b253defc164018c604a1ebbfebdf"
    major_login.external_storage_total = 36235
    major_login.external_storage_available = 31335
    major_login.internal_storage_total = 2519
    major_login.internal_storage_available = 703
    major_login.game_disk_storage_available = 25010
    major_login.game_disk_storage_total = 26628
    major_login.external_sdcard_avail_storage = 32992
    major_login.external_sdcard_total_storage = 36235
    major_login.login_by = 3
    major_login.library_path = "/data/app/com.dts.freefireth-YPKM8jHEwAJlhpmhDhv5MQ==/lib/arm64"
    major_login.reg_avatar = 1
    major_login.library_token = "5b892aaabd688e571f688053118a162b|/data/app/com.dts.freefireth-YPKM8jHEwAJlhpmhDhv5MQ==/base.apk"
    major_login.channel_type = 3
    major_login.cpu_type = 2
    major_login.cpu_architecture = "64"
    major_login.client_version_code = "2019118695"
    major_login.graphics_api = "OpenGLES2"
    major_login.supported_astc_bitset = 16383
    major_login.login_open_id_type = login_open_id_type
    major_login.analytics_detail = b"FwQVTgUPX1UaUllDDwcWCRBpWAUOUgsvA1snWlBaO1kFYg=="
    major_login.loading_time = 13564
    major_login.release_channel = "android"
    major_login.extra_info = "KqsHTymw5/5GB23YGniUYN2/q47GATrq7eFeRatf0NkwLKEMQ0PK5BKEk72dPflAxUlEBir6Vtey83XqF593qsl8hwY="
    major_login.android_engine_init_flag = 110009
    major_login.if_push = 1
    major_login.is_vpn = 1
    major_login.origin_platform_type = origin_platform_type
    major_login.primary_platform_type = primary_platform_type
    string = major_login.SerializeToString()
    return await encrypted_proto(string)
    
async def MajorLogin(payload):
    url = f"{login_url}MajorLogin"
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload, headers=Hr, ssl=ssl_context) as response:
            if response.status == 200: return await response.read()
            return None

async def GetLoginData(base_url, payload, token):
    url = f"{base_url}/GetLoginData"
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    Hr['Authorization']= f"Bearer {token}"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload, headers=Hr, ssl=ssl_context) as response:
            if response.status == 200: return await response.read()
            return None

async def DecRypTMajoRLoGin(MajoRLoGinResPonsE):
    proto = MajoRLoGinrEs_pb2.MajorLoginRes()
    proto.ParseFromString(MajoRLoGinResPonsE)
    return proto

async def DecRypTLoGinDaTa(LoGinDaTa):
    proto = PorTs_pb2.GetLoginData()
    proto.ParseFromString(LoGinDaTa)
    return proto

async def DecodeWhisperMessage(hex_packet):
    packet = bytes.fromhex(hex_packet)
    proto = DEcwHisPErMsG_pb2.DecodeWhisper()
    proto.ParseFromString(packet)
    return proto
    
async def decode_team_packet(hex_packet):
    packet = bytes.fromhex(hex_packet)
    proto = sQ_pb2.recieved_chat()
    proto.ParseFromString(packet)
    return proto
    
async def xAuThSTarTuP(TarGeT, token, timestamp, key, iv):
    uid_hex = hex(TarGeT)[2:]
    uid_length = len(uid_hex)
    encrypted_timestamp = await DecodE_HeX(timestamp)
    encrypted_account_token = token.encode().hex()
    encrypted_packet = await EnC_PacKeT(encrypted_account_token, key, iv)
    encrypted_packet_length = hex(len(encrypted_packet) // 2)[2:]
    if uid_length == 9: headers = '0000000'
    elif uid_length == 8: headers = '00000000'
    elif uid_length == 10: headers = '000000'
    elif uid_length == 7: headers = '000000000'
    else: print('Unexpected length') ; headers = '0000000'
    return f"0115{headers}{uid_hex}{encrypted_timestamp}00000{encrypted_packet_length}{encrypted_packet}"
    

async def cHTypE(H):
    if not H: 
        return 'Squid'
    elif H == 1: 
        return 'CLan'
    elif H == 2: 
        return 'PrivaTe'
    else:
        return 'Squid'  
    
async def SEndMsG(H, message, Uid, chat_id, key, iv, region):
    TypE = await cHTypE(H)
    
    if TypE == 'Squid': 
        msg_packet = await xSEndMsgsQ(message, chat_id, key, iv)
    elif TypE == 'CLan': 
        msg_packet = await xSEndMsg(message, 1, chat_id, chat_id, key, iv)
    elif TypE == 'PrivaTe': 
        msg_packet = await xSEndMsg(message, 2, Uid, Uid, key, iv)
    else:
        msg_packet = await xSEndMsgsQ(message, chat_id, key, iv)
        
    return msg_packet
    
    
async def SEndPacKeT(OnLinE , ChaT , TypE , PacKeT):
    if TypE == 'ChaT' and ChaT: whisper_writer.write(PacKeT) ; await whisper_writer.drain()
    elif TypE == 'OnLine': online_writer.write(PacKeT) ; await online_writer.drain()
    else: return 'UnsoPorTed TypE ! >> ErrrroR (:():)' 
    
async def safe_send_message(chat_type, message, target_uid, chat_id, key, iv, max_retries=3):
    for attempt in range(max_retries):
        try:
            P = await SEndMsG(chat_type, message, target_uid, chat_id, key, iv, region)
            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
            print(f"Message sent successfully on attempt {attempt + 1}")
            return True
        except Exception as e:
            print(f"Failed to send message (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5)  # Wait before retry
    return False

async def fast_emote_spam(uids, emote_id, key, iv, region):
    global fast_spam_running
    count = 0
    max_count = 25
    
    while fast_spam_running and count < max_count:
        for uid in uids:
            try:
                uid_int = int(uid)
                H = await Emote_k(uid_int, int(emote_id), key, iv, region)
                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', H)
            except Exception as e:
                print(f"Error in fast_emote_spam for uid {uid}: {e}")
        
        count += 1
        await asyncio.sleep(0.1) 

async def custom_emote_spam(uid, emote_id, times, key, iv, region):
    global custom_spam_running
    count = 0
    
    while custom_spam_running and count < times:
        try:
            uid_int = int(uid)
            H = await Emote_k(uid_int, int(emote_id), key, iv, region)
            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', H)
            count += 1
            await asyncio.sleep(0.1)
        except Exception as e:
            print(f"Error in custom_emote_spam for uid {uid}: {e}")
            break

async def evo_emote_spam(uids, number, key, iv, region):
    try:
        emote_id = EMOTE_MAP.get(int(number))
        if not emote_id:
            return False, f"Invalid number! Use 1-21 only."
        
        success_count = 0
        for uid in uids:
            try:
                uid_int = int(uid)
                H = await Emote_k(uid_int, emote_id, key, iv, region)
                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', H)
                success_count += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                print(f"Error sending evo emote to {uid}: {e}")
        
        return True, f"Sent evolution emote {number} (ID: {emote_id}) to {success_count} player(s)"
    
    except Exception as e:
        return False, f"Error in evo_emote_spam: {str(e)}"

async def evo_fast_emote_spam(uids, number, key, iv, region):
    global evo_fast_spam_running
    count = 0
    max_count = 25
    
    emote_id = EMOTE_MAP.get(int(number))
    if not emote_id:
        return False, f"Invalid number! Use 1-21 only."
    
    while evo_fast_spam_running and count < max_count:
        for uid in uids:
            try:
                uid_int = int(uid)
                H = await Emote_k(uid_int, emote_id, key, iv, region)
                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', H)
            except Exception as e:
                print(f"Error in evo_fast_emote_spam for uid {uid}: {e}")
        
        count += 1
        await asyncio.sleep(0.1)  
    
    return True, f"Completed fast evolution emote spam {count} times"
    
async def send_required_packets(key, iv, region, bot_uid):
    try:
        fields1 = {
            1: 100,
            2: {
                1: bot_uid,
                2: "1.118.1",  
                3: "Android",
                4: "en",
            }
        }
        
        fields2 = {
            1: 101,
            2: {
                1: "vivo",
                2: "1901",
                3: "arm64-v8a",
                4: str(time.time()),
            }
        }
        
        packets = []
        for fields in [fields1, fields2]:
            if region.lower() == "ind":
                packet_type = '0514'
            elif region.lower() == "bd":
                packet_type = "0519"
            else:
                packet_type = "0515"
                
            packet = await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), packet_type, key, iv)
            packets.append(packet)
        
        return packets
        
    except Exception as e:
        print(f"❌ Required packets error: {e}")
        return []

async def evo_custom_emote_spam(uids, number, times, key, iv, region):
    global evo_custom_spam_running
    count = 0
    
    emote_id = EMOTE_MAP.get(int(number))
    if not emote_id:
        return False, f"Invalid number! Use 1-21 only."
    
    while evo_custom_spam_running and count < times:
        for uid in uids:
            try:
                uid_int = int(uid)
                H = await Emote_k(uid_int, emote_id, key, iv, region)
                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', H)
            except Exception as e:
                print(f"Error in evo_custom_emote_spam for uid {uid}: {e}")
        
        count += 1
        await asyncio.sleep(0.1)
    
    return True, f"Completed custom evolution emote spam {count} times"

async def ArohiAccepted(uid,code,K,V):
    fields = {
        1: 4,
        2: {
            1: uid,
            3: uid,
            8: 1,
            9: {
            2: 161,
            4: "y[WW",
            6: 11,
            8: "1.114.18",
            9: 3,
            10: 1
            },
            10: str(code),
        }
        }
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , '0515' , K , V)


async def new_lag(key , iv):
    fields = {
        1: 15,
        2: {
            1: 804266360,
            2: 1
        }
    }
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , '0515' , key , iv)


async def convert_kyro_to_your_system(target_uid, chat_id, key, iv, nickname="DEVIL YT!!", title_id=904990072):
    try:
        fields = {
            1: 1, 
            2: {  
                1: int(target_uid),  
                2: int(chat_id),     
                5: int(datetime.now().timestamp()),
                8: f'{{"TitleID":{title_id},"type":"Title"}}',
                9: {  
                    1: f"[C][B][FF0000]{nickname}",  
                    2: 902048021,  
                    4: 330,                 
                    5: 1001000004,
                    8: "BOT TEAM",
                    10: 1,                  
                    11: 1,
                    13: {    
                        1: 2  
                    },
                    14: {   
                        1: 1158053040, 
                        2: 8,
                        3: "\x10\x15\x08\x0a\x0b\x15\x0c\x0f\x11\x04\x07\x02\x03\x0d\x0e\x12\x01\x05\x06"  
                    }
                },
                10: "en",  
                13: {     
                    2: 2,
                    3: 1
                },
                14: {}    
            }
        }
        
        proto_bytes = await CrEaTe_ProTo(fields)
        packet_hex = proto_bytes.hex()
        
        encrypted_packet = await encrypt_packet(packet_hex, key, iv)
        
        packet_length = len(encrypted_packet) // 2
        
        hex_length = f"{packet_length:04x}"
        
        zeros_needed = 6 - len(hex_length)
        packet_prefix = "121500" + ("0" * zeros_needed)
        
        final_packet_hex = packet_prefix + hex_length + encrypted_packet
        final_packet = bytes.fromhex(final_packet_hex)
        
        return final_packet
        
    except Exception as e:
        return None

async def send_kyro_title_adapted(chat_id, key, iv, target_uid, nickname="DEVIL YT!!", title_id=905190079):
    try:
        from kyro_title_pb2 import GenTeamTitle
        
        root = GenTeamTitle()
        root.type = 1
        
        nested_object = root.data
        nested_object.uid = int(target_uid)
        nested_object.chat_id = int(chat_id)
        nested_object.title = f'{{"TitleID":{title_id},"type":"Title"}}'
        nested_object.timestamp = int(datetime.now().timestamp())
        nested_object.language = "en"
        
        nested_details = nested_object.field9
        nested_details.Nickname = f"[C][B][FF0000]{nickname}"
        nested_details.avatar_id = 902048021
        nested_details.rank = 330
        nested_details.badge = 102000015
        nested_details.Clan_Name = "BOT TEAM"
        nested_details.field10 = 1
        nested_details.global_rank_pos = 1
        nested_details.badge_info.value = 2
        
        nested_details.prime_info.prime_uid = 1158053040
        nested_details.prime_info.prime_level = 8
        nested_details.prime_info.prime_hex = b"\x10\x15\x08\x0a\x0b\x15\x0c\x0f\x11\x04\x07\x02\x03\x0d\x0e\x12\x01\x05\x06"
        
        nested_options = nested_object.field13
        nested_options.url_type = 2
        nested_options.curl_platform = 1
        
        nested_object.empty_field.SetInParent()
        
        packet = root.SerializeToString().hex()
        
        encrypted_packet = await encrypt_packet(packet, key, iv)
        
        packet_length = len(encrypted_packet) // 2
        
        hex_length = f"{packet_length:04x}"
        
        zeros_needed = 6 - len(hex_length)
        packet_prefix = "121500" + ("0" * zeros_needed)
        
        final_packet_hex = packet_prefix + hex_length + encrypted_packet
        return bytes.fromhex(final_packet_hex)
        
    except Exception as e:
        return None

async def handle_title_final(inPuTMsG, uid, chat_id, key, iv, region, chat_type=0):
    
    parts = inPuTMsG.strip().split()
    
    if len(parts) == 1:
        target_uid = uid
    elif len(parts) == 2 and parts[1].isdigit():
        target_uid = parts[1]
    else:
        return
    
    all_titles = [
        904090023, 904090026, 904090027, 904290048, 904590058, 904590059,
        904790062, 904890068, 904990069, 904990070, 904990071, 904990072,
        905090075, 905190079
    ]
    
    selected_title = random.choice(all_titles)
    
    try:
        if not whisper_writer:
            return
        
        title_packet = await convert_kyro_to_your_system(target_uid, chat_id, key, iv, "DEVIL YT!!", selected_title)
        
        if title_packet and whisper_writer:
            whisper_writer.write(title_packet)
            await whisper_writer.drain()
            
    except Exception:
        pass
        
async def send_sticker(target_uid, chat_id, key, iv, nickname="DEVIL YT!!"):
    try:
        sticker_value = get_random_sticker()

        fields = {
            1: 1,
            2: {
                1: int(target_uid),
                2: int(chat_id),
                5: int(datetime.now().timestamp()),
                8: f'{{"StickerStr" : "{sticker_value}", "type":"Sticker"}}',
                9: {
                    1: f"[C][B][FF0000]{nickname}",
                    2: int(get_random_avatar()),
                    4: 330,
                    5: 102000015,
                    8: "BOT TEAM",
                    10: 1,
                    11: 1,
                    13: {1: 2},
                    14: {
                        1: 1158053040,
                        2: 8,
                        3: b"\x10\x15\x08\x0a\x0b\x15\x0c\x0f\x11\x04\x07\x02\x03\x0d\x0e\x12\x01\x05\x06"
                    }
                },
                10: "en",
                13: {
                    2: 2,
                    3: 1
                },
                14: {}
            }
        }

        proto_bytes = await CrEaTe_ProTo(fields)
        packet_hex = proto_bytes.hex()

        encrypted_packet = await encrypt_packet(packet_hex, key, iv)
        packet_length = len(encrypted_packet) // 2
        hex_length = f"{packet_length:04x}"

        zeros_needed = 6 - len(hex_length)
        packet_prefix = "121500" + ("0" * zeros_needed)

        final_packet_hex = packet_prefix + hex_length + encrypted_packet
        final_packet = bytes.fromhex(final_packet_hex)

        print(f"✅ Sticker Sent: {sticker_value}")
        return final_packet

    except Exception as e:
        print(f"❌ Sticker error: {e}")
        return None
    
async def RoomJoin(room_id, password, key, iv):
    try:
        from room_join_pb2 import join_room
        
        root = join_room()
        root.field_1 = 3  
        
        nested_object = root.field_2
        nested_object.field_1 = int(room_id)
        nested_object.field_2 = str(password)
        
        nested_8 = nested_object.field_8
        nested_8.field_1 = "IDC3"
        nested_8.field_2 = 149
        nested_8.field_3 = "IND"
        
        nested_object.field_9 = "\x01\x03\x04\x07\x09\x0a\x0b\x12\x0e\x16\x19\x20\x1d"  
        nested_object.field_10 = 1
        nested_object.field_12.SetInParent()  
        nested_object.field_13 = 1
        nested_object.field_14 = 1
        nested_object.field_16 = "en"
        
        nested_22 = nested_object.field_22
        nested_22.field_1 = 21
        
        packet_hex = root.SerializeToString().hex()
        
        encrypted_packet = await encrypt_packet(packet_hex, key, iv)
        packet_length = len(encrypted_packet) // 2
        
        hex_length = dec_to_hex(packet_length)  
        
        if len(hex_length) == 2:
            header = "0e15000000"
        elif len(hex_length) == 3:
            header = "0e1500000"
        elif len(hex_length) == 4:
            header = "0e150000"
        elif len(hex_length) == 5:
            header = "0e15000"
        else:
            header = "0e150000"
        
        final_packet_hex = header + hex_length + encrypted_packet
        
        return bytes.fromhex(final_packet_hex)
        
    except Exception as e:
        print(f"❌ Room join error: {e}")
        import traceback
        traceback.print_exc()
        return None
        
async def XRLeaveRoom(uid, key, iv):
    try:
        root = room_join_pb2.join_room()

        root.field_1 = 6

        nested_object = root.field_2
        nested_object.field_1 = int(uid)

        nested_object.field_8.field_1 = "IDC3"
        nested_object.field_8.field_2 = 149
        nested_object.field_8.field_3 = "BD"

        nested_object.field_9 = "\u0001\u0003\u0004\u0007\t\n\u000b\u0012\u000e\u0016\u0019 \u001d"
        nested_object.field_10 = 1
        nested_object.field_13 = 1
        nested_object.field_14 = 1
        nested_object.field_16 = "en"
        nested_object.field_22.field_1 = 21

        packet_hex = root.SerializeToString().hex()

        encrypted_packet = await encrypt_packet(packet_hex, key, iv)
        packet_length = len(encrypted_packet) // 2
        packet_len_hex = await base_to_hex(packet_length)

        if len(packet_len_hex) == 2:
            header = "0e15000000"
        elif len(packet_len_hex) == 3:
            header = "0e1500000"
        elif len(packet_len_hex) == 4:
            header = "0e150000"
        elif len(packet_len_hex) == 5:
            header = "0e15000"

        final_packet = header + packet_len_hex + encrypted_packet

        return bytes.fromhex(final_packet)

    except Exception as e:
        print(f"Error in XRLeaveRoom: {e}")
        return None
        
async def RoomJoin_fields(room_id, password, key, iv):
    try:
        fields = {
            1: 3,  
            2: {   
                1: int(room_id),   
                2: str(password),
                8: {
                    1: "IDC3",
                    2: 149,
                    3: "IND"
                },
                9: b"\x01\x03\x04\x07\x09\x0a\x0b\x12\x0e\x16\x19\x20\x1d",  
                10: 1,
                12: {},  
                13: 1,
                14: 1,
                16: "en",
                22: {  
                    1: 21
                }
            }
        }
        
        proto_bytes = await CrEaTe_ProTo(fields)
        packet_hex = proto_bytes.hex()
        
        encrypted_packet = await encrypt_packet(packet_hex, key, iv)
        packet_length = len(encrypted_packet) // 2
        hex_length = dec_to_hex(packet_length)
        
        if len(hex_length) == 2:
            header = "0e15000000"
        elif len(hex_length) == 3:
            header = "0e1500000"
        elif len(hex_length) == 4:
            header = "0e150000"
        elif len(hex_length) == 5:
            header = "0e15000"
        else:
            header = "0e150000"
        
        final_packet_hex = header + hex_length + encrypted_packet
        return bytes.fromhex(final_packet_hex)
        
    except Exception as e:
        print(f"❌ Room join fields error: {e}")
        return None

def remove_from_whitelist(uid_to_remove):
    global WHITELISTED_UIDS
    
    uid_str = str(uid_to_remove)
    
    if uid_str == "OWNER_UID": 
        return False, "Cannot remove bot owner from whitelist!"
    
    if uid_str not in WHITELISTED_UIDS:
        return False, f"UID {uid_str} not in whitelist"
    
    WHITELISTED_UIDS.remove(uid_str)
    return True, f"✅ Removed {uid_str} from whitelist"



async def handle_join_room_command(inPuTMsG, uid, chat_id, key, iv, region, chat_type):
    
    parts = inPuTMsG.strip().split()
    
    if len(parts) < 3:
        error_msg = f"""[B][C][FF0000]⛔ COMMAND REJECTED
\n[B][C][FFFFFF]Invalid command format.
\n[B][C][AAAAAA]Correct Usage: /join_room <room_id> <password>
\n[B][C][AAAAAA]Example: /join_room 123[C]456[C]789 0000
"""
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        return
    
    room_id = parts[1]
    password = parts[2]
    
    if not room_id.isdigit():
        error_msg = f"[B][C][FF0000]❌ Room ID must be numbers only!\n"
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        return
    
    initial_msg = f"[B][C][00FF00]🚀 JOINING CUSTOM ROOM...\n🏠 Room: {room_id}\n🔑 Password: {password}\n"
    await safe_send_message(chat_type, initial_msg, uid, chat_id, key, iv)
    
    try:
        room_packet = await RoomJoin(room_id, password, key, iv)
        
        if not room_packet:
            room_packet = await RoomJoin_fields(room_id, password, key, iv)
        
        if room_packet and online_writer:
            online_writer.write(room_packet)
            await online_writer.drain()
            
            print(f"✅ Room join packet sent! Room: {room_id}")
            
            success_msg = f"""[B][C][00FF00]✅ ROOM JOIN COMMAND SENT!

🏠 Room ID: {room_id}
🔑 Password: {password}
🤖 Status: Bot attempting to join room

💡 Check Free Fire to see if bot joined!
"""
        else:
            success_msg = f"[B][C][FF0000]❌ Failed to create room join packet!\n"
        
        await safe_send_message(chat_type, success_msg, uid, chat_id, key, iv)
        
    except Exception as e:
        error_msg = f"[B][C][FF0000]❌ Error joining room: {str(e)}\n"
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)

async def send_room_message_working(room_id, message, key, iv, region="ind"):
    try:
        room_packet = await Create_xr_room_packet_fixed(room_id, message, key, iv)
        
        if room_packet and online_writer:
            online_writer.write(room_packet)
            await online_writer.drain()
            
            print(f"✅ Room message sent via Create_xr_room_packet_fixed")
            return True
        
        return False
        
    except Exception as e:
        print(f"❌ Room message error: {e}")
        return False

async def handle_room_command(inPuTMsG, uid, chat_id, key, iv, region, chat_type):
    parts = inPuTMsG.strip().split()
    
    if len(parts) < 2:
        error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED \n[B][C][FFFFFF]Invalid command format.   \n[B][C][AAAAAA]Correct Usage: /room <uid>\n[B][C][AAAAAA]Example: /room 436[C]856[C]973[C]3\n"
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        return
    
    target_uid = parts[1]
    
    try:
        status_result, status_message = await check_player_status(target_uid, key, iv)
        
        packet = None
        player_status = None
        
        if not status_result:
            cached_data = load_from_cache(target_uid)
            if cached_data and 'packet' in cached_data:
                packet = cached_data['packet']
                player_status = cached_data.get('status', 'UNKNOWN')
                print(f"⚠️ Using cached data for {target_uid}")
            else:
                error_msg = f"[B][C][FF0000]❌ Player {target_uid} not found\n"
                await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
                return
        else:
            packet = status_result.get('packet', b'')
            player_status = get_player_status(packet)
        
        if not player_status or "IN ROOM" not in player_status:
            info_msg = f"""[B][C][FFFF00]📊 STATUS: {player_status or 'UNKNOWN'}

👤 Player: {target_uid}
❌ Not in custom room

💡 Player must join custom room first!"""
            await safe_send_message(chat_type, info_msg, uid, chat_id, key, iv)
            return
        
        room_id = get_idroom_by_idplayer(packet) if packet else None
        
        if not room_id:
            error_msg = f"[B][C][FF0000]❌ Failed to extract room ID\n"
            await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
            return
        
        success_msg = f"""[B][C][00FF00]✅ ROOM FOUND!

👤 Player: {target_uid}
🏠 Room ID: {room_id}
📊 Status: {player_status}
⚡ Data: {'CACHED' if not status_result else 'LIVE'}

💡 Quick join: /join_room {room_id} 0000
"""
        await safe_send_message(chat_type, success_msg, uid, chat_id, key, iv)
        
        
        spam_count = 5
        for i in range(spam_count):
            try:
                spam_packet = await Room_Spam(target_uid, room_id, f"Spam_{i+1}", key, iv)
                if spam_packet and online_writer:
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', spam_packet)
                    await asyncio.sleep(0.2)
            except Exception as e:
                print(f"Spam error: {e}")
        
        spam_msg = f"[B][C][00FF00]✅ Spammed {spam_count} invites!\n"
        await safe_send_message(chat_type, spam_msg, uid, chat_id, key, iv)
        
        
    except Exception as e:
        print(f"❌ Room command error: {e}")
        error_msg = f"[B][C][FF0000]❌ Error: {str(e)[:80]}\n"
        await safe_send_message(chat_type, error_msg, uid, chat_id, key, iv)
        
async def detect_emote_perfect(data_hex, key, iv):
    
    try:
        decrypted = await DeCode_PackEt(data_hex[10:])  
        packet_json = json.loads(decrypted)
        
        if packet_json.get('1') == 21:
            if '2' in packet_json and 'data' in packet_json['2']:
                emote_data = packet_json['2']['data']
                
                if ('1' in emote_data and '2' in emote_data and 
                    '5' in emote_data and 'data' in emote_data['5']):
                    
                    nested = emote_data['5']['data']
                    
                    if '1' in nested and '3' in nested:
                        return {
                            'type': 'emote',
                            'packet_type': 21,  
                            'identifier': emote_data.get('1', {}).get('data'),
                            'base_emote': emote_data.get('2', {}).get('data'),
                            'target_uid': nested.get('1', {}).get('data'),  
                            'emote_id': nested.get('3', {}).get('data'),
                            'confidence': 100.0,
                            'raw_packet': packet_json
                        }
        
        elif packet_json.get('1') == 26:  
            pass
        
        return None
        
    except Exception as e:
        print(f"❌ Perfect detection error: {e}")
        return None
        
async def detect_emote_with_sender(data_hex, key, iv):
    
    try:
        emote_info = await detect_emote_perfect(data_hex, key, iv)
        
        if not emote_info:
            return None
        
        packet_header = data_hex[:20]
        
        import re
        uid_pattern = r'(\d{9,11})'
        
        all_uids = re.findall(uid_pattern, data_hex)
        
        if len(all_uids) >= 2:
            target_uid = str(emote_info['target_uid'])
            
            for uid in all_uids:
                if uid != target_uid:
                    emote_info['sender_uid'] = int(uid)
                    emote_info['detection_method'] = 'uid_pattern'
                    
                    print(f"✅ SENDER FOUND: {uid} sent emote to {target_uid}")
                    return emote_info
        
        packet_json = emote_info['raw_packet']
        
        def find_sender_in_json(obj, target_uid):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k == 'data' and isinstance(v, (int, str)):
                        v_str = str(v)
                        if v_str.isdigit() and len(v_str) > 8:
                            if v_str != str(target_uid):
                                return int(v)
                    elif isinstance(v, dict):
                        result = find_sender_in_json(v, target_uid)
                        if result:
                            return result
            return None
        
        sender_uid = find_sender_in_json(packet_json, emote_info['target_uid'])
        if sender_uid:
            emote_info['sender_uid'] = sender_uid
            emote_info['detection_method'] = 'json_search'
            return emote_info
        
        emote_info['sender_uid'] = None
        return emote_info
        
    except Exception as e:
        print(f"❌ Sender detection error: {e}")
        return None

async def send_room_message_fixed(room_id, message, key, iv):
    try:
        room_packet = await Create_xr_room_packet_fixed(room_id, message, key, iv)
        
        if not room_packet:
            print("❌ Failed to create room packet")
            return False
        
        if whisper_writer:
            whisper_writer.write(room_packet)
            await whisper_writer.drain()
            print(f"✅ Room message sent via Whisper to room {room_id}")
            return True
        else:
            print("❌ whisper_writer not available!")
            return False
            
    except Exception as e:
        print(f"❌ Room message error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
async def send_title_packet_direct(target_uid, chat_id, key, iv, region="ind"):
    try:
        print(f"🎖️ Sending title to {target_uid} in chat {chat_id}")
        
        title_packet = await convert_kyro_to_your_system(target_uid, chat_id, key, iv)
        
        if title_packet and whisper_writer:
            whisper_writer.write(title_packet)
            await whisper_writer.drain()
            print(f"✅ Title sent via Whisper to {target_uid}")
            return True
            
    except Exception as e:
        print(f"❌ Error sending title directly: {e}")
        import traceback
        traceback.print_exc()
    
    return False

def extract_type_5(packet_json):
    if packet_json.get('1') == 5:
        try:
            if '2' in packet_json and 'data' in packet_json['2']:
                data = packet_json['2']['data']
                sender = data.get('1', {}).get('data')
                emote_id = data.get('4', {}).get('data')
                
                if sender:
                    return {
                        'sender_uid': sender,
                        'emote_id': emote_id or 909042007,  
                        'packet_type': 5,
                        'confidence': 'medium'
                    }
        except:
            pass
    return None

async def extract_emote_info(data_hex, key, iv):
    try:
        packet = await DeCode_PackEt(data_hex[10:])
        packet_json = json.loads(packet)
        
        structures = [
            lambda: extract_type_21(packet_json),
            lambda: extract_type_26(packet_json),
            lambda: extract_type_5(packet_json),
            lambda: generic_extract(packet_json)
        ]
        
        for extractor in structures:
            info = extractor()
            if info and info.get('sender_uid'):
                return info
        
        return None
        
    except Exception as e:
        print(f"❌ Extraction error: {e}")
        return None

def extract_type_21(packet_json):
    if packet_json.get('1') == 21:
        try:
            if ('2' in packet_json and 'data' in packet_json['2'] and
                '5' in packet_json['2']['data'] and 'data' in packet_json['2']['data']['5']):
                
                data = packet_json['2']['data']
                nested = data['5']['data']
                
                sender = nested.get('1', {}).get('data')
                emote_id = nested.get('3', {}).get('data')
                
                if sender and emote_id:
                    return {
                        'sender_uid': sender,
                        'emote_id': emote_id,
                        'packet_type': 21,
                        'confidence': 'high'
                    }
        except:
            pass
    return None

def extract_type_26(packet_json):
    if packet_json.get('1') == 26:
        try:
            if '2' in packet_json and 'data' in packet_json['2']:
                data = packet_json['2']['data']
                sender = data.get('1', {}).get('data')
                emote_id = data.get('2', {}).get('data')
                
                if sender and emote_id:
                    return {
                        'sender_uid': sender,
                        'emote_id': emote_id,
                        'packet_type': 26,
                        'confidence': 'high'
                    }
        except:
            pass
    return None

BIO_ENCRYPTION_KEY = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
BIO_ENCRYPTION_IV = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])

def decode_jwt_noverify(token: str):
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64).decode())
        return payload
    except Exception:
        return None

async def is_bot_in_squad(bot_uid, key, iv):
    global last_bot_status_check, cached_bot_status
    
    current_time = time.time()
    if (current_time - last_bot_status_check < bot_status_cache_time and 
        cached_bot_status is not None):
        return cached_bot_status
    
    try:
        status_packet = await createpacketinfo(bot_uid, key, iv)
        if status_packet and online_writer:
            online_writer.write(status_packet)
            await online_writer.drain()
            
            await asyncio.sleep(2)
            
            if bot_uid in status_response_cache:
                packet = status_response_cache[bot_uid].get('packet', b'')
                status = get_player_status(packet)
                
                in_squad = "INSQUAD" in status
                cached_bot_status = in_squad
                last_bot_status_check = current_time
                
                return in_squad
        
        return False
        
    except Exception as e:
        print(f"❌ Squad check error: {e}")
        return False

def analyze_squad_packet(packet_json):
    
    print("\n🔍 ANALYZING SQUAD PACKET STRUCTURE")
    print("="*50)
    
    if '5' not in packet_json or 'data' not in packet_json['5']:
        print("❌ Not a squad data packet")
        return None
    
    squad_data = packet_json['5']['data']
    
    candidate_fields = []
    
    for field_num in squad_data:
        field_info = squad_data[field_num]
        if 'data' not in field_info:
            continue
            
        data_value = field_info['data']
        
        if isinstance(data_value, list):
            print(f"✅ Field {field_num}: LIST with {len(data_value)} items")
            candidate_fields.append((field_num, 'list', data_value))
            
            if data_value and isinstance(data_value[0], dict):
                print(f"   First item keys: {list(data_value[0].keys())}")
                if '1' in data_value[0]:
                    uid = data_value[0]['1']['data']
                    print(f"   ↳ Contains UID: {uid}")
        
        elif isinstance(data_value, dict):
            keys = list(data_value.keys())
            numeric_keys = [k for k in keys if k.isdigit()]
            if len(numeric_keys) > 0:
                print(f"✅ Field {field_num}: DICT with numeric keys {numeric_keys[:5]}...")
                candidate_fields.append((field_num, 'dict', data_value))
    
    print("\n🎯 MOST LIKELY SQUAD MEMBERS FIELDS:")
    for field_num, field_type, data in candidate_fields:
        print(f"  Field {field_num} ({field_type})")
        
        if field_type == 'list':
            uids = []
            for item in data[:5]:  
                if isinstance(item, dict) and '1' in item:
                    uid = item['1']['data']
                    uids.append(uid)
            if uids:
                print(f"    ↳ Found UIDs: {uids}")
        
        elif field_type == 'dict':
            uids = []
            for key in list(data.keys())[:5]:  
                item = data[key]
                if isinstance(item, dict) and '1' in item:
                    uid = item['1']['data']
                    uids.append(uid)
            if uids:
                print(f"    ↳ Found UIDs: {uids}")
    
    return candidate_fields

def generic_extract(packet_json):
    uid = None
    emote_id = None
    
    def search(obj):
        nonlocal uid, emote_id
        
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == 'data' and isinstance(v, (int, str)) and str(v).isdigit():
                    num = int(v)
                    if 1000000 < num < 99999999999:  
                        if not uid:  
                            uid = num
                        elif str(v).startswith('909') and len(str(v)) >= 9:
                            emote_id = num
                
                elif isinstance(v, dict):
                    search(v)
                elif isinstance(v, list):
                    for item in v:
                        search(item)
    
    search(packet_json)
    
    if uid:
        return {
            'sender_uid': uid,
            'emote_id': emote_id or 909042007,  
            'packet_type': 'generic',
            'confidence': 'medium'
        }
    
    return None
    
async def auto_reply_with_emote(emote_info, key, iv):
    
    try:
        global CURRENT_BOT_UID
        bot_uid = int(CURRENT_BOT_UID)  
        
        sender_uid = emote_info['sender_uid']
        emote_id = emote_info['emote_id']
        
        reply_packet = await Emote_k(sender_uid, emote_id, key, iv, region)
        
        if online_writer:
            online_writer.write(reply_packet)
            await online_writer.drain()
            
            print(f"🤖 Bot replied with emote {emote_id} to {sender_uid}")
            
    except Exception as e:
        print(f"❌ Auto-reply error: {e}")

def extract_squad_members_correct(packet_json):
    
    print("\n🔍 EXTRACTING SQUAD MEMBERS")
    print("="*50)
    
    try:
        if ('5' not in packet_json or 
            'data' not in packet_json['5'] or 
            '2' not in packet_json['5']['data']):
            print("❌ Invalid packet structure")
            return []
        
        field2_data = packet_json['5']['data']['2']['data']
        
        squad_members = []
        
        for key in field2_data:
            if not key.isdigit():
                continue
                
            item = field2_data[key]['data']
            print(f"\n📦 Key {key}: Type = {type(item)}")
            
            if isinstance(item, dict):
                if '1' in item and '2' in item:
                    try:
                        uid = item['1']['data']
                        name = item['2']['data']
                        
                        if isinstance(uid, int) and uid > 1000000:
                            rank = item['4']['data'] if '4' in item else 0
                            
                            print(f"   ✅ PLAYER FOUND!")
                            print(f"      UID: {uid}")
                            print(f"      Name: {name}")
                            print(f"      Rank: {rank}")
                            
                            squad_members.append({
                                'slot': key,
                                'uid': uid,
                                'name': name,
                                'rank': rank
                            })
                        else:
                            print(f"   ❌ Not a UID: {uid}")
                            
                    except Exception as e:
                        print(f"   ❌ Error extracting player: {e}")
                else:
                    print(f"   ↳ Fields: {list(item.keys())[:5]}...")
            elif isinstance(item, (int, str)):
                print(f"   ↳ Value: {item}")
        
        print(f"\n🏆 TOTAL SQUAD MEMBERS FOUND: {len(squad_members)}")
        for member in squad_members:
            print(f"  • Slot {member['slot']}: {member['name']} (UID: {member['uid']})")
        
        return squad_members
        
    except Exception as e:
        print(f"❌ Extraction error: {e}")
        import traceback
        traceback.print_exc()
        return []
        
async def analyze_packet_structure(data_hex, key, iv):
    
    print(f"\n📦 PACKET ANALYSIS")
    print("="*50)
    
    print(f"📏 Length: {len(data_hex)} characters")
    print(f"🔢 Header: {data_hex[:10]}")
    
    try:
        if len(data_hex) > 20:
            decoded = await DeCode_PackEt(data_hex[10:])
            packet_json = json.loads(decoded)
            
            print(f"✅ Successfully decoded!")
            print(f"📊 Packet type (field 1): {packet_json.get('1', 'Unknown')}")
            
            print(f"\n📋 PACKET STRUCTURE:")
            print(f"Top-level fields: {list(packet_json.keys())}")
            
            if '1' in packet_json:
                print(f"  Field 1: {packet_json['1']}")
            
            import re
            emote_patterns = re.findall(r'909[0-9a-f]{6}', data_hex)
            if emote_patterns:
                print(f"\n🎭 EMOTE IDS FOUND IN HEX: {emote_patterns}")
            
            uid_patterns = re.findall(r'(\d{9,11})', data_hex)
            uids = [uid for uid in uid_patterns if not uid.startswith('909')]
            if uids:
                print(f"👤 UIDS FOUND IN HEX: {uids}")
            
            return packet_json
            
        else:
            print("❌ Packet too short to decode")
            return None
            
    except Exception as e:
        print(f"❌ Decode error: {e}")
        return None

async def RedZed_SendInv(bot_uid, uid, key, iv):
    try:
        fields = {
            1: 2, 
            2: {
                1: int(uid), 
                2: "IND", 
                3: 1, 
                4: 1, 
                6: "RedZedKing!!", 
                7: 330, 
                8: 1000, 
                9: 100, 
                10: "DZ", 
                12: 1, 
                13: int(uid), 
                16: 1, 
                17: {
                    2: 159, 
                    4: "y[WW", 
                    6: 11, 
                    8: "1.118.1", 
                    9: 3, 
                    10: 1
                }, 
                18: 306, 
                19: 18, 
                24: 902048021, 
                26: {}, 
                27: {
                    1: 11, 
                    2: int(bot_uid), 
                    3: 99999999999
                }, 
                28: {}, 
                31: {
                    1: 1, 
                    2: 32768
                }, 
                32: 32768, 
                34: {
                    1: bot_uid, 
                    2: 8, 
                    3: b"\x10\x15\x08\x0A\x0B\x13\x0C\x0F\x11\x04\x07\x02\x03\x0D\x0E\x12\x01\x05\x06"
                }
            }
        }
        
        if isinstance(fields[2][34][3], str):
            fields[2][34][3] = b"\x10\x15\x08\x0A\x0B\x13\x0C\x0F\x11\x04\x07\x02\x03\x0D\x0E\x12\x01\x05\x06"
        
        packet = await CrEaTe_ProTo(fields)
        packet_hex = packet.hex()
        
        final_packet = await GeneRaTePk(packet_hex, '0515', key, iv)
        
        return final_packet
        
    except Exception as e:
        print(f"❌ Error in RedZed_SendInv: {e}")
        import traceback
        traceback.print_exc()
        return None

async def test_emote_packet(target_uid, emote_id, key, iv, region="IND"):
    
    print(f"\n🎭 TESTING EMOTE PACKET")
    print("="*50)
    
    emote_packet = await Emote_k(target_uid, emote_id, key, iv, region)
    
    if not emote_packet:
        print("❌ Failed to create packet")
        return False
    
    packet_hex = emote_packet.hex()
    
    print(f"📦 Packet created!")
    print(f"   Length: {len(packet_hex)} characters")
    print(f"   Header: {packet_hex[:20]}")
    
    try:
        if len(packet_hex) > 20:
            payload = packet_hex[20:]  
            
            print(f"\n🔍 RAW PACKET STRUCTURE:")
            print(f"Full hex (first 200 chars):")
            print(packet_hex[:200] + "...")
            
            import re
            uid_hex = hex(target_uid)[2:]
            if uid_hex in packet_hex:
                print(f"✅ Target UID {target_uid} found in packet!")
            else:
                print(f"❌ Target UID not found in hex")
            
            emote_hex = hex(emote_id)[2:]
            if emote_hex in packet_hex:
                print(f"✅ Emote ID {emote_id} found in packet!")
            else:
                print(f"❌ Emote ID not found in hex")
        
        print(f"\n✅ Packet created successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Analysis error: {e}")
        return False
        
async def send_and_monitor_emote(target_uid, emote_id, key, iv, region, reader):
    
    print(f"\n🚀 SENDING TEST EMOTE")
    print(f"   👤 Target: {target_uid}")
    print(f"   🎭 Emote: {emote_id}")
    print("="*50)
    
    emote_packet = await Emote_k(target_uid, emote_id, key, iv, region)
    
    if not emote_packet:
        print("❌ Failed to create packet")
        return
    
    print("📤 Sending packet...")
    if online_writer:
        online_writer.write(emote_packet)
        await online_writer.drain()
        print("✅ Packet sent!")
    else:
        print("❌ No connection")
        return
    
    print("\n⏳ Waiting for response (2 seconds)...")
    
    responses = []
    start_time = time.time()
    
    while time.time() - start_time < 2:  
        try:
            if reader:
                response = await asyncio.wait_for(reader.read(9999), timeout=0.1)
                if response:
                    resp_hex = response.hex()
                    responses.append(resp_hex)
                    
                    print(f"📥 Got response #{len(responses)}")
                    print(f"   Length: {len(resp_hex)} chars")
                    print(f"   Header: {resp_hex[:10]}")
                    
                    if '909' in resp_hex:
                        print(f"   🎭 Contains emote ID!")
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            pass
    
    print(f"\n📊 RESPONSE SUMMARY")
    print(f"Total responses: {len(responses)}")
    
    if len(responses) > 0:
        print("✅ SUCCESS! Server accepted your emote packet!")
    else:
        print("⚠️ No immediate response (might still be processing)")
                
async def detect_and_hijack_emote(data_hex, key, iv, bot_uid, region):
    try:
        emote_info = await extract_emote_info(data_hex, key, iv)
        
        if not emote_info or not emote_info.get('sender_uid'):
            return False
        
        sender_uid = emote_info['sender_uid']
        emote_id = emote_info['emote_id']
        
        print(f"\n🎭 EMOTE DETECTED FOR HIJACK!")
        print(f"   👤 Original Sender: {sender_uid}")
        print(f"   🎭 Emote ID: {emote_id}")
        
        if int(sender_uid) == bot_uid:
            print("⚠️ Skipping - bot's own emote")
            return False
        
        print(f"🤖 HIJACKING EMOTE! Sending as bot {bot_uid}...")
        
        hijack_packet = await Emote_k(
            int(bot_uid),  
            int(emote_id),  
            key, iv, region
        )
        
        
        if hijack_packet and online_writer:
            online_writer.write(hijack_packet)
            await online_writer.drain()
            
            print(f"✅ Emote hijacked! Bot {bot_uid} now appears to do emote {emote_id}")
            return True
        
        return False
        
    except Exception as e:
        print(f"❌ Emote hijack error: {e}")
        return False
        
async def extract_emote_info_simple(data_hex, key, iv):
    try:
        decrypted = await DeCode_PackEt(data_hex[10:])
        if not decrypted:
            return None
            
        packet_json = json.loads(decrypted) if isinstance(decrypted, str) else decrypted
        
        if (packet_json.get('4', {}).get('data') == 22 and 
            '5' in packet_json):
            
            emote_data = packet_json['5']['data']
            
            sender_uid = None
            if '1' in emote_data:
                sender_uid = emote_data['1'].get('data')
            
            emote_id = None
            if '2' in emote_data:
                emote_id = emote_data['2'].get('data')
            
            if sender_uid and emote_id:
                print(f"🎭 Found emote: UID={sender_uid}, Emote={emote_id}")
                return {
                    "sender_uid": int(sender_uid),
                    "emote_id": int(emote_id),
                    "raw_packet": packet_json  
                }
                
    except Exception as e:
        print(f"Error in emote extraction: {e}")
        
    return None
        
async def SwitchLoneWolfDule(BotUid, key, iv):
    fields = {1: 17, 2: {1: BotUid, 2: 1, 3: 1, 4: 43, 5: "\u000b", 8: 1, 19: 1}}
    return await GenPacket((await CreateProtobufPacket(fields)).hex(), '0519', key, iv)        
        
async def KickTarget(target_uid, key, iv):
    fields = {1: 35, 2: {1: int(target_uid)}}
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), '0515' , key, iv)

async def build_get_backpack_proto(character_id: int) -> bytes:
    """Build plain protobuf for GetBackpack request (field 1 = character_id)"""
    def encode_varint(value: int) -> bytes:
        result = []
        while True:
            byte = value & 0x7F
            value >>= 7
            if value:
                result.append(byte | 0x80)
            else:
                result.append(byte)
                break
        return bytes(result)
    return bytes([0x08]) + encode_varint(character_id)

async def get_equipped_emote(access_token: str, region: str) -> int | None:
    """
    Call GetBackpack endpoint, parse response, extract currently equipped emote ID.
    Returns emote ID as int or None if not found.
    """
    url = f"{get_base_url(region).rstrip('/')}/GetBackpack"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/x-www-form-urlencoded",
        "ReleaseVersion": ob,
        "User-Agent": "Dalvik/2.1.0 (Linux; Android 9)",
        "X-Unity-Version": "2018.4.11f1",
        "X-GA": "v1 1",
        "Accept-Encoding": "gzip",
        "Connection": "Keep-Alive"
    }
    # Build request with character_id = 0 (server uses authenticated user)
    proto_hex = (await build_get_backpack_proto(0)).hex()
    encrypted_hex = encrypt_api(proto_hex)
    payload = bytes.fromhex(encrypted_hex)

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=payload, ssl=False) as resp:
            if resp.status != 200:
                return None
            content = await resp.read()
            data_hex = content.hex()
            
            # Use existing decryption/parsing function
            try:
                # Remove the 10-byte header if present (e.g., "0515..." etc.)
                if data_hex.startswith("0515") or data_hex.startswith("0514"):
                    decrypted_json = await DeCode_PackEt(data_hex[10:])
                else:
                    decrypted_json = await DeCode_PackEt(data_hex)
                    
                data = json.loads(decrypted_json)
                
                # Navigate to emote ID: field 2 -> field 8 -> field 1 -> field 2
                # Example structure: {"2":{"data":{"8":{"data":{"1":{"data":{"2":{"data":909000001}}}}}}}
                try:
                    emote_id = data['2']['data']['8']['data']['1']['data']['2']['data']
                    if isinstance(emote_id, int) and 909000000 <= emote_id <= 909999999:
                        return emote_id
                except (KeyError, TypeError):
                    # Fallback: search for any 9-digit number starting with 909
                    import re
                    text = json.dumps(data)
                    match = re.search(r'909\d{6}', text)
                    if match:
                        return int(match.group())
            except Exception as e:
                print(f"Failed to parse GetBackpack response: {e}")
            return None
                
#EMOTES BY PARAHEX X CODEX
async def Emote_k(TarGeT , idT, K, V,region):
    fields = {
        1: 21,
        2: {
            1: 804266360,
            2: equip_emote_id,
            5: {
                1: TarGeT,
                3: idT,
            }
        }
    }
    if region.lower() == "ind":
        packet = '0514'
    elif region.lower() == "bd":
        packet = "0519"
    else:
        packet = "0515"
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , packet , K , V)
    
def equip_emote_like_clan_join(emote_id: int, token: str, region: str) -> str:
    """
    Equip emote using EXACT same pattern as clan join.
    Uses enc() for encryption and same headers as RequestJoinClan.
    """
    try:
        base_url = get_base_url(region)
        url = base_url + "ChooseEmote"
        
        # Encrypt emote ID exactly like clan_id in RequestJoinClan
        encrypted_hex = enc(str(emote_id))   # Same enc() function used for clan join
        data = bytes.fromhex(encrypted_hex)
        
        # Same headers as RequestJoinClan
        from urllib.parse import urlparse
        parsed_url = urlparse(base_url)
        dynamic_host = parsed_url.netloc
        
        headers = {
            'X-Unity-Version': '2018.4.11f1',
            'ReleaseVersion': ob,
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-GA': 'v1 1',
            'Authorization': f'Bearer {token}',
            'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 7.1.2; ASUS_Z01QD Build/QKQ1.190825.002)',
            'Host': dynamic_host,
            'Connection': 'Keep-Alive',
            'Accept-Encoding': 'gzip'
        }
        
        response = requests.post(url, headers=headers, data=data, verify=False, timeout=10)
        
        if response.status_code == 200:
            return f"[B][C][00FF00]✅ Emote {emote_id} equipped successfully!"
        elif response.status_code == 400:
            return f"[B][C][FFFF00]⚠️ Emote {emote_id} may already be equipped or invalid."
        else:
            return f"[B][C][FF0000]❌ Failed to equip emote (HTTP {response.status_code})"
    except Exception as e:
        return f"[B][C][FF0000]❌ Error: {str(e)}"
        
async def create_hijacked_emote(hijacker_uid, emote_id, key, iv, region):
    try:
        fields = {
            1: 21,  
            2: {
                1: 804266360,  
                2: equip_emote_id,
                5: {
                    1: int(hijacker_uid),  
                    3: int(emote_id),      
                }
            }
        }
        
        if region.lower() == "ind":
            packet = '0514'
        elif region.lower() == "bd":
            packet = "0519"
        else:
            packet = "0515"
            
        return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), packet, key, iv)
        
    except Exception as e:
        print(f"❌ Error creating hijacked emote: {e}")
        return None
            
def analyze_hex_packet(packet_hex):
    
    print(f"\n🔬 HEX PACKET ANALYSIS")
    print("="*50)
    
    header = packet_hex[:10]
    print(f"Header (first 5 bytes): {header}")
    
    
    if header.startswith('05'):
        print("📡 Online connection packet")
    elif header.startswith('12'):
        print("💬 Whisper/Chat packet")
    
    import re
    
    hex_patterns = re.findall(r'[0-9a-f]{9,12}', packet_hex.lower())
    
    print(f"\n🔢 Hex sequences found:")
    for pattern in hex_patterns[:10]:  
        try:
            decimal = int(pattern, 16)
            if 1000000 < decimal < 99999999999:  
                print(f"  {pattern} → {decimal} (Possible UID)")
            elif decimal > 900000000:  
                print(f"  {pattern} → {decimal} (Possible emote ID)")
        except:
            print(f"  {pattern}")
    
    print(f"\n📝 Packet preview (first 200 chars):")
    print(packet_hex[:200])
    
    if len(packet_hex) > 200:
        print(f"... and {len(packet_hex) - 200} more characters")
        
def append_to_whitelist(uid_to_add):
    global WHITELISTED_UIDS
    
    uid_str = str(uid_to_add)
    
    if uid_str in WHITELISTED_UIDS:
        return False, f"UID {uid_str} already in whitelist"
    
    WHITELISTED_UIDS.add(uid_str)
    return True, f"✅ Added {uid_str} to whitelist"        
        
async def hijack_squad_emote(data_hex, key, iv, bot_uid, region, in_squad):
    if not in_squad:
        return False
    
    try:
        emote_info = await extract_emote_info(data_hex, key, iv)
        
        if not emote_info:
            return False
        
        sender_uid = emote_info['sender_uid']
        emote_id = emote_info['emote_id']
        
        print(f"\n🏆 SQUAD EMOTE HIJACK!")
        print(f"   👥 In squad: Yes")
        print(f"   👤 Original: {sender_uid}")
        print(f"   🎭 Emote: {emote_id}")
        
        hijack_packet = await create_hijacked_emote(bot_uid, emote_id, key, iv, region)
        
        if hijack_packet and online_writer:
            online_writer.write(hijack_packet)
            await online_writer.drain()
            
            print(f"✅ Squad emote hijacked by bot {bot_uid}!")
            
            await asyncio.sleep(0.3)
            original_packet = await Emote_k(int(sender_uid), int(emote_id), key, iv, region)
            online_writer.write(original_packet)
            await online_writer.drain()
            
            print(f"✅ Also sent original emote to maintain cover")
            
            return True
            
    except Exception as e:
        print(f"❌ Squad hijack error: {e}")
    
    return False      

async def TcPOnLine(ip, port, jwt_token ,bot_uid ,key, iv, AutHToKen, region, reconnect_delay=0.5):
    global online_writer, last_status_packet, status_response_cache, senthi
    global insquad, joining_team, whisper_writer ,last_0500_info
 
   
 
    if insquad is not None:
        insquad = None
    if joining_team is True:
        joining_team = False
    
    online_writer = None
    whisper_writer = None
    bot_id = bot_uid
    token = jwt_token
    while True:
        try:
            print(f"Attempting to connect to {ip}:{port}...")
            reader, writer = await asyncio.open_connection(ip, int(port))
            online_writer = writer

            bytes_payload = bytes.fromhex(AutHToKen)
            online_writer.write(bytes_payload)
            await online_writer.drain()
            print("Authentication token sent. Listening for emotes...")
            
            while True:
                data2 = await reader.read(9999)
                    
                if not data2: 
                    print("Connection closed by the server.")
                    break
                    
                data_hex = data2.hex()
                if SHOW_RAW_DATA:
                    print(f"Data : {data_hex}")
                if DECRYPT_DATA:
                    decrypted = await DeCode_PackEt(data_hex[10:])
                    print(f"Decrypted Data : {decrypted}")    
                    
                if data_hex.startswith("0500"):
                    try:
                        # Decode the 0500 packet (skip first 10 bytes header)
                        json_str = await DeCode_PackEt(data_hex[10:])
                        if json_str:
                            pkt = json.loads(json_str)
                            # Extract idT and squad_code from field 5
                            if '5' in pkt and 'data' in pkt['5']:
                                data5 = pkt['5']['data']
                                if '1' in data5 and 'data' in data5['1'] and '31' in data5 and 'data' in data5['31']:
                                    idT = data5['1']['data']          # field 1 → team ID
                                    squad_code = data5['31']['data']  # field 31 → secret code
                                    global last_0500_info
                                    last_0500_info = {'idT': idT, 'squad_code': squad_code}
                                    print(f"✅ Captured 0500: idT={idT}, squad_code={squad_code}")
                    except Exception as e:
                        print(f"0500 capture error: {e}") 
                                        
                
# =================== EMOTE DETECTION ===================
                if data_hex.startswith("0500"):
                    try:
                        if len(data_hex) > 30:  
                            emote_info = await extract_emote_info_simple(data_hex, key, iv)

                            if emote_info:
                                sender_uid = emote_info['sender_uid']
                                emote_id = emote_info['emote_id']
                                emote_sender = 909044006

                                print(f"\n🎯 EMOTE DETECTED!")
                                print(f"   👤 Sender UID: {sender_uid}")
                                print(f"   🎭 Emote ID: {emote_id}")
                                print(f"   📦 Packet Type: 0500 (Game Action)")
                                print(f"   🔘 Hijack mode: {'ON' if emote_hijack else 'OFF'}")

                                if int(sender_uid) == bot_uid:
                                    print("   ⚠️ Skipping - bot's own emote")
                                    continue

                                if emote_hijack:
                                    print(f"   🤖 Hijack ON – sending emotes...")
                                    bot_emote = await Emote_k(int(bot_uid), emote_id, key, iv, region)
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', bot_emote)

                                    player_emote = await Emote_k(int(sender_uid), emote_sender, key, iv, region)
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', player_emote)
                                else:
                                    print(f"   ⚠️ Hijack OFF – ignoring emote")

                    except Exception as e:
                        print(f"❌ Emote detection error: {e}")
                        continue
            
                 #AUTO ACCEPT 
                if data_hex.startswith('0500') and insquad is not None and joining_team == False:
                    try:
                        packet = await DeCode_PackEt(data_hex[10:])
                        packet_json = json.loads(packet)
                        
                        if packet_json.get('1') in [6, 7]: 
                             insquad = None
                             joining_team = False
                             print("Squad cancelled or exited (code 6/7).")
                             continue
                             
                    except Exception as e:
                        print(f"Error in auto-accept case 1: {e}")
                        pass
                
                if data_hex.startswith("0500") and insquad is None and joining_team == False:
                    try:
                        packet = await DeCode_PackEt(data_hex[10:])
                        packet_json = json.loads(packet)
    
                        uid = packet_json['5']['data']['1']['data']
                        invite_uid = packet_json['5']['data']['2']['data']['1']['data']
                        squad_owner = packet_json['5']['data']['1']['data']  
                        code = packet_json['5']['data']['8']['data']
  

                        emote_id = 909050008
                        global CURRENT_BOT_UID
                        bot_uid = int(CURRENT_BOT_UID)
    
                        if (not WHITELIST_ONLY) or (str(invite_uid) in WHITELISTED_UIDS):
                            print(f"✅ Whitelisted user {squad_owner} invited bot. Accepting...")
                        
                            SendInv = await RedZed_SendInv(bot_uid, invite_uid, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', SendInv)
                            inv_packet = await ArohiRefuse(squad_owner, uid, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', inv_packet)
        
                            print(f"Received squad invite from {squad_owner}, accepting...")                  
                            Join = await ArohiAccepted(squad_owner, code, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', Join)
        
                            await asyncio.sleep(2)
                            
                            await equip_random_bundle(key, iv, region)   
                            
                            await asyncio.sleep(1)                                
                                                                
                            emote_to_sender = await Emote_k(int(uid), emote_id, key, iv, region)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', emote_to_sender)
        
                            bot_emote = await Emote_k(int(bot_uid), emote_id, key, iv, region)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', bot_emote)
                            
                            
            
                            insquad = True
                            print(f"🤖 Bot joined squad of {squad_owner}")
        
        
        
                        else:
                            try:
                                print(f"🚫 Bot is private! Ignoring invite from {squad_owner}")
                                bot_uid = int(CURRENT_BOT_UID)
                                message_text = f" Can't accept Your request Talk to Bot Admin"
                                private_msg_packet = await xSEndMsg(
                                    Msg=message_text,
                                    Tp=2,  
                                    Tp2=int(squad_owner),  
                                    id=int(bot_uid),  
                                    K=key,
                                    V=iv
                                )
                                print("got it")

                                if private_msg_packet and whisper_writer:
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', private_msg_packet)
                                else:
                                    print("can't do it")
                    
                                    
                            except Exception as e:
                                print(" got an error in can't accept")
    

                    except Exception as e:
                        print(f"Error in auto-accept: {e}")
                        insquad = None
                        joining_team = False
                        continue
               
                if data_hex.startswith('0500') and len(data_hex) > 1000:
                    try:
                        packet = await DeCode_PackEt(data_hex[10:])
                        packet_json = json.loads(packet)
                    
                        packet_type = packet_json.get('1')
        
                        if packet_type in [6, 7, 8, 9, 10, 11, 12]:
                            print(f"🚪 Kick/Leave packet detected (Type: {packet_type})")
            
                            insquad = None
                            joining_team = False
            
                            print(f"✅ Bot reset after kick. Ready for new invites.")
                            
                            try:
                                if '5' in packet_json and 'data' in packet_json['5']:
                                    OwNer_UiD, CHaT_CoDe, SQuAD_CoDe = await GeTSQDaTa(packet_json)
                                    print(f"🔄 Attempting reconnection to squad {SQuAD_CoDe}...")
                    
                                    # Re-authenticate chat
                                    JoinCHaT = await AutH_Chat(3, OwNer_UiD, CHaT_CoDe, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', JoinCHaT)
                    
                                    print(f"✅ Chat re-authenticated for reconnection")
                            except:
                                print("⚠️ Could not extract squad info")
                                
                            continue  
        
                        elif '5' in packet_json and 'data' in packet_json['5']:
                            try:
                                OwNer_UiD, CHaT_CoDe, SQuAD_CoDe = await GeTSQDaTa(packet_json)
                
                                if insquad is None:
                                    print(f"🤖 Received squad data while not in squad. Attempting chat auth...")
                                    
                                    JoinCHaT = await AutH_Chat(3, OwNer_UiD, CHaT_CoDe, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', JoinCHaT)
                    
                                    welcome_msg = """[B][C][00FF00]🤖 Bot reconnected!"""
                                    P = await SEndMsG(0, welcome_msg, OwNer_UiD, OwNer_UiD, key, iv, region)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                    
                            except:
                                pass 
                
                    except Exception as e:
                        print(f"❌ Kick/reconnect handler error: {e}")
                        pass
                
                if insquad == True:
                    try:
                        packet = await DeCode_PackEt(data_hex[10:])
                        packet_json = json.loads(packet)
                        
                        OwNer_UiD , CHaT_CoDe , SQuAD_CoDe = await GeTSQDaTa(packet_json)
                        
                        print(f"Received squad data for joining team, attempting chat auth for {OwNer_UiD}...")
                        JoinCHaT = await AutH_Chat(3 , OwNer_UiD , CHaT_CoDe, key,iv)
                        await SEndPacKeT(whisper_writer , online_writer , 'ChaT' , JoinCHaT)
                        
                        def get_random_color(): return "_" 
                        message = """[B][C][FFFFFF]Welcome to [00FFFF]Advanced Bot

[B][C][FFFFFF]Available Features:
[B][C][FFFFFF]• Dance & Emote Commands
[B][C][FFFFFF]• Team & Squad Management
[B][C][FFFFFF]• Player Info & Status 
[B][C][FFFFFF]• Smart Spam Protection
[B][C][FFFFFF]• AI Chat Assistance

[B][C][FFFFFF]Use /help to see all commands

[B][C][FFFFFF]Devloper : [00FFFF]@NAYAN1M
[B][C][FFFFFF]Contact   : [00FFFF]@NAYAN1M

[B][C][FFFFFF]Status: Online & Active"""

                        P = await SEndMsG(0, message, OwNer_UiD, OwNer_UiD, key, iv, region)
                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)

                        joining_team = False
                        insquad = None
                            
                    except Exception as e:
                        print(f"Error in joining_team chat auth: {e}")
                        pass
                


                # ======= STATUS HANDLER =======
                if data_hex.startswith('0f00') and len(data_hex) > 100:
                    print(f"📡 Received status response packet")
    
                    try:
                        if '08' in data_hex:
                            proto_part = f'08{data_hex.split("08", 1)[1]}'
                        else:
                            print("⚠️ Status packet structure missing '08' marker.")
                            continue
        
                        parsed_data = get_available_room(proto_part)
                        if parsed_data:
                            parsed_json = json.loads(parsed_data)
            
                            if "2" in parsed_json and parsed_json["2"]["data"] == 15:
                                player_id = parsed_json["5"]["data"]["1"]["data"]["1"]["data"]
                
                                player_status = get_player_status(proto_part) 
                                print(f"✅ Parsed status for {player_id}: {player_status}")
                
                                cache_entry = {
                                    'status': player_status, 
                                    'packet': proto_part,
                                    'timestamp': time.time(),
                                    'full_packet': data_hex,
                                    'parsed_json': parsed_json
                                }
                
                                try:
                                    StatusData = parsed_json
                                    if ("5" in StatusData and "data" in StatusData["5"] and 
                                        "1" in StatusData["5"]["data"] and "data" in StatusData["5"]["data"]["1"] and 
                                        "3" in StatusData["5"]["data"]["1"]["data"] and "data" in StatusData["5"]["data"]["1"]["data"]["3"] and 
                                        StatusData["5"]["data"]["1"]["data"]["3"]["data"] == 1 and 
                                        "11" in StatusData["5"]["data"]["1"]["data"] and "data" in StatusData["5"]["data"]["1"]["data"]["11"] and 
                                        StatusData["5"]["data"]["1"]["data"]["11"]["data"] == 1):
                
                                        print(f"🎯 SPECIAL CONDITION MET: Player {player_id} is in SOLO mode with special flag 11=1")
                                        cache_entry['special_state'] = 'SOLO_WITH_FLAG_1'
                
                                except Exception as cond_error:
                                    print(f"⚠️ Error checking special condition: {cond_error}")
                                if "IN ROOM" in player_status:
                                    try:
                                        room_id = get_idroom_by_idplayer(proto_part)
                                        if room_id:
                                            cache_entry['room_id'] = room_id
                                            print(f"🏠 Room ID extracted: {room_id}")
                                    except Exception as room_error:
                                        print(f"Failed to extract room ID: {room_error}")
                
                                elif "INSQUAD" in player_status:
                                    try:
                                        leader_id = get_leader(proto_part)
                                        if leader_id:
                                            cache_entry['leader_id'] = leader_id
                                            print(f"👑 Leader ID: {leader_id}")
                                    except Exception as leader_error:
                                        print(f"Failed to extract leader: {leader_error}")
                
                                save_to_cache(player_id, cache_entry)
                                print(f"✅ Saved to cache: {player_id} = {player_status}")
                
                    except Exception as e:
                        print(f"❌ Error parsing status: {e}")
                        import traceback
                        traceback.print_exc()
                

            if online_writer is not None:
                online_writer.close()
                await online_writer.wait_closed()
                online_writer = None
            
            if whisper_writer is not None:
                try:
                    whisper_writer.close()
                    await whisper_writer.wait_closed()
                except:
                    pass
                whisper_writer = None
                
            insquad = None
            joining_team = False
            
            print(f"Connection closed. Reconnecting in {reconnect_delay} seconds...")

        except ConnectionRefusedError:
            print(f"Connection refused by server at {ip}:{port}.")
        except asyncio.TimeoutError:
            print(f"Connection attempt to {ip}:{port} timed out.")
        except Exception as e:
            if isinstance(e, RestartBot):
                raise
            print(f"- ErroR With {ip}:{port} - {e}")
            traceback.print_exc()
            
            if online_writer is not None:
                try:
                    online_writer.close()
                    await online_writer.wait_closed()
                except:
                    pass
                online_writer = None
            if whisper_writer is not None:
                try:
                    whisper_writer.close()
                    await whisper_writer.wait_closed()
                except:
                    pass
                whisper_writer = None
                
            insquad = None
            joining_team = False
            
        await asyncio.sleep(reconnect_delay)
        
                    

                            
async def TcPChaT(ip, port, AutHToKen, key, iv, LoGinDaTaUncRypTinG, ready_event, region , reconnect_delay=0.5):
    print(region, 'TCP CHAT')

    global whisper_writer , spammer_uid , spam_chat_id , spam_uid , online_writer , chat_id , XX , uid , Spy,data2, Chat_Leave, fast_spam_running, fast_spam_task, custom_spam_running, custom_spam_task, spam_request_running, spam_request_task, evo_fast_spam_running, evo_fast_spam_task, evo_custom_spam_running, evo_custom_spam_task, lag_running, lag_task, evo_cycle_running, evo_cycle_task,room_invite_spam_task , room_invite_spam_running , room_invite_spam_loop , WHITELIST_ONLY , emote_hijack , last_0500_info
    status_response_cache = {}
    cache_lock = asyncio.Lock()
    while True:
        try:
            reader , writer = await asyncio.open_connection(ip, int(port))
            whisper_writer = writer
            bytes_payload = bytes.fromhex(AutHToKen)
            whisper_writer.write(bytes_payload)
            await whisper_writer.drain()
            ready_event.set()
            if LoGinDaTaUncRypTinG.Clan_ID:
                clan_id = LoGinDaTaUncRypTinG.Clan_ID
                clan_compiled_data = LoGinDaTaUncRypTinG.Clan_Compiled_Data
                print('\n - TarGeT BoT in CLan ! ')
                print(f' - Clan Uid > {clan_id}')
                print(f' - BoT ConnEcTed WiTh CLan ChaT SuccEssFuLy ! ')
                pK = await AuthClan(clan_id , clan_compiled_data , key , iv)
                if whisper_writer: whisper_writer.write(pK) ; await whisper_writer.drain()
            while True:
                data = await reader.read(9999)
                if not data: break
                
                if data.hex().startswith("120000"):

                    msg = await DeCode_PackEt(data.hex()[10:])
                    chatdata = json.loads(msg)

                    try:
                        response = await DecodeWhisperMessage(data.hex()[10:])
                        uid = response.Data.uid
                        chat_id = response.Data.Chat_ID
                        XX = response.Data.chat_type
                        inPuTMsG = response.Data.msg.strip().lower()
                        print(f"Received message: {inPuTMsG} from UID: {uid}")

                        token = load_token()

                        try:
                            if token:
                                sender_name = await get_player_info_async(uid, token, region)
                            else:
                                sender_name = "Unknown"

                        except Exception as e:
                            print("Name fetch error:", e)
                            sender_name = "Unknown"

                    except Exception as e:
                        print("Decode error:", e)
                        continue

                    if response:                    
                    
                        # Tic Tac Toe commands
                        if inPuTMsG.strip().startswith('/ttt'):
                            parts = inPuTMsG.strip().split()
                            cmd = parts[0].lower()

                            # Show help
                            if len(parts) == 1 or (len(parts) == 2 and parts[1].lower() == 'help'):
                                help_msg = (
            "[B][C][00FF00]🎮 TIC TAC TOE HELP\n\n"
            "[FFFFFF]Commands:\n"
            "/ttt <opponent_uid> - Start a game with another player\n"
            "/ttt bot / ai       - Play against the bot (Medium AI)\n"
            "/ttt <bot_uid>      - Also plays against bot (use bot's UID)\n"
            "/move <position>    - Place your mark (positions 1-9)\n"
            "/ttt quit           - End current game\n\n"
                                )
                                await safe_send_message(response.Data.chat_type, help_msg, uid, chat_id, key, iv)
                                continue
                        
                            # Quit game
                            if len(parts) == 2 and parts[1].lower() == 'quit':
                                if chat_id in ttt_games:
                                    del ttt_games[chat_id]
                                    await safe_send_message(response.Data.chat_type, "[B][C][FFFF00]🛑 Game ended.", uid, chat_id, key, iv)
                                else:
                                    await safe_send_message(response.Data.chat_type, "[B][C][FF0000]❌ No active game.", uid, chat_id, key, iv)
                                continue

                            # Start new game
                            if len(parts) == 2:
                                opponent_input = parts[1]

                                # Determine if playing against bot
                                if opponent_input.lower() in ['bot', 'ai'] or opponent_input == str(CURRENT_BOT_UID):
                                    opponent_is_bot = True
                                    opponent_id = "BOT"
                                    opponent_display = "Bot (Medium AI)"
                                else:
                                    opponent_is_bot = False
                                    opponent_id = opponent_input
                                    opponent_display = "Unknown"

                                # Prevent self-play only if it's a real player (not bot)
                                if not opponent_is_bot and opponent_id == str(uid):
                                    error_msg = f"[B][C][FF0000]❌ You cannot play against yourself!"
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    continue

                                if chat_id in ttt_games:
                                    error_msg = f"[B][C][FF0000]❌ A game is already in progress in this chat. Use /ttt quit to end it."
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    continue

                                # Initialize game
                                ttt_games[chat_id] = {
                                    'board': [' ']*9,
                                    'players': [str(uid), opponent_id],
                                    'turn': str(uid),
                                    'moves': 0,
                                    'is_bot': opponent_is_bot   # store whether opponent is bot
                                }

                                # Fetch names for display
                                token = load_token()
                                p1_name = sender_name
                                p2_name = opponent_display
                                if not opponent_is_bot and token:
                                    p2_name = await get_player_info_async(opponent_id, token) or opponent_id

                                board_msg = render_ttt_board(ttt_games[chat_id]['board'])
                                start_msg = f"""[B][C][00FF00]🎮 TIC TAC TOE GAME STARTED!

[FFFFFF]Turn: {p1_name} (❌)

[00FFFF]{board_msg}

[FFFFFF]Use /move <1-9> to place your mark.
"""
                                await safe_send_message(response.Data.chat_type, start_msg, uid, chat_id, key, iv)
                                continue
                            else:
                                # Invalid format
                                error_msg = f"[B][C][FF0000]❌ Invalid command. Use /ttt help for usage."
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                        if inPuTMsG.strip().startswith('/move '):
                            parts = inPuTMsG.strip().split()
                            if len(parts) != 2:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED                        \n[B][C][FFFFFF]Invalid command format.                        \n[B][C][AAAAAA]Correct Usage: /move <position (1-9)>\n[B][C][AAAAAA]Example: /move 5"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            if chat_id not in ttt_games:
                                error_msg = f"[B][C][FF0000]❌ No active game in this chat. Start one with /ttt <opponent_uid> or /ttt bot"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            game = ttt_games[chat_id]

                            if str(uid) != game['turn']:
                                error_msg = f"[B][C][FF0000]❌ It's not your turn!"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            try:
                                pos = int(parts[1]) - 1
                                if pos < 0 or pos > 8:
                                    raise ValueError
                            except:
                                error_msg = f"[B][C][FF0000]❌ Invalid position. Use numbers 1-9."
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            if game['board'][pos] != ' ':
                                error_msg = f"[B][C][FF0000]❌ That cell is already taken."
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            # Determine marker for current player
                            marker = 'X' if str(uid) == game['players'][0] else 'O'
                            game['board'][pos] = marker
                            game['moves'] += 1

                            # Check win/draw after human move
                            winner = check_ttt_winner(game['board'])
                            board_msg = render_ttt_board(game['board'])

                            if winner:
                                # Game over, human won
                                token = load_token()
                                p1_name = await get_player_info_async(game['players'][0], token, region) if token else game['players'][0]
                                p2_name = "Bot" if game['is_bot'] else (await get_player_info_async(game['players'][1], token, region) if token else game['players'][1])
                                winner_name = p1_name if winner == 'X' else p2_name
                                over_msg = f"""[B][C][00FF00]🏆 GAME OVER!

[FFFFFF]Winner: {winner_name} ({winner})! 🎉

[00FFFF]{board_msg}"""
                                await safe_send_message(response.Data.chat_type, over_msg, uid, chat_id, key, iv)
                                del ttt_games[chat_id]
                                continue
                            elif game['moves'] == 9:
                                # Draw
                                over_msg = f"""[B][C][FFFF00]🤝 GAME OVER - DRAW!

[FFFFFF]It's a tie! Well played both.

[00FFFF]{board_msg}"""
                                await safe_send_message(response.Data.chat_type, over_msg, uid, chat_id, key, iv)
                                del ttt_games[chat_id]
                                continue

                            # Switch turn
                            if game['is_bot'] and game['players'][1] == "BOT":
                                game['turn'] = "BOT"
                                # Send board update and announce bot's turn
                                turn_msg = f"""[B][C][00FF00]🎮 YOUR MOVE!

[FFFFFF]Now it's the bot's turn...

[00FFFF]{board_msg}"""
                                await safe_send_message(response.Data.chat_type, turn_msg, uid, chat_id, key, iv)

                                # Trigger bot move
                                asyncio.create_task(bot_make_move(chat_id, key, iv, response.Data.chat_type, uid, chat_id))
                            else:
                                game['turn'] = game['players'][1] if game['turn'] == game['players'][0] else game['players'][0]
                                token = load_token()
                                next_uid = game['turn']
                                next_name = await get_player_info_async(next_uid, token, region) if token else next_uid
                                next_marker = 'X' if next_uid == game['players'][0] else 'O'
                                turn_msg = f"""[B][C][00FF00]🎮 MOVE PLACED!

[FFFFFF]Turn: {next_name} ({next_marker})

[00FFFF]{board_msg}"""
                                await safe_send_message(response.Data.chat_type, turn_msg, uid, chat_id, key, iv)
                                                           
                                # GET BAN STATUS - /check
                        if inPuTMsG.strip().startswith('/check '):
                            print('Processing check command in any chat type')

                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = (
    "[B][C][FF0000]⛔ COMMAND REJECTED\n"
    "[B][C][FFFFFF]Invalid command format.\n"
    "[B][C][AAAAAA]Correct Usage: /check <uid>\n"
    "[B][C][AAAAAA]Example: /check 436[C]856[C]973[C]3" 
)
                                await safe_send_message(
                                    response.Data.chat_type,
                                    error_msg,
                                    uid,
                                    chat_id,
                                    key,
                                    iv
                                )

                            else:
                                target_uid = parts[1]

                                initial_message = f"""[B][C][00FFFF]┌──────────┐                [B][C][FFFF00]⏳ PROCESSING ⏳ [B][C][00FFFF]└──────────┘
[C][B][FFFFFF]🆔 UID: {fixnum(target_uid)}
[C][B][FFFF00]⏳ Checking ban status..."""
                                await safe_send_message(
                                    response.Data.chat_type,
                                    initial_message,
                                    uid,
                                    chat_id,
                                    key,
                                    iv
                                )

                                loop = asyncio.get_event_loop()
                                with ThreadPoolExecutor() as executor:
                                    ban_result = await loop.run_in_executor(
                                        executor,
                                        get_ban_status,
                                        target_uid
                                    )

                                await safe_send_message(
                                    response.Data.chat_type,
                                    f"\n{ban_result}",
                                    uid,
                                    chat_id,
                                    key,
                                    iv
                                )
                                                                
                        # AI COMMAND - /ai
                        if inPuTMsG.strip().startswith('/ai '):
                            print('Processing AI command')
                            parts = inPuTMsG.strip().split(maxsplit=1)
                            if len(parts) < 2:
                                await safe_send_message(
                                    response.Data.chat_type,
                                    "[B][C][FF0000]⛔ Usage: /ai <message>\nExample: /ai hello",
                                    uid, chat_id, key, iv
                                )
                                continue

                            user_question = parts[1].strip()
                            if not user_question:
                                await safe_send_message(response.Data.chat_type,
                                    "[B][C][FF0000]❌ Kuch likho, jaise /ai free fire kya hai?",
            uid, chat_id, key, iv)
                                continue

    # Get sender name
                            token = load_token()
                            if token:
                                sender_name = await get_player_info_async(uid, token , region)
                            else:
                                sender_name = "User"

                            await safe_send_message(response.Data.chat_type,
                                f"[B][C][FFFF00]🤔 Soch raha hun...",
                                uid, chat_id, key, iv)

    # Get chat history
                            async with chat_history_lock:
                                history = chat_histories.get(chat_id, [])

    # Call Gemini function (sender_name passed)
                            reply, new_history = await talk_with_gemini(user_question, history, sender_name)

    # Save updated history
                            async with chat_history_lock:
                                chat_histories[chat_id] = new_history

                            await safe_send_message(response.Data.chat_type,
                                f"[C][B][FFFFFF]{reply}", uid, chat_id, key, iv)

                        if inPuTMsG.strip().startswith('/bio '):
                            print('Processing bio command in any chat type')

                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /bio <uid>\n[B][C][AAAAAA]Example: /bio 436[C]856[C]973[C]3" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            target_uid = parts[1]

                            if not target_uid.isdigit():
                                await safe_send_message(response.Data.chat_type,
            "[B][C][FF0000]❌ Invalid UID",
            uid, chat_id, key, iv)
                                continue

                            token = load_token()
                            if not token:
                                await safe_send_message(response.Data.chat_type,
        "[B][C][FF0000]❌ Token not available. Please check token.json",
        uid, chat_id, key, iv)
                                continue

                            initial_message = f"""[B][C][00FFFF]┌──────────┐                [B][C][FFFF00]⏳ PROCESSING ⏳ [B][C][00FFFF]└──────────┘
[C][B][FFFFFF]🆔 UID: {fixnum(target_uid)}
[C][B][FFFF00]⏳ Fetching player bio..."""
                            await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)

                            try:
                                bio = await get_player_bio(target_uid, token, region)
                            except Exception as e:
                                print("Bio command error:", e)
                                bio = None

                            if bio and str(bio).strip() not in ("", "N/A", "None"):
                                result_msg = f"{bio}"
                            else:
                                result_msg = f"[B][C][FF0000]❌ Could not fetch bio for UID {fixnum(target_uid)}.\nPlayer may be private or server error."

                            await safe_send_message(response.Data.chat_type, result_msg, uid, chat_id, key, iv)
                                
                        # CHECK ACCOUNT INFO COMMAND - /info
                        if inPuTMsG.strip().startswith('/info'):
                            print('Processing /info command')
                            parts = inPuTMsG.strip().split()
                            target_uid = parts[1] if len(parts) > 1 else str(uid)

                            if not target_uid.isdigit():
                                await safe_send_message(response.Data.chat_type,
                                "[B][C][FF0000]❌ Invalid UID! Must be numbers only.",
                                uid, chat_id, key, iv)
                                continue

                            token = load_token()
                            if not token:
                                await safe_send_message(response.Data.chat_type,
                                "[B][C][FF0000]❌ No token available.",
                                uid, chat_id, key, iv)
                                continue

                            await safe_send_message(response.Data.chat_type,
                            f"[B][C][00FFFF]┌──────────┐                [B][C][FFFF00]⏳ PROCESSING ⏳ [B][C][00FFFF]└──────────┘                      \n[C][B][FFFFFF]🆔 UID: {fixnum(target_uid)}           \n[C][B][FFFF00]⏳ Fetching player info...",
                            uid, chat_id, key, iv)

                            try:
                                messages = await GeT_PLayer_InFo(target_uid, token , region)
                                for msg in messages:
                                    await safe_send_message(response.Data.chat_type, msg, uid, chat_id, key, iv)
                                    await asyncio.sleep(0.2) 
                            except Exception as e:
                                await safe_send_message(response.Data.chat_type,
                                f"[B][C][FF0000]Error: {str(e)}", uid, chat_id, key, iv)
                                                               

                        # NEW COMMAND - /like
                        if inPuTMsG.strip().startswith('/like '):
                            print('Processing like command in any chat type')

                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = "[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /like <uid>\n[B][C][AAAAAA]Example: /like 436[C]856[C]973[C]3"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                target_uid = parts[1]
                                styled_uid = fixnum(target_uid)

                                processing_message = f"""[B][C][00FFFF]┌──────────┐                [B][C][FFFF00]⏳ PROCESSING ⏳ [B][C][00FFFF]└──────────┘
[C][B][FFFFFF]🆔 UID: {styled_uid}
[C][B][FFFF00]⏳ Sending like..."""
                                await safe_send_message(response.Data.chat_type, processing_message, uid, chat_id, key, iv)

                                loop = asyncio.get_event_loop()
                                with ThreadPoolExecutor() as executor:
                                    like_result = await loop.run_in_executor(
                                        executor, send_like, target_uid
                                    )

                                final_message = (
            f"[C][B][FFFF00]{like_result}"
                                )
                                await safe_send_message(response.Data.chat_type, final_message, uid, chat_id, key, iv)
                                
                        elif inPuTMsG.strip().startswith('/lw'):
                            print('Processing fast auto-start command')
                            global auto_start_running, auto_start_teamcode, stop_auto, auto_start_task
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Usage: /lw <team_code>\n[B][C][AAAAAA]Example: /lw 123456"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                team_code = parts[1]
                                if not team_code.isdigit():
                                    error_msg = f"[B][C][FF0000]❌ Team code must be numbers only!\n"
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    continue
        
                                if auto_start_running:
                                    error_msg = f"[B][C][FF0000]❌ Auto start already running (use /stop_auto first)\n"
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    continue
        
                                stop_auto = False
                                auto_start_running = True
                                auto_start_teamcode = team_code
        
                                initial_msg = f"""
[B][C][00FFFF]⚡ FAST AUTO START ACTIVATED!
🎯 Team Code: {team_code}
⏰ Start Spam: {fast_start_spam_duration} sec
⏳ Wait Time: {fast_wait_after_match} sec
🚀 Spam Delay: {fast_start_spam_delay} sec
🔄 Loop: Continuous

💡 To stop: /stop_auto
                        """
                                await safe_send_message(response.Data.chat_type, initial_msg, uid, chat_id, key, iv)
        
                                auto_start_task = asyncio.create_task(
                                    fast_auto_start_loop(team_code, uid, chat_id, response.Data.chat_type, key, iv, region)
                                )
            
                        if inPuTMsG.strip().startswith('/quick'):
                            print('Processing quick emote attack command')
    
                            parts = inPuTMsG.strip().split()
    
                            if len(parts) < 3:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /quick <team_code> [emote_id] [target_uid]\n[B][C][AAAAAA]Example: /quick ABC123\n[B][C][AAAAAA]Example: /quick ABC123" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                team_code = parts[1]
        
                                emote_id = parts[0]
                                target_uid = str(response.Data.uid)  
        
                                if len(parts) >= 3:
                                    emote_id = parts[2]
                                if len(parts) >= 4:
                                    target_uid = parts[3]
        
                                if target_uid == str(response.Data.uid):
                                    target_name = "Yourself"
                                else:
                                    target_name = f"UID {target_uid}"
        
                                initial_message = f"[B][C][FFFF00]⚡ QUICK EMOTE ATTACK!\n\n[FFFFFF]🎯 Team: [00FF00]{team_code}\n[FFFFFF]🎭 Emote: [00FF00]{emote_id}\n[FFFFFF]👤 Target: [00FF00]{target_name}\n[FFFFFF]⏱️ Estimated: [00FF00]2 seconds\n\n[FFFF00]Executing sequence...\n"
                                await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)
        
                                try:
                                    success, result = await ultra_quick_emote_attack(team_code, emote_id, target_uid, key, iv, region)
            
                                    if success:
                                        success_message = f"[B][C][00FF00]✅ QUICK ATTACK SUCCESS!\n\n[FFFFFF]🏷️ Team: [00FF00]{team_code}\n[FFFFFF]🎭 Emote: [00FF00]{emote_id}\n[FFFFFF]👤 Target: [00FF00]{target_name}\n\n[00FF00]Bot joined → emoted → left! ✅\n"
                                    else:
                                        success_message = f"[B][C][FF0000]❌ Regular attack failed: {result}\n"
                                    
                                    await safe_send_message(response.Data.chat_type, success_message, uid, chat_id, key, iv)
            
                                except Exception as e:
                                    print("failed")
                       
                        if inPuTMsG.strip().startswith('/join_room '):
                            print('Processing join_room command')
                            await handle_join_room_command(inPuTMsG, uid, chat_id, key, iv, region, response.Data.chat_type)
                          
                        if inPuTMsG.strip() == '/leave_room' or inPuTMsG.strip().startswith('/rmleave '):
                            print('Processing room leave command')
    
                            parts = inPuTMsG.strip().split()
                            target_uid = uid  
    
                            if len(parts) > 1:
                                target_uid = parts[1]
    
                            initial_msg = f"[B][C][00FF00]🚪 Leaving room...\nTarget UID: {target_uid}\n"
                            await safe_send_message(response.Data.chat_type, initial_msg, uid, chat_id, key, iv)
    
                            try:
                                leave_packet = await XRLeaveRoom(target_uid, key, iv)
        
                                if leave_packet:
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', leave_packet)
            
                                    success_msg = f"[B][C][00FF00]✅ Successfully left room!\n"
                                    await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                                else:
                                    error_msg = f"[B][C][FF0000]❌ Failed to create leave packet\n"
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
            
                            except Exception as e:
                                error_msg = f"[B][C][FF0000]❌ Error leaving room: {str(e)}\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)

                        #ROOM LAG-/rmlag
                        elif inPuTMsG.startswith('/rmlag '):
                            try:
                                parts = inPuTMsG.strip().split()

                                if len(parts) < 3:
                                    msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /rmlag <room_id> <password>\n[B][C][AAAAAA]Example: /rmlag 123[C]456[C]789[C] 000[C]0"
                                    await safe_send_message(response.Data.chat_type, msg, uid, chat_id, key, iv)
                                    return

                                room_id = parts[1]
                                password = parts[2]
        
                                async def run_simple_lag():
                                    start_time = time.time()
                                    while time.time() - start_time < 10:
                                        join_pkt = await RoomJoin(int(room_id), password, key, iv)
                                        if online_writer and join_pkt:
                                            online_writer.write(join_pkt)
                                            await online_writer.drain()
                
                                        leave_pkt = await XRLeaveRoom(int(uid), key, iv)
                                        if online_writer and leave_pkt:
                                            online_writer.write(leave_pkt)
                                            await online_writer.drain()
                
                                        await asyncio.sleep(0.10)
        
                                msg = f"[B][C][00FF00]✅ Room lag started for 10 seconds!"
                                await safe_send_message(response.Data.chat_type, msg, uid, chat_id, key, iv)
        
                                asyncio.create_task(run_simple_lag())

                            except Exception as e:
                                print("ROOMLAG error:", e)     
            
                        if inPuTMsG.strip().startswith('/inv '):
                            print('Processing invite command in any chat type')
                            
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /inv <uid>\n[B][C][AAAAAA]Example: /inv 123[C]456[C]789" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                target_uid = parts[1]
                                initial_message = f"[B][C][00FF00]\n5 Player Member Group invitation Sent Successfully To {fixnum(target_uid)}" 
                                await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)
                                
                                try:
                                    PAc = await OpEnSq(key, iv, region)
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', PAc)
                                    await asyncio.sleep(0.3)
                                    
                                    C = await cHSq(5, int(target_uid), key, iv, region)
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                                    await asyncio.sleep(0.3)
                                    
                                    V = await SEnd_InV(5, int(target_uid), key, iv, region)
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', V)
                                    await asyncio.sleep(0.3)
                                    
                                    E = await ExiT(None, key, iv)
                                    await asyncio.sleep(2)
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', E)                               
                                    
                                    
                                except Exception as e:
                                    error_msg = f"[B][C][FF0000]❌ ERROR sending invite: {str(e)}" 
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)   

                        if inPuTMsG.startswith(("/6")):
                            initial_message =  f"[B][C][00FF00]6 MEMBERS GROUP CREATED! \nAccept Invite Fast!" 
                            await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)
                            
                            PAc = await OpEnSq(key, iv, region)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', PAc)
                            
                            C = await cHSq(6, uid, key, iv, region)
                            await asyncio.sleep(0.3)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                            
                            V = await SEnd_InV(6, uid, key, iv, region)
                            await asyncio.sleep(0.3)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', V)
                            
                            E = await ExiT(None, key, iv)
                            await asyncio.sleep(3.5)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', E)                            
                                                        
                                                        
                        if inPuTMsG.startswith('t_31_p_veteran_wlcm_friend'):
                            print("got it")                            
                                                        
                        if inPuTMsG.startswith('/hijack_on'):
                            success_msg = f"[B][C][FF0000]The Hijack Is Now On\n"
                            await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                            emote_hijack = True
                            
                        if inPuTMsG.startswith('/hijack_off'):
                            success_msg = f"[B][C][FF0000]The Hijack Is Now OFF\n"
                            await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                            emote_hijack = False
                            
                        if inPuTMsG.strip().startswith('/dm'):
                            print('Processing private message command')
    
                            parts = inPuTMsG.strip().split(maxsplit=2)  
    
                            if len(parts) < 3:
                                error_msg = f"""[B][C][FF0000]⛔ COMMAND REJECTED
[B][C][FFFFFF]Invalid command format.
[B][C][AAAAAA]Correct Usage: /dm <target_uid> <message>
[B][C][AAAAAA]Example: /dm 123[C]456[C]789 Hello!
"""
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                return
    
                            target_uid = parts[1]
                            message_text = parts[2]
    
                            if not target_uid.isdigit() or len(target_uid) < 8:
                                error_msg = f"[B][C][FF0000]❌ Invalid UID! Must be 8+ digits\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                return
    
                            if len(message_text) > 100:
                                error_msg = f"[B][C][FF0000]❌ Message too long! Max 100 characters\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                return
    
                            initial_msg = f"[B][C][00FF00]📩 SENDING PRIVATE MESSAGE\n"
                            initial_msg += f"👤 To: {fixnum(target_uid)}\n"
                            initial_msg += f"📝 Message: {message_text[:30]}...\n"
                            initial_msg += f"⏳ Sending..." 
    
                            await safe_send_message(response.Data.chat_type, initial_msg, uid, chat_id, key, iv)
    
                            try:
                                bot_uid = int(CURRENT_BOT_UID)
        
                                private_msg_packet = await xSEndMsg(
                                    Msg=message_text,
                                    Tp=2,  
                                    Tp2=int(target_uid),  
                                    id=int(bot_uid),  
                                    K=key,
                                    V=iv
                                )
        
                                if private_msg_packet and whisper_writer:
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', private_msg_packet)
            
                                    success_msg = f"""[B][C][00FF00]✅ PRIVATE MESSAGE SENT!

👤 To: {fixnum(target_uid)}
📝 Message: {message_text}
✅ Status: Delivered

💡 Target will see this in their private messages!
"""
                                    await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                                    print(f"✅ Private message sent to {fixnum(target_uid)}: {message_text}")
                                else:
                                    error_msg = f"[B][C][FF0000]❌ Failed to create message packet!\n"
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
            
                            except Exception as e:
                                print(f"❌ Private message error: {e}")
                                error_msg = f"[B][C][FF0000]❌ Error: {str(e)[:50]}\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                
                        if inPuTMsG.strip().startswith('/friend_list'):
                            await handle_friend_list_command(inPuTMsG, uid, chat_id, key, iv, region, response.Data.chat_type)
                            continue
                            
                        if inPuTMsG.strip().startswith('/guild_members '):
                            print('Processing /clanmembers command')
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = (
            "[B][C][FF0000]⛔ COMMAND REJECTED\n"
            "[B][C][FFFFFF]Invalid command format.\n"
            "[B][C][AAAAAA]Correct Usage: /clanmembers <clan_id>\n"
            "[B][C][AAAAAA]Example: /clanmembers 123456789"
                                )
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            clan_id_str = parts[1]
                            if not clan_id_str.isdigit():
                                await safe_send_message(
                                    response.Data.chat_type,
                                    "[B][C][FF0000]❌ Clan ID must be numbers only!",
                                    uid, chat_id, key, iv
                                )
                                continue

                            clan_id = int(clan_id_str)

                            # Owner‑only restriction (like /friend_list)
                            if str(uid) != OWNER_UID:
                                error_msg = (
            "[B][C][FFA500]┌──────────┐                "
            "[B][C][FF0000]⛔ ACCESS DENIED ⛔ "
            "[B][C][FFA500]└──────────┘     "
            "[B][C][FFFFFF]⚠️ Only Admins Can Use This Command!"
                                )
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue
                        
                            token = load_token()
                            if not token:
                                await safe_send_message(
                                    response.Data.chat_type,
                                    "[B][C][FF0000]❌ No token available! Please check token.json",
                                    uid, chat_id, key, iv
                                )
                                continue

                            # Initial status message (like /friend_list)
                            status_msg = f"[B][C][FFFF00]📞 Fetching clan members for {fixnum(clan_id_str)} from Indian server..."
                            await safe_send_message(response.Data.chat_type, status_msg, uid, chat_id, key, iv)

                            # Fetch and display
                            messages = await fetch_clan_members(clan_id, token, region)
                            for msg in messages:
                                await safe_send_message(response.Data.chat_type, msg, uid, chat_id, key, iv)
                                await asyncio.sleep(0.3)   # small delay to avoid flooding
                                
                        if inPuTMsG.strip().startswith('/add '):
                            print('Processing addfriend command')
                            if str(uid) != OWNER_UID:
                                error_msg = "[B][C][FFA500]┌──────────┐                [B][C][FF0000]⛔ ACCESS DENIED ⛔ [B][C][FFA500]└──────────┘     [B][C][FFFFFF]⚠️ Only Admins Can Use This Command!" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = (
    "[B][C][FF0000]⛔ COMMAND REJECTED\n"
    "[B][C][FFFFFF]Invalid command format.\n"
    "[B][C][AAAAAA]Correct Usage: /add <uid>\n"
    "[B][C][AAAAAA]Example: /add 123[C]456[C]789"
)
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                target_uid = parts[1]
                                token = load_token()
                                if not token:
                                    error_msg = "[B][C][FF0000]❌ No token available! Please check token.json"
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                else:
                                    processing = f"""[B][C][00FFFF]┌──────────┐                [B][C][FFFF00]⏳ PROCESSING ⏳ [B][C][00FFFF]└──────────┘
[C][B][FFFFFF]🆔 UID: {fixnum(target_uid)}
[C][B][FFFF00]⏳ Friend Request Sending..."""
                                    await safe_send_message(response.Data.chat_type, processing, uid, chat_id, key, iv)

                                    result = await RequestAddingFriend_Uid_async(target_uid, token, region)

                                    await safe_send_message(response.Data.chat_type, result, uid, chat_id, key, iv)
                        # NEW COMMAND - /remove
                        if inPuTMsG.strip().startswith('/remove '):
                            await handle_remove_command(inPuTMsG, uid, chat_id, key, iv, region, response.Data.chat_type)
                            
                        if inPuTMsG.strip().startswith('/remove_all'):
                            print('Processing /remove_all command')
                            if str(uid) != OWNER_UID:
                                error_msg = (
            "[B][C][FFA500]┌──────────┐                "
            "[B][C][FF0000]⛔ ACCESS DENIED ⛔ "
            "[B][C][FFA500]└──────────┘     "
            "[B][C][FFFFFF]⚠️ Only Admins Can Use This Command!"
                                )
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            token = load_token()
                            if not token:
                                error_msg = "[B][C][FF0000]❌ No token available! Please check token.json"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            status_msg = f"[B][C][FFFF00]📞 Fetching friend list from Indian server..."
                            await safe_send_message(response.Data.chat_type, status_msg, uid, chat_id, key, iv)

                            friend_data = await get_friend_list(token, region)
                            if not friend_data:
                                error_msg = "[B][C][FF0000]❌ Failed to fetch friend list (API error or empty response)."
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            total = friend_data.get("Total", 0)
                            friends = friend_data.get("Friends", [])
                        
                            # Exclude owner from removal
                            friends_to_remove = [f for f in friends if f.get("uid") != OWNER_UID]
                            total_to_remove = len(friends_to_remove)
                            total_skipped = total - total_to_remove  # likely 1 if owner is in list

                            if total_to_remove == 0:
                                info_msg = f"[B][C][FFFF00]📭 No friends to remove."
                                await safe_send_message(response.Data.chat_type, info_msg, uid, chat_id, key, iv)
                                continue

                            remove_msg = f"[B][C][00FF00]🗑️ Removing {total_to_remove} friends...\n⏳ Please wait."
                            await safe_send_message(response.Data.chat_type, remove_msg, uid, chat_id, key, iv)

                            success_count = 0
                            fail_count = 0
                            for idx, friend in enumerate(friends_to_remove, 1):
                                friend_uid = friend.get("uid")
                                if not friend_uid:
                                    continue

                                loop = asyncio.get_event_loop()
                                with ThreadPoolExecutor() as executor:
                                    result = await loop.run_in_executor(executor, DeLet_Uid, friend_uid, token, region)

                                if "SUCCESS" in result:
                                    success_count += 1
                                else:
                                    fail_count += 1

                                # Progress update every 10 removals
                                if idx % 10 == 0:
                                    progress = f"[B][C][FFFF00]Progress: {idx}/{total_to_remove} removed..."
                                    await safe_send_message(response.Data.chat_type, progress, uid, chat_id, key, iv)

                                await asyncio.sleep(0.2)  # rate limit

                            final_msg = (
                                f"[B][C][00FF00]✅ REMOVAL COMPLETE!\n"
                                f"[FFFFFF]Total friends: {total}\n"
                                f"[00FF00]Removed: {success_count}\n"
                                f"[FF0000]Failed: {fail_count}" 
                            )
                            await safe_send_message(response.Data.chat_type, final_msg, uid, chat_id, key, iv)
                                                                
                                #GUILD JOIN
                        if inPuTMsG.strip().startswith('/guild_join '):
                            print('Processing official clan join command')
                            if str(uid) != OWNER_UID:
                                error_msg = "[B][C][FFA500]┌──────────┐                [B][C][FF0000]⛔ ACCESS DENIED ⛔ [B][C][FFA500]└──────────┘     [B][C][FFFFFF]⚠️ Only Admins Can Use This Command!" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /guild_join <clan_id>\n[B][C][AAAAAA]Example: /guild_join 123[C]456[C]789[C]"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue
                            clan_id = parts[1]
                            token = load_token()
                            if not token:
                                await safe_send_message(response.Data.chat_type, "[B][C][FF0000]❌ No token available!", uid, chat_id, key, iv)
                                continue
                            processing = f"""[B][C][00FFFF]┌──────────┐                [B][C][FFFF00]⏳ PROCESSING ⏳ [B][C][00FFFF]└──────────┘
[C][B][FFFFFF]🏷️ CLAN ID: {fixnum(clan_id)}
[C][B][FFFF00]⏳ Sending clan join request..."""
                            await safe_send_message(response.Data.chat_type, processing, uid, chat_id, key, iv)
                            result = await RequestJoinClan(clan_id, token, region)
                            await safe_send_message(response.Data.chat_type, result, uid, chat_id, key, iv)
                                    
                                    #GUILD LEAVE 
                        if inPuTMsG.strip().startswith('/guild_leave '):
                            print('Processing official clan leave command')
                            if str(uid) != OWNER_UID:
                                error_msg = "[B][C][FFA500]┌──────────┐                [B][C][FF0000]⛔ ACCESS DENIED ⛔ [B][C][FFA500]└──────────┘     [B][C][FFFFFF]⚠️ Only Admins Can Use This Command!" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /guild_leave <clan_id>\n[B][C][AAAAAA]Example: /guild_leave 123[C]456[C]"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue
                            clan_id = parts[1]
                            if not clan_id.isdigit():
                                await safe_send_message(response.Data.chat_type, "[B][C][FF0000]❌ Clan ID must be numbers only!", uid, chat_id, key, iv)
                                continue
                            token = load_token()
                            if not token:
                                await safe_send_message(response.Data.chat_type, "[B][C][FF0000]❌ No token available!", uid, chat_id, key, iv)
                                continue
                            processing = f"""[B][C][00FFFF]┌──────────┐                [B][C][FFFF00]⏳ PROCESSING ⏳ [B][C][00FFFF]└──────────┘
[C][B][FFFFFF]🏷️ CLAN ID: {fixnum(clan_id)}
[C][B][FFFF00]⏳ Sending clan leave request..."""
                            await safe_send_message(response.Data.chat_type, processing, uid, chat_id, key, iv)
                            result = await QuitClan(clan_id, token, region)
                            await safe_send_message(response.Data.chat_type, result, uid, chat_id, key, iv)
                            
                        if inPuTMsG.strip().startswith('/outfit '):
                            print('Processing /outfit command')
                            parts = inPuTMsG.strip().split()
                            if len(parts) != 7:   
                                error_msg = (
    "[B][C][FF0000]⛔ COMMAND REJECTED\n"
    "[B][C][FFFFFF]Invalid command format.\n"
    "[B][C][AAAAAA]Correct Usage: /outfit <character_id> <bottom> <shoe> <top> <facepaint> <mask>\n"
    "[B][C][AAAAAA]Example: /outfit 102[C]000[C]007[C] 204[C]052[C]005[C] 205[C]052[C]001[C] 203[C]052[C]001[C] 214[C]000[C]000[C] 211[C]052[C]006\n"
)
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            try:
                                character_id = int(parts[1])
                                skill_ids = [int(x) for x in parts[2:7]]
                            except ValueError:
                                await safe_send_message(response.Data.chat_type, "[B][C][FF0000]❌ All arguments must be numbers.", uid, chat_id, key, iv)
                                continue

                            token = load_token()
                            if not token:
                                await safe_send_message(response.Data.chat_type, "[B][C][FF0000]❌ No token available.", uid, chat_id, key, iv)
                                continue

                            await safe_send_message(response.Data.chat_type, f"[B][C][FFFF00]⏳ Changing outfit...", uid, chat_id, key, iv)

                            result = await change_clothes(character_id, skill_ids, token, region)
                            await safe_send_message(response.Data.chat_type, result, uid, chat_id, key, iv)
    
                        if inPuTMsG.strip().startswith('/title'):
                            await handle_title_final(inPuTMsG, uid, chat_id, key, iv, region, 0)
                            
                        if inPuTMsG.strip().startswith('/'):
                            await handle_title_final(inPuTMsG, uid, chat_id, key, iv, region, 0)
                        # NEW COMMAND-/sticker
                        if inPuTMsG.strip().startswith('/sticker'):
                            packet = await send_sticker(uid, chat_id, key, iv, nickname="DEVIL YT!!")
                            if packet:
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', packet)
                            
                        if inPuTMsG.startswith("/tc"): await SEndPacKeT(whisper_writer, online_writer, 'OnLine', await tc(key, iv))
                        
                        if inPuTMsG.startswith("/public"): await SEndPacKeT(whisper_writer, online_writer, 'OnLine', await Team_Public(key, iv))
                        
                        if inPuTMsG.startswith("/private"): await SEndPacKeT(whisper_writer, online_writer, 'OnLine', await Team_Private(key, iv))
                        
                        if inPuTMsG.strip().startswith('/kick'):
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /kick <uid>\n[B][C][AAAAAA]Example: /kick 123[C]456[C]789" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                target_uid = parts[1]
                                initial_message = f"""[B][C][00FFFF]┌──────────┐                [B][C][FFFF00]⏳ PROCESSING ⏳ [B][C][00FFFF]└──────────┘
[C][B][FFFFFF]🆔 UID: {fixnum(target_uid)}
[C][B][FFFF00]Kicking player from group..."""
                                await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)
                                
                                try:
                                    PAc = await KickTarget(target_uid, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', PAc)
                                    await asyncio.sleep(0.3)
                                except Exception as e:
                                    print(e)
                                                                

                        if inPuTMsG.startswith(("/3")):
                            initial_message =  f"[B][C][00FF00]3 MEMBERS GROUP CREATED! \nAccept Invite Fast!" 
                            await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)
                            
                            PAc = await OpEnSq(key, iv, region)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', PAc)
                            
                            C = await cHSq(3, uid, key, iv, region)
                            await asyncio.sleep(0.3)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                            
                            V = await SEnd_InV(3, uid, key, iv, region)
                            await asyncio.sleep(0.3)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', V)
                            
                            E = await ExiT(None, key, iv)
                            await asyncio.sleep(3.5)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', E)                            

                        if inPuTMsG.startswith('/room '):
                            await handle_room_command(inPuTMsG, uid, chat_id, key, iv, region, response.Data.chat_type)

                        if inPuTMsG.startswith(("/5")):
                            initial_message =  f"[B][C][00FF00]5 MEMBERS GROUP CREATED! \nAccept Invite Fast!" 
                            await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)
                            
                            PAc = await OpEnSq(key, iv, region)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', PAc)
                            
                            C = await cHSq(5, uid, key, iv, region)
                            await asyncio.sleep(0.3)  
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                            
                            V = await SEnd_InV(5, uid, key, iv, region)
                            await asyncio.sleep(0.3)  
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', V)
                            
                            E = await ExiT(None, key, iv)
                            await asyncio.sleep(3.5)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', E)                            

                        if inPuTMsG.strip() == "/dev":
                            formatted_uid = fixnum(OWNER_UID)

                            admin_message = f"""
[C][B][FFD700]    ✦─────────✦
       💧       BOT ADMIN
    ✦─────────✦

[C][B][00FF00]👑 BOT DEVELOPER: 
[C][B][FFFFFF]NAYAN 1M

[B][C][FFFFFF] INSTAGRAM : @NAYAN1M
[B][C][FFFFFF]Contact : @NAYAN1M

[C][B][00FF00]🆔 OWNER UIDs:
[FFFFFF]
{formatted_uid}

[C][B][00FF00]💼 BUSINESS:
[C][B][FFFFFF]Want to buy this bot?
[C][B][FFFFFF]DM me for cheapest price!

✦ ──PREMIUM QUALITY── ✦
"""
                            await safe_send_message(response.Data.chat_type, admin_message, uid, chat_id, key, iv)
                                

                        # Individual command handlers for /s1 to /s5
                        if inPuTMsG.strip().startswith('/s1'):
                            await handle_badge_command('s1', inPuTMsG, uid, chat_id, key, iv, region, response.Data.chat_type)
    
                        if inPuTMsG.strip().startswith('/s2'):
                            await handle_badge_command('s2', inPuTMsG, uid, chat_id, key, iv, region, response.Data.chat_type)

                        if inPuTMsG.strip().startswith('/s3'):
                            await handle_badge_command('s3', inPuTMsG, uid, chat_id, key, iv, region, response.Data.chat_type)

                        if inPuTMsG.strip().startswith('/s4'):
                            await handle_badge_command('s4', inPuTMsG, uid, chat_id, key, iv, region, response.Data.chat_type)

                        if inPuTMsG.strip().startswith('/s5'):
                            await handle_badge_command('s5', inPuTMsG, uid, chat_id, key, iv, region, response.Data.chat_type)
                            
                        if inPuTMsG.strip().startswith('/spam'):
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = "[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /spam <uid>\n[B][C][AAAAAA]Example: /spam 123[C]456[C]789" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                target_uid = parts[1]

                                total_requests = SPAM_REQUESTS  # e.g. 99
                                badge_sequence = ['s1', 's2', 's3', 's4', 's5']

                                try:
                                    await reset_bot_state(key, iv, region)

                                    count = 0

                                    while count < total_requests:
                                        current_badge = badge_sequence[count % len(badge_sequence)]
                                        badge_value = BADGE_VALUES[current_badge]

                                        join_packet = await request_join_with_badge(
                                            target_uid,
                                            badge_value,
                                            key,
                                            iv,
                                            region
                                        )

                                        await SEndPacKeT(
                                            whisper_writer,
                                            online_writer,
                                            'OnLine',
                                            join_packet
                                        )

                                        count += 1
                                        await asyncio.sleep(PACKET_DELAY_ULTRA_FAST) 

                                    await reset_bot_state(key, iv, region)

                                    success_msg = (
                f"[B][C][00FF00]✅ SPAM SUCCESS!\n"
                f"🎯 Target: {fixnum(target_uid)}\n"
                f"📦 Requests: {count}\n"
                f"🔄 Badges: s1 → s5\n"
                                    )
                                    await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)

                                except Exception as e:
                                    error_msg = f"[B][C][FF0000]❌ Spam error: {str(e)}\n"
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    
                        if inPuTMsG.startswith('/join'):
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /join <team_code>\n[B][C][AAAAAA]Example: /join 123[c]4567"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue
                        
                            CodE = parts[1]
                            uid = response.Data.uid

                            initial_message = f"""[B][C][FFFFFF]🔢 TEAM CODE: {CodE}\n
[C][B][FFFF00]⏳ Joining squad..."""
                            await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)

                            try:
                                EM = await GenJoinSquadsPacket(CodE, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', EM)

                                await asyncio.sleep(2)

                                try:
                                    await equip_random_bundle(key, iv, region)
                                    await asyncio.sleep(2)
                                    await auto_dual_emote(uid, key, iv, region)
                                except Exception as emote_error:
                                    print(f"Dual emote failed but join succeeded: {emote_error}")

                                success_message = f"[B][C][00FF00]✅ SUCCESS! Joined squad: {CodE}!\n💍 Dual Aura Boarder emote activated!\n🤖 Bot + You = 💕\n"
                                await safe_send_message(response.Data.chat_type, success_message, uid, chat_id, key, iv)

                            except Exception as e:
                                error_msg = f"[B][C][FF0000]❌ ERROR! Failed to join squad: {str(e)}\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                
                
                        if inPuTMsG.strip().startswith('/ghost'):
                            parts = inPuTMsG.strip().split(maxsplit=2)
                            if len(parts) < 3:
                                await safe_send_message(
                                    response.Data.chat_type,
                                    "[B][C][FF0000]⛔ Usage: /ghost <teamcode> <ghostname>",
                                    uid, chat_id, key, iv
                                )
                                continue

                            team_code = parts[1]
                            ghost_name = parts[2]   # ← exactly as typed, case preserved

                            await safe_send_message(
                                response.Data.chat_type,
                                f"[B][C][FFFF00]🎭 Ghost join to {team_code} as '{ghost_name}'...",
                                uid, chat_id, key, iv
                            )

                            # ---- 1. Join squad ----
                            join_pkt = await GenJoinSquadsPacket(team_code, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_pkt)

                            # ---- 2. Wait for 0500 packet (max 5 sec) ----
                            timeout = 5
                            start = time.time()
                            while not last_0500_info and (time.time() - start) < timeout:
                                await asyncio.sleep(0.2)

                            if not last_0500_info:
                                await safe_send_message(
                                    response.Data.chat_type,
                                    "[B][C][FF0000]❌ Failed to get 0500 packet. Invalid team code?",
                                    uid, chat_id, key, iv
                                )
        # Clean up: leave squad
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', await ExiT(None, key, iv))
                                continue

                            idT = last_0500_info['idT']
                            secret = last_0500_info['squad_code']

                            # ---- 3. Send ghost packet (preserves case) ----
                            ghost_pkt = await ghost_packet(idT, ghost_name, secret, key, iv, region)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', ghost_pkt)

    # ---- 4. Leave squad ----
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', await ExiT(None, key, iv))

                            await safe_send_message(
                                response.Data.chat_type,
                                f"[B][C][00FF00]✅ Ghost packet sent!\nTeam: {team_code}\nGhost name: {ghost_name} (case preserved)",
                                uid, chat_id, key, iv
                            )

    # Clear captured data for next use
                            last_0500_info = {}
                        
                        # NEW LAG COMMAND
                        if inPuTMsG.strip().startswith('/lag '):
                            print('Processing lag command in any chat type')
                            
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = f"[B][C][FF0000]❌ ERROR! Usage: /lag (team_code)\nExample: /lag ABC123\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                team_code = parts[1]
                                
                                # Stop any existing lag task
                                if lag_task and not lag_task.done():
                                    lag_running = False
                                    lag_task.cancel()
                                    await asyncio.sleep(0.1)
                                
                                # Start new lag task
                                lag_running = True
                                lag_task = asyncio.create_task(lag_team_loop(team_code, key, iv, region))
                                
                                # SUCCESS MESSAGE
                                success_msg = f"[B][C][00FF00]✅ SUCCESS! Lag attack started!\nTeam: {team_code}\nAction: Rapid join/leave\nSpeed: Ultra fast (milliseconds)\n"
                                await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)

                        # STOP LAG COMMAND
                        if inPuTMsG.strip() == '/stop lag':
                            if lag_task and not lag_task.done():
                                lag_running = False
                                lag_task.cancel()
                                success_msg = f"[B][C][00FF00]✅ SUCCESS! Lag attack stopped successfully!\n"
                                await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                            else:
                                error_msg = f"[B][C][FF0000]❌ ERROR! No active lag attack to stop!\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                #NEW LAG-/attack
                        if inPuTMsG.strip().startswith('/attack'):
                            print('Processing /attack command in any chat type')
    
                            initial_message = f"[B][C]{get_random_colour()}\nStarting new lag attack...\n"
                            await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)
    
                            try:
                                for i in range(2111):
                                    C = await new_lag(key, iv)  
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                                    await asyncio.sleep(0.003)
        
                                success_message = f"[B][C][00FF00]✅ SUCCESS! new lag attack completed!\n"
                                await safe_send_message(response.Data.chat_type, success_message, uid, chat_id, key, iv)
                                print('new lag attack finished')
        
                            except Exception as e:
                                error_msg = f"[B][C][FF0000]❌ ERROR in attack command: {str(e)}\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                print(f"attack command error: {e}")

                        # STOP LAG COMMAND
                        if inPuTMsG.strip() == '/stop lag':
                            if lag_task and not lag_task.done():
                                lag_running = False
                                lag_task.cancel()
                                success_msg = f"[B][C][00FF00]✅ SUCCESS! Lag attack stopped successfully!\n"
                                await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                            else:
                                error_msg = f"[B][C][FF0000]❌ ERROR! No active lag attack to stop!\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)

                        if inPuTMsG.startswith('/exit'):
                            initial_message = f"[C][B][FF0000]╔═════════╗[C][B][FFFFFF]                  LEAVING GROUP [C][B][00FF00]╚═════════╝     \n[C][B][FFFF00]🚪 Bot is leaving...[84D4F1]            \n[C][B][FF00FF]👋 Goodbye![FFA500]" 
                            await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)
                            
                            leave = await ExiT(uid,key,iv)
                            await SEndPacKeT(whisper_writer , online_writer , 'OnLine' , leave)                                    
                                   
                                #TEAM SPAM MESSAGE COMMAND
                        if inPuTMsG.strip().lower().startswith('/ms '):
                            print('Processing /ms command')

                            try:
                                parts = inPuTMsG.strip().split(maxsplit=1)

                                if len(parts) < 2:
                                    error_msg = (
    "[B][C][FF0000]⛔ COMMAND REJECTED\n"
    "[B][C][FFFFFF]Invalid command format.\n"
    "[B][C][AAAAAA]Correct Usage: /ms <single_word>\n"
    "[B][C][AAAAAA]Example: /ms NAYAN"
)
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                else:
                                    words = parts[1].strip().split()
                                    if len(words) != 1:
                                        error_msg = "[B][C][FF0000]❌ ERROR! Only one word allowed after /ms."
                                        await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    else:
                                        user_message = words[0].upper()  

                                        for i in range(1, len(user_message) + 1):
                                            partial_message = user_message[:i]
                                            color = get_random_colour()
                                            colored_message = f"[B][C]{color} {partial_message}"
                                            await safe_send_message(response.Data.chat_type, colored_message, uid, chat_id, key, iv)
                                            await asyncio.sleep(0.3)

                            except Exception as e:
                                error_msg = f"[B][C][FF0000]❌ ERROR! Something went wrong:\n{str(e)}"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)    
                        

                        if inPuTMsG.strip().startswith('!e'):
                            print(f'Processing emote command in chat type: {response.Data.chat_type}')
                            
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 3:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: !e <uid> <emote_id>\n[B][C][AAAAAA]Example: !e 123[C]456[C]789 909000001"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue
                                
                            initial_message = f'[B][C]{get_random_colour()}\nSending emote to target...\n'
                            await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)

                            uid2 = uid3 = uid4 = uid5 = None
                            s = False
                            target_uids = []

                            try:
                                target_uid = int(parts[1])
                                target_uids.append(target_uid)
                                uid2 = int(parts[2]) if len(parts) > 2 else None
                                if uid2: target_uids.append(uid2)
                                uid3 = int(parts[3]) if len(parts) > 3 else None
                                if uid3: target_uids.append(uid3)
                                uid4 = int(parts[4]) if len(parts) > 4 else None
                                if uid4: target_uids.append(uid4)
                                uid5 = int(parts[5]) if len(parts) > 5 else None
                                if uid5: target_uids.append(uid5)
                                idT = int(parts[-1])  

                            except ValueError as ve:
                                print("ValueError:", ve)
                                s = True
                            except Exception as e:
                                print(f"Error parsing emote command: {e}")
                                s = True

                            if not s:
                                try:
                                    for target in target_uids:
                                        H = await Emote_k(target, idT, key, iv, region)
                                        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', H)
                                        await asyncio.sleep(0.1)
                                    
                                    success_msg = f"[B][C][00FF00]✅ SUCCESS! Emote {idT} sent to {len(target_uids)} player(s)!\nTargets: {', '.join(map(str, target_uids))}\n"
                                    await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)

                                except Exception as e:
                                    error_msg = f"[B][C][FF0000]❌ ERROR sending emote: {str(e)}\n"
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                error_msg = f"[B][C][FF0000]❌ ERROR! Invalid UID format. Usage: !e (uid) (emote_id)\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                                                
                                
                        # NEW COMMAND -: /dance
                        if inPuTMsG.strip().startswith('/dance'):
                            print(f'Processing emote command in chat type: {response.Data.chat_type}')

                            parts = inPuTMsG.strip().split()
    
                            if len(parts) == 2:
                                target_uids = [int(uid)]
                                emote_identifier = parts[1].strip().lower()
        
                                initial_message = f'[B][C]{get_random_colour()}\nSending emote to yourself...'
                                await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)
    
                            elif len(parts) >= 3:
                                target_uids = []
                                emote_identifier = parts[-1].strip().lower()
                                
                                for part in parts[1:-1]:
                                    if part.isdigit() and len(part) >= 7:  
                                        target_uids.append(int(part))
        
                                if not target_uids:
                                    error_msg = (
    "[B][C][FF0000]⛔ COMMAND REJECTED\n"
    "[B][C][FFFFFF]Invalid command format.\n"
    "[B][C][AAAAAA]Correct Usage: /dance [uid] [emote_number/name]\n"
    "[B][C][AAAAAA]Example: /dance 123[C]456[C]789 1\n"
    "[B][C][AAAAAA]Example: /dance 123[C]456[C]789 hello\n"
    "[B][C][AAAAAA]Example: /dance 1 (for yourself)\n"
    "[B][C][AAAAAA]Example: /dance hello (for yourself)"
)
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    continue
            
                                initial_message = f'[B][C]{get_random_colour()}\nSending emote to target(s)...'
                                await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)
    
                            else:
                                error_msg = (
    "[B][C][FF0000]⛔ COMMAND REJECTED\n"
    "[B][C][FFFFFF]Invalid command format.\n"
    "[B][C][AAAAAA]Correct Usage: /dance [uid] [emote_number/name]\n"
    "[B][C][AAAAAA]Example: /dance 1\n"
    "[B][C][AAAAAA]Example: /dance hello\n"
    "[B][C][AAAAAA]Example: /dance 123[C]456[C]789 1\n"
    "[B][C][AAAAAA]Example: /dance 123[C]456[C]789 hello\n"
)
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            import os
                            emote_map_by_number = {}  
                            emote_map_by_name = {}    
                            emote_names_list = []     

                            try:
                                script_dir = os.path.dirname(os.path.abspath(__file__))
                                emotes_path = os.path.join(script_dir, 'emotes.json')

                                with open(emotes_path, 'r', encoding='utf-8') as f:
                                    emotes_data = json.load(f)
            
                                    for entry in emotes_data:
                                        num = str(entry['Number']).strip()
                                        emote_id = str(entry['Id']).strip()
                                        emote_name = entry.get('Name', '').strip().lower()
                
                                        emote_map_by_number[num] = emote_id
                
                                        if emote_name:
                                            emote_map_by_name[emote_name] = emote_id
                    
                                        if emote_name:
                                            emote_names_list.append(emote_name)

                            except FileNotFoundError:
                                error_msg = "[C][B][FF0000]Error: emotes.json file not found.\nPlease contact admin to add the file."
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue
                            except (json.JSONDecodeError, KeyError) as e:
                                error_msg = f"[C][B][FF0000]Error: emotes.json format incorrect.\n{str(e)}"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue
                            except Exception as e:
                                error_msg = f"[C][B][FF0000]Error loading emotes: {str(e)}"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            emote_id_to_send = None
                            emote_name_display = None

                            if emote_identifier.isdigit():
                                if emote_identifier in emote_map_by_number:
                                    emote_id_to_send = emote_map_by_number[emote_identifier]
                                    for entry in emotes_data:
                                        if str(entry['Number']) == emote_identifier:
                                            emote_name_display = entry.get('Name', f"Emote {emote_identifier}")
                                            break
                                    if not emote_name_display:
                                        emote_name_display = f"Emote {emote_identifier}"
                                else:
                                    max_emote = len(emote_map_by_number)
                                    error_msg = (
                f"[B][C][FF0000]Invalid emote number: {emote_identifier}\n"
                f"[00FF00]Available numbers: 1-{max_emote}\n"
                f"[00FF00]Try: /dance [1-{max_emote}]"
                                    )
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    continue
                            else:
                                if emote_identifier in emote_map_by_name:
                                    emote_id_to_send = emote_map_by_name[emote_identifier]
                                    emote_name_display = emote_identifier.title()
                                else:
                                    similar = [name for name in emote_names_list if emote_identifier in name]
                                    if similar:
                                        error_msg = (
                    f"[B][C][FF0000]Emote '{emote_identifier}' not found.\n"
                    f"[00FF00]Similar names: {', '.join(similar[:5])}\n"
                    f"[FFFFFF]Try one of these."
                                        )
                                    else:
                                        popular_names = ['hello', 'dance', 'ak', 'scar', 'p90', 'm60', 'breakdance', 'kungfu']
                                        available = [name for name in popular_names if name in emote_names_list]
                                        if available:
                                            error_msg = (
                        f"[B][C][FF0000]Emote '{emote_identifier}' not found.\n"
                        f"[00FF00]Popular names: {', '.join(available)}\n"
                        f"[FFFFFF]Use /emote_list to see all names."
                                            )
                                        else:
                                            error_msg = (
                        f"[B][C][FF0000]Emote '{emote_identifier}' not found.\n"
                        f"[FFFFFF]Use /emote_list to see all available emotes."
                                            )
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    continue

                            try:
                                sent_count = 0
                                for target in target_uids:
                                    H = await Emote_k(target, int(emote_id_to_send), key, iv, region)
                                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', H)
                                    sent_count += 1
                                    await asyncio.sleep(0.1)

                                if len(target_uids) == 1 and target_uids[0] == int(uid):
                                    success_msg = (
                f"[B][C][00FF00]✅ SUCCESS!\n"
                f"🎭 Emote: {emote_name_display} (ID: {emote_id_to_send})\n"
                f"🎯 Sent to: Yourself\n"
                f"😊 Enjoy your emote!"
                                    )
                                else:
                                    # Sent to others
                                    success_msg = (
                f"[B][C][00FF00]✅ SUCCESS!\n"
                f"🎭 Emote: {emote_name_display} (ID: {emote_id_to_send})\n"
                f"🎯 Sent to: {sent_count} player(s)\n"
                f"👥 Targets: {', '.join(map(str, target_uids))}\n"
                                    )
        
                                await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)

                            except Exception as e:
                                error_msg = f"[B][C][FF0000]❌ ERROR sending emote: {str(e)}\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)

                                #GALI SPAM MESSAGE 
                        if inPuTMsG.strip().startswith('/gali '):
                            print('Processing /gali command')

                            try:
                                parts = inPuTMsG.strip().split(maxsplit=1)

                                if len(parts) < 2:
                                    error_msg = (
    "[B][C][FF0000]⛔ COMMAND REJECTED\n"
    "[B][C][FFFFFF]Invalid command format.\n"
    "[B][C][AAAAAA]Correct Usage: /gali <name>\n"
    "[B][C][AAAAAA]Example: /gali hater"
)
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                else:
                                    name = parts[1].strip()
            
                                    if name.lower() in [n.lower() for n in BLOCKED_NAMES]:
                                        error_msg = (
                                            f"[B][C][FF0000]⚠️ WARNING!\n"
                                            f"[FFFFFF]You cannot target '{name}'!\n"
                                            f"[FF0000]Bot owner protected! ⛔\n"
                                            f"[FFFFFF]Try another name."
                                        )
                                        await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                        continue
            
                                    messages = [
    f"{name} ᴛƐ3ʀɪ ꜱƐ3xʏ ʙʜ4ᴇɴ ᴋɪ ᴄʜ4xᴛ ᴍᴇ ᴍᴇ ʟ04ᴅᴀ ᴅᴀ4ᴀʟ ᴋᴀʀ ʀᴀᴀᴛ ʙʜᴀʀ ᴊᴏʀ ᴊᴏʀ ꜱᴇ ᴄʜ40ᴅᴜɴ4ɢᴀ",
    f"{name} ᴍᴀ4ᴅʜᴇʀx4ʜᴏᴅ ᴛƐ3ʀɪ ᴍᴀ́ᴀ ᴋɪ ᴋᴀʟɪ ɢ4ɴᴅ ᴍ3Ɛ ʟᴀ4ɴᴅ ᴍᴀ4ʀᴜ",
    f"{name} ᴛ3Ɛʀɪ ʙʜ4Ɛɴ ᴋɪ ᴛɪɢʜᴛ ᴄʜ4xᴛ ᴋᴏ 5ɢ ᴋɪ ꜱᴘᴇᴇᴅ ꜱᴇ ᴄʜ40ᴅ ᴅᴜ",
    f"{name} ᴛƐ3ʀɪ ʙᴇ4ʜᴇɴ ᴋɪ ᴄʜ4xᴛ ᴍᴇ ʟ4ɴᴅ ᴍᴀ4ʀᴜ",
    f"{name} ᴛƐ3ʀɪ ᴍᴀ́4ᴀ ᴋɪ ᴄʜ4xᴛ 360 ʙᴀʀ",
    f"{name} ᴛƐ3ʀɪ ʙƐ4ʜƐɴ ᴋɪ ᴄʜ4xᴛ 720 ʙᴀʀ",
    f"{name} ʙᴇ4ʜᴇɴ ᴋᴇ ʟ0ᴅᴇ",
    f"{name} ᴍᴀ4ᴅᴀʀᴄ4ʜxᴅ",
    f"{name} ʙᴇ4ᴛᴇ ᴛƐ3ʀᴀ ʙᴀᴀᴘ ʜᴜɴ ᴍᴇ",
    f"{name} ɢ4ɴᴅᴜ ᴀᴘɴᴇ ʙᴀ4ᴀᴘ ᴋᴏ ʜ8 ᴅᴇɢᴀ",
    f"{name} ᴋɪ ᴍᴀ̀4ᴀ ᴋɪ ᴄʜ4xᴛ ᴘᴇʀ ɴɪɢʜᴛ 4000",
    f"{name} ᴋɪ ʙƐ3ʜƐɴ ᴋɪ ᴄʜ4xᴛ ᴘᴇʀ ɴɪɢʜᴛ 8000",
    f"{name} ʀ4ɴᴅɪ ᴋᴇ ʙᴀᴄ4ʜʜƐ ᴀᴘɴᴇ ʙᴀᴘ ᴋᴏ ʜ8 ᴅᴇɢᴀ",
    f"ɪɴᴅɪᴀ ᴋᴀ ɴᴏ-1 ɢ4ɴᴅᴜ {name}",
    f"ᴄʜᴀᴘᴀʟ ᴄʜ0ʀ {name}",
    f"{name} ᴛƐ3ʀɪ ᴍᴀ̀ᴀ ᴋᴏ ɢʙ ʀᴏᴀᴅ ᴘᴇ ʙᴇ4ᴛʜᴀ ᴋᴇ ᴄʜ4xᴅᴜɴɢᴀ",
    f"{name} ʙᴇ4ᴛᴀ ᴊʜᴜʟᴀ ᴊʜᴜʟ ᴀᴘɴᴇ ʙᴀ4ᴀᴘ ᴋᴏ ᴍᴀᴛ ʙʜᴜʟ"
                                    ]

                                    for msg in messages:
                                        colored_message = f"[B][C]{get_random_color()} {msg.replace('{Name}', name.upper())}"
                                        await safe_send_message(response.Data.chat_type, colored_message, uid, chat_id, key, iv)
                                        await asyncio.sleep(2)

                            except Exception as e:
                                error_msg = f"[B][C][FF0000]❌ ERROR! Something went wrong:\n{str(e)}"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)                        
                                                                

                        # EVO CYCLE START COMMAND - /random
                        if inPuTMsG.strip().startswith('/random'):
                            print('Processing evo cycle start command')
                            parts = inPuTMsG.strip().split()
                            uids = []
                            sender_uid = str(response.Data.uid)
                            if len(parts) == 1:
                                uids.append(sender_uid)
                                print(f"Using sender UID only: {sender_uid}")
                            else:
                                for part in parts[1:]:
                                    if part.isdigit() and len(part) >= 7:
                                        uids.append(part)
                                        print(f"Added target UID: {part}")

                            task_key = f"{chat_id}_{sender_uid}"

                            if task_key in evo_cycle_tasks and not evo_cycle_tasks[task_key].done():
                                evo_cycle_tasks[task_key].cancel()
                                await asyncio.sleep(0.5)  

                            task = asyncio.create_task(evo_cycle_spam(uids, key, iv, region))

                            def task_done_callback(t):
                                evo_cycle_tasks.pop(task_key, None)

                            task.add_done_callback(task_done_callback)
                            evo_cycle_tasks[task_key] = task

                            if len(parts) == 1:
                                success_msg = (
            "[B][C][00FF00]✅ SUCCESS!\n"
            "🎯 Target: Yourself\n"
            "🎭 Emotes: All 18 evolution emotes\n"
            "⏰ Delay: 5 seconds\n"
            "🔄 Loop: Until /ruk bhai\n"
                                )
                            else:
                                success_msg = (
            "[B][C][00FF00]✅ SUCCESS!\n"
            f"🎯 Targets: {len(uids)} player(s)\n"
            "🎭 Emotes: All 18 evolution emotes\n"
            "⏰ Delay: 5 seconds\n"
            "🔄 Loop: Until /ruk bhai\n"
                                )

                            await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                            print(f"Evolution cycle started for UIDs: {uids}")
                        
                        # EVO CYCLE STOP COMMAND - /ruk bhai
                        if inPuTMsG.strip() == '/ruk bhai':
                            task_key = f"{chat_id}_{uid}"
                            if task_key in evo_cycle_tasks and not evo_cycle_tasks[task_key].done():
                                evo_cycle_tasks[task_key].cancel()
                                success_msg = f"[B][C][00FF00]✅ SUCCESS! Evolution emote cycle stopped successfully!\n"
                            else:
                                success_msg = f"[B][C][FF0000]❌ ERROR! No active evolution emote cycle to stop!\n"
                            await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)

                        if inPuTMsG.strip().startswith('/fast'):
                            print('Processing fast emote spam in any chat type')
                            
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 3:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /fast <uid1> [uid2] [uid3] [uid4] <emoteid>\n[B][C][AAAAAA]Example: /fast 123[C]456[C]789 909000001" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                uids = []
                                emote_id = None
                                
                                for part in parts[1:]:
                                    if part.isdigit():
                                        if len(part) > 3:  
                                            uids.append(part)
                                        else:
                                            emote_id = part
                                    else:
                                        break
                                
                                if not emote_id and parts[-1].isdigit():
                                    emote_id = parts[-1]
                                
                                if not uids or not emote_id:
                                    error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /fast <uid1> [uid2] [uid3] [uid4] <emoteid>\n[B][C][AAAAAA]Example: /fast 123[C]456[C]789 909000001" 
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                else:
                                    if fast_spam_task and not fast_spam_task.done():
                                        fast_spam_running = False
                                        fast_spam_task.cancel()
                                    
                                    fast_spam_running = True
                                    fast_spam_task = asyncio.create_task(fast_emote_spam(uids, emote_id, key, iv, region))
                                    
                                    success_msg = f"[B][C][00FF00]✅ SUCCESS! Fast emote spam started!\nTargets: {len(uids)} players\nEmote: {emote_id}\nSpam count: 25 times\n"
                                    await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)

                        if inPuTMsG.strip().startswith('/p'):
                            print('Processing custom emote spam in any chat type')
                            
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 4:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /p <uid> <emote_id> <times>\n[B][C][AAAAAA]Example: /p 123[C]456[C]789 909000001 10" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                try:
                                    target_uid = parts[1]
                                    emote_id = parts[2]
                                    times = int(parts[3])
                                    
                                    if times <= 0:
                                        error_msg = f"[B][C][FF0000]❌ ERROR! Times must be greater than 0!\n"
                                        await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    elif times > 100:
                                        error_msg = f"[B][C][FF0000]❌ ERROR! Maximum 100 times allowed for safety!\n"
                                        await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    else:
                                        if custom_spam_task and not custom_spam_task.done():
                                            custom_spam_running = False
                                            custom_spam_task.cancel()
                                            await asyncio.sleep(0.5)
                                        
                                        custom_spam_running = True
                                        custom_spam_task = asyncio.create_task(custom_emote_spam(target_uid, emote_id, times, key, iv, region))
                                        
                                        success_msg = f"[B][C][00FF00]✅ SUCCESS! Custom emote spam started!\nTarget: {fixnum(target_uid)}\nEmote: {emote_id}\nTimes: {times}\n"
                                        await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                                        
                                except ValueError:
                                    error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid number format.\n[B][C][AAAAAA]Correct Usage: /p <uid> <emote_id> <times>\n[B][C][AAAAAA]Example: /p 123[C]456[C]789 909000001 10"
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                except Exception as e:
                                    error_msg = f"[B][C][FF0000]❌ ERROR! {str(e)}\n"
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    
                        if inPuTMsG.strip().startswith('/spm_inv '):
                            print('Processing spam invite command in any chat type')

                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /spm_inv <uid>\n[B][C][AAAAAA]Example: /spm_inv 123[C]456[C]789" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                target_uid = parts[1]
                                initial_message = f"[B][C]{get_random_colour()}\nStarting spam invite: sending invites in batches of 7..." 
                                await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)

                                spm_inv_running = True 
                                total_invites = 57
                                batch_size = 7
                                count = 0

                                try:
                                    while count < total_invites and spm_inv_running:
                                        current_batch = min(batch_size, total_invites - count)

                                        for _ in range(current_batch):
                                            if not spm_inv_running:
                                                break

                                            PAc = await OpEnSq(key, iv, region)
                                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', PAc)
                                            await asyncio.sleep(0.1)

                                            C = await cHSq(5, int(target_uid), key, iv, region)
                                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                                            await asyncio.sleep(0.1)

                                            V = await SEnd_InV(5, int(target_uid), key, iv, region)
                                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', V)
                                            await asyncio.sleep(0.1)

                                            E = await ExiT(None, key, iv)
                                            await asyncio.sleep(0.1)
                                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', E)

                                            count += 1

                                        if count % batch_size == 0 and spm_inv_running:
                                            pause_message = f"[B][C][FFFF00]⏳ Pausing 10 seconds after 3 invites...\n"
                                            await safe_send_message(response.Data.chat_type, pause_message, uid, chat_id, key, iv)
                                            await asyncio.sleep(10)

                                    success_message = f"[B][C][00FF00]✅ SUCCESS! Finished spamming 30 invites to {fixnum(target_uid)}!\n"
                                    await safe_send_message(response.Data.chat_type, success_message, uid, chat_id, key, iv)

                                except Exception as e:
                                    error_msg = f"[B][C][FF0000]❌ ERROR sending invite: {str(e)}" 
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    
                        if inPuTMsG.strip().startswith('/spmroom '):
                            print('Processing room invite spam command')
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /spmroom <uid> [count]\n[B][C][AAAAAA]Example: /spmroom 123[C]456[C]789 30" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                continue

                            target_uid = parts[1]
                            count = 30   
                            if len(parts) >= 3:
                                try:
                                    count = int(parts[2])
                                    if count <= 0 or count > 500:
                                        count = 30
                                except:
                                    count = 30

                            if not target_uid.isdigit():
                                await safe_send_message(response.Data.chat_type, "[B][C][FF0000]❌ UID must be numbers only!", uid, chat_id, key, iv)
                                continue

                            if room_invite_spam_task and not room_invite_spam_task.done():
                                room_invite_spam_running = False
                                room_invite_spam_task.cancel()
                                await asyncio.sleep(0.5)

                            room_invite_spam_running = True
                            room_invite_spam_task = asyncio.create_task(
                                room_invite_spam_loop(target_uid, count, key, iv)
                            )

                            formatted_uid = fixnum(target_uid)   
                            start_msg = f"[B][C][00FF00]✅ Room invite spam started!\n🎯 Target: {formatted_uid}\n📦 Count: {count}\n⏱️ 0.1s delay\n"
                            await safe_send_message(response.Data.chat_type, start_msg, uid, chat_id, key, iv)

                            asyncio.create_task(handle_room_spam_completion(room_invite_spam_task, target_uid, count, uid, chat_id, response.Data.chat_type, key, iv))

                        if inPuTMsG.strip().startswith('/status '):
                            print('Processing status command')
    
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /status <player_uid>\n[B][C][AAAAAA]Example: /status 123[C]456[C]789" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                return
    
                            target_uid = parts[1]
    
                            print(f"\n🔍 BEFORE clearing cache:")
                            debug_file_cache()
                            
                            clear_cache_entry(target_uid)
    
                            initial_msg = f"""[B][C][00FFFF]┌──────────┐                [B][C][FFFF00]⏳ PROCESSING ⏳ [B][C][00FFFF]└──────────┘
[C][B][FFFFFF]🆔 UID: {fix_num(target_uid)}
[C][B][FFFF00]⏳ Checking player status..."""
                            await safe_send_message(response.Data.chat_type, initial_msg, uid, chat_id, key, iv)
                            
                            try:
                                status_packet = await createpacketinfo(target_uid, key, iv)
                                if not status_packet:
                                    error_msg = f"[B][C][FF0000]❌ Failed to create status packet!\n"
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                    return
        
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', status_packet)
                                print(f"📤 Sent status request for {fixnum(target_uid)}")
        
                                max_retries = 12  
                                response_received = False
        
                                for attempt in range(max_retries):
                                    print(f"⏳ Checking file cache... attempt {attempt + 1}/{max_retries}")
            
                                    cache_data = load_from_cache(target_uid)
                                    if cache_data:
                                        print(f"🎯 FOUND in file cache! Status: {cache_data['status']}")
                                        response_received = True
                
                                        print(f"📦 Cache data keys: {list(cache_data.keys())}")
                
                                        status_msg = f"[B][C][FFFF00]📊 PLAYER STATUS\n"
                                        status_msg += f"────────────────\n"
                                        status_msg += f"👤 UID: {fix_num(target_uid)}\n"
                                        status_msg += f"📊 Status: {cache_data['status']}\n"
                
                                        if "IN ROOM" in cache_data['status']:
                                            if 'room_id' in cache_data:
                                                status_msg += f"🏠 Room ID: {fix_num(cache_data['room_id'])}\n"
                                                status_msg += f"💡 Use: /roomspam {fixnum(target_uid)}\n"
                                            else:
                                                status_msg += f"🏠 Room ID: Not available\n"
                
                                        elif "INSQUAD" in cache_data['status']:
                                            if 'leader_id' in cache_data:
                                                status_msg += f"👑 Leader: {fix_num(cache_data['leader_id'])}\n"

                                            try:
                                                if 'parsed_json' in cache_data:
                                                    parsed = cache_data['parsed_json']
                                                    if '5' in parsed and 'data' in parsed['5']:
                                                        squad_data = parsed['5']['data']['1']['data']
                                                        if '9' in squad_data and 'data' in squad_data['9']:
                                                            members = squad_data['9']['data']
                                                            max_members = squad_data['10']['data'] + 1
                                                            status_msg += f"👥 Squad: {members}/{max_members}\n"
                                            except:
                                                pass
                
                                        elif "OFFLINE" in cache_data['status']:
                                            status_msg += f"🔴 Player is offline\n"
                
                                        elif "INGAME" in cache_data['status']:
                                            status_msg += f"🎮 Player is in a match\n"
                
                                        elif "SOLO" in cache_data['status']:
                                            status_msg += f"👤 Player is solo\n"
                
                                        status_msg += f"────────────────\n"
                                        status_msg += f"✅ Real-time data\n"
                
                                        await safe_send_message(response.Data.chat_type, status_msg, uid, chat_id, key, iv)
                
                                        print(f"\n✅ AFTER successful response:")
                                        debug_file_cache()
                
                                        break
            
                                    await asyncio.sleep(0.5)
                                                        
                                if not response_received:
                                    print(f"\n❌ FAILED after {max_retries} tries")
                                    debug_file_cache()
            
                                    error_msg = f"[B][C][FF0000]❌ STATUS CHECK FAILED\n"
                                    error_msg += f"────────────────\n"
                                    error_msg += f"👤 UID: {fix_num(target_uid)}\n"
                                    error_msg += f"📛 No response from server\n"
                                    error_msg += f"────────────────\n"
                                    error_msg += f"💡 Possible issues:\n"
                                    error_msg += f"• Player is offline\n"
                                    error_msg += f"• Server is busy\n"
                                    error_msg += f"• Try again in 10 seconds\n"
            
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
            
                            except Exception as e:
                                print(f"❌ Status command error: {e}")
                                import traceback
                                traceback.print_exc()
        
                                error_msg = f"[B][C][FF0000]❌ Error: {str(e)[:50]}\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)

                        # NEW EVO COMMANDS
                        if inPuTMsG.strip().startswith('/evo '):
                            print('Processing evo command in any chat type')
                            
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /evo <uid1> [uid2] [uid3] [uid4] <number(1-21)>\n[B][C][AAAAAA]Example: /evo 123[C]456[C]789 1" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                uids = []
                                number = None
                                
                                for part in parts[1:]:
                                    if part.isdigit():
                                        if len(part) <= 2: 
                                            number = part
                                        else:
                                            uids.append(part)
                                    else:
                                        break
                                
                                if not number and parts[-1].isdigit() and len(parts[-1]) <= 2:
                                    number = parts[-1]
                                
                                if not uids or not number:
                                    error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /evo <uid1> [uid2] [uid3] [uid4] <number(1-21)>\n[B][C][AAAAAA]Example: /evo 123[C]456[C]789 1"
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                else:
                                    try:
                                        number_int = int(number)
                                        if number_int not in EMOTE_MAP:
                                            error_msg = f"[B][C][FF0000]❌ ERROR! Number must be between 1-21 only!\n"
                                            await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                        else:
                                            initial_message = f"[B][C]{get_random_color()}\nSending evolution emote {number_int}..." 
                                            await safe_send_message(response.Data.chat_type, initial_message, uid, chat_id, key, iv)
                                            
                                            success, result_msg = await evo_emote_spam(uids, number_int, key, iv, region)
                                            
                                            if success:
                                                success_msg = f"[B][C][00FF00]✅ SUCCESS! {result_msg}\n"
                                                await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                                            else:
                                                error_msg = f"[B][C][FF0000]❌ ERROR! {result_msg}\n"
                                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                            
                                    except ValueError:
                                        error_msg = f"[B][C][FF0000]❌ ERROR! Invalid number format! Use 1-21 only.\n"
                                        await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)

                        if inPuTMsG.strip().startswith('/evo_fast '):
                            print('Processing evo_fast command in any chat type')
                            
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 2:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /evo_fast <uid1> [uid2] [uid3] [uid4] <number(1-21)>\n[B][C][AAAAAA]Example: /evo_fast 123[C]456[C]789 1" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                uids = []
                                number = None
                                
                                for part in parts[1:]:
                                    if part.isdigit():
                                        if len(part) <= 2:  
                                            number = part
                                        else:
                                            uids.append(part)
                                    else:
                                        break
                                
                                if not number and parts[-1].isdigit() and len(parts[-1]) <= 2:
                                    number = parts[-1]
                                
                                if not uids or not number:
                                    error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /evo_fast <uid1> [uid2] [uid3] [uid4] <number(1-21)>\n[B][C][AAAAAA]Example: /evo_fast 123[C]456[C]789 1" 
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                else:
                                    try:
                                        number_int = int(number)
                                        if number_int not in EMOTE_MAP:
                                            error_msg = f"[B][C][FF0000]❌ ERROR! Number must be between 1-21 only!\n"
                                            await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                        else:
                                            if evo_fast_spam_task and not evo_fast_spam_task.done():
                                                evo_fast_spam_running = False
                                                evo_fast_spam_task.cancel()
                                                await asyncio.sleep(0.5)
                                            
                                            evo_fast_spam_running = True
                                            evo_fast_spam_task = asyncio.create_task(evo_fast_emote_spam(uids, number_int, key, iv, region))

                                            emote_id = EMOTE_MAP[number_int]
                                            success_msg = f"[B][C][00FF00]✅ SUCCESS! Fast evolution emote spam started!\nTargets: {len(uids)} players\nEmote: {number_int} (ID: {emote_id})\nSpam count: 25 times\nInterval: 0.1 seconds\n"
                                            await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                                            
                                    except ValueError:
                                        error_msg = f"[B][C][FF0000]❌ ERROR! Invalid number format! Use 1-21 only.\n"
                                        await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)

                        # NEW EVO_CUSTOM COMMAND
                        if inPuTMsG.strip().startswith('/evo_c '):
                            print('Processing evo_c command in any chat type')
                            
                            parts = inPuTMsG.strip().split()
                            if len(parts) < 3:
                                error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /evo_c <uid1> [uid2] [uid3] [uid4] <number(1-21)> <time(1-100)>\n[B][C][AAAAAA]Example: /evo_c 123[C]456[C]789 1 10" 
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                            else:
                                uids = []
                                number = None
                                time_val = None
                                
                                for part in parts[1:]:
                                    if part.isdigit():
                                        if len(part) <= 2:
                                            if number is None:
                                                number = part
                                            elif time_val is None:
                                                time_val = part
                                            else:
                                                uids.append(part)
                                        else:
                                            uids.append(part)
                                    else:
                                        break
                                
                                if not time_val and len(parts) >= 3:
                                    last_part = parts[-1]
                                    if last_part.isdigit() and len(last_part) <= 3:
                                        time_val = last_part
                                        if time_val in uids:
                                            uids.remove(time_val)
                                
                                if not uids or not number or not time_val:
                                    error_msg = f"[B][C][FF0000]⛔ COMMAND REJECTED\n[B][C][FFFFFF]Invalid command format.\n[B][C][AAAAAA]Correct Usage: /evo_c <uid1> [uid2] [uid3] [uid4] <number(1-21)> <time(1-100)>\n[B][C][AAAAAA]Example: /evo_c 123[C]456[C]789 1 10" 
                                    await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                else:
                                    try:
                                        number_int = int(number)
                                        time_int = int(time_val)
                                        
                                        if number_int not in EMOTE_MAP:
                                            error_msg = f"[B][C][FF0000]❌ ERROR! Number must be between 1-21 only!\n"
                                            await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                        elif time_int < 1 or time_int > 100:
                                            error_msg = f"[B][C][FF0000]❌ ERROR! Time must be between 1-100 only!\n"
                                            await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                        else:
                                            if evo_custom_spam_task and not evo_custom_spam_task.done():
                                                evo_custom_spam_running = False
                                                evo_custom_spam_task.cancel()
                                                await asyncio.sleep(0.5)
                                            
                                            evo_custom_spam_running = True
                                            evo_custom_spam_task = asyncio.create_task(evo_custom_emote_spam(uids, number_int, time_int, key, iv, region))
                                            
                                            emote_id = EMOTE_MAP[number_int]
                                            success_msg = f"[B][C][00FF00]✅ SUCCESS! Custom evolution emote spam started!\nTargets: {len(uids)} players\nEmote: {number_int} (ID: {emote_id})\nRepeat: {time_int} times\nInterval: 0.1 seconds\n"
                                            await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                                            
                                    except ValueError:
                                        error_msg = f"[B][C][FF0000]❌ ERROR! Invalid number/time format! Use numbers only.\n"
                                        await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)


                        if inPuTMsG.strip() == '/stop evo_fast':
                            if evo_fast_spam_task and not evo_fast_spam_task.done():
                                evo_fast_spam_running = False
                                evo_fast_spam_task.cancel()
                                success_msg = f"[B][C][00FF00]✅ SUCCESS! Evolution fast spam stopped successfully!\n"
                                await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                            else:
                                error_msg = f"[B][C][FF0000]❌ ERROR! No active evolution fast spam to stop!\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)

                        if inPuTMsG.strip() == '/stop evo_c':
                            if evo_custom_spam_task and not evo_custom_spam_task.done():
                                evo_custom_spam_running = False
                                evo_custom_spam_task.cancel()
                                success_msg = f"[B][C][00FF00]✅ SUCCESS! Evolution custom spam stopped successfully!\n"
                                await safe_send_message(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                            else:
                                error_msg = f"[B][C][FF0000]❌ ERROR! No active evolution custom spam to stop!\n"
                                await safe_send_message(response.Data.chat_type, error_msg, uid, chat_id, key, iv)

                        # BUNDLE COMMAND - /bundle
                        if inPuTMsG.strip().startswith('/bundle'):
                            parts = inPuTMsG.strip().split()

                            if len(parts) < 2:
                                await safe_send_message(
                                    response.Data.chat_type,
            "[B][C][00FF00]Available bundles:\n\n"
            "[FFFFFF]• rampage • cannibal • devil\n"
            "[FFFFFF]• scorpio • frostfire • paradox\n"
            "[FFFFFF]• naruto • aurora • midnight\n"
            "[FFFFFF]• itachi • dreamspace\n\n"
            "[00FF00]Use:\n"
            "[FFFFFF]/bundle <name>\n"
            "[FFFFFF]/bundle <name> 2",
                                    uid, chat_id, key, iv
                                )
                                return

                            bundle_name = parts[1].lower()
                            look_type = 2 if len(parts) >= 3 and parts[2] == "2" else 1

                            bundle_ids = {
        "rampage": 914000002,
        "cannibal": 914000003,
        "devil": 914038001,
        "scorpio": 914039001,
        "frostfire": 914042001,
        "paradox": 914044001,
        "naruto": 914047001,
        "aurora": 914047002,
        "midnight": 914048001,
        "itachi": 914050001,
        "dreamspace": 914051001
                            }

                            if bundle_name not in bundle_ids:
                                await safe_send_message(
                                    response.Data.chat_type,
            f"[B][C][FF0000]❌ Bundle '{bundle_name}' not found!",
                                    uid, chat_id, key, iv
                                )
                                return

                            bundle_id = bundle_ids[bundle_name]

                            await safe_send_message(
                                response.Data.chat_type,
                                f"[B][C][00FF00]🎬 Playing Animation | {bundle_name}",
                                uid, chat_id, key, iv
                            )
                        
                            try:
                                anim_packet = await animation_packet(bundle_id, key, iv)
                                if anim_packet and online_writer:
                                    await SEndPacKeT(
                                        whisper_writer, online_writer, 'OnLine', anim_packet
                                    )                        

                                await asyncio.sleep(2.7)

                                look_packet = await Look_Changer(
                                    bundle_id, key, iv, look_type, region
                                )

                                if look_packet and online_writer:
                                    await SEndPacKeT(
                                        whisper_writer, online_writer, 'OnLine', look_packet
                                    )

                                    await safe_send_message(
                                        response.Data.chat_type,
                f"[B][C][00FF00]✅ WEARED | {bundle_name} | Look {look_type}",
                                        uid, chat_id, key, iv
                                    )
                                else:
                                    await safe_send_message(
                                        response.Data.chat_type,
                "[B][C][FF0000]❌ Packet create failed",
                                        uid, chat_id, key, iv
                                    )

                            except Exception as e:
                                await safe_send_message(
                                    response.Data.chat_type,
            f"[B][C][FF0000]❌ Error: {str(e)[:60]}",
                                    uid, chat_id, key, iv
                                )                               
                                        
                        if inPuTMsG.strip() == '/restart':
                            print('Processing restart command')
    
                            await safe_send_message(
                                response.Data.chat_type,
                                "[B][C][00FF00]🔄 Restarting bot...",
                                uid, chat_id, key, iv
                            )
    
                            raise RestartBot()
                            
                        #EMOTE LIST COMMAND
                        if inPuTMsG.strip().lower() in ("/emote_list", "/emt_list"):     

                            part1 = """[B][C][00FF00]Emotes List - Part 1/8:
[FFFFFF]p90 , m60 , mp5 , groza , thompson_evo
[FFFFFF]m10_red , mp40_blue , m10_green , xm8 , ak
[FFFFFF]mp40 , m4a1 , famas , scar , ump , m18
[FFFFFF]fist , g18 , an94 , woodpecker , money , heart
[FFFFFF]rose , throne , pirate , car , cobra , ghost
[FFFFFF]sholay , blade , hello , dab , chicken , dance
[FFFFFF]babyshark , pushup , dragon , highfive , selfie
[FFFFFF]breakdance , kungfu , thor , rasengan , ninja"""

                            await safe_send_message(response.Data.chat_type, part1, uid, chat_id, key, iv)  
                            await asyncio.sleep(0.2)  

                            part2 = """[B][C][00FF00]Emotes List - Part 2/8:
[FFFFFF]clone , fireball , hammer , valentineheart , rampagebook
[FFFFFF]guildflag , fish , inosuke , mummydance , shuffling
[FFFFFF]dangerousgame , jaguardance , threaten , shakewithme
[FFFFFF]devilsmove , furiousslam , moonflip , wigglewalk
[FFFFFF]battledance , shakeitup , gloriousspin , cranekick
[FFFFFF]partydance , jigdance , soulshaking , healingdance
[FFFFFF]topdj , deathglare , powerofmoney , eatmydust
[FFFFFF]bonappetit , aimfire , swan , teatime , bringiton
[FFFFFF]whyohwhy , fancyhands , shimmy , doggie , challengeon"""
                            await safe_send_message(response.Data.chat_type, part2, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)

                            part3 = """[B][C][00FF00]Emotes List - Part 3/8:
[FFFFFF]lasso , imrich , morepractice , ffws2021 , dracossoul
[FFFFFF]goodgame , greetings , walker , bornoflight , mythosfour
[FFFFFF]championgrab , winandchill , hadouken , bloodwraith
[FFFFFF]bigsmash , fancysteps , allincontrol , debugging , waggorwave
[FFFFFF]crazyguitar , poof , chosenvictor , challenger , partygame5
[FFFFFF]partygame6 , partygame3 , partygame4 , partygame7
[FFFFFF]partygame1 , partygame8 , partygame2 , dribbleking
[FFFFFF]ffwsguitar , mindit , goldencombo , sickmoves , rapswag
[FFFFFF]battleinstyle , rulersflag , moneythrow , endlessbullets"""
                            await safe_send_message(response.Data.chat_type, part3, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)
   
                            part4 = """[B][C][00FF00]Emotes List - Part 4/8:
[FFFFFF]smoothsway , number1 , fireslam , heartbroken
[FFFFFF]rockpaperscissors , shatteredreality , haloofmusic
[FFFFFF]burntbbq , switchingsteps , creedslay , leapoffail
[FFFFFF]rhythmgirl , helicoptership , kungfutigers , possessedwarrior
[FFFFFF]raiseyourthumb , fireborn , goldenfeather , comeanddance
[FFFFFF]dropkick , sitdown , booyahsparks , ffwsdance , easypeasy
[FFFFFF]winnerthrow , weightofvictory , chronicle , collapse
[FFFFFF]flaminggroove , energetic , ridicule , teasewaggor
[FFFFFF]greatconductor , fakedeath , twerk , brheroic , brmaster
[FFFFFF]csheroic , csmaster , yesido , freemoney , singersb03"""
                            await safe_send_message(response.Data.chat_type, part4, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)

                            part5 = """[B][C][00FF00]Emotes List - Part 5/8:
[FFFFFF]singersb0203 , singersb010203 , victoriouseagle , flyingsaucer
[FFFFFF]weaponmagician , bobbledance , weighttraining , beautifullove
[FFFFFF]groovemoves , howlersrage , louderplease , ninjastand
[FFFFFF]creatorinaction , ghostfloat , shibasurf , waiterwalk
[FFFFFF]grafficameraman , agileboxer , sunbathing , skateboardswag
[FFFFFF]phantomtamer , signal , eternaldescent , swaggydance , admire
[FFFFFF]reindeerfloat , bamboodance , constellationdance , trophygrab
[FFFFFF]starryhands , yum , happydancing , juggle , neonsign
[FFFFFF]beasttease , drachentear , clapdance , influencer , macarena
[FFFFFF]technoblast , valentine , angrywalk , makesomenoise , crocohooray"""
                            await safe_send_message(response.Data.chat_type, part5, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)

                            part6 = """[B][C][00FF00]Emotes List - Part 6/8:
[FFFFFF]scorpionspin , cindersummon , shallwedance , spinmaster
[FFFFFF]festival , artisticdance , forwardbackward , scorpionfriend
[FFFFFF]achingpower , earthlyforce , grenademagic , ohyeah
[FFFFFF]graceonwheels , flex , firebeasttamer , crimsontunes
[FFFFFF]swaggyvsteps , chromaticfinish , smashthefeather , sonoroussteps
[FFFFFF]chromaticpop , chromatwist , birthofjustice , spidersense
[FFFFFF]chromasonicshot , playwiththunderbolt , anniversary , wisdomswing
[FFFFFF]thunderflash , whirlpool , flyinginksword , dancepuppet
[FFFFFF]highknees , feeltheelectricity , whacacotton , honorablemention
[FFFFFF]brgrandmaster , csgm , monsterclubbing , basudaradance"""
                            await safe_send_message(response.Data.chat_type, part6, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)

                            part7 = """[B][C][00FF00]Emotes List - Part 7/8:
[FFFFFF]stirfryfrostfire , moneyrain , frostfirecalling , stompingfoot
[FFFFFF]thisway , excellentservice , lvl100 , realtiger , celebrationschuss
[FFFFFF]dawnvoyage , lamborghiniride , toiletman , handgrooves , kemusan
[FFFFFF]ribbitrider , innerself , emperortreasure , whysochaos , hugefeast
[FFFFFF]colorburst , dragonswipe , samba , speedsummon , whatamatch
[FFFFFF]whatapair , bytemounting , unicyclist , basketrafting , happylamb
[FFFFFF]paradox , harmoniousparadox , raiseyourthumb2 , claphands
[FFFFFF]donedeal , starcatcher , paradoxwings , zombified , honkup
[FFFFFF]cyclone , springrocker , giddyup , goosydance , captainvictor
[FFFFFF]youknowimgood , stepstep , superyay , moonwalk , flowersalute"""
                            await safe_send_message(response.Data.chat_type, part7, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)

                            part8 = """[B][C][00FF00]Emotes List - Part 8/8:
[FFFFFF]foxyrun , waggorsseesaw , floatingmeditation , naatunaatu
[FFFFFF]championswalk , auraboarder , booyahchamp , controlledcombustion
[FFFFFF]cheerstovictory , shoeshining , gunspinning , crowdpleaser
[FFFFFF]nosweat , magmaquake , maxfirepower , canttouchthis , firestarter
[FFFFFF]ffwsflag , beatdrop , spatialawareness , trapping , soaringup
[FFFFFF]wontbowdown , aurora , couchfortwo , flutterdash , slipperythrone
[FFFFFF]acceptancespeech , lovemelovemenot , scissorsavvy , thinker
[FFFFFF]matchcountdown , hiptwists , jkt48 , stormyascent , thousandyears
[FFFFFF]ninjasign , ninjarun , clonejutsu , rescue , midnightperuse
[FFFFFF]guitargroove , keyboardplayer , ondrums , chacchac , pillowfight
[FFFFFF]targetpractice , goofycamel , hitasix , flagsummon , swiftsteps
[FFFFFF]carnivalfunk , slurp , paint , halftime , throwin , bailalorocky
[FFFFFF]bigdill , handraise , owl , slapandtwist , sidewiggle , creationdays
[FFFFFF]rainingcoins , clapclaphooray , infiniteloops , p90surfer , boxingmachine
[FFFFFF]flyingguns , comicbarf , driveby , pedalmetal , spearspin , guildflag
[FFFFFF]discodazzle , squatchallenge , winninggoal , headhigh , ninjasummon
[FFFFFF]finalbattle , foreheadpoke , fireballjutsu , flyingraijin , thor
[FFFFFF]circle , drumtwirl , bunnyaction , broomswoosh , bladefromheart
[FFFFFF]mapread , tomato , tacticalmoveout , bunnywiggle , flamingheart
[FFFFFF]rainorshine , sholay , peakpoints , AuraFarming , dream , angelic , shower
[FFFFFF]motorbike , bow , petals , puffyride"""
                            await safe_send_message(response.Data.chat_type, part8, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)
                            
                        if inPuTMsG.strip().lower() == "hi": 
                            greeting_message = f"[B][C][FFFFFF]Hello {sender_name}"
                            await safe_send_message(response.Data.chat_type, greeting_message, uid, chat_id, key, iv)
                            
                                                            
                        #HELP COMMAND -/help
                        if inPuTMsG.strip().lower() in ("help", "/help", "menu", "/menu", "commands"):     
                            print(f"Help command detected from UID: {uid}")

                            header = (  
                        f"[C][B][FFD700]Hey {sender_name} Welcome to NAYAN BOT!\n\n"  
                        "[C][B][FFFFFF]Type commands to interact with me.\n"  
                        "[C][B][00FF00]Below are all available commands:")  

                            await safe_send_message(response.Data.chat_type, header, uid, chat_id, key, iv)  
                            await asyncio.sleep(0.2)  

                               # ───── Group Commands ─────  
                            group_commands = (
    "[C][B][FF00FF]┌── [FFFFFF]🔹 GROUP COMMANDS 🔹\n"
    "[C][B][FF00FF]├── [00FF00]/3 [FFFFFF]- Create 3 Player Group.\n"
    "[C][B][FF00FF]├── [00FF00]/5 [FFFFFF]- Create 5 Player Group.\n"
    "[C][B][FF00FF]├── [00FF00]/6 [FFFFFF]- Create 6 Player Group.\n"
    "[C][B][FF00FF]├── [00FF00]/inv [uid] [FFFFFF]- Invite Player to Current Group.\n"
    "[C][B][FF00FF]├── [00FF00]/join [team_code] [FFFFFF]- Join Team Using Team Code.\n"
    "[C][B][FF00FF]└── [00FF00]/exit [FFFFFF]- Leave Current Group."
                            )
                            await safe_send_message(response.Data.chat_type, group_commands, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)

                            # ───── Group Commands ─────  
                            player_info = (
    "[C][B][0000FF]┌── [FFFFFF]🔹 INFO COMMANDS 🔹\n"
    "[C][B][0000FF]├── [00FF00]/info [uid] [FFFFFF]- Full Player Information.\n"
    "[C][B][0000FF]├── [00FF00]/like [uid] [FFFFFF]- Send 100 Likes.\n"
    "[C][B][0000FF]├── [00FF00]/status [uid] [FFFFFF]- Check Player Status.\n"
    "[C][B][0000FF]├── [00FF00]/bio [uid] [FFFFFF]- Get Player Bio.\n"
    "[C][B][0000FF]└── [00FF00]/check [uid] [FFFFFF]- Check Ban Status.\n"
                            )
                            await safe_send_message(response.Data.chat_type, player_info, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)
        
                            advanced_commands = (
    "[C][B][FF8800]┌── [FFFFFF]🔹 ADVANCED COMMANDS 🔹\n"
    "[C][B][FF8800]├── [00FF00]/lw [team_code] [FFFFFF]- Start Level Up Bot (24×7) For Team.\n"
    "[C][B][FF8800]├── [00FF00]/stop_auto [FFFFFF]- Stop Level Up Bot.\n"
    "[C][B][FF8800]└── [00FF00]/s1 /s2 /s3 /s4 /s5 [uid] [FFFFFF]- Send Join Requests With Different Badges.\n"
    "[C][B][FF8800]├── [00FF00]/spm_inv [uid] [FFFFFF]- Spam Invites (30x) To Player.\n"
    "[C][B][FF8800]├── [00FF00]/ghost [team_code] [name] [FFFFFF]- Ghost Join Team Using Code.\n"
    "[C][B][FF8800]├── [00FF00]/attack [team_code] [FFFFFF]-Start Attack Lag On Team\n"
    "[C][B][FF8800]└── [00FF00]/lag [team_code] [FFFFFF]- Start Lag Attack On Team.\n"
                            )
                            await safe_send_message(response.Data.chat_type, advanced_commands, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)

                            # ───── Room Commands ─────  
                            room_commands = (
    "[C][B][80FF00]┌── [FFFFFF]🔹 ROOM COMMANDS 🔹\n"
    "[C][B][80FF00]├── [00FF00]/join_room [room_id] [password] [FFFFFF]- Join Custom Room.\n"
    "[C][B][80FF00]├── [00FF00]/leave_room [uid] [FFFFFF]- Leave Custom Room.\n"
    "[C][B][80FF00]├── [00FF00]/spmroom [uid] [FFFFFF]- Room Invite Spam.\n"
    "[C][B][80FF00]├── [00FF00]/rmlag [room_id] [password] [FFFFFF]- Room Lag Attack (10 Seconds).\n"
    "[C][B][80FF00]└── [00FF00]/room [uid] [room_id] [FFFFFF]- Join Request With New V-Badge In Room.\n"
                            )
                            await safe_send_message(response.Data.chat_type, room_commands, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)

                            # ───── Emote Commands ─────  
                            emote_commands = (
    "[C][B][FFD700]┌── [FFFFFF]🔹 EMOTE COMMANDS 🔹\n"
    "[C][B][FFD700]├── [00FF00]!e [uid] [emote_id] [FFFFFF]- Send Single Emote To Player.\n"
    "[C][B][FFD700]├── [00FF00]/dance [uid] [1-385] [FFFFFF]- Play Emote By Number (1-385).\n"
    "[C][B][FFD700]├── [00FF00]/dance [uid] [name] [FFFFFF]- Play Emote By Name (ex: hello, ak).\n"
    "[C][B][FFD700]├── [00FF00]/fast [uid] [emote_id] [FFFFFF]- Fast Emote (25x) To Player.\n"
    "[C][B][FFD700]├── [00FF00]/p [uid] [emote_id] [x] [FFFFFF]- Custom Emote X Times To Player.\n"
    "[C][B][FFD700]└── [00FF00]/quick [team_code] [emote_id] [target_uid] [FFFFFF]- Quick Emote And Solo.\n"
                            )
                            await safe_send_message(response.Data.chat_type, emote_commands, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)

                            # ───── Evolution Emotes ─────  
                            evo_commands = (
    "[C][B][8000FF]┌── [FFFFFF]🔹 EVOLUTION COMMANDS 🔹\n"
    "[C][B][8000FF]├── [00FF00]/evo [uid] [1-21] [FFFFFF]- Send Evolution Emote To Player.\n"
    "[C][B][8000FF]├── [00FF00]/evo_fast [uid] [1-21] [FFFFFF]- Fast Evolution Emote (25x) To Player.\n"
    "[C][B][8000FF]├── [00FF00]/evo_c [uid] [1-21] [x] [FFFFFF]- Custom Evolution Emote X Times To Player.\n"
    "[C][B][8000FF]├── [00FF00]/random [uid] [FFFFFF]- Auto Cycle All Evolution Emotes For Player.\n"
    "[C][B][8000FF]└── [00FF00]/ruk bhai [FFFFFF]- Stop Evolution Emote Cycle.\n"
                            )
                            await safe_send_message(response.Data.chat_type, evo_commands, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)

                            # ───── Tools & AI ─────  
                            ai_commands = (
    "[C][B][00FFFF]┌── [FFFFFF]🔹 FUNNY COMMANDS 🔹\n"
    "[C][B][00FFFF]├── [00FF00]/ms <text> [FFFFFF]- Send Custom Spam Message.\n"
    "[C][B][00FFFF]├── [00FF00]/gali [name] [FFFFFF]- Send Custom Name Gali Spam Message.\n"
    "[C][B][00FFFF]├── [00FF00]/dm [target_uid] [message] [FFFFFF]- Send Custom Message To The Specified Player.\n"    
    "[C][B][00FFFF]├── [00FF00]/bundle [name] [FFFFFF]- Bundle Send To Bot.\n"
    "[C][B][00FFFF]├── [00FF00]/outfit [character_id] [bottom] [shoe] [top] [face] [mask] [FFFFFF]- Change Bot's Outfit.\n"
    "[C][B][00FFFF]├── [00FF00]/kick [uid] [FFFFFF]- Kick Specified Player.\n"
    "[C][B][00FFFF]└──  [00FF00]/ai [question] [FFFFFF]- Ask AI Anything.\n"
                            )
                            await safe_send_message(response.Data.chat_type, ai_commands, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)

                            # ───── Admin Commands ─────  
                            admin_commands = (
    "[C][B][FF1493]┌── [FFFFFF]🔹 ADMIN COMMANDS 🔹\n"
    "[C][B][FF1493]├── [00FF00]/friend_list [FFFFFF]- Show Bot Friend List.\n"
    "[C][B][FF1493]├── [00FF00]/add [uid] [FFFFFF]- Send Friend To Specified User.\n"
    "[C][B][FF1493]├── [00FF00]/remove [uid] [FFFFFF]- Remove Friend From List.\n"
    "[C][B][FF1493]├── [00FF00]/guild_join [guild_id] [FFFFFF]- Join The Specified Guild.\n"
    "[C][B][FF1493]├── [00FF00]/guild_leave [guild_id] [FFFFFF]- Leave The Specified Guild.\n"
    "[C][B][FF1493]├── [00FF00]/guild_members [guild_id] [FFFFFF]- Check The Guild Members And Glory.\n"
    "[C][B][FF1493]└── [00FF00]/restart [FFFFFF]- Restart The Bot.\n"
                            )

                            await safe_send_message(response.Data.chat_type, admin_commands, uid, chat_id, key, iv)  
                            await asyncio.sleep(0.2)  
                          
                            # ───── Tip Commands ─────  
                            tip_commands = (
    "[C][B][00CED1]┌── [FFFFFF]🔹 EXTRA COMMANDS 🔹\n"
    "[C][B][00CED1]├── [00FF00]/emote_list [FFFFFF]- Show The Emote List.\n"
    "[C][B][00CED1]├── [00FF00]/bundle [FFFFFF]- Show The Bundle List.\n"
    "[C][B][00CED1]├── [00FF00]/sticker [FFFFFF]- Sends Sticker Via Bot.\n"
    "[C][B][00CED1]├── [00FF00]/title [FFFFFF]- Sends Title Via Bot.\n"
    "[C][B][00CED1]├── [00FF00]/hijack on/off [FFFFFF]- Emote Hijack Enable/disable.\n"
    "[C][B][00CED1]├── [00FF00]/ttt help[FFFFFF]- Show TicTacToe Game help menu.\n"
    "[C][B][00CED1]└──  [00FF00]/dev [FFFFFF]- Admin Information.\n"
                            )

                            await safe_send_message(response.Data.chat_type, tip_commands, uid, chat_id, key, iv)
                            await asyncio.sleep(0.2)
                        response = None
                            
            whisper_writer.close() ; await whisper_writer.wait_closed() ; whisper_writer = None
                    
                    	
                    	
        except Exception as e:
            if isinstance(e, RestartBot):
                raise   
            print(f"ErroR {ip}:{port} - {e}")
            whisper_writer = None
        await asyncio.sleep(reconnect_delay)

async def MaiiiinE():
    global LOGIN_METHOD, LOGIN_TYPE

    # ========== SELECT LOGIN METHOD (accs or direct) ==========
    if LOGIN_METHOD is None:
        print("\n" + "="*50)
        print("📂 LOGIN METHOD SELECTOR")
        print("="*50)
        print("1. Use accs.txt (UID + Password)")
        print("2. Use direct.txt (AccessToken only)")
        print("="*50)
        while True:
            choice = input("👉 Enter 1 or 2: ").strip()
            if choice == "1":
                LOGIN_METHOD = "accs"
                break
            elif choice == "2":
                LOGIN_METHOD = "direct"
                break
            print("❌ Invalid choice. Please enter 1 or 2.")
        print(f"✅ Using login method: {LOGIN_METHOD}")
        print("="*50 + "\n")
    else:
        print(f"✅ Using pre‑configured login method: {LOGIN_METHOD}")

    # ========== SELECT LOGIN TYPE (Mobile or PC) ==========
    if LOGIN_TYPE is None:
        print("\n" + "="*50)
        print("🤖 LOGIN TYPE SELECTOR")
        print("="*50)
        print("1. Mobile")
        print("2. PC")
        print("="*50)
        while True:
            choice = input("👉 Enter 1 or 2: ").strip()
            if choice == "1":
                LOGIN_TYPE = 1
                break
            elif choice == "2":
                LOGIN_TYPE = 2
                break
            print("❌ Invalid choice. Please enter 1 or 2.")
        print(f"✅ Using {('Mobile' if LOGIN_TYPE==1 else 'PC')} login type")
        print("="*50 + "\n")
    else:
        print(f"✅ Using pre‑configured login type: {'Mobile' if LOGIN_TYPE==1 else 'PC'}")

    # ---------- BAAKI KA PURANA CODE (Kuch nahi badlega) ----------
    print("📁 Loading credentials...")
    
    open_id = None
    access_token = None
    
    if LOGIN_METHOD == "direct":
        direct_file = "direct.txt"
        if not os.path.exists(direct_file):
            print(f"❌ {direct_file} not found!")
            return None
        
        try:
            with open(direct_file, "r", encoding="utf-8") as f:
                access_token = f.read().strip()
                
            if not access_token:
                print("❌ direct.txt is empty")
                return None
                
            print("✅ Using access token from direct.txt")
            
            inspect_data = await get_token_inspect_data(access_token)
            if not inspect_data:
                print("❌ Failed to inspect token – invalid or expired access token")
                return None
                
            open_id = inspect_data.get('open_id')
            platform = int(inspect_data.get('platform', 4))
            
            if not open_id:
                print("❌ No open_id found in token inspection")
                return None
                
            print(f"✅ Token inspection successful")
            
        except Exception as e:
            print(f"❌ Error reading direct.txt: {e}")
            return None
            
    elif LOGIN_METHOD == "accs":
        credentials = None
        
        try:
            with open("accs.txt", 'r', encoding='utf-8') as f:
                content = f.read().strip()
            
            if content.startswith('{') and content.endswith('}'):
                print("✅ Detected JSON format")
                json_data = json.loads(content)
                if isinstance(json_data, dict) and json_data:
                    first_uid = list(json_data.keys())[0]
                    first_password = json_data[first_uid]
                    credentials = (first_uid, first_password)
                    print(f"✅ Loaded from JSON: UID={first_uid}")
        except:
            pass
        
        if not credentials:
            credentials = load_credentials_from_file("accs.txt")
        
        if not credentials:
            print("❌ Failed to load credentials from accs.txt!")
            print("💡 Please edit accs.txt with your credentials")
            print("📝 Recommended JSON format:")
            print('   {"4870750605": "A9A27888F2CAFED604B02214FD54E0CC2CFBD1AC7B0973836490DDB61FD1668AA9A27888F2CAFED604B02214FD54E0CC2CFBD1AC7B0973836490DDB61FD1668A"}')
            return None
        
        try:
            if isinstance(credentials, tuple) or isinstance(credentials, list):
                Uid, Pw = credentials[0], credentials[1]
            elif isinstance(credentials, dict):
                Uid = list(credentials.keys())[0]
                Pw = credentials[Uid]
            else:
                print(f"❌ Unexpected credentials format: {type(credentials)}")
                return None
        except (ValueError, IndexError, TypeError) as e:
            print(f"❌ Error extracting credentials: {e}")
            return None
        
        print(f"✅ Credentials loaded successfully")
        print(f"👤 UID: {Uid}")
        
        open_id, access_token = await GeNeRaTeAccEss(Uid, Pw)
        if not open_id or not access_token:
            print("❌ Error - Invalid Account (Check UID/Password)")
            return None
        
    else:
        print("❌ Invalid login method")
        return None

    # For direct login, use the detected platform; for accs, use guest platform (4)
    if LOGIN_METHOD == "direct":
        configs = [(platform, platform, platform, platform)]
    else:
        configs = [(4, 4, 4, 4)]

    MajoRLoGinauTh = None
    for (oidt, lidt, opt, ppt) in configs:
        if LOGIN_TYPE == 1:   
            PyL = await EncRypTMajoRLoGin_mobile(
                open_id, access_token, region="IND", lang_code="en",
                open_id_type_val=str(oidt),
                login_open_id_type_val=lidt,
                origin_platform_type_val=str(opt),
                primary_platform_type_val=str(ppt)
            )
        else:                  
            PyL = await EncRypTMajoRLoGin_pc(
                open_id, access_token,
                open_id_type=str(oidt),
                login_open_id_type=lidt,
                origin_platform_type=str(opt),
                primary_platform_type=str(ppt)
            )
        MajoRLoGinResPonsE = await MajorLogin(PyL)
        if MajoRLoGinResPonsE:
            MajoRLoGinauTh = await DecRypTMajoRLoGin(MajoRLoGinResPonsE)
            if MajoRLoGinauTh and MajoRLoGinauTh.token:
                break

    if not MajoRLoGinauTh or not MajoRLoGinauTh.token:
        print("❌ Target Account => Banned / Not Registered!")
        return None

    token = MajoRLoGinauTh.token
    if not token:
        print("❌ No authentication token received!")
        return None

    try:
        import json
        import time
        from datetime import datetime

        region = getattr(MajoRLoGinauTh, 'region', 'IND')

        token_data = {
            "token": token,
            "saved_at": time.time(),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "bot_uid": str(Uid) if LOGIN_METHOD == "accs" else "unknown",
            "region": region,
            "source": "main.py_bot_login"
        }

        with open("token.json", "w") as f:
            json.dump(token_data, f, indent=2)

        print("✅ Token saved to token.json")
        print(f"📝 Token info: Region={region}, UID={token_data['bot_uid']}")

    except Exception as e:
        print(f"⚠️ Warning: Could not save token to file: {e}")
        import traceback
        traceback.print_exc()

    UrL = MajoRLoGinauTh.url

    os.system('clear')
    print("=" * 50)
    print("🤖 BOT - INITIALIZING")
    print("=" * 50)
    print("🔄 Starting TCP Connections...")
    print("📡 Connecting to Free Fire servers...")
    print("🌐 Server connection established")

    region = getattr(MajoRLoGinauTh, 'region', 'IND')
    ToKen = token
    TarGeT = MajoRLoGinauTh.account_uid
    global CURRENT_BOT_UID
    CURRENT_BOT_UID = str(TarGeT)          
    key = MajoRLoGinauTh.key
    iv = MajoRLoGinauTh.iv
    timestamp = MajoRLoGinauTh.timestamp

    print(f"🔐 Authentication successful")
    print(f"👤 Account UID: {TarGeT}")
    print(f"🌍 Region: {region}")
    print(f"🔑 Token: {ToKen[:30]}...")

    LoGinDaTa = await GetLoginData(UrL, PyL, ToKen)
    if not LoGinDaTa:
        print("❌ Error - Getting Ports From Login Data!")
        return None

    LoGinDaTaUncRypTinG = await DecRypTLoGinDaTa(LoGinDaTa)

    OnLinePorTs = LoGinDaTaUncRypTinG.Online_IP_Port
    ChaTPorTs = LoGinDaTaUncRypTinG.AccountIP_Port

    print(f"📡 Online Server: {OnLinePorTs}")
    print(f"💬 Chat Server: {ChaTPorTs}")

    OnLineiP, OnLineporT = OnLinePorTs.split(":")
    ChaTiP, ChaTporT = ChaTPorTs.split(":")

    acc_name = LoGinDaTaUncRypTinG.AccountName
    print(f"👋 Welcome, {acc_name}!")

    AutHToKen = await xAuThSTarTuP(int(TarGeT), ToKen, int(timestamp), key, iv)

    ready_event = asyncio.Event()

    print("\n🚀 Starting bot services...")

    task1 = asyncio.create_task(TcPChaT(ChaTiP, ChaTporT, AutHToKen, key, iv, LoGinDaTaUncRypTinG, ready_event, region))
    task2 = asyncio.create_task(TcPOnLine(OnLineiP, OnLineporT, ToKen, TarGeT, key, iv, AutHToKen, region))

    os.system('clear')
    print("🤖 BOT - STARTING")
    print("=" * 50)

    for i in range(1, 4):
        dots = "." * i
        print(f"🔄 Loading{dots}")
        time.sleep(0.3)

    os.system('clear')
    print("🤖 BOT - CONNECTING")
    print("=" * 50)
    print("┌────────────────────────────────────┐")
    print("│ ██████████████████████████████████ │")
    print("└────────────────────────────────────┘")

    print("\n⏳ Waiting for chat connection...")
    try:
        await asyncio.wait_for(ready_event.wait(), timeout=10)
        print("✅ Chat connection established!")
    except asyncio.TimeoutError:
        print("⚠️ Chat connection timeout, continuing...")

    os.system('clear')
    print("=" * 50)
    print("🤖 BOT - ONLINE")
    print("=" * 50)
    print(f"🔹 UID: {TarGeT}")
    print(f"🔹 Name: {acc_name}")
    print(f"🔹 Region: {region}")
    print(f"🔹 Status: 🟢 READY")
    print(f"🔹 Chat Server: {ChaTiP}:{ChaTporT}")
    print(f"🔹 Online Server: {OnLineiP}:{OnLineporT}")
    print("=" * 50)
    print("💡 Commands available in squad/guild chat")
    print("💡 Type /help for command list")
    print("=" * 50)

    print("\n📊 System Check:")
    print(f"📁 Working directory: {os.getcwd()}")
    print(f"📁 Cache file: {CACHE_FILE}")

    try:
        test_data = {'test': 'ok', 'timestamp': time.time()}
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(test_data, f)
        print("✅ Cache file write test: PASSED")
    except Exception as e:
        print(f"⚠️ Cache file write test: {e}")

    if os.path.exists("token.json"):
        print("✅ token.json file exists")
        try:
            with open("token.json", "r") as f:
                token_info = json.load(f)
            age = time.time() - token_info.get('saved_at', 0)
            print(f"✅ Token age: {age:.1f} seconds")
        except:
            print("⚠️ Could not read token.json")
    else:
        print("❌ token.json not found!")
    
    print("\n🎯 Bot is now running...")
    print("📡 Listening for commands and invitations")

    # ================= DYNAMIC EMOTE FETCHING =================
    global equip_emote_id                       # <-- added
    try:
        base_url = get_base_url(region)
        # Fetch currently equipped emote from the account
        equipped_emote = await get_equipped_emote(ToKen, region)
        if equipped_emote:
            print(f"✅ Detected currently equipped emote: {equipped_emote}")
            equip_emote_id = equipped_emote     # <-- replace hardcoded value
            equip_emote_like_clan_join(equipped_emote, ToKen, region)
        else:
            print("⚠️ Could not detect equipped emote, using fallback hardcoded value")
            equip_emote_like_clan_join(equip_emote_id, ToKen, region)
    except Exception as e:
        print(f"⚠️ Equip emote fetch failed: {e}")
        equip_emote_like_clan_join(equip_emote_id, ToKen, region)
    # ==========================================================

    try:
        await asyncio.wait_for(asyncio.gather(task1, task2), timeout=30 * 60)
    except asyncio.TimeoutError:
        print("Auto restart after 7 hours")
        raise RestartBot()
    except RestartBot:
        raise
    except asyncio.CancelledError:
        print("\n🛑 Bot tasks cancelled")
    except Exception as e:
        print(f"\n❌ Error in bot tasks: {e}")
        import traceback
        traceback.print_exc()

    return None

if __name__ == '__main__':
    asyncio.run(StarTinG())
    
  