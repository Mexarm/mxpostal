#!/usr/bin/python
# -*- coding: utf-8 -*-
import re
import json
from slugify import slugify
RE_FLAGS = re.IGNORECASE|re.MULTILINE|re.DOTALL
REGEX_CP1 = re.compile(r'c\W*p\W*(\d{5}|\d{4})',RE_FLAGS)
REGEX_CP2 = re.compile(r'(\d{5})',RE_FLAGS)
REGEX_COL = r'^.+?((%s)\W\W*[\w\W]+?)(?:,|\n|\r|c[\W]*p|\d{5}|$)'
REGEX_COL1 = re.compile(REGEX_COL%('colonia|col',),RE_FLAGS)
REGEX_COL2 = re.compile(REGEX_COL%('fraccionamiento|fracc',),RE_FLAGS)
REGEX_COL3 = re.compile(REGEX_COL%('barrio|parque|zona',),RE_FLAGS)

REGEX_BUSSINESS1 = re.compile(r'\sDE\s+C[\.\s]*V[\.\s$]*',RE_FLAGS) #SA de CV
REGEX_BUSSINESS2 = re.compile(r'S[\.\s]*DE\s+R[\.\s]*L[\.\s$]*',RE_FLAGS) #S DE RL
REGEX_BUSSINESS3 = re.compile(r'\s+S[\.\s]*C[\.\s$]*',RE_FLAGS) #SC
REGEX_BUSSINESS4 = re.compile(r'\s+A[\.\s]*C[\.\s$]*',RE_FLAGS) #AC
REGEX_BUSSINESS5 = re.compile(r'\s+S[\.\s]*A[\.\s\b$]',RE_FLAGS) #SA
REGEX_BUSSINESS6 = re.compile(r'\s+C\.V\.',RE_FLAGS) #CV
REGEX_BUSSINESS7 = re.compile(r'\s+I[\.\s]*A[\.\s$]*P[\.\s$]*',RE_FLAGS) #IAP
REGEX_BUSSINESS8 = re.compile(r'((\bCOOPERATIV\b)|(\bbufete\b)|(\bcomision\b)|(\bgrupo\b)|(\binstitu\b)|(\bgobierno\b)|(\bSA$))',RE_FLAGS) #coopertiva

ESTADO_COMMON_NAME = json.loads(open("estado_common_name.json","rb").read())
ALIAS_ESTADO = json.loads(open("alias_estados.json","rb").read())

def get_cp_from_str(line):
    cp = REGEX_CP1.search(line)
    if cp: return cp.group(1)
    cp = REGEX_CP2.search(line)
    if cp: return cp.group(1)
    return None

def get_colonia_from_str(line):
    col = REGEX_COL1.search(line)
    if col: return col.group(1)
    col = REGEX_COL2.search(line)
    if col: return col.group(1)
    col = REGEX_COL3.search(line)
    if col: return col.group(1)
    return

def get_callenum_from_str(line):
    col = REGEX_COL1.search(line)
    if col: return line[0:col.start(1)]
    col = REGEX_COL2.search(line)
    if col: return line[0:col.start(1)]
    col = REGEX_COL3.search(line)
    if col: return line[0:col.start(1)]
    return

def format_cp(codigo_postal):
    if isinstance(codigo_postal,basestring):
        codigo_postal = cp_to_int(codigo_postal)
    return '%05d' % (codigo_postal,)

def cp_to_int(codigo_postal):
    if not codigo_postal \
        or isinstance(codigo_postal,int) \
        or not isinstance(codigo_postal,basestring):
        return codigo_postal
    try:
        cp = int(codigo_postal)
    except:
        cp = 0
    return cp

def alias_to_cve_estado(estado):
    s_alias_estado = { slugify(k):ALIAS_ESTADO[k] for k in ALIAS_ESTADO }
    s_estado = slugify(estado)
    s_alias = [slugify(a) for a in ALIAS_ESTADO.keys()]
    matches = [alias for alias in s_alias if s_estado in alias ]
    if matches:
        matches.sort(key=len)
        return s_alias_estado[matches[0]]
    return False

def estado_to_common_name(estado):
    if estado.upper() in ESTADO_COMMON_NAME:
        return ESTADO_COMMON_NAME[estado.upper()]
    return estado

def dedup(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def delete_empty(seq):
    return [x for x in seq if len(x) >0]    

def is_bussiness(text):
    if REGEX_BUSSINESS1.search(text) or \
       REGEX_BUSSINESS2.search(text) or \
       REGEX_BUSSINESS3.search(text) or \
       REGEX_BUSSINESS4.search(text) or \
       REGEX_BUSSINESS5.search(text) or \
       REGEX_BUSSINESS6.search(text) or \
       REGEX_BUSSINESS7.search(text) or \
       REGEX_BUSSINESS8.search(text):
        return True
    return False

