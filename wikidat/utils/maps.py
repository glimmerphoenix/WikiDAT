# -*- coding: utf-8 -*-
######
# tags.py
# Module defining regular expressions and logical tags for WikiDAT
#
# Author: Felipe Ortega and Aaron Halfaker
#
######

import re

"""
A map from file extension to the command for extracting the
data to STDOUT. STDERR is redirected to /dev/null to
capture just STDOUT
"""
EXTENSIONS = {
    'xml': "cat 2>/dev/null",
    'bz2': "bzcat 2>/dev/null",
    '7z':  "7za e -so 2>/dev/null",
    'lzma': "lzcat 2>/dev/null",
    'gz': "zcat 2>/dev/null"
}

"""
A regular expression for extracting the final extension of a file.
"""
EXT_RE = re.compile(r'\.([^\.]+)$')

"""
List of regular expressions for detection of Featured Articles,
Featured Lists (if they exist in that language) and Good Articles
"""
DE_FA_RE = re.compile(r'(\{\{[Ee]xzellent\|.*\|[0-9]*\}\})')
DE_FLIST_RE = re.compile(r'(\{\{[Ii]nformativ\}\})')
DE_GA_RE = re.compile(r'(\{\{[Ll]esenswert\|.*\|[0-9]*\}\})')

EN_FA_RE = re.compile(r'(\{\{[Ff]eatured [Aa]rticle\}\})')
EN_FLIST_RE = re.compile(r'(\{\{[Ff]eatured [Ll]ist\}\})')
EN_GA_RE = re.compile(r'(\{\{[Gg]ood [Aa]rticle\}\})')

ES_FA_RE = re.compile(r'(\{\{[Aa]rt.*culo [Dd]estacado\}\})')
ES_GA_RE = re.compile(u'(\{\{[Aa]rt.*culo [Bb]ueno\}\})')

FR_FA_RE = re.compile(r'(\{\{Article de qualit.*\|.*\}\})')
FR_GA_RE = re.compile(r'({{[Bb]on [Aa]rticle\|.*\}\})')

IT_FA_RE = re.compile(r'(\{\{[Vv]etrina\|.*\}\})')
IT_GA_RE = re.compile(u'(\{\{Voce di qualità\|.*\}\})')

JA_FA_RE = re.compile(r'(\{\{[Ff]eatured.*[Aa]rticle\}\})')
JA_GA_RE = re.compile(r'(\{\{[Gg]ood [Aa]rticle\}\})')

NL_FA_RE = re.compile(r'(\{\{[Ee]talage\}\})')

PL_FA_RE = re.compile(r'(\{\{[Mm]edal\}\})')
PL_GA_RE = re.compile(u'(\{\{[Dd]obry [Aa]rtykuł\}\})')

PT_FA_RE = re.compile(r'(\{\{[Aa]rtigo [Dd]estacado\}\})')
PT_GA_RE = re.compile(r'(\{\{[Aa]rtigo [Bb]om\}\})')

# TODO: Possible change, eliminate | and leave .* only before '}}'
RU_FA_RE = re.compile(u'(\{\{Избранная статья\|.*\}\})')
RU_GA_RE = re.compile(u'(\{\{Хорошая статья\|.*\}\})')

ZH_FA_RE = re.compile(r'(\{\{[Ff]eatured.*[Aa]rticle\}\})')

SV_FA_RE = re.compile(r'(\{\{[Uu]tm.*rkt\}\})')

TR_FA_RE = re.compile(u'(\{\{[Ss]eçkin [Mm]adde\}\})')

FI_FA_RE = re.compile(r'(\{\{[Ss]uositeltu\}\})')

CS_FA_RE = re.compile(u'(\{\{[Nn]ejlepší článek\}\})')

ID_FA_RE = re.compile(r'(\{\{[Ff]eatured.*[Aa]rticle\}\})')

TH_FA_RE = re.compile(u'(\{\{บทความคัดสรร\}\})')

AR_FA_RE = re.compile(u'(\{\{مقالة مختارة\}\})')

KO_FA_RE = re.compile(u'(\{\{알찬 글 딱지\}\})')

HE_FA_RE = re.compile(u'(\{\{ערך מומלץ\}\})')

NO_FA_RE = re.compile(r'(\{\{[Uu]tmerket\}\})')

HU_FA_RE = re.compile(r'(\{\{[Kk]iemelt.*\}\})')

VI_FA_RE = re.compile(u'(\{\{[Ss]ao chọn lọc.*\}\})')

UK_FA_RE = re.compile(u'(\{\{медаль.*\}\})')

DA_FA_RE = re.compile(r'(\{\{[Ff]remragende.*\}\})')
DA_GA_RE = re.compile(r'(\{\{[Gg]od\}\})')

# WARNING: Two possible templates!!
FA_FA_RE = re.compile(u'(\{\{مقاله برگزیده\}\})|(\{\{نوشتار برگزیده\}\})')

RO_FA_RE = re.compile(r'(\{\{[Aa]rticol de [Cc]alitate\}\})')

# WARNING: Two possible templates!!
CA_FA_RE = re.compile(r'(\{\{[Aa]rticle de [Qq]ualitat\}\})|(\{\{1000\+AdQ.*\}\})')

BG_FA_RE = re.compile(u'(\{\{Избрана статия.*\}\})')

HR_FA_RE = re.compile(u'(\{\{[Ii]zdvojeni članak.*\}\})')

EL_FA_RE = re.compile(u'(\{\{[Αα]ξιόλογο άρθρο\}\})')

SK_FA_RE = re.compile(u'(\{\{[Pp]erfektný článok\}\})')

SR_FA_RE = re.compile(u'(\{\{изабрани\}\})')

LT_FA_RE = re.compile(r'(\{\{SavStr\}\})')

SL_FA_RE = re.compile(r'(\{\{zvezdica\}\})')

ET_FA_RE = re.compile(r'(\{\{eeskujulikud artiklid\}\})')

MS_FA_RE = re.compile(r'(\{\{rencana pilihan\}\})')

EU_FA_RE = re.compile(r'(\{\{[Nn]abarmendutako artikulua\}\})')

GL_FA_RE = re.compile(r'(\{\{[Aa]rtigo de calidade\}\})')

IS_FA_RE = re.compile(u'(\{\{Úrvalsgrein\}\})')
IS_GA_RE = re.compile(u'(\{\{Gæðagrein\}\})')

FO_FA_RE = re.compile(u'(\{\{Mánaðargrein\}\})')

# TODO: Check validity of the following template
SIMPLE_FA_RE = re.compile(r'(\{\{[Ff]eatured [Aa]rticle\}\})')

# Dictionary of supported languages for Featured Article detection
FA_RE = {'dewiki': DE_FA_RE, 'enwiki': EN_FA_RE, 'eswiki': ES_FA_RE,
         'frwiki': FR_FA_RE, 'itwiki': IT_FA_RE, 'jawiki': JA_FA_RE,
         'nlwiki': NL_FA_RE, 'plwiki': PL_FA_RE, 'ptwiki': PT_FA_RE,
         'ruwiki': RU_FA_RE, 'zhwiki': ZH_FA_RE, 'svwiki': SV_FA_RE,
         'trwiki': TR_FA_RE, 'fiwiki': FI_FA_RE, 'cswiki': CS_FA_RE,
         'idwiki': ID_FA_RE, 'thwiki': TH_FA_RE, 'arwiki': AR_FA_RE,
         'kowiki': KO_FA_RE, 'hewiki': HE_FA_RE, 'nowiki': NO_FA_RE,
         'huwiki': HU_FA_RE, 'viwiki': VI_FA_RE, 'ukwiki': UK_FA_RE,
         'dawiki': DA_FA_RE, 'fawiki': FA_FA_RE, 'rowiki': RO_FA_RE,
         'cawiki': CA_FA_RE, 'bgwiki': BG_FA_RE, 'hrwiki': HR_FA_RE,
         'elwiki': EL_FA_RE, 'skwiki': SK_FA_RE, 'srwiki': SR_FA_RE,
         'ltwiki': LT_FA_RE, 'slwiki': SL_FA_RE, 'etwiki': ET_FA_RE,
         'mswiki': MS_FA_RE, 'euwiki': EU_FA_RE, 'glwiki': GL_FA_RE,
         'simplewiki': SIMPLE_FA_RE,
         'iswiki': IS_FA_RE, 'fowiki': FO_FA_RE, 'scowiki': None,
         'furwiki': None
         }

# Dictionary of supported languages for Featured List detection
FLIST_RE = {'dewiki': DE_FLIST_RE, 'enwiki': EN_FLIST_RE, 'eswiki': None,
            'frwiki': None, 'itwiki': None, 'jawiki': None,
            'nlwiki': None, 'plwiki': None, 'ptwiki': None,
            'ruwiki': None, 'zhwiki': None, 'svwiki': None, 'trwiki': None,
            'fiwiki': None, 'cswiki': None, 'idwiki': None, 'thwiki': None,
            'arwiki': None, 'kowiki': None, 'hewiki': None, 'nowiki': None,
            'huwiki': None, 'viwiki': None, 'ukwiki': None, 'dawiki': None,
            'fawiki': None, 'rowiki': None, 'cawiki': None, 'bgwiki': None,
            'hrwiki': None, 'elwiki': None, 'skwiki': None, 'srwiki': None,
            'ltwiki': None, 'slwiki': None, 'etwiki': None, 'mswiki': None,
            'euwiki': None, 'glwiki': None, 'simplewiki': None,
            'iswiki': None, 'fowiki': None, 'scowiki': None, 'furwiki': None
            }

# Dictionary of supported languages for Good Article detection
GA_RE = {'dewiki': DE_GA_RE, 'enwiki': EN_GA_RE, 'eswiki': ES_GA_RE,
         'frwiki': FR_GA_RE, 'itwiki': IT_GA_RE, 'jawiki': JA_GA_RE,
         'nlwiki': None, 'plwiki': PL_GA_RE, 'ptwiki': PT_GA_RE,
         'ruwiki': RU_GA_RE, 'zhwiki': None, 'svwiki': None, 'trwiki': None,
         'fiwiki': None, 'cswiki': None, 'idwiki': None, 'thwiki': None,
         'arwiki': None, 'kowiki': None, 'hewiki': None, 'nowiki': None,
         'huwiki': None, 'viwiki': None, 'ukwiki': None, 'dawiki': DA_GA_RE,
         'fawiki': None, 'rowiki': None, 'cawiki': None, 'bgwiki': None,
         'hrwiki': None, 'elwiki': None, 'skwiki': None, 'srwiki': None,
         'ltwiki': None, 'slwiki': None, 'etwiki': None, 'mswiki': None,
         'euwiki': None, 'glwiki': None, 'simplewiki': None,
         'iswiki': IS_GA_RE, 'fowiki': None, 'scowiki': None, 'furwiki': None
         }
