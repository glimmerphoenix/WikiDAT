# -*- coding: utf-8 -*-
######
# Aaron Halfaker and Felipe Ortega
######

from lxml import etree
import MySQLdb, hashlib, sys, codecs, re, os, subprocess, time
import warnings

class Parser(object):
    
    EXTENSIONS = {
    'xml': "cat",
    'bz2': "bzcat",
    '7z':  "7za e -so 2>/dev/null",
    'lzma':"lzcat"
    }
    """
    A map from file extension to the command to run to extract the 
    data to standard out.
    """

    EXT_RE = re.compile(r'\.([^\.]+)$')
    """
    A regular expression for extracting the final extension of a file.
    """
    
    ## List of regular expressions for Featured Article detection
    DE_FA_RE = re.compile(r'(\{\{[Ee]xzellent\|.*\|[0-9]*\}\})')
    DE_FLIST_RE = re.compile(r'\{\{[Ii]nformativ\}\}')
    
    EN_FA_RE = re.compile(r'(\{\{[Ff]eatured [Aa]rticle\}\})')
    EN_FLIST_RE = re.compile(r'(\{\{[Ff]eatured [Ll]ist\}\})')
    
    ES_FA_RE = re.compile(r'(\{\{[Aa]rt.*culo [Dd]estacado\}\})')
    
    FR_FA_RE = re.compile(r'(\{\{Article de qualit.*\|.*\}\})')

    IT_FA_RE = re.compile(r'(\{\{[Vv]etrina\|.*\}\})')

    JA_FA_RE = re.compile(r'(\{\{[Ff]eatured.*[Aa]rticle\}\})')

    NL_FA_RE = re.compile(r'(\{\{[Ee]talage\}\})') 

    PL_FA_RE = re.compile(r'(\{\{[Mm]edal\}\})')

    PT_FA_RE = re.compile(r'(\{\{[Aa]rtigo [Dd]estacado\}\})')

    # Possible change, eliminate | and leave .* only before '}}'
    RU_FA_RE = re.compile(ur'(\{\{Избранная статья\|.*\}\})')
    
    ZH_FA_RE = re.compile(r'(\{\{[Ff]eatured.*[Aa]rticle\}\})')
    
    SV_FA_RE = re.compile(r'(\{\{[Uu]tm.*rkt\}\})')
    
    TR_FA_RE = re.compile(ur'(\{\{[Ss]eçkin [Mm]adde\}\})')
    
    FI_FA_RE = re.compile(r'(\{\{[Ss]uositeltu\}\})')
    
    CS_FA_RE = re.compile(ur'(\{\{[Nn]ejlepší článek\}\})')
    
    ID_FA_RE = re.compile(r'(\{\{[Ff]eatured.*[Aa]rticle\}\})')
    
    TH_FA_RE = re.compile(ur'(\{\{บทความคัดสรร\}\})')
    
    AR_FA_RE = re.compile(ur'(\{\{مقالة مختارة\}\})')
    
    KO_FA_RE = re.compile(ur'(\{\{알찬 글 딱지\}\})')
    
    HE_FA_RE = re.compile(ur'(\{\{ערך מומלץ\}\})')
    
    NO_FA_RE = re.compile(r'(\{\{[Uu]tmerket\}\})')
    
    HU_FA_RE = re.compile(r'(\{\{[Kk]iemelt.*\}\})')
    
    VI_FA_RE = re.compile(ur'(\{\{[Ss]ao chọn lọc.*\}\})')
    
    UK_FA_RE = re.compile(ur'(\{\{медаль.*\}\})')
    
    DA_FA_RE = re.compile(r'(\{\{[Ff]remragende.*\}\})')
    
    # Two possible templates!!
    FA_FA_RE = re.compile(ur'(\{\{مقاله برگزیده\}\})|(\{\{نوشتار برگزیده\}\})')
    
    RO_FA_RE = re.compile(r'(\{\{[Aa]rticol de [Cc]alitate\}\})')
    
    # Two possible templates!!
    CA_FA_RE = re.compile(r'(\{\{[Aa]rticle de [Qq]ualitat\}\})|(\{\{1000\+AdQ.*\}\})')
    
    BG_FA_RE = re.compile(ur'(\{\{Избрана статия.*\}\})')
    
    HR_FA_RE = re.compile(ur'(\{\{[Ii]zdvojeni članak.*\}\})')
    
    EL_FA_RE = re.compile(ur'(\{\{[Αα]ξιόλογο άρθρο\}\})')
    
    SK_FA_RE = re.compile(ur'(\{\{[Pp]erfektný článok\}\})')
    
    SR_FA_RE = re.compile(ur'(\{\{изабрани\}\})')
    
    LT_FA_RE = re.compile(r'(\{\{SavStr\}\})')
    
    SL_FA_RE = re.compile(r'(\{\{zvezdica\}\})')
    
    ET_FA_RE = re.compile(r'(\{\{eeskujulikud artiklid\}\})')
    
    MS_FA_RE = re.compile(r'(\{\{rencana pilihan\}\})')
    
    EU_FA_RE = re.compile(r'(\{\{[Nn]abarmendutako artikulua\}\})')
    
    GL_FA_RE = re.compile(r'(\{\{[Aa]rtigo de calidade\}\})')
    
    IS_FA_RE = re.compile(ur'\{\{Úrvalsgreinar\}\}')
    
    FO_FA_RE = re.compile(ur'\{\{Mánaðargrein\}\}')
    
    # TODO: Check this one
    SIMPLE_FA_RE = re.compile(r'(\{\{[Ff]eatured [Aa]rticle\}\})')
    
    ## List of regexps for Good Article detection
    DA_GA_RE = re.compile(r'\{\{God\}\}')
    
    IS_GA_RE = re.compile(ur'\{\{Gæðagrein\}\}')
    
    
    # List of supported languages for Featured Article detection
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
            'iswiki': IS_FA_RE, 'fowiki': FO_FA_RE
            }
    
    # List of supported languages for Featured List detection        
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
                'iswiki': None, 'fowiki': None
                }
             
    # List of supported languages for Good Article detection   
    GA_RE = {'dewiki': None, 'enwiki': None, 'eswiki': None,
                'frwiki': None, 'itwiki': None, 'jawiki': None,
                'nlwiki': None, 'plwiki': None, 'ptwiki': None,
                'ruwiki': None, 'zhwiki': None, 'svwiki': None, 'trwiki': None,
                'fiwiki': None, 'cswiki': None, 'idwiki': None, 'thwiki': None,
                'arwiki': None, 'kowiki': None, 'hewiki': None, 'nowiki': None,
                'huwiki': None, 'viwiki': None, 'ukwiki': None, 'dawiki': DA_GA_RE,
                'fawiki': None, 'rowiki': None, 'cawiki': None, 'bgwiki': None,
                'hrwiki': None, 'elwiki': None, 'skwiki': None, 'srwiki': None,
                'ltwiki': None, 'slwiki': None, 'etwiki': None, 'mswiki': None,
                'euwiki': None, 'glwiki': None, 'simplewiki': None,
                'iswiki': IS_GA_RE, 'fowiki': None
            }
    
    def __init__(self, cursor, lang, log_file):
        # DB connection
        self.db = cursor
        # Wikipedia Language ('dewiki', 'enwiki', 'eswiki', 'frwiki', 'itwiki',
        # 'jawiki', 'nlwiki', 'plwiki', 'ptwiki', 'ruwiki')
        self.lang = lang.lower()
        # Name of the log file
        self.log_file = log_file
        
        if (self.lang in Parser.FA_RE) and (self.lang in Parser.FLIST_RE):
            self.fa_pat = Parser.FA_RE[self.lang]
            self.flist_pat = Parser.FLIST_RE[self.lang]
            
        if (self.lang in Parser.GA_RE):
            self.ga_pat = Parser.GA_RE[self.lang]
            
        else:
            raise RuntimeError('Unsupported language ' + self.lang)
        
        # Contributor values
        self.contrib_dict = None
        # Page values
        self.page_dict = None
        # Revision values
        self.rev_dict = None
        # Stores id of parent revision
        self.rev_parent_id = None
        
        # Cache of user ids; table people contains unique user ids
        self.users_cache = {'-1':'NA', '0':'Anonymous'}
        # Cahce of IPs:
        # self.ips_cache = {}
        # self.users_list = [-1,0]
        
        # Query strings to inster data in DB
        self.ns_insert = '' # Namespaces
        self.user_insert = '' # People (user ids and login)
        self.page_insert = '' # Pages
        self.rev_insert = '' # Revisions
        self.rev_hash = '' # Revision text hash
        
        # Counters to send insert queries to DB
        self.page_insert_rows = 0
        self.rev_insert_rows = 0
        
        # Stores SHA-256 hash of revision text
        self.text_hash = hashlib.sha256()
        
        # Performance statistics
        self.revisions = 0
        self.pages = 0
        
    def parse(self, path):
        new_user = False # Flag to insert new users in people table
        
        self.in_stream = self._open_dump(self._check_dump(path))
        
        for event, elem in etree.iterparse(self.in_stream):
            
            # Drop tag namespace
            tag = elem.tag.split('}')[1]
            
            if tag == 'namespaces':
                self.ns_dict = {int(c.attrib.get('key')):c.text for c in elem}
                self.ns_dict[0] = ''
                
                ns_list = ''
                for ns in self.ns_dict.iteritems():
                    ns_list = "".join([ns_list, '(', str(ns[0]), ',',
                                        "'", ns[1], "'),"])
                ns_list = ns_list[:-1]
                
                self.ns_insert = "".join(["INSERT INTO namespaces VALUES",
                                          ns_list])
                
                #print self.ns_insert
                # Write self.ns_insert to DB
                self.send_query(self.db, self.ns_insert, 5, self.log_file)
            
            if tag == 'contributor':
                # Build dict {tag:text} for contributor info
                self.contrib_dict = {x.tag.split('}')[1]:x.text for x in elem}
                
            elif tag == 'revision':
                self.revisions += 1
                # Starting new page
                if self.page_dict is None:
                    page = elem.getparent()
                    # Build dict {tag:text} for all children of page
                    # above first revision tag
                    self.page_dict = {x.tag.split('}')[1]:x.text for x in page}
                
                # Build dict {tag:text} for all children of revision
                self.rev_dict = {x.tag.split('}')[1]:x.text for x in elem}
                
                # ### TEXT-RELATED OPERATIONS ###
                # Calculate SHA-256 hash, length of revision text and check
                # for REDIRECT
                # TODO: Inspect why there are pages without text
                if self.rev_dict['text'] is not None:
                    text = self.rev_dict['text'].encode('utf-8')
                    self.text_hash.update(text)
                    
                    self.rev_dict['len_text'] = str(len(text))
                    
                    if self.rev_dict['text'][0:9].upper() == '#REDIRECT':
                        self.rev_dict['redirect'] = '1'
                    else:
                        self.rev_dict['redirect'] = '0'
                    
                    # FA and FList detection
                    # TODO: Add support FA detection in more languages
                    # Currently the top-10 languages are supported
                    
                    mfa = self.fa_pat.search(self.rev_dict['text'])
                    # Case of standard language, one type of FA template
                    if (mfa is not None and len(mfa.groups()) == 1):
                        self.rev_dict['is_fa'] = '1'
                    # Case of fawiki or cawiki, 2 types of FA templates
                    # Possible matches: (A, None) or (None, B)
                    elif (mfa is not None and len(mfa.groups()) == 2 and\
                        (mfa.groups()[1] is None or mfa.groups()[0] is None) ):
                        self.rev_dict['is_fa'] = '1'
                    else:
                        self.rev_dict['is_fa'] = '0'
                    
                    # Check if FLIST is supported in this language, detect if so
                    if self.flist_pat is not None:
                        mflist = self.flist_pat.search(self.rev_dict['text'])
                        if mflist is not None and len(mflist.groups()) == 1:
                            self.rev_dict['is_flist'] = '1'
                        else:
                            self.rev_dict['is_flist'] = '0'
                    else:
                        self.rev_dict['is_flist'] = '0'
                        
                    # Check if GA is supported in this language, detect if so
                    if self.ga_pat is not None:
                        mga = self.ga_pat.search(self.rev_dict['text'])
                        if mga is not None and len(mga.groups()) == 1:
                            self.rev_dict['is_ga'] = '1'
                        else:
                            self.rev_dict['is_ga'] = '0'
                    else:
                        self.rev_dict['is_ga'] = '0'
                    
                else:
                    self.rev_dict['len_text'] = '0'
                    self.rev_dict['redirect'] = '0'
                    self.rev_dict['is_fa'] = '0'
                    self.rev_dict['is_flist'] = '0'
                    self.rev_dict['is_ga'] = '0'
                    self.text_hash.update('')

                # Build extended insert for revision and revision_hash
                new_rev_insert = "".join(["(", self.rev_dict['id'], ",",
                                        self.page_dict['id'], ","])
                
                new_rev_hash = "".join(["(", self.rev_dict['id'], ",",
                                    self.page_dict['id'], ","])
                
                # Check that revision has a valid contributor
                # and build extended insert for people
                if len(self.contrib_dict) > 0:                   
                    # Anonymous user
                    if 'ip' in self.contrib_dict:
                        #if self.contrib_dict['ip'] not in self.ips_cache and\
                            #self.contrib_dict['ip'] is not None:
                            ## Activate flag to insert new IP info in DB
                            #new_user = True
                            
                            #self.ips_cache[self.contrib_dict['ip']] = None
                            #new_user_insert = "".\
                                            #join(["(0,'",
                                            #self.contrib_dict['ip'],"')"])
                        #else:
                            #new_user = False
                        new_user = False
                        
                        new_rev_insert = "".join([new_rev_insert, "0,"])
                        new_rev_hash = "".join([new_rev_hash, "0,"])
                    # Registered user
                    else:
                        # If this is a new user add info to people table
                        if self.contrib_dict['id'] not in self.users_cache:
                            # Standard case of new username
                            if self.contrib_dict['username'] is not None:
                                # Activate flag to insert new user info in DB
                                new_user = True
                                
                                self.users_cache[self.contrib_dict['id']] = None
                                # self.users_list.append(self.contrib_dict['id'])
                                # Feed info for extended insert to table people
                                new_user_insert = "".\
                                            join(["(",self.contrib_dict['id'],
                                            ",'",
                                            self.contrib_dict['username'].\
                                            replace("\\","\\\\").\
                                            replace("'","\\'").\
                                            replace('"', '\\"'),
                                            "')"])
                            # Handle strange case of new user w/o username
                            else:
                                self.users_cache[self.contrib_dict['id']] = 'NA'
                                new_user = False
                            
                        else:
                            if self.users_cache[self.contrib_dict['id']] == 'NA' and\
                            self.contrib_dict['username'] is not None:
                                new_user = True
                                
                                self.users_cache[self.contrib_dict['id']] = None
                                # self.users_list.append(self.contrib_dict['id'])
                                # Feed info for extended insert to table people
                                new_user_insert = "".\
                                            join(["(",self.contrib_dict['id'],
                                            ",'",
                                            self.contrib_dict['username'].\
                                            replace("\\","\\\\").\
                                            replace("'","\\'").\
                                            replace('"', '\\"'),
                                            "')"])
                            
                            else:
                                new_user = False
                            
                        new_rev_insert = "".join([new_rev_insert,
                                            self.contrib_dict['id'],","])
                        new_rev_hash = "".join([new_rev_hash,
                                            self.contrib_dict['id'],","])
                
                # TODO: Inspect why there are revisions without contributor
                # Mark revision as missing contributor
                else:
                    new_user = False
                    
                    new_rev_insert = "".join([new_rev_insert, "-1, "])
                    
                    new_rev_hash = "".join([new_rev_hash, "-1, "])
                
                # rev_timestamp
                self.rev_dict['timestamp'] = self.rev_dict['timestamp'].\
                                            replace('Z','').replace('T',' ')
                new_rev_insert = "".join([new_rev_insert, "'", 
                                            self.rev_dict['timestamp'], "',"])
                # rev_len
                new_rev_insert = "".join([new_rev_insert, 
                                            self.rev_dict['len_text'], ","])
                # rev_parent_id
                if self.rev_parent_id is not None:
                    new_rev_insert = "".join([new_rev_insert, 
                                                self.rev_parent_id, ","])
                else:
                    new_rev_insert = "".join([new_rev_insert, "NULL,"])
                
                # rev_redirect
                new_rev_insert = "".join([new_rev_insert, 
                                        self.rev_dict['redirect'], ","])
                
                if 'minor' in self.rev_dict:
                    new_rev_insert = "".join([new_rev_insert, "1,"])
                else:
                    new_rev_insert = "".join([new_rev_insert, "0,"])
                    
                # Add is_fa and is_flist fields
                new_rev_insert = "".join([new_rev_insert, 
                                            self.rev_dict['is_fa'], ",",
                                            self.rev_dict['is_flist'], ",",
                                            self.rev_dict['is_ga'], ","])
                
                if 'comment' in self.rev_dict and\
                        self.rev_dict['comment'] is not None:
                    new_rev_insert = "".join([new_rev_insert,'"',
                                        self.rev_dict['comment'].\
                                        replace("\\","\\\\").\
                                        replace("'","\\'").\
                                        replace('"', '\\"'),'")'])
                else:
                    new_rev_insert = "".join([new_rev_insert, "'')"])
                
                # Finish insert for revision_hash
                new_rev_hash = "".join([new_rev_hash,
                                      "'", self.text_hash.hexdigest(), "')"])
                
                # ### INSERT QUERIES BUILDING ###
                # First iteration
                # Always allow at least one row in extended inserts
                if self.rev_insert_rows == 0:
                    # Case of people
                    # First values are always 0: anonymous and -1:missing
                    self.user_insert = "".join(["INSERT INTO people ",
                                                    "VALUES(-1, 'NA'),",
                                                    "(0, 'Anonymous')"])
                    if new_user:
                        self.user_insert = "".join([self.user_insert, ",",
                                                    new_user_insert])
                    # Case of revision
                    self.rev_insert = "".join(["INSERT INTO revision ",
                                                "VALUES", new_rev_insert])
                    
                    # Case of revision_hash 
                    self.rev_hash = "".join(["INSERT INTO revision_hash VALUES",
                                            new_rev_hash])
                    # Update general rows counter
                    self.rev_insert_rows += 1
                
                # Extended inserts not full yet
                #Append new row to self.rev_insert
                elif self.rev_insert_rows <= 100:
                    # Case of people
                    if new_user:
                        if len(self.user_insert) > 0:
                            self.user_insert = "".join([self.user_insert, ",",
                                                        new_user_insert])
                        else:
                            self.user_insert = "".join(["INSERT INTO people ",
                                                    "VALUES", new_user_insert])
                    
                    # Case of revision
                    self.rev_insert = "".join([self.rev_insert, ",", 
                                                new_rev_insert])
                    
                    # Case of revision_hash
                    self.rev_hash="".join([self.rev_hash, ",",
                                           new_rev_hash])
                    # Update general rows counter
                    self.rev_insert_rows += 1
               
                # Flush extended inserts and start over new queries
                else:
                    # Case of people
                    if len(self.user_insert) > 0:
                        self.send_query(self.db, self.user_insert, 5, 
                                        self.log_file)
                        
                        if new_user:
                            self.user_insert = "".join(["INSERT INTO people ",
                                                        "VALUES", 
                                                        new_user_insert])
                        else:
                            self.user_insert = ""
                    else:
                        if new_user:
                            self.user_insert = "".join(["INSERT INTO people ",
                                                        "VALUES", 
                                                        new_user_insert])
                    
                    # Case of revision
                    self.send_query(self.db, self.rev_insert, 5, 
                                    self.log_file)
                    
                    self.rev_insert = "".join(["INSERT INTO revision ",
                                            "VALUES", new_rev_insert])
                    # Case of revision_hash
                    self.send_query(self.db, self.rev_hash, 5, 
                                    self.log_file)
                    
                    self.rev_hash = "".join(["INSERT INTO revision_hash ",
                                              "VALUES", new_rev_hash])
                    # Update general rows counter
                    self.rev_insert_rows = 1
                
                # Save rev_id (rev_parent_id of the next revision item)
                self.rev_parent_id = self.rev_dict['id']
                # Clear up revision and contributor dictionaries
                self.rev_dict = None
                self.contrib_dict = None
                text = None
                # Delete this revision to clear memory
                elem.clear()
                # Also eliminate now-empty references from the root node to
                # <logitem>. Credits to Liza Daly
                # http://www.ibm.com/developerworks/xml/library/x-hiperfparse/#listing1
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
                
                if self.revisions % 1000 == 0:
                    print "%s revisions for %s pages  " % (self.revisions, 
                                                        self.pages) +\
                        time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime())
                
            elif tag == 'page':
                self.pages += 1
                # Build extended insert for page
                new_page_insert = "".join(["(", self.page_dict['id'], ",",
                                            self.page_dict['ns'], ",'",
                                            self.page_dict['title'].\
                                            replace("\\","\\\\").\
                                            replace("'","\\'").\
                                            replace('"', '\\"'), 
                                            "',"])
                if 'restrictions' in self.page_dict:
                    new_page_insert = "".join([new_page_insert,
                                            "'", self.page_dict['restrictions'],
                                            "')"])
                else:
                    new_page_insert = "".join([new_page_insert, "'')"])
                
                # Build page insert
                if self.page_insert_rows == 0:
                    self.page_insert = "".join(["INSERT INTO page ",
                                              "VALUES", new_page_insert])
                    self.page_insert_rows += 1
                    
                elif self.page_insert_rows <= 100:
                    self.page_insert = "".join([self.page_insert, ",",
                                                new_page_insert])
                    # Update rows counter
                    self.page_insert_rows += 1
                else:
                    self.send_query(self.db, self.page_insert, 5, 
                                    self.log_file)
                    
                    self.page_insert = "".join(["INSERT INTO page ",
                                              "VALUES", new_page_insert])
                    # Update rows counter
                    self.page_insert_rows = 1
                
                # Clear page dictionary
                self.page_dict = None
                # Reset rev_parent_id
                self.rev_parent_id = None
                # Delete this page to clear memory
                elem.clear()
                # Also eliminate now-empty references from the root node to
                # <logitem>. Credits to Liza Daly
                # http://www.ibm.com/developerworks/xml/library/x-hiperfparse/#listing1
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
                
                #print("%5s, %4s" % (event, elem.tag))
        
        # Send last extended insert for people, if needed
        if len(self.user_insert) > 0:
            self.send_query(self.db, self.user_insert, 5, self.log_file)
                
        # Send last extended insert for page
        self.send_query(self.db, self.page_insert, 5, self.log_file)
        
        # Send last extended insert for revision
        self.send_query(self.db, self.rev_insert, 5, self.log_file)
        
        # Send last extended insert for revision_hash
        self.send_query(self.db, self.rev_hash, 5, self.log_file)

        return self.pages, self.revisions
        
    def send_query(self, cursor, query, ntimes, log_file):
        """
        Send query to DB. Attempt 'ntimes' consecutive times before giving up
        cursor: cursor to open DB connection
        query: query to be sent to DB
        ntimes: number of sending attempts
        log_file: name of file to log errors
        """
        # TODO: Handle errors properly with logger library
        #chances = 0
        #while chances < ntimes:
        with warnings.catch_warnings():
            # Change filter action to 'error' to raise warnings as if they
            # were exceptions, to record them in the log file
            warnings.simplefilter('ignore', MySQLdb.Warning)
            try:
                cursor.execute(query)
            except (Exception), e:
                self.f = codecs.open(log_file,'a',
                                            'utf-8')
                self.f.write(str(e)+"\n")
                self.f.write("".join([query[0:79],
                            "\n******************************"]))
                self.f.close()
                #chances += 1
            else:
                pass
                #break
  
    def _check_dump(self, path):
        """
        Verifies that a file exists at a given path and that the file has a 
        known extension type.
        
        :Parameters:
            path : `str`
                the path to a dump file
            
        """
        path = os.path.expanduser(path)
        if not os.path.isfile(path):
            raise FileTypeError("Can't find file %s" % path)
        
        match = Parser.EXT_RE.search(path)
        if match == None:
            raise FileTypeError("No extension found for %s." % path)
        elif match.groups()[0] not in Parser.EXTENSIONS:
            raise FileTypeError("File type %r is not supported." % path)
        else:
            return path

    #def _dumpFiles(self, paths):
        #"""
        #Produces a `multiprocessing.Queue` containing path for each value in
        #`paths` to be used by the `Processor`s.
        
        #:Parameters:
            #paths : iterable
                #the paths to add to the processing queue
        #"""
        #q = Queue()
        #for path in paths: q.put(dumpFile(path))
        #return q

    def _open_dump(self, path):
        """
        Turns a path to a dump file into a file-like object of (decompressed)
        XML data.
        
        :Parameters:
            path : `str`
                the path to the dump file to read
        """
        match = Parser.EXT_RE.search(path)
        ext = match.groups()[0]
        p = subprocess.Popen(
            "%s %s" % (Parser.EXTENSIONS[ext], path), 
            shell=True, 
            stdout=subprocess.PIPE,
            stderr=open(os.devnull, "w")
        )
        #sys.stderr.write(p.stdout.read(1000))
        #return False
        return p.stdout

if __name__ == '__main__':
    db_name = sys.argv[1]
    lang = sys.argv[2]
    f = sys.argv[3]
    log_file = sys.argv[4]
    conn = MySQLdb.Connect (host = 'localhost', port = 3306, user = 'root', 
                            passwd='phoenix',db = db_name,
                            charset="utf8", use_unicode=True)
    conn.autocommit(True)
    cursor = conn.cursor()
    
    # Arguments: DB connection, wiki lang and name of log file
    # Currently supported: 'enwiki', 'dewiki'
    parser = Parser(cursor, lang, log_file)
    
    print "Parsing file " +  f
    start = time.clock()
    pages, revisions = parser.parse(f)
    end = time.clock()
    print "Successfully parsed %s revisions " % revisions +\
          "in %s pages within %.6f mins" % (pages, (end - start)/60.)
      
    conn.close()
    