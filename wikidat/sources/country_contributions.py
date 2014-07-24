'''
Create table of country contributions for each wiki page.

@author: carlosm
'''

import sys
import MySQLdb as db
from pandas import DataFrame
from pandas.io.sql import read_sql
from wikidat.utils.ipresolver import getCountryCode 

if __name__ == '__main__':
    db_name = sys.argv[1]
    db_user = sys.argv[2]
    db_pass = sys.argv[3]

    # Load data for each page
    con = db.connect('localhost',db_user,db_pass,db_name)
    query = 'SELECT rev_user, rev_ip, page_id, page_namespace, name FROM revision ' + \
        'INNER JOIN page ON page.page_id=revision.rev_page ' + \
        'INNER JOIN namespaces ON page.page_namespace=namespaces.code'
    data     = read_sql(query, con)
    con.close()

    # Map IP addresses to country codes
    data['country'] = data['rev_ip'].apply(lambda ip: getCountryCode(ip))
    data['country'] = data['country'].fillna('Unknown')

    # Create country contributions for each page
    con = db.connect('localhost',db_user,db_pass,db_name)
    for page_id, page_revs in data.groupby('page_id'):
        nRevs = len(page_revs)
        cRevs = page_revs.groupby('country').size() / nRevs
    
        # Insert into country_contrib
        # Values: page_id, country (cRevs.keys()), contributions(cRevs.keys())
        df = DataFrame(cRevs, columns=['contribution'] )
        df['page_id'] = page_id
        df.to_sql(con=con, name='country_contrib', if_exists='append', flavor='mysql')
    
    con.close()
