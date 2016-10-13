#!/usr/bin/env python

import sys
import datetime
import argparse
import urllib
import sqlite3 as sqlite
from bda.awstatsparser.parser import ParsedStatistics


def db_connect(db_path):
    """ connect to a sqlite db located in db_path
        usally db_path contains the name of awstat site
        if db is not exists, it will be created
        return the connection
    """
    conn = sqlite.connect(db_path)
    conn.row_factory = sqlite.Row

    try:
       conn.execute("""
CREATE TABLE statistics(
    id  INTEGER  PRIMARY KEY,
    url TEXT NOT NULL,
    entry TEXT NOT NULL,
    bandwidth TEXT NOT NULL,
    exit TEXT NOT NULL,
    pages TEXT NOT NULL,
    last_changes TEXT NOT NULL
);
"""
       )
    except:
       pass
    return conn


def awstat_cache(conn, awstat_site, awstat_location, month=None):
    """ Parse the awstat_location to get data for awstat_host
        if month is None, it create a new cache db
        otherwise update the existing db with new month data
        - awstat_site is the host registered in awstat, ex. www.rmportal.net
        - awstat_location is the path of awstat folder, ex. /var/www/awstat
        - month is the month you want parse, ex. 072016
    """
    parser = ParsedStatistics(site=awstat_site, location=awstat_location)
    toanalize_keys = parser.available
    # sorting keys 012007 for 200701
    toanalize_keys.sort(key=lambda x: (x[-4:], x[:2]))
    
    if month:
       if month not in toanalize_keys:
          print "%s not in awstats" % month
          return

       toanalize_keys = [month]

    for key in toanalize_keys:
       sider = parser[key]['SIDER']
       print key
       for url, stat in sider.items():
          quoted_url = urllib.quote(url)
          db_stat = conn.execute('SELECT * FROM statistics WHERE url="%s"' % quoted_url)
          rows_stat = db_stat.fetchall()
          current_changes = "%s|%s|%s|%s|%s" % (key, stat['entry'], stat['bandwidth'], stat['exit'], stat['pages'])
          if len(rows_stat) > 0:
             row_stat = rows_stat[0]
             last_key, last_entry, last_bandwidth, last_exit, last_pages = row_stat['last_changes'].split('|')
             if key!=last_key:
                entry = stat['entry'] + row_stat['entry']
                bandwidth = stat['bandwidth'] + row_stat['bandwidth']
                exit = stat['exit'] + row_stat['exit']
                pages = stat['pages'] + row_stat['pages']
             else:
                entry = (int(stat['entry']) - int(str(last_entry))) + int(row_stat['entry'])
                bandwidth = (int(stat['bandwidth']) - int(str(last_bandwidth))) + int(row_stat['bandwidth'])
                exit = (int(stat['exit']) - int(str(last_exit))) + int(row_stat['exit'])
                pages = (int(stat['pages']) - int(last_pages)) + int(row_stat['pages'])

             conn.execute('UPDATE statistics set entry = "%s", bandwidth = "%s", exit = "%s", pages = "%s", last_changes = "%s" where url="%s"' % (
                 entry, bandwidth, exit, pages, current_changes, url
             ))
          else:
             conn.execute(
                 """
INSERT INTO statistics (url, entry, bandwidth, exit, pages, last_changes)
   VALUES (?, ?, ?, ?, ?, ?)
                 """ , (quoted_url, stat['entry'], stat['bandwidth'], stat['exit'], stat['pages'], current_changes))
       
          conn.commit()


def start_parsing(db_path, awstat_site, awstat_location, month):
    """
    """

    conn = db_connect(db_path)
    awstat_cache(conn, awstat_site, awstat_location, month)
    conn.close()

    return


def main():
    parser = argparse.ArgumentParser(description="""
Build a sqlite db cache to get statistics from awstats in multiyear
"""
    )
    parser.add_argument('-db', '--db-path', help='path of sqlite db', required=True)
    parser.add_argument('-l', '--awstats-location', help='path of awstats log folder', required=True)
    parser.add_argument('-s', '--awstats-site', help='site in awstats to analyze',required=True)
    parser.add_argument('-m', '--month', help='month to analyze. ex. 062016',required=False)
    parser.add_argument('-r', '--recreate', help='True to recreate the db', required=False)
    args = parser.parse_args()

    month = args.month

    if not args.recreate and not args.month:
       yestarday = datetime.datetime.today() - datetime.timedelta(1)
       month = yestarday.strftime("%m%Y")

    start_parsing(args.db_path, args.awstats_site, args.awstats_location, month)


if __name__ == "__main__":
    sys.exit(main())

