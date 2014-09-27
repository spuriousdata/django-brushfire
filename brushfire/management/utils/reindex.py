#!/usr/bin/env python

import sys
import os
import json
import logging
from time import sleep
from requests import Request, Session, get

rows, cols = os.popen('stty size', 'r').read().split()
cols = int(cols)
del rows

def main():
    logging.basicConfig(level=logging.DEBUG)
    reindex('http://localhost:8080/solr', '/dataimport', 'collection2', 'collection1')
    
def url(*parts):
    def stripslashes(x):
        if x.startswith("/"):
            return stripslashes(x[1:])
        if x.endswith("/"):
            return stripslashes(x[:-1])
        return x
    return "/".join([stripslashes(x) for x in parts])

def progbar_update(n, stream):
    start = " %2d%% [" % (n * 100)
    end =  "]\r"
    blocks = (cols - (len(start) + len(end) - 1))
    mid = "=" * (int(blocks * n) - 1)
    stream.write(start + mid + ">" + " " * (blocks - (len(mid)+1)) + end)
    stream.flush()

def reindex(host, handler, core, swap_core, core_admin='admin/cores', output_stream=sys.stderr):
    try:
        base_url = url(host, core, handler)
        index_req = Request('GET', base_url, params={'wt': 'json', 'command': 'full-import', 'clean':'true'}).prepare()
        test_req = Request('GET', base_url, params={'wt': 'json'}).prepare()
        optimize_req = Request('GET', base_url, params={'wt': 'json', 'optimize': 'true'}).prepare()
        
        numdocs_estimate = 1
        if swap_core:
            swap_req = Request('GET', url(host, core_admin), params={
                    'wt': 'json', 
                    'action': 'swap', 
                    'core': core, 
                    'other': swap_core}
            ).prepare()
            try:
                # use "other" core if we're going to swap
                numdocs_estimate = get(url(host, swap_core, 'select'), 
                        params={'wt': 'json', 'rows': '0', 'q': '*:*'}).json()['response']['numFound']
            except:
                pass
        else:
            try:
                # use index core otherwise
                numdocs_estimate = get(url(host, core, 'select'), 
                        params={'wt': 'json', 'rows': '0', 'q': '*:*'}).json()['response']['numFound']
            except:
                pass
            
        # start index process
        logging.info("Starting search index")
        try:
            resp = Session().send(index_req)
        except Exception as e:
            logging.exception(e)
            logging.debug(resp.url)
        if resp.status_code != 200:
            raise Exception("%s\n\nIndexing Failed!, response code: (%d) with body:\n%s" % (resp.url, resp.status_code, resp.text))

        # poll every minute until Indexing Complete or error
        test_session = Session()
        while True:
            sleep(60)
            resp = test_session.send(test_req)
            logging.debug("Testing DIH for completion...")
            if resp.status_code != 200:
                logging.warning("Solr DIH failed!, response code: (%d) with body:\n%s" % (resp.status_code, resp.text))
                continue
            
            data = resp.json()
            
            if data['status'] == 'busy':
                done = int(data['statusMessages']['Total Documents Processed'])
                if done > 0 and done < numdocs_estimate:
                    progbar_update(float(done) / numdocs_estimate, output_stream) 
                elif done > 0:
                    print >> output_stream, "%d docs completed" % done
                continue

            if data['statusMessages'][''].startswith('Indexing completed'):
                logging.debug("...DIH complete")
                break

            if data['status'] not in ('idle', 'busy'):
                logging.debug("Indexing Failed!")
                raise Exception("Indexing Failed!, response code: (%d) with body:\n%s" % (resp.status_code, resp.text))

        # optimize index
        logging.debug("Optimising Index")
        resp = Session().send(optimize_req)
        if resp.status_code != 200:
            raise Exception("Optimize Failed!, response code: (%d) with body:\n%s" % (resp.status_code, resp.text))

        # swap secondary index with main index
        logging.debug("Swapping Cores")
        resp = Session().send(swap_req)
        if resp.status_code != 200:
            raise Exception("Core Swap Failed!, response code: (%d) with body:\n%s" % (resp.status_code, resp.text))

        logging.info("Search index complete")
    except KeyboardInterrupt:
        from pprint import pformat as pp
        print >> output_stream, "\n" + pp(get(base_url, params={'wt': 'json', 'command': 'abort'}).json())
    except Exception, ex:
        logging.exception(ex)

if __name__ == '__main__':
    main()

