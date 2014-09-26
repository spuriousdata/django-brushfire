#!/usr/bin/env python

import httplib2
import json
import logging
from time import sleep

logging.basicConfig(level=logging.DEBUG)

def main():
    reindex()

def reindex():
    try:
        http = httplib2.Http()
        index_url = "http://localhost:8080/solr/collection2/dataimport?command=full-import&clean=true"
        test_url = "http://localhost:8080/solr/collection2/dataimport?wt=json"
        optimize_url = "http://localhost:8080/solr/collection2/update?optimize=true&wt=json"
        swap_url = "http://localhost:8080/solr/admin/cores?wt=json&action=swap&core=collection1&other=collection2"

        # start index process
        logging.info("Starting search index")
        try:
            resp, data = http.request(index_url)
        except Exception as e:
            logging.exception(e)
            logging.debug(index_url)
        if resp['status'] != '200':
            raise Exception("Indexing Failed!, response code: (%d) with body:\n%s" % (resp['status'], data))

        # poll every minute until Indexing Complete or error
        while True:
            sleep(60)
            resp, data = http.request(test_url)
            logging.debug("Testing DIH for completion...")
            if resp['status'] != '200':
                logging.warning("Solr DIH failed!, response code: (%d) with body:\n%s" % (resp['status'], data))
                continue
            resp = json.loads(data)

            if resp['status'] == 'busy':
                logging.debug("...DIH still working")
                continue

            if resp['statusMessages'][''].startswith('Indexing completed'):
                logging.debug("...DIH complete")
                break

            if resp['status'] not in ('idle', 'busy'):
                logging.debug("Indexing Failed!")
                raise Exception("Indexing Failed!, response code: (%d) with body:\n%s" % (resp['status'], data))

        # optimize index
        logging.debug("Optimising Index")
        resp, data = http.request(optimize_url)
        if resp['status'] != '200':
            raise Exception("Optimize Failed!, response code: (%d) with body:\n%s" % (resp['status'], data))

        # swap secondary index with main index
        logging.debug("Swapping Cores")
        resp, data = http.request(swap_url)
        if resp['status'] != '200':
            raise Exception("Core Swap Failed!, response code: (%d) with body:\n%s" % (int(resp['status']), data))

        logging.info("Search index complete")
    except Exception, ex:
        logging.exception(ex)

if __name__ == '__main__':
    main()

