# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '20 Apr 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, parallel_bulk
import argparse
from getpass import getpass

from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient


class ElasticsearchMigrate:

    def __init__(self, source_index, dest_index, keep_id=False, **es_kwargs):
        self.source = Elasticsearch(['https://jasmin-es1.ceda.ac.uk'])
        self.dest = CEDAElasticsearchClient(**es_kwargs)
        self.source_index = source_index
        self.dest_index = dest_index
        self.keep_id = keep_id

    def gendata(self):

        all_data = scan(self.source, size=8000, index=self.source_index)

        for item in all_data:
            response = {
                '_index': self.dest_index,
                '_source': item['_source'],
            }
            yield response

    def migrate(self):
        for success, info in parallel_bulk(self.dest, self.gendata(), chunk_size=1000, thread_count=8, queue_size=8):
            if not success:
                print(info)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('SOURCE', help='Source index name')
    parser.add_argument('DEST', help='Destination index name')
    parser.add_argument('--keep-id',
                        dest='PRESERVE_ID',
                        help='Preserve id from source index in dest index',
                        action='store_true')

    args = parser.parse_args()

    username = input('Username: ')
    password = getpass()
    prompt = input(f'You are about to migrate from {args.SOURCE} to {args.DEST}.'
                   f' ID preservation is set to {args.PRESERVE_ID}.'
                   f' Is that OK? [Y/n]')

    if not prompt.lower() in ('', 'y'):
        print('Terminating migration. No migration performed')
        exit()

    es_kwargs = {
        'http_auth': (username, password)
    }

    esm = ElasticsearchMigrate(args.SOURCE, args.DEST, args.PRESERVE_ID, **es_kwargs)
    esm.migrate()


if __name__ == '__main__':
    main()
