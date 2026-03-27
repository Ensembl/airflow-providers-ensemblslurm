import argparse
from datetime import datetime
from elasticsearch import Elasticsearch
import certifi
import urllib3
import ssl
from elasticsearch.connection import create_ssl_context


class ElasticsearchConnectionManager:
    def __init__(self, host: str, port: int, user: str = "", password: str = "", with_ssl: bool = False):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.ssl = with_ssl
        self.client = None

    def __enter__(self):
        urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)
        ssl_context = create_ssl_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        self.client = Elasticsearch(
            hosts=[{'host': self.host, 'port': self.port, "scheme": "https" if self.ssl else "http"}],
            ssl_context=ssl_context,
            http_auth=(self.user, self.password),
            verify_certs=self.ssl,
            ca_certs=certifi.where() if self.ssl else None
        )
        if not self.client.ping():
            raise RuntimeError(f"Cannot connect to Elasticsearch server: {self.host}:{self.port}")
        return self.client

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.client.transport.close()


def fetch_latest_event_record(es_host, es_port, es_user, es_password, index_name, run_name=None,
                              event_status=["completed"]) -> dict:
    with ElasticsearchConnectionManager(es_host, es_port, es_user, es_password, with_ssl=True) as es:
        query = {
            "bool": {
                "must": [
                    {"term": {"runName.keyword": run_name}},
                ],
                "filter": [
                    {
                        "terms": {
                          "event.keyword": event_status
                        }
                    }
                ]
            }
        }

        search_body = {
            "query": query,
            "sort": [{"utcTime": {"order": "desc"}}],
            "size": 1
        }

        response = es.search(index=index_name, body=search_body)
        if response['hits']['total'] == 0:
            return {}
        return response['hits']['hits'][0]['_source']