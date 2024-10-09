import re

from collections import Counter

from sparkcc import CCSparkJob
import json
from WebContentExtractor import WebContentExtractor
class ExtractCNContentJob(CCSparkJob):
    """ Extract raw html content  in Common Crawl WARC files"""
    name = "ExtractCNContent"
    extractor=WebContentExtractor()

    def process_record(self, record,charset):
        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return
        if not self.is_html(record):
            # skip non-HTML or unknown content types
            return
        
        try:
            status = record.http_headers.get_statuscode()
            if status != '200' :
                return 
            
            ip =  record.rec_headers.get_header('WARC-IP-Address')
            html_content = record.content_stream().read()
            
            res = ExtractCNContentJob.extractor.extract(html_content,charset)
            content_type= record.http_headers.get_header('Content-Type')
            url=record.rec_headers.get_header('WARC-Target-URI')
            if res:
                title, content =res

                item={'title':title,
                        'content':content,
                        'url':url,
                        'ip':ip,
                        'content_type':content_type,
                        'status':status,
                        'lang':'zho'}
                yield json.dumps(item,ensure_ascii=False),1
                
        except Exception as e:
            self.get_logger().error('process_record\t'+str(e))
            return
        
        return 0

if __name__ == '__main__':
    job = ExtractCNContentJob()
    job.run()
