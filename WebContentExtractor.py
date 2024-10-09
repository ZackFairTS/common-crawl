#coding:utf-8
from goose3 import Goose
from goose3.text import StopWordsChinese
import re

class WebContentExtractor:
    def __init__(self,lang='zh-cn'):
        self.lang=lang
        if lang and lang=='zh-cn':
            self.extractor = Goose({'stopwords_class': StopWordsChinese})
        else:
            self.extractor = Goose()
    
    def extract(self,raw_html,charset='utf-8'):
        if not raw_html:
            return None
        
        
        html_content = str(raw_html, charset.lower(), errors="ignore")
        article = self.extractor.extract(raw_html=html_content)
        if article.cleaned_text:
            # if  self.lang :
            #     detected_lang=detect(article.cleaned_text[:20])
            #     if detected_lang!=self.lang:
            #         return None
            
            content =self.post_process(article.cleaned_text)
            return (article.title,content)
        self.extractor.close()
        return None
    
    def post_process(self,content):
        content=content.replace('\r\n','\n')
        content = re.sub(r'\n+','\n',content)
        return content