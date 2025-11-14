"""
영화진흥위원회(KOBIS) Open API 연동 모듈
"""

import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class KobisAPI:
    """KOBIS API 클라이언트"""
    
    BASE_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
    
    @staticmethod
    def format_date(date_obj: datetime) -> str:
        # datetime 객체를 YYYYMMDD 형식으로 변환
        return date_obj.strftime('%Y%m%d')
    
    @staticmethod
    def get_yesterday() -> str:
        # 어제 날짜를 YYYYMMDD 형식으로 반환
        yesterday = datetime.now() - timedelta(days=1)
        return KobisAPI.format_date(yesterday)