"""
AWS S3 연동 유틸리티 모듈
"""

import boto3
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Manager:
    """S3 파일 업로드/다운로드 관리자"""
    
    def __init__(
        self, 
        bucket_name: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = 'ap-northeast-2'
    ):

        self.bucket_name = bucket_name
        
        # S3 클라이언트 초기화
        if aws_access_key_id and aws_secret_access_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
        else:
            # 환경변수나 IAM Role 사용
            self.s3_client = boto3.client('s3', region_name=region_name)
    