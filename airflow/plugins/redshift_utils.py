"""
Amazon Redshift 연동 유틸리티 모듈
S3에서 Redshift로 데이터 적재, COPY/UNLOAD 등
"""

import psycopg2
from psycopg2.extras import execute_batch
import logging
from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class RedshiftManager:
    """Redshift 데이터베이스 관리자"""
    
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
    
    @contextmanager
    def get_connection(self):
        """
        Redshift 연결을 컨텍스트 매니저로 제공
        자동으로 연결 종료 처리
        """
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=30
            )
            logger.info(f"✅ Redshift 연결 성공: {self.host}/{self.database}")
            yield conn
        except psycopg2.Error as e:
            logger.error(f"❌ Redshift 연결 실패: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.info("🔌 Redshift 연결 종료")
    
    def execute_query(
        self, 
        query: str, 
        params: Optional[Tuple] = None,
        fetch: bool = False
    ) -> Optional[List[Tuple]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params)
                
                if fetch:
                    result = cursor.fetchall()
                    logger.info(f"✅ 쿼리 실행 완료: {len(result)}건 조회")
                    return result
                else:
                    conn.commit()
                    logger.info("✅ 쿼리 실행 완료")
                    return None
                    
            except psycopg2.Error as e:
                conn.rollback()
                logger.error(f"❌ 쿼리 실행 실패: {e}")
                raise
            finally:
                cursor.close()
    
    
    def test_connection(self) -> bool:
        """
        연결 테스트
        
        Returns:
            연결 성공 여부
        """
        try:
            result = self.execute_query("SELECT 1", fetch=True)
            return result[0][0] == 1
        except Exception as e:
            logger.error(f"❌ 연결 테스트 실패: {e}")
            return False