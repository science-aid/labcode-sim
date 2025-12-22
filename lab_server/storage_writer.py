"""軽量ストレージライター（Write専用）

責任分離型設計: labcode-simはWrite操作のみを担当。
Read + 管理機能はlabcode-log-serverが担当。

シンプルなif-else分岐でS3/ローカルを切り替え。
レジストリパターンは不要（2モードのみのため）。

使用例:
    from storage_writer import get_storage_writer

    writer = get_storage_writer()
    writer.save("runs/1/log.txt", b"Hello World")
    writer.save_text("runs/1/config.yaml", "key: value")
    writer.save_json("runs/1/metadata.json", {"key": "value"})
"""

import os
import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class StorageWriter:
    """軽量ストレージライター（Write専用）"""

    def __init__(self):
        """
        環境変数から設定を読み込み、適切なバックエンドを初期化

        環境変数:
        - STORAGE_MODE: 's3' (デフォルト) または 'local'
        - S3_BUCKET_NAME: S3バケット名
        - S3_ENDPOINT_URL: S3エンドポイントURL（オプション）
        - AWS_ACCESS_KEY_ID: AWSアクセスキー
        - AWS_SECRET_ACCESS_KEY: AWSシークレットキー
        - LOCAL_STORAGE_PATH: ローカルストレージパス
        """
        self._mode = os.getenv('STORAGE_MODE', 's3').lower()

        if self._mode == 's3':
            self._init_s3()
        else:
            self._init_local()

        logger.info(f"StorageWriter initialized: mode={self._mode}")

    def _init_s3(self):
        """S3バックエンドを初期化"""
        import boto3

        client_kwargs = {
            'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'region_name': os.getenv('AWS_DEFAULT_REGION', 'ap-northeast-1')
        }

        endpoint_url = os.getenv('S3_ENDPOINT_URL')
        if endpoint_url:
            client_kwargs['endpoint_url'] = endpoint_url

        self._s3_client = boto3.client('s3', **client_kwargs)
        self._bucket_name = os.getenv('S3_BUCKET_NAME', 'labcode-dev-artifacts')

    def _init_local(self):
        """ローカルバックエンドを初期化"""
        base_path = os.getenv('LOCAL_STORAGE_PATH', '/data/storage')
        self._base_path = Path(base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)

    @property
    def mode(self) -> str:
        """現在のストレージモードを返す ('s3' または 'local')"""
        return self._mode

    def save(self, path: str, content: bytes, content_type: str = 'application/octet-stream') -> bool:
        """
        ファイルを保存

        Args:
            path: 保存先パス（相対パス形式、例: 'runs/1/log.txt'）
            content: ファイル内容（バイト列）
            content_type: MIMEタイプ

        Returns:
            bool: 成功時True
        """
        if self._mode == 's3':
            return self._save_s3(path, content, content_type)
        else:
            return self._save_local(path, content)

    def _save_s3(self, path: str, content: bytes, content_type: str) -> bool:
        """S3に保存"""
        try:
            self._s3_client.put_object(
                Bucket=self._bucket_name,
                Key=path,
                Body=content,
                ContentType=content_type
            )
            logger.debug(f"S3 upload success: {path}")
            return True
        except Exception as e:
            logger.error(f"S3 upload failed: {path} - {e}")
            return False

    def _save_local(self, path: str, content: bytes) -> bool:
        """ローカルファイルシステムに保存"""
        try:
            full_path = self._base_path / path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            with open(full_path, 'wb') as f:
                f.write(content)
            logger.debug(f"Local save success: {path}")
            return True
        except Exception as e:
            logger.error(f"Local save failed: {path} - {e}")
            return False

    def save_text(self, path: str, text: str, encoding: str = 'utf-8') -> bool:
        """
        テキストファイルを保存

        Args:
            path: 保存先パス
            text: テキスト内容
            encoding: 文字エンコーディング

        Returns:
            bool: 成功時True
        """
        return self.save(path, text.encode(encoding), content_type='text/plain')

    def save_json(self, path: str, data: dict, indent: int = 2) -> bool:
        """
        JSONファイルを保存

        Args:
            path: 保存先パス
            data: 辞書データ
            indent: インデント幅

        Returns:
            bool: 成功時True
        """
        content = json.dumps(data, ensure_ascii=False, indent=indent)
        return self.save(path, content.encode('utf-8'), content_type='application/json')


# シングルトンインスタンス
_writer_instance: Optional[StorageWriter] = None


def get_storage_writer() -> StorageWriter:
    """StorageWriterのシングルトンインスタンスを取得"""
    global _writer_instance
    if _writer_instance is None:
        _writer_instance = StorageWriter()
    return _writer_instance


# 後方互換性のためのエイリアス
# 既存コードで get_storage() を使用している場合のため
def get_storage() -> StorageWriter:
    """StorageWriterのシングルトンインスタンスを取得（後方互換性エイリアス）"""
    return get_storage_writer()


# StorageServiceエイリアス（後方互換性）
StorageService = StorageWriter
