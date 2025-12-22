"""ストレージサービス抽象化レイヤー（後方互換性ラッパー）

責任分離型設計: labcode-simはWrite専用（StorageWriter）
storage_writer.pyの機能を再エクスポート。

使用例:
    from storage_service import StorageService, get_storage

    storage = get_storage()
    storage.save("runs/1/log.txt", b"Hello World", content_type="text/plain")
    storage.save_json("runs/1/metadata.json", {"key": "value"})
"""

# storage_writerから再エクスポート（後方互換性維持）
from storage_writer import (
    StorageWriter,
    StorageWriter as StorageService,  # 後方互換性エイリアス
    get_storage_writer,
    get_storage_writer as get_storage,  # 後方互換性エイリアス
)

__all__ = [
    'StorageWriter',
    'StorageService',
    'get_storage_writer',
    'get_storage',
]
