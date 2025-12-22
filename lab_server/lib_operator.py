"""オペレータークラス

実験機器（マシン）のシミュレーションを行うクラス。
StorageServiceを使用してメタデータを保存する。
"""

from typing import List
from time import sleep
from random import uniform
import json

from storage_service import get_storage


class Operator:
    """実験機器（オペレーター）の基底クラス"""

    id: str
    type: str
    task_input: List[str]
    task_output: List[str]
    storage_address: str  # 相対パス形式（例: runs/1/operators/tecan_infinite_200_pro/）

    def __init__(self, id: str, type: str, manipulate_list: List[dict], storage_address: str):
        """
        オペレーターを初期化

        Args:
            id: オペレーターID（例: tecan_infinite_200_pro）
            type: オペレータータイプ
            manipulate_list: マニピュレート設定リスト
            storage_address: 親のstorage_address（例: runs/1/）
        """
        self.id = id
        self.type = type
        # storage_addressを統一形式で設定: runs/{run_id}/operators/{operator_id}/
        self.storage_address = f"{storage_address}operators/{id}/"

        # 該当するmanipulateが1つしかないことを想定している。
        manipulate = [m for m in manipulate_list if m['name'] == type][0]
        if manipulate.get('input'):
            self.task_input = [input['id'] for input in manipulate['input']]
        if manipulate.get('output'):
            self.task_output = [output['id'] for output in manipulate['output']]

    def run(self):
        """
        オペレーターを実行し、メタデータを保存する

        Returns:
            str: 実行結果
        """
        storage = get_storage()

        # ランダムな時間だけ待つ（シミュレーション）
        running_time = uniform(1, 3)
        sleep(running_time)

        # メタデータを生成
        metadata = {
            "operator_id": self.id,
            "type": self.type,
            "status": "completed",
            "metadata": "sample_metadata"
        }

        # StorageServiceを使用してメタデータを保存
        metadata_path = f"{self.storage_address}metadata.json"
        storage.save_json(metadata_path, metadata)
        print(f"Operator metadata saved: {metadata_path}")

        return "done"
