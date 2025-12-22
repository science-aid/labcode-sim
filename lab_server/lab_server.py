from typing import List, Dict, TypedDict
from datetime import datetime
from fastapi import FastAPI, File, UploadFile, HTTPException
from timestamp import timestamp, timestamp_filename
# from time import sleep
from pathlib import Path
# from random import uniform
# from pathlib import Path
from log import OperationLog, TransportLog
from io import StringIO
from machines import HumanPlateServer, TecanFluent480, OpentronsOT2, TecanInfinite200Pro, HumanStoreLabware
from util import calculate_md5
from lib_operator import Operator
from storage_service import StorageService, get_storage
from time import sleep
from random import uniform
# from lib_operator import Operator
# from .operator import Operator
import yaml
import requests
import random
import os

LOG_SERVER_URL = 'http://log_server:8000'

# ストレージサービスの初期化（シングルトン）
# 環境変数STORAGE_MODEで 's3' または 'local' を指定
storage = get_storage()
print(f"Storage service initialized: mode={storage.mode}")

app = FastAPI()


class Connection(TypedDict):
    input_source: str
    input_content: str
    output_source: str
    output_content: str
    is_data: bool


class OperationsInProtocol(TypedDict):
    id: str
    type: str


class Operation:
    db_id: int
    process_db_id: int
    process_name: str
    name: str
    started_at: str | None
    finished_at: str | None
    status: str
    storage_address: str
    run_storage_address: str  # Runのstorage_address（親パス）
    is_transport: bool
    is_data: bool

    def __init__(
            self,
            process_db_id,
            process_name,
            name,
            storage_address,
            is_transport,
            is_data,
            run_storage_address: str = ""
    ):
        self.process_db_id = process_db_id
        self.process_name = process_name
        self.name = name
        self.started_at = None
        self.finished_at = None
        self.status = "not started"
        self.storage_address = storage_address
        self.run_storage_address = run_storage_address
        self.is_transport = is_transport
        self.is_data = is_data

    def post(self):
        response = requests.post(
            url=f'{LOG_SERVER_URL}/api/operations/',
            data={
                "process_id": self.process_db_id,
                "name": self.name,
                "status": self.status,
                "storage_address": "",
                "is_transport": self.is_transport,
                "is_data": self.is_data
            }
        )

        # Error handling
        if response.status_code != 200:
            raise Exception(
                f"Failed to create operation. "
                f"Status: {response.status_code}, "
                f"Response: {response.text}"
            )

        response_data = response.json()
        if "id" not in response_data:
            raise Exception(
                f"Unexpected response format. "
                f"Expected 'id' field, got: {response_data}"
            )

        self.db_id = response_data["id"]
        # storage_addressを統一形式に変更: runs/{run_id}/operations/{op_id}/
        self.storage_address = f"{self.run_storage_address}operations/{self.db_id}/"
        requests.patch(
            url=f'{LOG_SERVER_URL}/api/operations/{self.db_id}',
            data={
                "attribute": "storage_address",
                "new_value": self.storage_address
            }
        )

    def run(self):
        self.started_at = datetime.now().isoformat()
        self.status = "running"

        requests.patch(
            url=f'{LOG_SERVER_URL}/api/operations/{self.db_id}',
            data={
                "attribute": "started_at",
                "new_value": self.started_at
            }
        )
        requests.patch(
            url=f'{LOG_SERVER_URL}/api/operations/{self.db_id}',
            data={
                "attribute": "status",
                "new_value": self.status
            }
        )

        # シミュレーション: ランダムな実行時間
        running_time = uniform(1, 3)
        sleep(running_time)

        self.finished_at = datetime.now().isoformat()
        self.status = "completed"

        # ログ内容を生成
        log_content = f"Operation {self.name} completed at {self.finished_at}"

        # StorageServiceを使用してログを保存（S3またはローカル）
        log_path = f"{self.storage_address}log.txt"
        storage.save(log_path, log_content.encode('utf-8'), content_type='text/plain')
        print(f"Operation log saved: {log_path}")

        # DBにもログを保存（既存の動作を維持）
        requests.patch(
            url=f'{LOG_SERVER_URL}/api/operations/{self.db_id}',
            data={
                "attribute": "log",
                "new_value": log_content
            }
        )

        requests.patch(
            url=f'{LOG_SERVER_URL}/api/operations/{self.db_id}',
            data={
                "attribute": "finished_at",
                "new_value": self.finished_at
            }
        )
        requests.patch(
            url=f'{LOG_SERVER_URL}/api/operations/{self.db_id}',
            data={
                "attribute": "status",
                "new_value": self.status
            }
        )


class Process:
    db_id: int
    run_id: int
    type: str
    id_in_protocol: str
    storage_address: str
    run_storage_address: str  # Runのstorage_address（親パス）

    def __init__(self, run_id, type, id_in_protocol, storage_address, run_storage_address: str = ""):
        self.run_id = run_id
        self.type = type
        self.id_in_protocol = id_in_protocol
        self.storage_address = storage_address
        self.run_storage_address = run_storage_address

    def post(self):
        response = requests.post(
            url=f'{LOG_SERVER_URL}/api/processes/',
            data={
                "name": self.id_in_protocol,
                "run_id": self.run_id,
                "storage_address": ""
            }
        )

        # Error handling
        if response.status_code != 200:
            raise Exception(
                f"Failed to create process. "
                f"Status: {response.status_code}, "
                f"Response: {response.text}"
            )

        response_data = response.json()
        if "id" not in response_data:
            raise Exception(
                f"Unexpected response format. "
                f"Expected 'id' field, got: {response_data}"
            )

        self.db_id = response_data["id"]
        # storage_addressを統一形式に変更: runs/{run_id}/processes/{process_id}/
        self.storage_address = f"{self.run_storage_address}processes/{self.db_id}/"
        requests.patch(
            url=f'{LOG_SERVER_URL}/api/processes/{self.db_id}',
            data={
                    "attribute": "storage_address",
                    "new_value": self.storage_address
            }
        )

    def operation_mapping(self, machines: List[Operator]) -> Operation:
        if self.id_in_protocol in ["input", "output"]:
            operation = Operation(
                process_db_id=self.db_id,
                process_name=self.id_in_protocol,
                name=self.id_in_protocol,
                storage_address='',  # post()で設定される
                is_transport=False,
                is_data=False,
                run_storage_address=self.run_storage_address
            )
            return operation
        suit_machine = random.choice([machine for machine in machines if machine.type == self.type])
        operation = Operation(
            process_db_id=self.db_id,
            process_name=self.id_in_protocol,
            name=suit_machine.id,
            storage_address='',  # post()で設定される
            is_transport=False,
            is_data=False,
            run_storage_address=self.run_storage_address
        )
        return operation


def connection_to_operation(connection_list: List[Connection], process_list: List[Process], operation_list: List[Operation], run_storage_address: str = ""):
    connections = [{
        "input_source": connection['input'][0],
        "input_content": connection['input'][1],
        "output_source": connection['output'][0],
        "output_content": connection['output'][1],
        "is_data": connection['is_data']
    } for connection in connection_list]
    operation_list_from_connection = []
    edge_list = []
    for connection in connections:
        source_process = [process for process in process_list if process.id_in_protocol == connection['input_source']][0]
        operation = Operation(
            process_db_id=source_process.db_id,
            process_name=source_process.id_in_protocol,
            name=f"{connection['input_source']}_{connection['input_content']}_{connection['output_source']}_{connection['output_content']}",
            storage_address='',  # post()で設定される
            is_transport=True,
            is_data=connection["is_data"],
            run_storage_address=run_storage_address
        )
        operation_name_from = [operation.name for operation in operation_list if operation.process_name == connection['input_source']][0]
        operation_name_to = [operation.name for operation in operation_list if operation.process_name == connection['output_source']][0]
        if connection["is_data"]:
            edge_list.append({"from": operation_name_from, "to": operation_name_to})
        else:
            operation_list_from_connection.append(operation)
            edge_list.append({"from": operation_name_from, "to": operation.name})
            edge_list.append({"from": operation.name, "to": operation_name_to})

    return operation_list_from_connection, edge_list


def create_process_and_operation_and_edge(run_id, protocol_dict, machines, run_storage_address: str = ""):
    processes = protocol_dict["operations"]
    connections = protocol_dict["connections"]

    process_list = [
        Process(
            run_id=run_id,
            type=process["type"],
            id_in_protocol=process["id"],
            storage_address='',  # post()で設定される
            run_storage_address=run_storage_address
        ) for process in processes
    ]

    input_process = Process(
        run_id=run_id,
        type="input",
        id_in_protocol="input",
        storage_address="",
        run_storage_address=run_storage_address
    )
    output_process = Process(
        run_id=run_id,
        type="output",
        id_in_protocol="output",
        storage_address="",
        run_storage_address=run_storage_address
    )

    process_list += [input_process, output_process]
    [process.post() for process in process_list]

    operation_list = [process.operation_mapping(machines=machines) for process in process_list]
    operation_list_from_connection, edge_list = connection_to_operation(connections, process_list, operation_list, run_storage_address)
    operation_list += operation_list_from_connection
    [operation.post() for operation in operation_list]

    edge_db_id_list = []
    for edge in edge_list:
        operation_db_id_from = [operation.db_id for operation in operation_list if operation.name == edge["from"]][0]
        operation_db_id_to = [operation.db_id for operation in operation_list if operation.name == edge["to"]][0]
        edge_db_id_list.append({
            "from": operation_db_id_from,
            "to": operation_db_id_to
        })

    for edge in edge_db_id_list:
        response = requests.post(
            url=f'{LOG_SERVER_URL}/api/edges/',
            data={
                "run_id": run_id,
                "from_id": edge["from"],
                "to_id": edge["to"]
            }
        )
    return operation_list, edge_list


def create_plan(connections: List[Dict[str, str]]) -> List[str]:
    """
    Create a plan from a protocol yaml file using a topological sort
    :param protocol_yaml_path: path to the protocol yaml file
    :return: a list of steps in the order they should
    """
    # make edge_list unique
    # edge_list = list(set(connections))
    edge_list = list(set([(connection['from'], connection['to']) for connection in connections]))
    node_list = list(set([edge[0] for edge in edge_list] + [edge[1] for edge in edge_list]))
    graph = {node: [] for node in node_list}
    for edge in edge_list:
        graph[edge[0]].append(edge[1])

    ret_list = []
    seen = {node: False for node in graph.keys()}

    def dfs(graph: Dict[str, str], node: str):
        seen[node] = True
        for child_node in graph[node]:
            if seen[child_node]:
                continue
            dfs(graph, child_node)
        ret_list.append(node)

    [dfs(graph, node) for node in graph.keys() if not seen[node]]
    ret_list.reverse()
    return ret_list


async def read_uploaded_yaml(yaml_file: UploadFile = File(...)):
    if not yaml_file.filename.endswith(('.yaml', '.yml')):
        raise HTTPException(status_code=400, detail="Uploaded file must be a YAML file")
    try:
        # ファイルの内容を読み取る
        contents = await yaml_file.read()
        contents_str = contents.decode('utf-8')
        # yamlファイルを読み取る
        yaml_data = yaml.safe_load(StringIO(contents_str))
        return yaml_data
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


async def calc_md5_from_file(file: UploadFile = File(...)):
    file_content_bytes = await file.read()  # bytesで取得
    file_content_str = file_content_bytes.decode("utf-8")
    md5 = calculate_md5(file_content_str)
    await file.seek(0)
    return md5


def upload_file(content: bytes, path: str, content_type: str = 'text/plain') -> bool:
    """
    ファイルをストレージにアップロード（StorageService経由）

    Args:
        content: アップロードするファイルの内容（bytes）
        path: ストレージパス（相対パス形式）
        content_type: コンテントタイプ

    Returns:
        成功時True、失敗時False
    """
    result = storage.save(path, content, content_type=content_type)
    if result:
        print(f"File upload success: {path}")
    else:
        print(f"File upload failed: {path}")
    return result


async def upload_yaml_file(file: UploadFile, storage_address: str, filename: str) -> bool:
    """
    UploadFileからYAMLファイルをストレージにアップロード

    Args:
        file: アップロードされたファイル
        storage_address: ストレージのベースパス（例: runs/1/）
        filename: 保存するファイル名

    Returns:
        成功時True、失敗時False
    """
    await file.seek(0)
    content = await file.read()
    await file.seek(0)

    path = f"{storage_address}{filename}"
    return upload_file(content, path, content_type='application/x-yaml')


@app.post("/run_experiment")
async def run_experiment(project_id: int, protocol_name, user_id: int, protocol_yaml: UploadFile = File(...), manipulate_yaml: UploadFile = File(...)):
    protocol_md5 = await calc_md5_from_file(protocol_yaml)
    protocol = await read_uploaded_yaml(protocol_yaml)
    manipulates = await read_uploaded_yaml(manipulate_yaml)

    # 一旦空のstorage_addressでRun作成（run_id取得後に更新）
    response = requests.post(
        url=f'{LOG_SERVER_URL}/api/runs/',
        data={
            "project_id": project_id,
            "file_name": protocol_name,
            "checksum": protocol_md5,
            "user_id": user_id,
            "storage_address": ""  # 一旦空で作成
        }
    )

    # Error handling
    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Failed to create run. Log server response: {response.text}"
        )

    response_data = response.json()
    if "id" not in response_data:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected response format from log server. Expected 'id' field, got: {response_data}"
        )

    run_id = response_data["id"]

    # storage_addressを動的に生成（統一形式: runs/{run_id}/）
    run_storage_address = f"runs/{run_id}/"

    # storage_addressを更新
    update_response = requests.patch(
        url=f'{LOG_SERVER_URL}/api/runs/{run_id}',
        data={"attribute": "storage_address", "new_value": run_storage_address}
    )
    if update_response.status_code != 200:
        print(f"Warning: Failed to update storage_address for run {run_id}: {update_response.text}")

    # storage_modeを更新（現在のストレージモードを記録）
    update_mode_response = requests.patch(
        url=f'{LOG_SERVER_URL}/api/runs/{run_id}',
        data={"attribute": "storage_mode", "new_value": storage.mode}
    )
    if update_mode_response.status_code != 200:
        print(f"Warning: Failed to update storage_mode for run {run_id}: {update_mode_response.text}")

    # プロトコルファイルをストレージにアップロード（StorageService経由）
    print(f"Uploading YAML files to storage: {run_storage_address}")
    await upload_yaml_file(protocol_yaml, run_storage_address, "protocol.yaml")
    await upload_yaml_file(manipulate_yaml, run_storage_address, "manipulate.yaml")

    # マシン初期化（storage_addressは動的生成された値を使用）
    machines = [
        HumanPlateServer("human_plate_server", manipulates, run_storage_address),
        TecanFluent480("tecan_fluent_480", manipulates, run_storage_address),
        OpentronsOT2("opentrons_ot2", manipulates, run_storage_address),
        TecanInfinite200Pro("tecan_infinite_200_pro", manipulates, run_storage_address),
        HumanStoreLabware("human_store_labware", manipulates, run_storage_address),
    ]
    operation_list, edge_list = create_process_and_operation_and_edge(
        run_id=run_id,
        protocol_dict=protocol,
        machines=machines,
        run_storage_address=run_storage_address
    )
    plan = create_plan(edge_list)
    run_start_time = datetime.now().isoformat()
    requests.patch(url=f'{LOG_SERVER_URL}/api/runs/{run_id}', data={"attribute": "started_at", "new_value": run_start_time})
    requests.patch(url=f'{LOG_SERVER_URL}/api/runs/{run_id}', data={"attribute": "status", "new_value": "running"})
    for operation_name in plan:
        operation = [operation for operation in operation_list if operation.name == operation_name][0]
        operation.run()
    run_finish_time = datetime.now().isoformat()
    requests.patch(url=f'{LOG_SERVER_URL}/api/runs/{run_id}', data={"attribute": "finished_at", "new_value": run_finish_time})
    requests.patch(url=f'{LOG_SERVER_URL}/api/runs/{run_id}', data={"attribute": "status", "new_value": "completed"})

    return {"run_id": run_id, "storage_address": run_storage_address, "status": "completed"}
