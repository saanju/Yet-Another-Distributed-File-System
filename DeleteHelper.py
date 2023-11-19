import dfs_pb2
import dfs_pb2_grpc
import sys
import concurrent
import threading


class DeleteHelper():
    def __init__(self, namenode_instance):
        self.ip_channel_dict = namenode_instance.GetActiveIpChannelDict()
        self.success = True

    def DeleteDataFromDataNodes(self, username, filename, meta_data):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as thread_executor:
            executor_list = {thread_executor.submit(
                self.DeleteDataChunkFromDataNodes, meta, username, filename): meta for meta in meta_data}
            for thread_future in concurrent.futures.as_completed(executor_list):
                try:
                    thread_future.result()
                except Exception as e:
                    print(e)
        print("All Data Chunk Deletion Tasks Completed")
        return self.success

    def DeleteDataChunkFromDataNodes(self, meta, username, filename):
        print(
            f"Delete Data Chunk Task Executed : {threading.current_thread()}")

        seq_no, datanode1_ip, datanode2_ip, datanode3_ip = meta[0], str(
            meta[1]), str(meta[2]), str(meta[3])
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as thread_executor:
            executor_list = {thread_executor.submit(
                self.DeleteDataChunkFromIndividualDataNodes, datanode_ip, username, filename, seq_no): datanode_ip for datanode_ip in meta[1:]}
            for thread_future in concurrent.futures.as_completed(executor_list):
                try:
                    thread_future.result()
                except Exception as e:
                    print(e)
        print("All Data Chunk Deletion from Individual Data Nodes Completed")

    def DeleteDataChunkFromIndividualDataNodes(self, datanode_ip, username, filename, seq_no):
        print(
            f"Delete Data Chunk from Individual Task Executed : {threading.current_thread()}")

        if (datanode_ip in self.ip_channel_dict):
            datanode_channel = self.ip_channel_dict[datanode_ip]
            print(f"Deleting Data Chunk in DataNode : {datanode_ip}")
        else:
            print(f"Could not delete Data Chunk in DataNode : {datanode_ip}")
            return
        datanode_stub = dfs_pb2_grpc.DataTransferServiceStub(datanode_channel)
        response = datanode_stub.DeleteFileChunk(
            dfs_pb2.FileDataChunkInfo(username=username, filename=filename, seq_no=seq_no))
        self.success = self.success and response.success

        print(
            f"Received from Datanode : {datanode_ip} Message : {response.message}")
