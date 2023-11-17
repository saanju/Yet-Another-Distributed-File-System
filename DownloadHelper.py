import sys
import redis_db as db
import dfs_pb2
import dfs_pb2_grpc
import threading
import concurrent


class DownloadHelper():
    def __init__(self, namenode_instance) -> None:
        self.ip_channel_dict = namenode_instance.GetActiveIpChannelDict()
        self.seq_no_data_dict = {}

    def GetDataFromDataNodes(self, username, filename, meta_data):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as thread_executor:
            executor_list = {thread_executor.submit(
                self.GetDataFromIndividualDataNode, meta, username, filename): meta for meta in meta_data}
            for future in concurrent.futures.as_completed(executor_list):
                try:
                    future.result()
                except Exception as e:
                    print(e)
        print("All Data Retrieval Tasks Complete")

        complete_data = self.CombineDataChunks()
        return complete_data

    def GetDataFromIndividualDataNode(self, meta, username, filename):
        print(f"Task Executed : {threading.current_thread()}")
        # print(meta)
        seq_no, datanode1_ip, datanode2_ip, datanode3_ip = meta[0], str(
            meta[1]), str(meta[2]), str(meta[3])
        # print("In GetData ")
        data = bytes("", "utf-8")
        result = {}

        if (datanode1_ip in self.ip_channel_dict):
            datanode_channel = self.ip_channel_dict[datanode1_ip]
            print(f"Fetching Data from DataNode : {datanode1_ip}")
        elif (datanode2_ip in self.ip_channel_dict):
            datanode_channel = self.ip_channel_dict[datanode2_ip]
            print(f"Fetching Data from DataNode : {datanode2_ip}")
        elif (datanode3_ip in self.ip_channel_dict):
            datanode_channel = self.ip_channel_dict[datanode2_ip]
            print(f"Fetching Data from DataNode : {datanode2_ip}")
        else:
            print(f"Original and Replica Nodes for given containing user file is down")
            return
        datanode_stub = dfs_pb2_grpc.DataTransferServiceStub(datanode_channel)
        responses = datanode_stub.GetFileChunk(dfs_pb2.FileDataChunkInfo(
            username=username, filename=filename, seq_no=seq_no))
        for response in responses:
            data += response.data

        self.seq_no_data_dict[seq_no] = data

    def CombineDataChunks(self):
        complete_file_data = bytes("", "utf-8")
        total_no_chunks = len(self.seq_no_data_dict)

        for i in range(1, total_no_chunks+1):
            complete_file_data += self.seq_no_data_dict[i]

        return complete_file_data
