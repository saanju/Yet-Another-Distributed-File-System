from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
import json
from threading import Thread
import dfs_pb2_grpc
import dfs_pb2
import grpc
import time
import os
from dotenv import load_dotenv
import sys
from DownloadHelper import DownloadHelper
from DeleteHelper import DeleteHelper
import redis_db as db
import subprocess

load_dotenv()

SHARD_SIZE = int(os.getenv('SHARD_SIZE'))


class NameNodeServicer(dfs_pb2_grpc.DataTransferServiceServicer):
    def __init__(self, namenode_ip, namenode_port, datanode_ips):
        self.namenode_ip = namenode_ip
        self.namenode_port = namenode_port
        self.datanode_ips = datanode_ips
        self.all_datanode_ips = datanode_ips
        self.ip_channel_dict = {}
        self.InitIPChannelDicts()

    def InitIPChannelDicts(self):
        additional_ips = [
            ip for ip in self.datanode_ips if ip not in self.all_datanode_ips]
        self.all_datanode_ips.extend(additional_ips)
        for ip in self.datanode_ips:
            res = self.IsChannelAlive(ip)
            if (res != False):
                self.ip_channel_dict[ip] = res

    def CheckIPChannelDicts(self):
        additional_ips = [
            ip for ip in self.datanode_ips if ip not in self.all_datanode_ips]
        self.all_datanode_ips.extend(additional_ips)
        for ip in self.datanode_ips:
            res = self.IsChannelAlive(ip)
            if (res != False) and (ip not in self.ip_channel_dict):
                self.ip_channel_dict[ip] = res
            elif (res == False) and (ip in self.ip_channel_dict):
                del self.ip_channel_dict[ip]

    def IsChannelAlive(self, ip):
        try:
            channel = grpc.insecure_channel('{}:80'.format(ip))
            grpc.channel_ready_future(channel).result(timeout=1)
        except grpc.FutureTimeoutError:
            return False
        return channel

    def FileExists(self, username, filename):
        res = db.key_exists(username+"_"+filename)
        print("Is File Present : ", res)
        return res

    def FileSearch(self, request, context):
        username, filename = request.username, request.filename
        if (self.FileExists(username, filename)):
            return dfs_pb2.Ack(success=True, message=f"File {filename} for user {username} exists")
        else:
            return dfs_pb2.Ack(success=False, message=f"File {filename} for user {username} does not exist")

    def FileList(self, request, context):
        username = request.username
        if (db.key_exists(username) == False):
            return dfs_pb2.UserFileList(filenames="", message="User not found")
        else:
            userfiles = db.get_user_files(username)
            return dfs_pb2.UserFileList(filenames=json.dumps(userfiles), message="Retrieved User files successfully")

    def GetThreeLeastUtilizedNodes(self):
        min_val1 = min_val2 = min_val3 = 305.00
        least_loaded_node1 = least_loaded_node2 = least_loaded_node3 = ""
        for ip in self.datanode_ips:
            res_channel = self.IsChannelAlive(ip)
            if (res_channel != False):
                datanode_stub = dfs_pb2_grpc.DataTransferServiceStub(
                    res_channel)
                datanode_stats = datanode_stub.IsDataNodeAlive(dfs_pb2.Empty())
                total = 300.00-(float(datanode_stats.cpu_usage) +
                                float(datanode_stats.disk_space)+float(datanode_stats.used_mem))
                if ((total/3) < min_val1):
                    min_val3 = min_val2
                    min_val2 = min_val1
                    min_val1 = total/3
                    least_loaded_node3 = least_loaded_node2
                    least_loaded_node2 = least_loaded_node1
                    least_loaded_node1 = ip
                elif ((total/3) < min_val2):
                    min_val3 = min_val2
                    min_val2 = total/3
                    least_loaded_node3 = least_loaded_node2
                    least_loaded_node2 = ip
                elif ((total/3) < min_val3):
                    min_val3 = total/3
                    least_loaded_node3 = ip

        if (least_loaded_node1 == ""):
            return -1, -1, -1
        return least_loaded_node1, least_loaded_node2, least_loaded_node3

    def SendDataInStream(self, data_bytes, username, filename, seq_no):
        chunk_size = 4000000
        start, end = 0, chunk_size
        while (True):
            chunk = data_bytes[start:end]
            if (len(chunk) == 0):
                break
            start = end
            end += chunk_size
            yield dfs_pb2.FileDataChunk(username=username, filename=filename, data=chunk, seq_no=seq_no)

    def UploadFile(self, request_iterator, context):
        least_loaded_node1, least_loaded_node2, least_loaded_node3 = self.GetThreeLeastUtilizedNodes()
        if (least_loaded_node1 == -1):
            return dfs_pb2.Ack(success=False, message="No Active DataNode found")

        print(
            f"DataNodes found DN1 : {least_loaded_node1} replica DN2 : {least_loaded_node2} replica DN3 : {least_loaded_node3}")

        datanode1_channel = self.ip_channel_dict[least_loaded_node1]
        datanode1_stub = dfs_pb2_grpc.DataTransferServiceStub(
            datanode1_channel)
        # print("After Stub1")
        if (least_loaded_node2 != "" and least_loaded_node2 in self.ip_channel_dict):
            datanode2_channel = self.ip_channel_dict[least_loaded_node2]
            datanode2_stub = dfs_pb2_grpc.DataTransferServiceStub(
                datanode2_channel)
            if (least_loaded_node3 != "" and least_loaded_node3 in self.ip_channel_dict):
                datanode3_channel = self.ip_channel_dict[least_loaded_node3]
                datanode3_stub = dfs_pb2_grpc.DataTransferServiceStub(
                    datanode3_channel)
            else:
                datanode3_stub = None
        else:
            datanode2_stub = None
        filename, username = "", ""
        data = bytes("", 'utf-8')
        # for request in request_iterator:
        #     username, filename = request.username, request.filename
        #     print("Key for file : ", username+"_"+filename)
        #     if (self.FileExists(username, filename)):
        #         print("File Already Exists Sending Negative Ack")
        #         return dfs_pb2.Ack(success=False, message="File already exists for this user. Please rename or delete file first")
        #     break
        username, filename = "", ""
        curr_data_size = 0
        curr_data_bytes = bytes("", 'utf-8')
        seq_no = 1
        meta_data = []
        check_file_existence = False
        for request in request_iterator:
            if (check_file_existence == False):
                username, filename = request.username, request.filename
                print("Key for file : ", username+"_"+filename)
                if (self.FileExists(username, filename)):
                    print("File Already Exists Sending Negative Ack")
                    return dfs_pb2.Ack(success=False, message="File already exists for this user. Please rename or delete file first")
                check_file_existence = True
            if ((curr_data_size+sys.getsizeof(request.data)) > SHARD_SIZE):
                # Perform Metadata Storage Here
                curr_meta_data = [seq_no, "", "", ""]
                datanode1_response = datanode1_stub.StoreFileChunk(self.SendDataInStream(
                    curr_data_bytes, username, filename, seq_no))
                if (datanode1_response.success):
                    curr_meta_data[1] = least_loaded_node1
                if datanode2_stub is not None:
                    datanode2_response = datanode2_stub.StoreFileChunk(self.SendDataInStream(
                        curr_data_bytes, username, filename, seq_no))
                    if (datanode2_response.success):
                        curr_meta_data[2] = least_loaded_node2
                if datanode3_stub is not None:
                    datanode3_response = datanode3_stub.StoreFileChunk(self.SendDataInStream(
                        curr_data_bytes, username, filename, seq_no))
                    if (datanode3_response.success):
                        curr_meta_data[3] = least_loaded_node3
                meta_data.append(curr_meta_data)
                curr_data_bytes = request.data
                curr_data_size = sys.getsizeof(request.data)
                seq_no += 1
            else:
                curr_data_size += sys.getsizeof(request.data)
                curr_data_bytes += request.data

        if (curr_data_size > 0):
            curr_meta_data = [seq_no, "", "", ""]
            datanode1_response = datanode1_stub.StoreFileChunk(self.SendDataInStream(
                curr_data_bytes, username, filename, seq_no))
            if (datanode1_response.success):
                curr_meta_data[1] = least_loaded_node1
            if datanode2_stub is not None:
                datanode2_response = datanode2_stub.StoreFileChunk(self.SendDataInStream(
                    curr_data_bytes, username, filename, seq_no))
                if (datanode2_response.success):
                    curr_meta_data[2] = least_loaded_node2
            if datanode3_stub is not None:
                datanode3_response = datanode3_stub.StoreFileChunk(self.SendDataInStream(
                    curr_data_bytes, username, filename, seq_no))
                if (datanode3_response.success):
                    curr_meta_data[3] = least_loaded_node3
            meta_data.append(curr_meta_data)

        if (datanode1_response.success):
            # Store Metadata Storage Operations
            db.save_meta_data_namenode(username, filename, meta_data)
            db.save_user_file(username, filename)

        return dfs_pb2.Ack(success=True, message="File Saved Successfully")

    def DownloadFile(self, request, context):

        if (self.FileExists(request.username, request.filename) == 0):
            yield dfs_pb2.FileData(username=request.username, filename=request.filename, data=bytes("", 'utf-8'))
            return
        print("Fetching the MetaData")
        meta_data = db.parse_meta_data(request.username, request.filename)

        print(meta_data)

        download_helper_instance = DownloadHelper(self)
        complete_file_data = download_helper_instance.GetDataFromDataNodes(
            request.username, request.filename, meta_data)
        print("Sending File to Client")
        max_chunk_size = 4000000  # Limitation of gRPC
        start, end = 0, max_chunk_size
        while (True):
            curr_chunk = complete_file_data[start:end]
            if (len(curr_chunk) == 0):
                break
            start = end
            end += max_chunk_size
            yield dfs_pb2.FileData(username=request.username, filename=request.filename, data=curr_chunk)

    def FileDelete(self, request, context):
        username, filename = request.username, request.filename
        if (self.FileExists(username, filename) == False):
            return dfs_pb2.Ack(success=False, message="Could not find file")

        file_metadata = db.parse_meta_data(username, filename)
        print(file_metadata)

        delete_helper_instance = DeleteHelper(self)
        response_success = delete_helper_instance.DeleteDataFromDataNodes(
            username, filename, file_metadata)
        if (response_success):
            db.delete_meta_data_namenode(username, filename)
            db.delete_user_file(username, filename)
            return dfs_pb2.Ack(success=True, message="Successfully deleted the file")
        else:
            return dfs_pb2.Ack(success=False, message="Failedd to delete the file")

    def UpdateFile(self, request_iterator, context):
        least_loaded_node1, least_loaded_node2, least_loaded_node3 = self.GetThreeLeastUtilizedNodes()
        if (least_loaded_node1 == -1):
            return dfs_pb2.Ack(success=False, message="No Active DataNode found")

        print(
            f"DataNodes found DN1 : {least_loaded_node1} replica DN2 : {least_loaded_node2} replica DN3 : {least_loaded_node3}")

        datanode1_channel = self.ip_channel_dict[least_loaded_node1]
        datanode1_stub = dfs_pb2_grpc.DataTransferServiceStub(
            datanode1_channel)
        # print("After Stub1")
        if (least_loaded_node2 != "" and least_loaded_node2 in self.ip_channel_dict):
            datanode2_channel = self.ip_channel_dict[least_loaded_node2]
            datanode2_stub = dfs_pb2_grpc.DataTransferServiceStub(
                datanode2_channel)
            if (least_loaded_node3 != "" and least_loaded_node3 in self.ip_channel_dict):
                datanode3_channel = self.ip_channel_dict[least_loaded_node3]
                datanode3_stub = dfs_pb2_grpc.DataTransferServiceStub(
                    datanode3_channel)
            else:
                datanode3_stub = None
        else:
            datanode2_stub = None
        filename, username = "", ""
        data = bytes("", 'utf-8')
        # for request in request_iterator:
        #     username, filename = request.username, request.filename
        #     print("Key for file : ", username+"_"+filename)
        #     if (self.FileExists(username, filename)):
        #         print("File Already Exists Sending Negative Ack")
        #         return dfs_pb2.Ack(success=False, message="File already exists for this user. Please rename or delete file first")
        #     break
        username, filename = "", ""
        curr_data_size = 0
        curr_data_bytes = bytes("", 'utf-8')
        seq_no = 1
        meta_data = []
        check_file_existence = False
        for request in request_iterator:
            if (check_file_existence == False):
                username, filename = request.username, request.filename
                print("Key for file : ", username+"_"+filename)
                file_exists = self.FileExists(username, filename)
                if (file_exists == False):
                    print("File Doesnt Exist Uploading File")
                else:
                    print("Deleting existing file chunks")
                    file_metadata = db.parse_meta_data(username, filename)
                    print(file_metadata)

                    delete_helper_instance = DeleteHelper(self)
                    response_success = delete_helper_instance.DeleteDataFromDataNodes(
                        username, filename, file_metadata)
                    if (response_success == True):
                        print("Existing File Chunks Successfully")
                    else:
                        print("Failed to Delete existing File Chunks")
                    # return dfs_pb2.Ack(success=False, message="File already exists for this user. Please rename or delete file first")
                check_file_existence = True
            if ((curr_data_size+sys.getsizeof(request.data)) > SHARD_SIZE):
                # Perform Metadata Storage Here
                curr_meta_data = [seq_no, "", "", ""]
                datanode1_response = datanode1_stub.StoreFileChunk(self.SendDataInStream(
                    curr_data_bytes, username, filename, seq_no))
                if (datanode1_response.success):
                    curr_meta_data[1] = least_loaded_node1
                if datanode2_stub is not None:
                    datanode2_response = datanode2_stub.StoreFileChunk(self.SendDataInStream(
                        curr_data_bytes, username, filename, seq_no))
                    if (datanode2_response.success):
                        curr_meta_data[2] = least_loaded_node2
                if datanode3_stub is not None:
                    datanode3_response = datanode3_stub.StoreFileChunk(self.SendDataInStream(
                        curr_data_bytes, username, filename, seq_no))
                    if (datanode3_response.success):
                        curr_meta_data[3] = least_loaded_node3
                meta_data.append(curr_meta_data)
                curr_data_bytes = request.data
                curr_data_size = sys.getsizeof(request.data)
                seq_no += 1
            else:
                curr_data_size += sys.getsizeof(request.data)
                curr_data_bytes += request.data

        if (curr_data_size > 0):
            curr_meta_data = [seq_no, "", "", ""]
            datanode1_response = datanode1_stub.StoreFileChunk(self.SendDataInStream(
                curr_data_bytes, username, filename, seq_no))
            if (datanode1_response.success):
                curr_meta_data[1] = least_loaded_node1
            if datanode2_stub is not None:
                datanode2_response = datanode2_stub.StoreFileChunk(self.SendDataInStream(
                    curr_data_bytes, username, filename, seq_no))
                if (datanode2_response.success):
                    curr_meta_data[2] = least_loaded_node2
            if datanode3_stub is not None:
                datanode3_response = datanode3_stub.StoreFileChunk(self.SendDataInStream(
                    curr_data_bytes, username, filename, seq_no))
                if (datanode3_response.success):
                    curr_meta_data[3] = least_loaded_node3
            meta_data.append(curr_meta_data)

        if (datanode1_response.success):
            # Store Metadata Storage Operations
            db.save_meta_data_namenode(username, filename, meta_data)
            db.delete_user_file(username, filename)
            db.save_user_file(username, filename)
        return dfs_pb2.Ack(success=True, message="File Updated Successfully")

    def GetActiveIpChannelDict(self):
        self.GetAllAvailableIPAddresses()
        return self.ip_channel_dict

    def ThreadExecuteReadIPAddresses(self):
        while (True):
            self.GetAllAvailableIPAddresses()

    def ThreadHeartBeat(self):
        while (True):
            for ip in self.all_datanode_ips:
                res = self.IsChannelAlive(ip)
                if (res != False):
                    datanode_stub = dfs_pb2_grpc.DataTransferServiceStub(res)
                    heartbeat_response = datanode_stub.IsDataNodeAlive(
                        dfs_pb2.Empty())
                    # print(f"Received Heartbeat from Datanode : {ip}")
                    # print(
                    #     f"With CPU Usage : {heartbeat_response.cpu_usage}\tDisk Space : {heartbeat_response.disk_space}\tMemory Used : {heartbeat_response.used_mem}")
                else:
                    print(f"Received No Heartbeat from Datanode : {ip}")

    def GetAllAvailableIPAddresses(self):
        # process = subprocess.Popen(
        #     "python3 /usr/files/DockerNetwork.py", shell=True, stdout=subprocess.PIPE)
        config = ""
        with open("/usr/files/config.json", "r") as f:
            config = f.read()
        config = json.loads(config)
        datanode_ips = [container["IPv4Address"][:-3]
                        for container in config["containers"]]
        self.datanode_ips = datanode_ips
        self.CheckIPChannelDicts()

    # def Message(self, request, context):
    #     print(f"NameNode received : {request.word} from client")
    #     dnode_channel = grpc.insecure_channel('{}'.format('172.21.0.4:80'))
    #     dnode_stub = dfs_pb2_grpc.DataTransferServiceStub(
    #         dnode_channel)
    #     response = dnode_stub.Message(
    #         dfs_pb2.testM(word=request.word))
    #     print(f"Response from DataNode : {response.word}")
    #     return dfs_pb2.testM(word=f"{response.word}")


def main():
    config = ""
    with open("/usr/files/config.json", "r") as f:
        config = f.read()
    config = json.loads(config)
    namenode_ip = config["namenode"]["IPv4Address"][:-3]
    namenode_port = 80
    datanode_ips = [container["IPv4Address"][:-3]
                    for container in config["containers"]]

    print(f"Starting NameNode at IP : {namenode_ip}:{namenode_port}")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    namenode_servicer_instance = NameNodeServicer(
        namenode_ip, namenode_port, datanode_ips)
    dfs_pb2_grpc.add_DataTransferServiceServicer_to_server(
        namenode_servicer_instance, server)
    server.add_insecure_port('[::]:{}'.format("80"))
    server.start()
    print(f"Started NameNode at IP : {namenode_ip}:{namenode_port}")

    thread1 = Thread(target=NameNodeServicer.ThreadHeartBeat,
                     args=(namenode_servicer_instance,))
    print("Starting HeartBeat Service")
    thread1.start()
    print("Started HeartBeat Service")
    thread2 = Thread(target=NameNodeServicer.ThreadExecuteReadIPAddresses, args=(
        namenode_servicer_instance,))
    print("Starting IP Address Updation Thread")
    thread2.start()
    print("Started IP Address Updation Thread")
    try:
        while True:
            time.sleep(60*60*24)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
