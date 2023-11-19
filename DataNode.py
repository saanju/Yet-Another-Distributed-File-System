from concurrent import futures
from threading import Thread
import dfs_pb2_grpc
import dfs_pb2
import grpc
import time
import redis_db as db
import psutil


class DataNodeServicer(dfs_pb2_grpc.DataTransferServiceServicer):
    def StoreFileChunk(self, request_iterator, context):
        data_chunk = bytes("", "utf-8")
        username = ""
        filename = ""
        seq_no = -1
        for request in request_iterator:
            username = request.username
            filename = request.filename
            seq_no = request.seq_no
            data_chunk += request.data

        # db.set_data(username+"_"+filename, data_chunk)
        db.set_data(username+"_"+filename+"_"+str(seq_no), data_chunk)
        return dfs_pb2.Ack(success=True, message="File saved in DataNode successfully")

    def GetFileChunk(self, request, context):
        username, filename, seq_no = request.username, request.filename, request.seq_no
        data_chunk = db.get_data(username+"_"+filename+"_"+str(seq_no))
        data_chunk = bytes(data_chunk, "utf-8")
        return self.SendDataInStream(data_chunk, username, filename, seq_no)

    def DeleteFileChunk(self, request, context):
        username, filename, seq_no = request.username, request.filename, request.seq_no
        db.delete_entry_with_key(username+"_"+filename+"_"+str(seq_no))
        return dfs_pb2.Ack(success=True, message="File Chunk Deleted Successfully")

    def IsDataNodeAlive(self, request, context):
        cpu_usage = str(psutil.cpu_percent())
        disk_space = str(psutil.virtual_memory()[2])
        used_mem = str(psutil.disk_usage('/')[3])
        return dfs_pb2.DataNodeStats(cpu_usage=cpu_usage, disk_space=disk_space, used_mem=used_mem)

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

    def Message(self, request, context):
        print(f"DataNode received : {request.word}")
        return dfs_pb2.testM(word=f"Hello from NameNode received {request.word}")


def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfs_pb2_grpc.add_DataTransferServiceServicer_to_server(
        DataNodeServicer(), server)
    server.add_insecure_port('[::]:{}'.format("80"))
    server.start()

    try:
        while True:
            time.sleep(60*60*24)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
