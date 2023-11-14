from concurrent import futures
from threading import Thread
import test_message_pb2_grpc
import test_message_pb2
import grpc
import time


class DataNodeServicer(test_message_pb2_grpc.DataTransferServiceServicer):
    def Message(self, request, context):
        print(f"DataNode received : {request.word}")
        return test_message_pb2.testM(word=f"Hello from NameNode received {request.word}")


if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    test_message_pb2_grpc.add_DataTransferServiceServicer_to_server(
        DataNodeServicer(), server)
    server.add_insecure_port('[::]:{}'.format("80"))
    server.start()

    try:
        while True:
            time.sleep(60*60*24)
    except KeyboardInterrupt:
        server.stop(0)
