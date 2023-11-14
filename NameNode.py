from concurrent import futures
from threading import Thread
import test_message_pb2_grpc
import test_message_pb2
import grpc
import time


class NameNodeServicer(test_message_pb2_grpc.DataTransferServiceServicer):
    def Message(self, request, context):
        print(f"NameNode received : {request.word} from client")
        dnode_channel = grpc.insecure_channel('{}'.format('172.21.0.4:80'))
        dnode_stub = test_message_pb2_grpc.DataTransferServiceStub(
            dnode_channel)
        response = dnode_stub.Message(
            test_message_pb2.testM(word=request.word))
        print(f"Response from DataNode : {response.word}")
        return test_message_pb2.testM(word=f"{response.word}")


def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    test_message_pb2_grpc.add_DataTransferServiceServicer_to_server(
        NameNodeServicer(), server)
    server.add_insecure_port('[::]:{}'.format("80"))
    server.start()


if __name__ == "__main__":
    main()

    try:
        while True:
            time.sleep(60*60*24)
    except KeyboardInterrupt:
        server.stop(0)
