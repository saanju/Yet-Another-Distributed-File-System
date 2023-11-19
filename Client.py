from concurrent import futures
from threading import Thread
import dfs_pb2_grpc
import dfs_pb2
import grpc
import time
import os
import json
import sys


def get_file_chunks():

    MAX_CHUNK_SIZE = 4000000

    username = input("Enter Username : ")
    filename = input("Enter Filename : ")

    file_to_upload = os.path.join('/usr/files/files', filename)
    s_time = time.time()

    with open(file_to_upload, "rb") as f:
        while True:
            chunk = f.read(MAX_CHUNK_SIZE)
            # print(chunk)
            if not chunk:
                break
            fdata = dfs_pb2.FileData(
                username=username, filename=filename, data=chunk)
            yield fdata
    print("Time taken to Upload : ", time.time()-s_time)


def upload_file(stub):
    data_chunks = get_file_chunks()
    response = stub.UploadFile(data_chunks)
    if (response.success):
        print("File Uploaded Successfully")
    else:
        print("ERROR : Failed to Upload Message : ", response.message)


def download_file(stub):
    username = input("Enter Username : ")
    filename = input("Enter Filename : ")
    data = bytes("", "utf-8")
    s_time = time.time()
    response_iter = stub.DownloadFile(
        dfs_pb2.FileInfo(username=username, filename=filename))
    for response in response_iter:
        filename = response.filename
        data += response.data
    if (len(data) == 0):
        print(f"File {filename} not found for user {username}")
        return

    print("Time taken to Download : ", time.time()-s_time)
    op_file_path = os.path.join('/usr/files/downloads', filename)
    f = open(op_file_path, "wb")
    f.write(data)
    f.close()
    print(f"File {filename} Downloaded to {op_file_path}")


def delete_file(stub):
    username = input("Enter Username : ")
    filename = input("Enter Filename : ")
    response = stub.FileDelete(dfs_pb2.FileInfo(
        username=username, filename=filename))
    print(response.message)


def check_file_presence(stub):
    username = input("Enter Username : ")
    filename = input("Enter Filename : ")
    response = stub.FileSearch(dfs_pb2.FileInfo(
        username=username, filename=filename))

    if (response.success):
        print(response.message)
    else:
        print(response.message)


def update_file(stub):
    response = stub.UpdateFile(get_file_chunks())
    if (response.success):
        print("File Successfully Updated")
    else:
        print("Failed to update the file Message ", response.message)


def get_list_of_user_file(stub):
    username = input("Enter Username : ")
    response = stub.FileList(dfs_pb2.UserInfo(username=username))
    if (response.filenames == ""):
        print(response.message)
    else:
        print(f"User {username} Files")
        userfiles = json.loads(response.filenames)
        for file in userfiles:
            print(file)


def get_user_input(stub):
    print("==================================================")
    print("1. Upload a file")
    print("2. Download a file")
    print("3. Delete a file")
    print("4. Check a file in DFS")
    print("5. Update a file in DFS")
    print("6. Get all file for a User")
    print("==================================================")
    selected_option = input("Please Choose an Input : \n")

    if (selected_option == '1'):
        upload_file(stub)
    elif (selected_option == '2'):
        download_file(stub)
    elif (selected_option == '3'):
        delete_file(stub)
    elif (selected_option == '4'):
        check_file_presence(stub)
    elif (selected_option == '5'):
        update_file(stub)
    elif (selected_option == '6'):
        get_list_of_user_file(stub)
    else:
        print("Please Choose out of given options")


def main():
    config = ""
    with open("/usr/files/config.json", "r") as f:
        config = f.read()
    config = json.loads(config)
    namenode_channel = grpc.insecure_channel(
        '{}:80'.format(config['namenode']['IPv4Address'][:-3]))
    try:
        grpc.channel_ready_future(namenode_channel).result(timeout=1)
    except grpc.FutureTimeoutError:
        print("Connection to NameNode failed : Timeout")
    else:
        print("Connected to NameNode")
    namenode_stub = dfs_pb2_grpc.DataTransferServiceStub(
        namenode_channel)
    get_user_input(namenode_stub)
    # test = input("Enter a message to be sent : ")
    # response = namenode_stub.Message(test_message_pb2.testM(word=test))
    # print(f"Received {response.word}")


if __name__ == "__main__":
    main()
