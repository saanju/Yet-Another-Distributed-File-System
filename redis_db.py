import redis
import ast
from dotenv import load_dotenv
import os
import json

load_dotenv()

REDIS_PORT = int(os.getenv('REDIS_PORT'))

r_conn = redis.StrictRedis(host='localhost', port=REDIS_PORT, db=0)


def save_meta_data_namenode(username, filename, meta_data):
    key = username+"_"+filename
    meta_data_json = json.dumps(meta_data)
    r_conn.set(key, meta_data_json)


def save_meta_data_datanode(username, filename, meta_data):
    key = username+"_"+filename
    meta_data_json = json.dumps(meta_data)
    r_conn.set(key, meta_data_json)


def delete_meta_data_namenode(username, filename):
    r_conn.delete(username+"_"+filename)


def key_exists(key):
    return r_conn.exists(key)


def set_data(key, value):
    r_conn.set(key, value)


def get_data(key):
    return (r_conn.get(key)).decode("utf-8")


def get_user_files(username):
    return json.loads(r_conn.get(username+"_").decode('utf-8'))


def delete_entry_with_key(key):
    r_conn.delete(key)


def parse_meta_data(username, filename):
    key = username+"_"+filename
    return json.loads(r_conn.get(key).decode('utf-8'))


def save_user_file(username, filename):
    if (key_exists(username)):
        userfiles = json.loads(r_conn.get(username+"_").decode('utf-8'))
    else:
        userfiles = []
    userfiles.append(filename)
    r_conn.set(username+"_", json.dumps(userfiles))


def delete_user_file(username, filename):
    if (key_exists(username)):
        userfiles = json.loads(r_conn.get(username+"_").decode('utf-8'))
    else:
        userfiles = []
    if (len(userfiles) > 0):
        userfiles.remove(filename)
    r_conn.set(username+"_", json.dumps(userfiles))


def test_hash():
    print(r_conn.hgetall("hb") == {})


def update_file_dir_meta(username, filename, directory):
    if (directory == ""):
        print(username, r_conn.hgetall(username))
        if (r_conn.hgetall(username) == {}):
            r_conn.hmset(username, {"children": json.dumps([filename])})
        else:
            root_dir_res = r_conn.hgetall(username)
            root_dir_res = json.loads(
                root_dir_res[b'children'].decode("utf-8"))
            root_dir_res.append(filename)
            r_conn.hmset(username, {"children": json.dumps(root_dir_res)})
        return 0

    curr_hash = username+"/"+directory
    if (r_conn.hgetall(curr_hash) == {}):
        r_conn.hmset(curr_hash, {"children": json.dumps([filename])})
    else:
        directory_res = r_conn.hgetall(curr_hash)
        directory_res = json.loads(directory_res[b'children'].decode("utf-8"))
        directory_res.append(filename)
        r_conn.hmset(curr_hash, {"children": json.dumps(directory_res)})
    return 0


def delete_file_dir_meta(username, filename, directory):
    if (directory == ""):
        if (r_conn.hgetall(username) == {}):
            return 0
            r_conn.hmset(username, {"children": json.dumps([filename])})
        else:
            root_dir_res = r_conn.hgetall(username)
            root_dir_res = json.loads(
                root_dir_res[b'children'].decode("utf-8"))
            root_dir_res.remove(filename)
            r_conn.hmset(username, {"children": json.dumps(root_dir_res)})
        return 0

    curr_hash = username+"/"+directory
    if (r_conn.hgetall(curr_hash) == {}):
        return 0
        r_conn.hmset(curr_hash, {"children": json.dumps([filename])})
    else:
        directory_res = r_conn.hgetall(curr_hash)
        directory_res = json.loads(directory_res[b'children'].decode("utf-8"))
        directory_res.remove(filename)
        r_conn.hmset(curr_hash, {"children": json.dumps(directory_res)})
    return 0


def create_directory(username, directory):
    if ("/" not in directory):
        if (r_conn.hgetall(username) == {}):
            r_conn.hmset(username, {"children": json.dumps([directory])})
        else:
            user_root_dir_res = r_conn.hgetall(
                username)[b'children'].decode("utf-8")
            user_root_dir_res = json.loads(user_root_dir_res)
            user_root_dir_res.append(directory)
            r_conn.hmset(username, {"children": json.dumps(user_root_dir_res)})

    curr_hash = username+"/"+directory
    if (r_conn.hgetall(curr_hash) != {}):
        return "Directory Already Exists"
    else:
        parent_directory = get_parent_directory(curr_hash)
        print(curr_hash, parent_directory)
        if (parent_directory != username):
            parent_directory_res = r_conn.hgetall(parent_directory)
            print(parent_directory, parent_directory_res)
            if (parent_directory_res != {}):
                parent_directory_content_list = parent_directory_res[b'children'].decode(
                    "utf-8")
                parent_directory_content_list = json.loads(
                    parent_directory_content_list)
                parent_directory_content_list.append(directory.split("/")[-1])
                r_conn.hmset(parent_directory, {
                             "children": json.dumps(parent_directory_content_list)})
        r_conn.hmset(curr_hash, {"children": json.dumps([])})
        return "Done"
# Here directory means entire path to directory starting from root


def delete_directory(username, directory):
    curr_hash = username+"/"+directory
    if (r_conn.hgetall(curr_hash) == {}):
        return []
    else:
        parent_directory = get_parent_directory(curr_hash)
        if (parent_directory != username):
            parent_directory_res = r_conn.hgetall(parent_directory)
            if (parent_directory_res != {}):
                parent_directory_content_list = parent_directory_res[b'children']
                parent_directory_content_list = json.loads(
                    parent_directory_content_list)
                # print(parent_directory_res, res, type(res))
                parent_directory_content_list.remove(directory.split("/")[-1])
                r_conn.hmset(parent_directory, {
                             "children": json.dumps(parent_directory_content_list)})
        del_dir_children = r_conn.hgetall(
            curr_hash)[b'children'].decode("utf-8")
        r_conn.hdel(curr_hash, "children")
        r_conn.delete(curr_hash)
        return del_dir_children


def get_parent_directory(directory):
    path_split = directory.split("/")
    if (len(path_split) > 0):
        path_split.pop()
    parent_path = "/".join(path_split)
    return parent_path


def move_directory(username, source_directory, dest_directory):
    source_parent_path = get_parent_directory(username+"/"+source_directory)
    dest_parent_path = get_parent_directory(username+"/"+dest_directory)
    source_hash = username+"/"+source_directory
    dest_hash = username+"/"+dest_directory
    if (source_directory == dest_directory):
        return -1
    if (r_conn.hgetall(source_hash) == {}):
        return -1
    if (r_conn.hgetall(source_parent_path) == {}):
        return -1
    if (r_conn.hgetall(dest_parent_path) == {}):
        return -1
    source_parent_list = r_conn.hgetall(
        source_parent_path)[b'children'].decode("utf-8")
    source_parent_list = json.loads(
        source_parent_list)
    if (len(source_parent_list) > 0):
        source_parent_list.remove(source_directory.split("/")[-1])
    r_conn.hmset(source_parent_path, {
                 "children": json.dumps(source_parent_list)})
    source_dir_content = r_conn.hgetall(source_hash)[
        b'children'].decode("utf-8")
    source_dir_content = json.loads(
        source_dir_content)
    r_conn.hdel(source_hash, "children")
    r_conn.delete(source_hash)
    dest_parent_list = r_conn.hgetall(
        dest_parent_path)[b'children'].decode("utf-8")
    dest_parent_list = json.loads(
        dest_parent_list)
    dest_parent_list.append(dest_directory.split("/")[-1])
    r_conn.hmset(dest_parent_path, {"children": json.dumps(dest_parent_list)})
    r_conn.hmset(dest_hash, {"children": json.dumps(source_dir_content)})
    return 0


def copy_directory(username, source_directory, dest_directory):
    source_parent_path = get_parent_directory(username+"/"+source_directory)
    dest_parent_path = get_parent_directory(username+"/"+dest_directory)
    source_hash = username+"/"+source_directory
    dest_hash = username+"/"+dest_directory
    if (source_directory == dest_directory):
        return -1
    if (r_conn.hgetall(source_hash) == {}):
        return -1
    if (r_conn.hgetall(source_parent_path) == {}):
        return -1
    if (r_conn.hgetall(dest_parent_path) == {}):
        return -1
    source_parent_list = r_conn.hgetall(
        source_parent_path)[b'children'].decode("utf-8")
    source_parent_list = json.loads(source_parent_list)
    source_dir_content = r_conn.hgetall(source_hash)[
        b'children'].decode("utf-8")
    source_dir_content = json.loads(source_dir_content)
    dest_parent_list = r_conn.hgetall(
        dest_parent_path)[b'children'].decode("utf-8")
    dest_parent_list = json.loads(dest_parent_list)
    dest_parent_list.append(dest_hash.split("/")[-1])
    r_conn.hmset(dest_parent_path, {"children": json.dumps(dest_parent_list)})
    r_conn.hmset(dest_hash, {"children": json.dumps(source_dir_content)})
    return 0


def list_file_directories(username, directory):
    curr_hash = username+"/"+directory
    if (r_conn.hgetall(curr_hash) == {}):
        return [-1]
    else:
        curr_dir_files = r_conn.hgetall(curr_hash)[b'children'].decode("utf-8")
        return json.loads(curr_dir_files)


def traverse(username, curr_directory, command, dest_directory):
    if (command == "" and dest_directory == ""):
        return json.loads(r_conn.hgetall(username)[b'children'].decode("utf-8"))

    if (command == "cd" and dest_directory == ".." and curr_directory != username):
        parent_dir = get_parent_directory(username+"/"+curr_directory)
        parent_res = r_conn.hgetall(parent_dir)
        if (parent_res == {}):
            return []
        else:
            return json.loads(parent_res[b'children'].decode("utf-8"))

    if (command == "cd" and ("/" not in dest_directory)):
        current_dir_list = r_conn.hgetall(username+"/"+curr_directory)
        if (curr_directory == ""):
            current_dir_list = r_conn.hgetall(username)
        if (current_dir_list == {}):
            return []
        else:
            current_dir_list = json.loads(
                current_dir_list[b'children'].decode("utf-8"))
            if dest_directory in current_dir_list:
                if (curr_directory == ""):
                    return json.loads(r_conn.hgetall(username+"/"+dest_directory)[b'children'].decode("utf-8"))
                return json.loads(r_conn.hgetall(username+"/"+curr_directory+"/"+dest_directory)[b'children'].decode("utf-8"))
            else:
                return []
