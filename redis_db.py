import redis
import ast
from dotenv import load_dotenv
import os
import json

load_dotenv()

REDIS_PORT = int(os.getenv('REDIS_PORT'))

r_conn = redis.StrictRedis(host='localhost', port=REDIS_PORT, db=0)
KEY_SEPARATOR = "_"


class DFSRedisMetadata:
    def __init__(self):
        self.DIRECTORY_KEY = 'directories'
        self.FILE_KEY = 'files'

    def create_directory(self, username, directory_name, parent_path=None):
        key = f"{username}:{directory_name}"
        directory_data = {'name': directory_name, 'type': 'directory', 'children': []}

        if parent_path:
            parent_key = f"{username}:{parent_path}"
            parent_data = json.loads(r_conn.hget(self.DIRECTORY_KEY, parent_key) or '{}')
            parent_data['children'].append(directory_data)
            r_conn.hset(self.DIRECTORY_KEY, parent_key, json.dumps(parent_data))

        r_conn.hset(self.DIRECTORY_KEY, key, json.dumps(directory_data))

    def delete_directory(username, directory_path):
    """Delete a directory and its contents."""
    directory_key = get_directory_key(username, directory_path)

    # Get all keys under the directory key
    keys = r_conn.keys(f"{directory_key}{KEY_SEPARATOR}*")

    # Delete all keys associated with the directory
    for key in keys:
        r_conn.delete(key)


    def move_directory(self, username, source_path, destination_path):
        source_key = f"{username}:{source_path}"
        destination_key = f"{username}:{destination_path}"

        source_data = json.loads(r_conn.hget(self.DIRECTORY_KEY, source_key) or '{}')
        destination_data = json.loads(r_conn.hget(self.DIRECTORY_KEY, destination_key) or '{}')

        if source_data:
            if 'children' in source_data:
                for child in source_data['children']:
                    child_key = f"{username}:{destination_path}:{child['name']}"
                    child['name'] = destination_path
                    r_conn.hset(self.DIRECTORY_KEY, child_key, json.dumps(child))

            r_conn.hset(self.DIRECTORY_KEY, destination_key, json.dumps(source_data))
            r_conn.hdel(self.DIRECTORY_KEY, source_key)
    def get_directory_content(username, directory_path):
    """Get the content (files and subdirectories) of a directory."""
    directory_key = get_directory_key(username, directory_path)
    
    # Get all keys under the directory key
    keys = r_conn.keys(f"{directory_key}{KEY_SEPARATOR}*")

    content = []

    for key in keys:
        # Check if the key represents a subdirectory or file
        key_type = r_conn.hget(key, "type").decode("utf-8")

        if key_type == "directory":
            # If it's a directory, add it to the content
            subdir_path = key.decode("utf-8").split(KEY_SEPARATOR, 2)[2]
            content.append({"type": "directory", "name": subdir_path})
        elif key_type == "file":
            # If it's a file, add it to the content along with metadata
            filename = key.decode("utf-8").split(KEY_SEPARATOR)[-1]
            metadata = json.loads(r_conn.hget(key, "metadata").decode("utf-8"))
            content.append({"type": "file", "name": filename, "metadata": metadata})

    return content


def get_file_key(username, filename):
    """Generate a key for a file."""
    return f"{username}{KEY_SEPARATOR}{filename}"

def get_directory_key(username, directory_path):
    """Generate a key for a directory."""
    return f"{username}{KEY_SEPARATOR}{directory_path}"

def save_file(username, filename, content):
    """Create or update a file in Redis."""
    file_key = get_file_key(username, filename)
    r_conn.set(file_key, content)

def delete_file(username, filename):
    """Delete a file from Redis."""
    file_key = get_file_key(username, filename)
    r_conn.delete(file_key)

def move_file(username, old_filename, new_filename):
    """Move a file to a new location in Redis."""
    old_file_key = get_file_key(username, old_filename)
    new_file_key = get_file_key(username, new_filename)
    content = r_conn.get(old_file_key)
    
    # Delete the old file
    delete_file(username, old_filename)
    
    # Save the content to the new location
    save_file(username, new_filename, content)

def copy_file(username, source_filename, destination_filename):
    """Copy a file to a new location in Redis."""
    source_file_key = get_file_key(username, source_filename)
    destination_file_key = get_file_key(username, destination_filename)
    content = r_conn.get(source_file_key)
    
    # Save the content to the new location
    save_file(username, destination_filename, content)

def list_directory(username, directory_path):
    """List files and directories within a directory in Redis."""
    directory_key = get_directory_key(username, directory_path)
    keys = r_conn.keys(f"{directory_key}{KEY_SEPARATOR}*")
    
    files_and_directories = []
    for key in keys:
        _, name = key.split(KEY_SEPARATOR, 1)
        files_and_directories.append(name)
    
    return files_and_directories

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
    return json.loads(r_conn.get(username).decode('utf-8'))


def delete_entry_with_key(key):
    r_conn.delete(key)


def parse_meta_data(username, filename):
    key = username+"_"+filename
    return json.loads(r_conn.get(key).decode('utf-8'))


def save_user_file(username, filename):
    if (key_exists(username)):
        userfiles = json.loads(r_conn.get(username).decode('utf-8'))
    else:
        userfiles = []
    userfiles.append(filename)
    r_conn.set(username, json.dumps(userfiles))


def delete_user_file(username, filename):
    if (key_exists(username)):
        userfiles = json.loads(r_conn.get(username).decode('utf-8'))
    else:
        userfiles = []
    if (len(userfiles) > 0):
        userfiles.remove(filename)
    r_conn.set(username, json.dumps(userfiles))
