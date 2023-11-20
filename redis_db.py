import redis
import ast
from dotenv import load_dotenv
import os
import json

load_dotenv()

REDIS_PORT = int(os.getenv('REDIS_PORT'))

r_conn = redis.StrictRedis(host='localhost', port=REDIS_PORT, db=0)
KEY_SEPARATOR = "_"

def get_directory_key(username, directory_path):
    return f"{username}{KEY_SEPARATOR}{directory_path}"

def get_file_key(username, directory_path, filename):
    return f"{username}{KEY_SEPARATOR}{directory_path}{KEY_SEPARATOR}{filename}"

def create_directory(username, directory_name, parent_path=None):
    key = get_directory_key(username, directory_name)
    directory_data = {'name': directory_name, 'type': 'directory', 'children': []}

    if parent_path:
        parent_key = get_directory_key(username, parent_path)
        parent_data = json.loads(r_conn.hget('directories', parent_key) or '{}')
        parent_data['children'].append(directory_data)
        r_conn.hset('directories', parent_key, json.dumps(parent_data))

    r_conn.hset('directories', key, json.dumps(directory_data))

def delete_directory(username, directory_path):
    """Delete a directory and its contents."""
    directory_key = get_directory_key(username, directory_path)

    # Get all keys under the directory key
    keys = r_conn.keys(f"{directory_key}{KEY_SEPARATOR}*")

    # Delete all keys associated with the directory
    for key in keys:
        r_conn.delete(key)

def move_directory(username, source_directory, destination_directory):
    source_directory_key = get_directory_key(username, source_directory)
    files_and_subdirectories = json.loads(r_conn.hget('directories', source_directory_key) or '{}')['children']

    if not files_and_subdirectories:
        print(f"Source directory '{source_directory}' not found.")
        return

    destination_directory_key = get_directory_key(username, destination_directory)
    r_conn.hset('directories', destination_directory_key, json.dumps({'name': destination_directory, 'type': 'directory', 'children': []}))

    for item in files_and_subdirectories:
        move_item(username, source_directory, destination_directory, item)

    delete_directory(username, source_directory)

def move_item(username, source_directory, destination_directory, item):
    source_item_key = get_directory_key(username, source_directory) + KEY_SEPARATOR + item['name']
    destination_item_key = get_directory_key(username, destination_directory) + KEY_SEPARATOR + item['name']

    item_data = json.loads(r_conn.hget('directories', source_item_key) or '{}')
    r_conn.hset('directories', destination_item_key, json.dumps(item_data))
    r_conn.hdel('directories', source_item_key)

def set_data(key, value):
    r_conn.set(key, value)

def get_directory_content(username, directory_path):
    """Get the content (files and subdirectories) of a directory."""
    directory_key = get_directory_key(username, directory_path)

    # Get all keys under the directory key
    keys = r_conn.keys(f"{directory_key}{KEY_SEPARATOR}*")

    content = []

    for key in keys:
        # Check if the key represents a subdirectory or file
        key_type = json.loads(r_conn.hget(key, "type") or '{}').get('type')

        if key_type == "directory":
            # If it's a directory, add it to the content
            subdir_path = key.decode("utf-8").split(KEY_SEPARATOR, 2)[-1]
            content.append({"type": "directory", "name": subdir_path})
        elif key_type == "file":
            # If it's a file, add it to the content along with metadata
            filename = key.decode("utf-8").split(KEY_SEPARATOR)[-1]
            metadata = json.loads(r_conn.hget(key, "metadata") or '{}')
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
