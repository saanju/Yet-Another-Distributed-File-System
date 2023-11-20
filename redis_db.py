import redis
import ast
from dotenv import load_dotenv
import os
import json

load_dotenv()

REDIS_PORT = int(os.getenv('REDIS_PORT'))

r_conn = redis.StrictRedis(host='localhost', port=REDIS_PORT, db=0)
KEY_SEPARATOR = "_"

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
