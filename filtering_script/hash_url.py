import hashlib

def hash_url(url):
    hasher = hashlib.sha256()
    hasher.update(url.encode('utf-8'))
    return hasher.hexdigest()