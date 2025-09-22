from hashlib import sha256
BUF=1024*1024
def file_sha256(path):
    h=sha256()
    with open(path,'rb') as f:
        while True:
            b=f.read(BUF)
            if not b: break
            h.update(b)
    return h.hexdigest()
