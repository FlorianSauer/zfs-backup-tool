class SshHost(object):
    def __init__(self, host: str, user: str = None, port: int = None, key_path: str = None):
        self.host = host
        self.user = user
        self.port = port
        self.key_path = key_path
