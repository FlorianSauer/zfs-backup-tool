class SshHost(object):
    def __init__(self, host: str, user: str = None, port: int = None, key_path: str = None):
        self.host = host
        self.user = user
        self.port = port
        self.key_path = key_path

    def __str__(self):
        keyfile_postfix = "" if self.key_path is None else " (key: {})".format(self.key_path)
        return "SshHost({}@{}:{}{})".format(self.user, self.host, self.port, keyfile_postfix)
