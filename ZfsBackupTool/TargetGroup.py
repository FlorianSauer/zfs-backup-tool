from typing import List


class TargetGroup(object):
    def __init__(self, paths: List[str], name: str):
        self.paths = paths
        self.name = name
