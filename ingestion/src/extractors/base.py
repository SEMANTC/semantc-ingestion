# src/extractors/base.py

class BaseExtractor:
    def extract(self):
        raise NotImplementedError("the 'extract' method must be implemented by subclasses")