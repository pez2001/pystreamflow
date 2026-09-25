import logging, json

class SimpleJsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            'asctime': self.formatTime(record),
            'name': record.name,
            'level': record.levelname,
            'message': record.getMessage()
        })

def setup_logging(level='INFO'):
    handler = logging.StreamHandler()
    formatter = SimpleJsonFormatter()
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = [handler]
