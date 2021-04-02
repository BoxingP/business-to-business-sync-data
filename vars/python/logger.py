import logging
import logging.config
import yaml


class Logger(object):
    def __init__(self, name, default_path='logging_config.yaml', default_level=logging.DEBUG):
        self.path = default_path
        self.level = default_level
        with open(self.path, 'r', encoding='UTF-8') as file:
            config = yaml.safe_load(file.read())
        self.logger = self.get_logger(name, config)
        return

    def get_logger(self, name, config):
        logging.config.dictConfig(config)
        logger = logging.getLogger(name)
        logger.setLevel(self.level)
        return logger

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)
