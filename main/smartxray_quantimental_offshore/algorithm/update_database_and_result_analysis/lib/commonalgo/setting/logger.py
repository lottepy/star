'''
Common logger library

[Config]
log_dir: log files directory. default './log'
sentry_dsn: sentry dsn. default no sentry
rollover_when: rollover interval. default 'MIDNIGHT'
	S - Seconds
	M - Minutes
	H - Hours
	D - Days
	MIDNIGHT - roll over at midnight
rollover_backup_count: how many backup log files are kept. default 30
	if rollover_backup_count = 0, all log files are kept.
	if rollover_backup_count > 0, when rollover is done, no more than rollover_backup_count files are kept - the oldest ones are deleted.

[Normal Python Program]
# config.py
# Add LOGGER_CONFIG
LOGGER_CONFIG = {
	'log_dir': './log',
	'sentry_dsn': 'http://xxxxxxxx',
}
#if no config.py, will use './log' as log_dir

[Integrate Django]
# settings.py
# Add LOGGER_CONFIG
LOGGER_CONFIG = {
	'log_dir': './log',
	'sentry_dsn': 'http://xxxxxxxx',
	'sentry_project_release': '1860da72a8c86bf4832a598410186ae668bbaf20',  # used by sentry to track which git release does error belong to
}
# Add 'raven.contrib.django.raven_compat' in INSTALLED_APPS
INSTALLED_APPS = (
	...,
	'raven.contrib.django.raven_compat',
)
# Add 'raven.contrib.django.middleware.SentryLogMiddleware' in MIDDLEWARE_CLASSES
MIDDLEWARE_CLASSES = (
	...,
	'raven.contrib.django.middleware.SentryLogMiddleware',
)

# 2019-04-17
only use:
log.data		(to access.log)
log.info		(to info.log)
log.exception	(to error.log)
log.debug		(to debug.log)
'''

import logging
# from log_request_id import local

log = None
LOGGING = None


class InfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.INFO


class WarningFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.WARNING


def _log_record_exception(func):
    def _func(self):
        try:
            return func(self)
        except:
            log.exception('log_exception|thread=%s:%s,file=%s:%s,func=%s:%s,log=%s',
                          self.process, self.thread, self.filename, self.lineno, self.module, self.funcName, self.msg)
            raise

    return _func


def append_exc(func):
    def _append_exc(*args, **kwargs):
        if 'exc_info' not in kwargs:
            kwargs['exc_info'] = True
        return func(*args, **kwargs)

    return _append_exc


def append_msg_id(func):
    def _append_msg_id(msg, *args, **kwargs):
        if hasattr(local, 'request_id'):
            msg = 'msg_id={0}|'.format(local.request_id) + msg
        return func(msg, *args, **kwargs)
    return _append_msg_id


def init_logger(log_dir=None, sentry_dsn=None, release_sha=None, rollover_when='MIDNIGHT',
                rollover_backup_count=30, **kwargs):
    if log_dir is None:
        log_dir = './log'

    import os
    import sys
    log_dir = os.path.abspath(log_dir)
    if log_dir and not os.path.exists(log_dir):
        os.mkdir(log_dir)

    logger_config = {
        'version': 1,
        'disable_existing_loggers': True,
        'filters': {
            'info_only': {
                '()': InfoFilter
            },
            'warning_only': {
                '()': WarningFilter
            },
        },
        'formatters': {
            'standard': {
                # 'format': '%(asctime)s.%(msecs)03d|%(levelname)s|%(process)d:%(thread)d|%(pathname)s:%(lineno)d|%(module)s.%(funcName)s|%(message)s',
                'format': '%(asctime)s.%(msecs)03d|%(levelname)s|%(process)d:%(thread)d|%(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
            'short': {
                'format': '%(asctime)s.%(msecs)03d|%(levelname)s|%(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
            'data': {
                'format': '%(asctime)s.%(msecs)03d|%(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
            "wrap_line": {
                'format': "%(message)s\n"
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'wrap_line'
            },
            'file_fatal': {
                'level': 'CRITICAL',
                'class': 'common.loggingmp.MPTimedRotatingFileHandler',
                'filename': os.path.join(log_dir, 'error.log').replace('\\', '/'),
                'when': rollover_when,
                'backupCount': rollover_backup_count,
                'formatter': 'standard',
            },
            'file_error': {
                'level': 'ERROR',
                'class': 'common.loggingmp.MPTimedRotatingFileHandler',
                'filename': os.path.join(log_dir, 'error.log').replace('\\', '/'),
                'when': rollover_when,
                'backupCount': rollover_backup_count,
                'formatter': 'standard',
            },
            'file_warning': {
                'level': 'WARNING',
                'class': 'common.loggingmp.MPTimedRotatingFileHandler',
                'filename': os.path.join(log_dir, 'warning.log').replace('\\', '/'),
                'when': rollover_when,
                'backupCount': rollover_backup_count,
                'formatter': 'standard',
                'filters': ['warning_only'],
            },
            'file_info': {
                'level': 'INFO',
                'class': 'common.loggingmp.MPTimedRotatingFileHandler',
                'filename': os.path.join(log_dir, 'info.log').replace('\\', '/'),
                'when': rollover_when,
                'backupCount': rollover_backup_count,
                'formatter': 'short',
                'filters': ['info_only'],
            },
            'file_data': {
                'level': 'INFO',
                'class': 'common.loggingmp.MPTimedRotatingFileHandler',
                'filename': os.path.join(log_dir, 'access.log').replace('\\', '/'),
                'when': rollover_when,
                'backupCount': rollover_backup_count,
                'formatter': 'data',
            },
        },
        'loggers': {
            'main': {
                'handlers': ['file_error', 'file_info', 'file_warning'],
                'level': 'DEBUG',
                'propagate': True,
            },
            'opentsdb-py': {
                'handlers': ['file_error', 'file_info', 'file_warning'],
                'level': 'DEBUG',
                'propagate': True,
            },
            'data': {
                'handlers': ['file_data'],
                'level': 'DEBUG',
                'propagate': True,
            },
            'django.request': {
                'handlers': ['file_fatal', 'file_error', 'file_info'],
                'level': 'ERROR',
                'propagate': True,
            },
            'tornado.access': {
                'handlers': ['file_data'],
                'level': 'DEBUG',
                'propagate': True,
            },
            'tornado.application': {
                'handlers': ['file_fatal', 'file_error', 'file_info'],
                'level': 'DEBUG',
                'propagate': True,
            },
            'tornado.general': {
                'handlers': ['file_fatal', 'file_error', 'file_info'],
                'level': 'DEBUG',
                'propagate': True,
            },
        }
    }

    is_django_app = False
    is_debug = False
    is_test = False
    try:
        from django.conf import settings
        is_django_app = settings.configured
        if is_django_app:
            is_debug = settings.DEBUG
            is_test = 'TEST' in dir(settings) and settings.TEST
    except Exception as e:
        print(e)
    if not is_django_app:
        import config
        is_debug = 'DEBUG' in dir(config) and config.DEBUG
        is_test = 'TEST' in dir(config) and config.TEST

    if is_debug:
        logger_config['handlers']['file_debug'] = {
            'level': 'DEBUG',
            'class': 'common.loggingmp.MPTimedRotatingFileHandler',
            'filename': os.path.join(log_dir, 'debug.log').replace('\\', '/'),
            'when': rollover_when,
            'backupCount': rollover_backup_count,
            'formatter': 'short',
        }
        db_handlers = ['file_debug']
        logger_config['loggers']['django.db.backends'] = {
            'handlers': db_handlers,
            'level': 'DEBUG',
            'propagate': True,
        }
        if kwargs.get("show_console_db_log", False):
            db_handlers.append('console')
    elif not is_test:
        loggers = logger_config['loggers']
        for logger_item in loggers:
            if loggers[logger_item]['level'] == 'DEBUG':
                loggers[logger_item]['level'] = 'INFO'

    if not is_debug and sentry_dsn is not None:
        try:
            import raven
            if is_django_app:
                logger_config['handlers']['sentry'] = {
                    # 'level': 'WARNING',
                    'level': 'ERROR',
                    'class': 'raven.contrib.django.raven_compat.handlers.SentryHandler',
                    'formatter': 'short',
                }
                settings.RAVEN_CONFIG = {'dsn': sentry_dsn, 'release': release_sha}
            else:
                logger_config['handlers']['sentry'] = {
                    # 'level': 'WARNING',
                    'level': 'ERROR',
                    'class': 'raven.handlers.logging.SentryHandler',
                    'dsn': sentry_dsn,
                    # 'auto_log_stacks': True,
                    'formatter': 'short',
                    'release': release_sha,
                }
            logger_config['loggers']['django.request']['handlers'].append('sentry')
            logger_config['loggers']['main']['handlers'].append('sentry')
        except Exception as e:
            print(e)

    work_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../')
    recover_path = False
    if work_dir not in sys.path:
        sys.path.append(work_dir)
        recover_path = True

    import logging
    try:
        import logging.config
        logging.config.dictConfig(logger_config)
    except:
        import loggerconfig
        loggerconfig.dictConfig(logger_config)

    if recover_path:
        sys.path.remove(work_dir)

    global log
    global LOGGING
    LOGGING = logger_config
    log = logging.getLogger('main')
    log.exception = append_msg_id(append_exc(log.error))
    log.info = append_msg_id(log.info)
    log.warning = append_msg_id(log.warning)
    log.debug = append_msg_id(log.debug)
    log.assertion = log.critical
    log.data = logging.getLogger('data').info
    logging.LogRecord.getMessage = _log_record_exception(logging.LogRecord.getMessage)


# try init log
def try_init_logger():
    try:
        from django.conf import settings

        setting_keys = dir(settings)
        if 'LOGGER_CONFIG' in setting_keys:
            init_logger(**settings.LOGGER_CONFIG)
        elif 'LOGGING' in setting_keys and settings.LOGGING:
            import logging
            global log
            log = logging.getLogger('main')
            log.exception = append_exc(log.error)
            log.data = logging.getLogger('data').info
        else:
            init_logger()

        settings.LOGGING = LOGGING
    except:
        try:
            import config
            init_logger(**config.LOGGER_CONFIG)
        except:
            try:
                init_logger()
            except Exception as e:
                print(e)


if log is None:
    try_init_logger()
