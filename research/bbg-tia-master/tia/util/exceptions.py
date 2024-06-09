class BBGException(Exception):
    pass


class BBGInitError(BBGException):
    pass


class BBGSessionStartError(BBGInitError):
    pass


class BBGServiceOpenError(BBGInitError):
    pass


class BBGSubscriptionError(BBGException):
    pass


class BBGSecurityError(BBGException):
    pass


class BBGFieldError(BBGSecurityError):
    pass
