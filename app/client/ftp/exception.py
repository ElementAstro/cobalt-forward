class FTPError(Exception):
    """FTP基础异常类"""
    pass


class FTPConnectionError(FTPError):
    """连接相关异常"""
    pass


class FTPAuthError(FTPError):
    """认证相关异常"""
    pass


class FTPTransferError(FTPError):
    """传输相关异常"""
    pass
