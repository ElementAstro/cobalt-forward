class SSHException(Exception):
    """Base SSH exception class for all SSH-related errors"""
    pass


class SSHConnectionError(SSHException):
    """Exception raised for connection errors"""
    pass


class SSHAuthenticationError(SSHException):
    """Exception raised for authentication errors"""
    pass


class SSHCommandError(SSHException):
    """Exception raised for command execution errors"""
    
    def __init__(self, message: str, command: str = None, exit_code: int = None, stdout: str = None, stderr: str = None):
        self.command = command
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(message)


class SSHTimeoutError(SSHException):
    """Exception raised for timeout related errors"""
    pass


class SSHFileTransferError(SSHException):
    """Exception raised for file transfer errors"""
    
    def __init__(self, message: str, source_path: str = None, destination_path: str = None):
        self.source_path = source_path
        self.destination_path = destination_path
        super().__init__(message)


class SSHPoolError(SSHException):
    """Exception raised for connection pool errors"""
    pass
