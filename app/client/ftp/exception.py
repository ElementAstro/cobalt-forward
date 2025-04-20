class FTPError(Exception):
    """Base exception class for FTP operations
    
    All FTP-related exceptions should inherit from this class.
    """
    def __init__(self, message: str = "FTP operation error"):
        """Initialize exception with message
        
        Args:
            message: Error message
        """
        self.message = message
        super().__init__(self.message)


class FTPConnectionError(FTPError):
    """Connection-related exceptions
    
    Raised when there are issues with establishing or maintaining 
    an FTP connection, such as network problems, timeouts, or
    server unavailability.
    """
    def __init__(self, message: str = "FTP connection error"):
        """Initialize exception with message
        
        Args:
            message: Error message
        """
        super().__init__(message)


class FTPAuthError(FTPError):
    """Authentication-related exceptions
    
    Raised when there are authentication issues, such as invalid
    credentials, expired passwords, or insufficient permissions.
    """
    def __init__(self, message: str = "FTP authentication error"):
        """Initialize exception with message
        
        Args:
            message: Error message
        """
        super().__init__(message)


class FTPTransferError(FTPError):
    """Transfer-related exceptions
    
    Raised when there are issues during file transfer operations,
    such as interrupted transfers, disk space issues, or file
    access problems.
    """
    def __init__(self, message: str = "FTP transfer error"):
        """Initialize exception with message
        
        Args:
            message: Error message
        """
        super().__init__(message)


class FTPCommandError(FTPError):
    """Command execution exceptions
    
    Raised when there are issues with FTP command execution,
    such as unsupported commands or invalid parameters.
    """
    def __init__(self, message: str = "FTP command error"):
        """Initialize exception with message
        
        Args:
            message: Error message
        """
        super().__init__(message)


class FTPDirectoryError(FTPError):
    """Directory operation exceptions
    
    Raised when there are issues with directory operations,
    such as permission problems, non-existent directories,
    or path-related issues.
    """
    def __init__(self, message: str = "FTP directory error"):
        """Initialize exception with message
        
        Args:
            message: Error message
        """
        super().__init__(message)


class FTPSecurityError(FTPError):
    """Security-related exceptions
    
    Raised when there are security issues, such as certificate
    validation failures, encryption problems, or security policy
    violations.
    """
    def __init__(self, message: str = "FTP security error"):
        """Initialize exception with message
        
        Args:
            message: Error message
        """
        super().__init__(message)


class FTPConfigError(FTPError):
    """Configuration-related exceptions
    
    Raised when there are issues with FTP configuration,
    such as invalid settings, missing required parameters,
    or configuration file problems.
    """
    def __init__(self, message: str = "FTP configuration error"):
        """Initialize exception with message
        
        Args:
            message: Error message
        """
        super().__init__(message)


class FTPTimeoutError(FTPConnectionError):
    """Timeout-related exceptions
    
    Raised when operations time out, such as connection attempts,
    data transfers, or command responses.
    """
    def __init__(self, message: str = "FTP operation timed out"):
        """Initialize exception with message
        
        Args:
            message: Error message
        """
        super().__init__(message)


class FTPDataError(FTPTransferError):
    """Data processing exceptions
    
    Raised when there are issues with data processing during transfer,
    such as corrupted data, unexpected formats, or encoding problems.
    """
    def __init__(self, message: str = "FTP data processing error"):
        """Initialize exception with message
        
        Args:
            message: Error message
        """
        super().__init__(message)
