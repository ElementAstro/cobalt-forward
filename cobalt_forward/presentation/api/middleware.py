"""
HTTP middleware components for request/response processing.

This module provides middleware for error handling, performance monitoring,
security, and other cross-cutting concerns.
"""

import time
import uuid
from typing import Any, Dict, Callable, Awaitable

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from ...infrastructure.config.models import SecurityConfig, PerformanceConfig


class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    """Middleware for centralized error handling."""

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        """Process request and handle errors."""
        try:
            response = await call_next(request)
            return response

        except Exception as e:
            # Log the error
            import logging
            logger = logging.getLogger(__name__)
            logger.exception(
                f"Unhandled error in {request.method} {request.url}: {e}")

            # Return error response
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal server error",
                    "message": str(e) if request.app.debug else "An unexpected error occurred",
                    "request_id": getattr(request.state, "request_id", None)
                }
            )


class PerformanceMiddleware(BaseHTTPMiddleware):
    """Middleware for performance monitoring and metrics collection."""

    def __init__(self, app: Any, config: PerformanceConfig) -> None:
        super().__init__(app)
        self.config = config
        self.metrics = {
            "request_count": 0,
            "total_time": 0.0,
            "avg_response_time": 0.0,
            "slow_requests": 0
        }

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        """Process request and collect performance metrics."""
        if not self.config.enabled:
            return await call_next(request)

        # Start timing
        start_time = time.time()

        # Add request ID
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id

        # Process request
        response = await call_next(request)

        # Calculate metrics
        end_time = time.time()
        duration = end_time - start_time

        # Update metrics
        self.metrics["request_count"] += 1
        self.metrics["total_time"] += duration
        self.metrics["avg_response_time"] = (
            self.metrics["total_time"] / self.metrics["request_count"]
        )

        # Check for slow requests
        slow_threshold = self.config.alert_thresholds.get("slow_request", 1.0)
        if duration > slow_threshold:
            self.metrics["slow_requests"] += 1

        # Add performance headers
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time"] = f"{duration:.3f}s"

        # Log performance data
        if hasattr(request.app.state, "container"):
            try:
                from ...infrastructure.logging.setup import LoggingManager
                container = request.app.state.container
                logging_manager = container.try_resolve(LoggingManager)

                if logging_manager:
                    logging_manager.log_performance(
                        f"{request.method} {request.url.path}",
                        duration=duration,
                        status_code=response.status_code,
                        request_id=request_id
                    )
            except Exception:
                pass  # Don't fail request if logging fails

        return response


class SecurityMiddleware(BaseHTTPMiddleware):
    """Middleware for security headers and basic protection."""

    def __init__(self, app: Any, config: SecurityConfig) -> None:
        super().__init__(app)
        self.config = config
        self.rate_limit_store: Dict[str, Dict[str, Any]] = {}

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        """Process request with security checks."""

        # Rate limiting
        if self.config.rate_limit_enabled:
            if not await self._check_rate_limit(request):
                return JSONResponse(
                    status_code=429,
                    content={"error": "Rate limit exceeded",
                             "message": "Too many requests"}
                )

        # Process request
        response = await call_next(request)

        # Add security headers
        self._add_security_headers(response)

        return response

    async def _check_rate_limit(self, request: Request) -> bool:
        """Check if request is within rate limits."""
        client_ip = request.client.host if request.client else "unknown"
        current_time = time.time()
        window_start = current_time - self.config.rate_limit_window

        # Clean old entries
        if client_ip in self.rate_limit_store:
            self.rate_limit_store[client_ip]["requests"] = [
                req_time for req_time in self.rate_limit_store[client_ip]["requests"]
                if req_time > window_start
            ]
        else:
            self.rate_limit_store[client_ip] = {"requests": []}

        # Check current request count
        request_count = len(self.rate_limit_store[client_ip]["requests"])

        if request_count >= self.config.rate_limit_requests:
            return False

        # Add current request
        self.rate_limit_store[client_ip]["requests"].append(current_time)
        return True

    def _add_security_headers(self, response: Response) -> None:
        """Add security headers to response."""
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "connect-src 'self' ws: wss:"
        )
