from pathlib import Path
from typing import List, Dict, Optional, Callable, Any, Union, Tuple
import os
import zipfile
import asyncio
import lz4.frame  # type: ignore
import magic  # type: ignore
import shutil
import hashlib
from watchdog.observers import Observer  # type: ignore
from watchdog.events import FileSystemEventHandler, FileSystemEvent  # type: ignore
from loguru import logger


class FileOperationEnhancer:
    def __init__(self):
        """Initialize the file operation enhancer with MIME type detection and monitoring support"""
        self.mime = magic.Magic(mime=True)  # type: ignore
        self.observer: Optional[Observer] = None  # type: ignore
        self.watch_handlers: Dict[str, FileSystemEventHandler] = {}

    def compress_files(self, files: List[str], output_zip: str) -> str:
        """Compress files into a ZIP archive

        Args:
            files: List of file paths to compress
            output_zip: Output ZIP file path

        Returns:
            Path to compressed file
        """
        with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in files:
                if os.path.isdir(file):
                    for root, _, filenames in os.walk(file):
                        for filename in filenames:
                            filepath = os.path.join(root, filename)
                            arcname = os.path.relpath(
                                filepath, os.path.dirname(file))
                            zipf.write(filepath, arcname)
                else:
                    zipf.write(file, os.path.basename(file))
        return output_zip

    async def async_compress_files(self, files: List[str], output_zip: str) -> str:
        """Asynchronously compress files into a ZIP archive

        Args:
            files: List of file paths to compress
            output_zip: Output ZIP file path

        Returns:
            Path to compressed file
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.compress_files, files, output_zip)

    def compress_with_lz4(self, file_path: str) -> str:
        """Compress a file using LZ4 algorithm

        Args:
            file_path: Path to file to compress

        Returns:
            Path to compressed file
        """
        output_path = f"{file_path}.lz4"
        with open(file_path, 'rb') as source:
            # Use binary mode and proper type casting
            with lz4.frame.open(output_path, 'wb', compression_level=lz4.frame.COMPRESSIONLEVEL_MINHC) as dest:  # type: ignore
                # Cast dest to BinaryIO for type checker
                shutil.copyfileobj(source, dest, 1024*1024)  # type: ignore
        return output_path

    def decompress_file(self, zip_file: str, extract_path: str) -> None:
        """Extract files from a ZIP archive

        Args:
            zip_file: Path to ZIP file
            extract_path: Directory to extract files to
        """
        with zipfile.ZipFile(zip_file, 'r') as zipf:
            zipf.extractall(extract_path)

    def detect_file_type(self, file_path: str) -> str:
        """Detect MIME type of a file

        Args:
            file_path: Path to file

        Returns:
            MIME type string
        """
        return self.mime.from_file(file_path)  # type: ignore

    def split_large_file(self, file_path: str, chunk_size: int = 1024*1024) -> int:
        """Split a large file into smaller chunks

        Args:
            file_path: Path to file to split
            chunk_size: Size of each chunk in bytes

        Returns:
            Number of chunks created
        """
        chunk_number = 0

        with open(file_path, 'rb') as f:
            while True:
                chunk_data = f.read(chunk_size)
                if not chunk_data:
                    break

                output_chunk_path = f"{file_path}.part{chunk_number:04d}"
                with open(output_chunk_path, 'wb') as chunk_file:
                    chunk_file.write(chunk_data)

                chunk_number += 1

        # Write metadata file
        with open(f"{file_path}.manifest", 'w') as manifest:
            manifest.write(f"original_file: {os.path.basename(file_path)}\n")
            manifest.write(f"chunk_size: {chunk_size}\n")
            manifest.write(f"total_chunks: {chunk_number}\n")

        return chunk_number

    def merge_file_chunks(self, base_path: str, total_chunks: int, output_path: str) -> None:
        """Merge file chunks back into a single file

        Args:
            base_path: Base path of chunk files
            total_chunks: Number of chunks to merge
            output_path: Output file path

        Raises:
            FileNotFoundError: If a chunk file is missing
        """
        with open(output_path, 'wb') as outfile:
            for i in range(total_chunks):
                chunk_path = f"{base_path}.part{i:04d}"
                if os.path.exists(chunk_path):
                    with open(chunk_path, 'rb') as infile:
                        shutil.copyfileobj(
                            infile, outfile, 1024*1024)  # Use 1MB buffer
                else:
                    raise FileNotFoundError(
                        f"Chunk file not found: {chunk_path}")

    def watch_directory(self, directory: str, callback: Callable[[str], None]) -> None:
        """Monitor a directory for file changes

        Args:
            directory: Directory path to monitor
            callback: Function to call when files change
        """
        class Handler(FileSystemEventHandler):
            def on_modified(self, event: FileSystemEvent) -> None:
                if not event.is_directory:
                    # Convert event.src_path to string if it's bytes
                    src_path = str(event.src_path) if isinstance(event.src_path, bytes) else event.src_path
                    callback(src_path)

        if directory not in self.watch_handlers:
            handler = Handler()
            self.watch_handlers[directory] = handler

            if not self.observer:
                self.observer = Observer()  # type: ignore
                self.observer.start()  # type: ignore

            self.observer.schedule(handler, directory, recursive=True)  # type: ignore
            logger.info(f"Started monitoring directory: {directory}")

    def stop_watching(self, directory: Optional[str] = None) -> None:
        """Stop directory monitoring

        Args:
            directory: Specific directory to stop monitoring, or None to stop all
        """
        if directory and directory in self.watch_handlers:
            if self.observer:
                self.observer.unschedule(self.watch_handlers[directory])  # type: ignore
            del self.watch_handlers[directory]
            logger.info(f"Stopped monitoring directory: {directory}")
        elif self.observer:
            self.observer.stop()  # type: ignore
            self.observer.join()  # type: ignore
            self.observer = None
            self.watch_handlers.clear()
            logger.info("Stopped all directory monitoring")

    def batch_process_files(self, files: List[str], operation: Callable[..., Any], *args: Any, **kwargs: Any) -> List[Tuple[str, Union[Any, str], bool]]:
        """Process multiple files with the same operation

        Args:
            files: List of file paths
            operation: Function to apply to each file
            *args, **kwargs: Additional arguments for the operation

        Returns:
            List of (file_path, result, success) tuples
        """
        results: List[Tuple[str, Union[Any, str], bool]] = []
        for file in files:
            try:
                result = operation(file, *args, **kwargs)
                results.append((file, result, True))
            except Exception as e:
                logger.error(f"Error processing file {file}: {str(e)}")
                results.append((file, str(e), False))
        return results

    def create_file_snapshot(self, directory: str) -> Dict[str, Dict[str, Union[int, float, str]]]:
        """Create a snapshot of files in a directory with metadata

        Args:
            directory: Directory path

        Returns:
            Dictionary with file paths as keys and metadata as values
        """
        snapshot: Dict[str, Dict[str, Union[int, float, str]]] = {}
        for root, _, files in os.walk(directory):
            rel_path = os.path.relpath(root, directory)
            for file in files:
                file_path = os.path.join(root, file)
                rel_file_path = os.path.join(rel_path, file).replace('\\', '/')
                if rel_file_path.startswith('./'):
                    rel_file_path = rel_file_path[2:]

                try:
                    stat_info = os.stat(file_path)
                    snapshot[rel_file_path] = {
                        'size': stat_info.st_size,
                        'mtime': stat_info.st_mtime,
                        'md5': self._calculate_file_hash(file_path)
                    }
                except Exception as e:
                    logger.warning(
                        f"Unable to get file info for {file_path}: {str(e)}")
        return snapshot

    def _calculate_file_hash(self, file_path: str, algorithm: str = 'md5', buffer_size: int = 8192) -> str:
        """Calculate file hash using specified algorithm

        Args:
            file_path: Path to file
            algorithm: Hash algorithm to use (md5, sha1, sha256)
            buffer_size: Buffer size for reading file

        Returns:
            Hexadecimal hash digest

        Raises:
            ValueError: If algorithm is not supported
        """
        if algorithm.lower() == 'md5':
            hash_obj = hashlib.md5()
        elif algorithm.lower() == 'sha1':
            hash_obj = hashlib.sha1()
        elif algorithm.lower() == 'sha256':
            hash_obj = hashlib.sha256()
        else:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")

        with open(file_path, 'rb') as f:
            while True:
                data = f.read(buffer_size)
                if not data:
                    break
                hash_obj.update(data)

        return hash_obj.hexdigest()

    def compare_snapshots(self, old_snapshot: Dict[str, Dict[str, Union[int, float, str]]],
                          new_snapshot: Dict[str, Dict[str, Union[int, float, str]]]) -> Dict[str, List[str]]:
        """Compare two directory snapshots to find changes

        Args:
            old_snapshot: Previous snapshot dictionary
            new_snapshot: Current snapshot dictionary

        Returns:
            Dictionary with added, removed, and modified file lists
        """
        added: List[str] = [
            path for path in new_snapshot if path not in old_snapshot]
        removed: List[str] = [
            path for path in old_snapshot if path not in new_snapshot]
        modified: List[str] = [
            path for path in new_snapshot if path in old_snapshot and
            (new_snapshot[path]['md5'] != old_snapshot[path]['md5'] or
             new_snapshot[path]['size'] != old_snapshot[path]['size'])
        ]

        return {
            'added': added,
            'removed': removed,
            'modified': modified
        }
