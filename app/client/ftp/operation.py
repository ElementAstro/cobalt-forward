import os
from pathlib import Path
from typing import List
import zipfile
import asyncio
import lz4.frame

import magic
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class FileOperationEnhancer:
    def __init__(self):
        self.mime = magic.Magic(mime=True)
        self.observer = None
        self.watch_handlers = {}

    def compress_files(self, files: List[str], output_zip: str):
        """压缩文件"""
        with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in files:
                zipf.write(file)
        return output_zip

    async def async_compress_files(self, files: List[str], output_zip: str):
        """异步压缩文件"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.compress_files, files, output_zip)

    def compress_with_lz4(self, file_path: str) -> str:
        """使用LZ4压缩文件"""
        output_path = f"{file_path}.lz4"
        with open(file_path, 'rb') as source:
            with lz4.frame.open(output_path, 'wb') as dest:
                while True:
                    chunk = source.read(1024 * 1024)  # 1MB chunks
                    if not chunk:
                        break
                    dest.write(chunk)
        return output_path

    def decompress_file(self, zip_file: str, extract_path: str):
        """解压文件"""
        with zipfile.ZipFile(zip_file, 'r') as zipf:
            zipf.extractall(extract_path)

    def detect_file_type(self, file_path: str) -> str:
        """检测文件类型"""
        return self.mime.from_file(file_path)

    def split_large_file(self, file_path: str, chunk_size: int = 1024*1024):
        """分割大文件"""
        base_path = Path(file_path)
        chunk_number = 0

        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                chunk_path = base_path.with_suffix(f'.part{chunk_number}')
                with open(chunk_path, 'wb') as chunk_file:
                    chunk_file.write(chunk)
                chunk_number += 1
        return chunk_number

    def merge_file_chunks(self, base_path: str, total_chunks: int, output_path: str):
        """合并文件块"""
        with open(output_path, 'wb') as outfile:
            for i in range(total_chunks):
                chunk_path = f"{base_path}.part{i}"
                with open(chunk_path, 'rb') as chunk:
                    outfile.write(chunk.read())
                os.remove(chunk_path)

    def watch_directory(self, directory: str, callback):
        """监控目录变化"""
        class Handler(FileSystemEventHandler):
            def on_modified(self, event):
                if not event.is_directory:
                    callback(event.src_path)

        if directory not in self.watch_handlers:
            handler = Handler()
            self.watch_handlers[directory] = handler

            if not self.observer:
                self.observer = Observer()

            self.observer.schedule(handler, directory, recursive=False)
            if not self.observer.is_alive():
                self.observer.start()

    def stop_watching(self, directory: str = None):
        """停止目录监控"""
        if directory and directory in self.watch_handlers:
            self.observer.unschedule(self.watch_handlers.pop(directory))
        elif self.observer:
            self.observer.stop()
            self.observer.join()
            self.watch_handlers.clear()

    def batch_process_files(self, files: List[str], operation: callable, *args, **kwargs):
        """批量处理文件"""
        results = []
        for file in files:
            try:
                result = operation(file, *args, **kwargs)
                results.append(
                    {"file": file, "status": "success", "result": result})
            except Exception as e:
                results.append(
                    {"file": file, "status": "failed", "error": str(e)})
        return results

    def create_file_snapshot(self, directory: str) -> dict:
        """创建目录文件快照"""
        snapshot = {}
        for root, _, files in os.walk(directory):
            for file in files:
                full_path = os.path.join(root, file)
                snapshot[full_path] = {
                    'mtime': os.path.getmtime(full_path),
                    'size': os.path.getsize(full_path),
                    'checksum': self.calculate_file_checksum(full_path)
                }
        return snapshot
