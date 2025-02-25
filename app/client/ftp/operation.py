from pathlib import Path
from typing import List, Dict, Any, Tuple
import os
import zipfile
import asyncio
import lz4.frame
import magic
import shutil
import hashlib
from concurrent.futures import ThreadPoolExecutor
import tempfile
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from loguru import logger


class FileOperationEnhancer:
    def __init__(self):
        self.mime = magic.Magic(mime=True)
        self.observer = None
        self.watch_handlers = {}

    def compress_files(self, files: List[str], output_zip: str):
        """压缩文件"""
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

    async def async_compress_files(self, files: List[str], output_zip: str):
        """异步压缩文件"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.compress_files, files, output_zip)

    def compress_with_lz4(self, file_path: str) -> str:
        """使用LZ4压缩文件"""
        output_path = f"{file_path}.lz4"
        with open(file_path, 'rb') as source:
            with lz4.frame.open(output_path, 'wb', compression_level=lz4.frame.COMPRESSIONLEVEL_MINHC) as dest:
                # 1MB chunks for optimal memory usage
                shutil.copyfileobj(source, dest, 1024*1024)
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
                chunk_data = f.read(chunk_size)
                if not chunk_data:
                    break

                output_chunk_path = f"{file_path}.part{chunk_number:04d}"
                with open(output_chunk_path, 'wb') as chunk_file:
                    chunk_file.write(chunk_data)

                chunk_number += 1

        # 写入元数据文件
        with open(f"{file_path}.manifest", 'w') as manifest:
            manifest.write(f"original_file: {os.path.basename(file_path)}\n")
            manifest.write(f"chunk_size: {chunk_size}\n")
            manifest.write(f"total_chunks: {chunk_number}\n")

        return chunk_number

    def merge_file_chunks(self, base_path: str, total_chunks: int, output_path: str):
        """合并文件块"""
        with open(output_path, 'wb') as outfile:
            for i in range(total_chunks):
                chunk_path = f"{base_path}.part{i:04d}"
                if os.path.exists(chunk_path):
                    with open(chunk_path, 'rb') as infile:
                        shutil.copyfileobj(
                            infile, outfile, 1024*1024)  # 使用1MB缓冲区
                else:
                    raise FileNotFoundError(f"找不到文件块: {chunk_path}")

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
                self.observer.start()

            self.observer.schedule(handler, directory, recursive=True)
            logger.info(f"开始监控目录: {directory}")

    def stop_watching(self, directory: str = None):
        """停止目录监控"""
        if directory and directory in self.watch_handlers:
            self.observer.unschedule(self.watch_handlers[directory])
            del self.watch_handlers[directory]
            logger.info(f"停止监控目录: {directory}")
        elif self.observer:
            self.observer.stop()
            self.observer.join()
            self.observer = None
            self.watch_handlers.clear()
            logger.info("停止所有目录监控")

    def batch_process_files(self, files: List[str], operation: callable, *args, **kwargs):
        """批量处理文件"""
        results = []
        for file in files:
            try:
                result = operation(file, *args, **kwargs)
                results.append((file, result, True))
            except Exception as e:
                logger.error(f"处理文件 {file} 时出错: {str(e)}")
                results.append((file, str(e), False))
        return results

    def create_file_snapshot(self, directory: str) -> dict:
        """创建目录文件快照"""
        snapshot = {}
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
                    logger.warning(f"无法获取文件信息 {file_path}: {str(e)}")
        return snapshot

    def _calculate_file_hash(self, file_path: str, algorithm: str = 'md5', buffer_size: int = 8192) -> str:
        """计算文件哈希值"""
        if algorithm.lower() == 'md5':
            hash_obj = hashlib.md5()
        elif algorithm.lower() == 'sha1':
            hash_obj = hashlib.sha1()
        elif algorithm.lower() == 'sha256':
            hash_obj = hashlib.sha256()
        else:
            raise ValueError(f"不支持的哈希算法: {algorithm}")

        with open(file_path, 'rb') as f:
            while True:
                data = f.read(buffer_size)
                if not data:
                    break
                hash_obj.update(data)

        return hash_obj.hexdigest()

    def compare_snapshots(self, old_snapshot: Dict, new_snapshot: Dict) -> Dict[str, List[str]]:
        """比较两个快照，返回变更的文件"""
        added = [path for path in new_snapshot if path not in old_snapshot]
        removed = [path for path in old_snapshot if path not in new_snapshot]
        modified = [
            path for path in new_snapshot if path in old_snapshot and
            (new_snapshot[path]['md5'] != old_snapshot[path]['md5'] or
             new_snapshot[path]['size'] != old_snapshot[path]['size'])
        ]

        return {
            'added': added,
            'removed': removed,
            'modified': modified
        }
