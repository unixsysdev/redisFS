#!/usr/bin/env python3

import os
import sys
import stat
import time
import zlib
import errno
import trio
import pyfuse3
import logging
import redis
from typing import Dict, List, Tuple
from dataclasses import dataclass

# Configure logging with more detail
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('redisfs.log')
    ]
)
logger = logging.getLogger('RedisFUSE')

# Constants for optimization
BLOCK_SIZE = 1024 * 64  # 64KB blocks
COMPRESSION_THRESHOLD = 1024  # Only compress blocks larger than 1KB
MAX_BLOCKS_PER_READ = 8  # Maximum number of blocks to read at once
REDIS_BATCH_SIZE = 100  # Number of keys to process in each Redis batch operation

@dataclass
class Block:
    """Represents a block of file data"""
    offset: int
    data: bytes
    compressed: bool

class RedisFUSE(pyfuse3.Operations):
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        super().__init__()
        logger.info(f"Initializing RedisFUSE with host={redis_host}, port={redis_port}, db={redis_db}")

        try:
            self.redis = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=True,
                retry_on_timeout=True,
                socket_keepalive=True,
                socket_connect_timeout=5
            )
            # Test Redis connection
            self.redis.ping()
            logger.info("Successfully connected to Redis")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

        # Initialize basic filesystem structure
        self.inode_to_path = {}
        self.path_to_inode = {}
        self.next_inode = int(self.redis.get('fs:inode_counter') or pyfuse3.ROOT_INODE + 1)

        # Initialize root if not exists
        if not self.redis.exists('fs:/'):
            logger.info("Initializing root directory")
            now = time.time()
            self.redis.hset('fs:/', mapping={
                'type': 'dir',
                'mode': '0755',
                'uid': str(os.getuid()),
                'gid': str(os.getgid()),
                'size': '0',
                'atime': str(now),
                'mtime': str(now),
                'ctime': str(now),
                'blocks': '0',
                'inode': str(pyfuse3.ROOT_INODE),
                'nlink': '2',
            })
            self.redis.set('fs:inode_counter', self.next_inode)
            inode = pyfuse3.ROOT_INODE
        else:
            # Populate root inode
            attrs = self.redis.hgetall('fs:/')
            inode = int(attrs.get('inode', pyfuse3.ROOT_INODE))
            if inode <= 0:
                inode = pyfuse3.ROOT_INODE
                self.redis.hset('fs:/', 'inode', str(inode))
            self.next_inode = max(self.next_inode, inode + 1)

        # Always set mappings for root inode
        self.inode_to_path[inode] = '/'
        self.path_to_inode['/'] = inode

        # File handle management
        self.fd_counter = 0
        self.fd_inode_map = {}
        self.fd_open_flags = {}

    def _compress_data(self, data: bytes) -> Tuple[bytes, bool]:
        """Compress data if beneficial"""
        if len(data) > COMPRESSION_THRESHOLD:
            try:
                compressed = zlib.compress(data)
                if len(compressed) < len(data):
                    return compressed, True
            except Exception as e:
                logger.warning(f"Compression failed: {e}")
        return data, False

    def _decompress_data(self, data: bytes, is_compressed: bool) -> bytes:
        """Decompress data if it was compressed"""
        if is_compressed:
            try:
                return zlib.decompress(data)
            except Exception as e:
                logger.error(f"Decompression failed: {e}")
                raise pyfuse3.FUSEError(errno.EIO)
        return data

    async def _read_blocks(self, path: str, start_block: int, num_blocks: int) -> List[Block]:
        """Read multiple blocks efficiently using pipelining"""
        blocks = []
        logger.debug(f"Reading blocks {start_block} to {start_block + num_blocks - 1} for {path}")

        try:
            pipe = self.redis.pipeline()
            for block_num in range(start_block, start_block + num_blocks):
                pipe.hgetall(f'fs:{path}:block:{block_num}')

            results = pipe.execute()

            for i, block_data in enumerate(results):
                if block_data:
                    offset = int(block_data['offset'])
                    data = block_data['data'].encode('latin1')
                    compressed = block_data['compressed'] == '1'
                    if compressed:
                        data = self._decompress_data(data, compressed)
                    blocks.append(Block(offset, data, compressed))

            return blocks
        except Exception as e:
            logger.error(f"Error reading blocks: {e}")
            raise pyfuse3.FUSEError(errno.EIO)

    async def _write_blocks(self, path: str, blocks: List[Block]) -> None:
        """Write multiple blocks efficiently using pipelining"""
        logger.debug(f"Writing {len(blocks)} blocks for {path}")

        try:
            pipe = self.redis.pipeline()

            for block in blocks:
                data, compressed = self._compress_data(block.data)
                block_num = block.offset // BLOCK_SIZE
                block_key = f'fs:{path}:block:{block_num}'

                pipe.hset(block_key, mapping={
                    'offset': str(block.offset),
                    'data': data.decode('latin1'),
                    'compressed': '1' if compressed else '0'
                })

            pipe.execute()
        except Exception as e:
            logger.error(f"Error writing blocks: {e}")
            raise pyfuse3.FUSEError(errno.EIO)

    async def read(self, fh: int, offset: int, size: int) -> bytes:
        """Read file data using block-based access"""
        inode = self.fd_inode_map.get(fh)
        if inode is None:
            logger.error(f"read: Invalid file handle {fh}")
            raise pyfuse3.FUSEError(errno.EBADF)
        path = self.inode_to_path.get(inode)
        if path is None:
            logger.error(f"read: Inode {inode} not found")
            raise pyfuse3.FUSEError(errno.ENOENT)
        logger.debug(f"read: {path} offset={offset} size={size}")

        try:
            # Calculate block range
            start_block = offset // BLOCK_SIZE
            end_offset = offset + size
            end_block = (end_offset + BLOCK_SIZE - 1) // BLOCK_SIZE
            num_blocks = end_block - start_block

            # Read blocks
            blocks = await self._read_blocks(path, start_block, num_blocks)
            if not blocks:
                return b''

            # Combine blocks and extract requested range
            result = bytearray()
            for block in blocks:
                block_start = block.offset
                block_end = block.offset + len(block.data)
                if block_end > offset and block_start < offset + size:
                    start = max(0, offset - block_start)
                    end = min(len(block.data), offset + size - block_start)
                    result.extend(block.data[start:end])

            return bytes(result)

        except Exception as e:
            logger.error(f"Error reading {path}: {e}")
            raise pyfuse3.FUSEError(errno.EIO)

    async def write(self, fh: int, offset: int, buf: bytes) -> int:
        """Write file data using block-based access"""
        inode = self.fd_inode_map.get(fh)
        if inode is None:
            logger.error(f"write: Invalid file handle {fh}")
            raise pyfuse3.FUSEError(errno.EBADF)
        path = self.inode_to_path.get(inode)
        if path is None:
            logger.error(f"write: Inode {inode} not found")
            raise pyfuse3.FUSEError(errno.ENOENT)
        logger.debug(f"write: {path} offset={offset} size={len(buf)}")

        try:
            # Prepare blocks
            blocks = []
            remaining = len(buf)
            buf_offset = 0

            while remaining > 0:
                block_offset = ((offset + buf_offset) // BLOCK_SIZE) * BLOCK_SIZE
                write_size = min(remaining, BLOCK_SIZE - ((offset + buf_offset) % BLOCK_SIZE))

                # Read existing block if necessary
                existing_blocks = await self._read_blocks(path, block_offset // BLOCK_SIZE, 1)
                if existing_blocks:
                    block_data = bytearray(existing_blocks[0].data)
                else:
                    block_data = bytearray(BLOCK_SIZE)

                # Write data to block
                start_idx = (offset + buf_offset) % BLOCK_SIZE
                end_idx = start_idx + write_size
                block_data[start_idx:end_idx] = buf[buf_offset:buf_offset + write_size]

                blocks.append(Block(block_offset, bytes(block_data[:end_idx]), False))

                remaining -= write_size
                buf_offset += write_size

            # Write blocks
            await self._write_blocks(path, blocks)

            # Update file size and mtime
            size = max(offset + len(buf), int(self.redis.hget(f'fs:{path}', 'size') or 0))
            self.redis.hset(f'fs:{path}', mapping={
                'size': str(size),
                'mtime': str(time.time()),
                'blocks': str((size + BLOCK_SIZE - 1) // BLOCK_SIZE)
            })

            return len(buf)

        except Exception as e:
            logger.error(f"Error writing to {path}: {e}")
            raise pyfuse3.FUSEError(errno.EIO)

    async def getattr(self, inode: int, ctx=None) -> pyfuse3.EntryAttributes:
        """Get file attributes"""
        path = self.inode_to_path.get(inode)
        if path is None:
            logger.warning(f"getattr: inode {inode} not found in inode_to_path mapping")
            raise pyfuse3.FUSEError(errno.ENOENT)
        logger.debug(f"getattr: {path} (inode={inode})")

        try:
            attrs = self.redis.hgetall(f'fs:{path}')
            if not attrs:
                logger.warning(f"No attributes found for {path}")
                raise pyfuse3.FUSEError(errno.ENOENT)

            entry = pyfuse3.EntryAttributes()
            entry.st_ino = inode
            mode = int(attrs['mode'], 8)
            if attrs['type'] == 'dir':
                entry.st_mode = stat.S_IFDIR | mode
                entry.st_nlink = int(attrs.get('nlink', '2'))
            else:
                entry.st_mode = stat.S_IFREG | mode
                entry.st_nlink = int(attrs.get('nlink', '1'))
            entry.st_uid = int(attrs['uid'])
            entry.st_gid = int(attrs['gid'])
            entry.st_size = int(attrs['size'])
            entry.st_atime_ns = int(float(attrs['atime']) * 1e9)
            entry.st_mtime_ns = int(float(attrs['mtime']) * 1e9)
            entry.st_ctime_ns = int(float(attrs['ctime']) * 1e9)
            entry.st_blksize = BLOCK_SIZE
            entry.st_blocks = (entry.st_size + 511) // 512  # Number of 512-byte blocks

            return entry
        except Exception as e:
            logger.error(f"Error in getattr for {path}: {e}")
            raise pyfuse3.FUSEError(errno.EIO)

    async def lookup(self, parent_inode: int, name: bytes, ctx=None) -> pyfuse3.EntryAttributes:
        """Look up a directory entry by name"""
        name = name.decode('utf-8') if isinstance(name, bytes) else name
        parent = self.inode_to_path.get(parent_inode)
        if parent is None:
            logger.warning(f"lookup: parent inode {parent_inode} not found")
            raise pyfuse3.FUSEError(errno.ENOENT)
        path = os.path.join(parent, name).replace('//', '/')
        logger.debug(f"lookup: {path}")

        try:
            if not self.redis.exists(f'fs:{path}'):
                logger.debug(f"Path not found: {path}")
                raise pyfuse3.FUSEError(errno.ENOENT)

            attrs = self.redis.hgetall(f'fs:{path}')
            inode = int(attrs.get('inode', 0))
            if inode <= 0:
                inode = int(self.redis.incr('fs:inode_counter'))
                self.redis.hset(f'fs:{path}', 'inode', str(inode))
                self.redis.set('fs:inode_counter', max(self.next_inode, inode + 1))

            self.path_to_inode[path] = inode
            self.inode_to_path[inode] = path
            self.next_inode = max(self.next_inode, inode + 1)

            return await self.getattr(inode)
        except Exception as e:
            logger.error(f"Error in lookup for {path}: {e}")
            raise

    async def opendir(self, inode, ctx):
        """Open a directory and return a file handle"""
        logger.debug(f"opendir: inode={inode}")
        self.fd_counter += 1
        fh = self.fd_counter
        self.fd_inode_map[fh] = inode
        return fh

    async def readdir(self, fh, off, token):
        """Read directory entries"""
        inode = self.fd_inode_map.get(fh)
        if inode is None:
            logger.warning(f"readdir: invalid file handle {fh}")
            raise pyfuse3.FUSEError(errno.EBADF)
        path = self.inode_to_path.get(inode)
        if path is None:
            logger.warning(f"readdir: inode {inode} not found")
            raise pyfuse3.FUSEError(errno.ENOENT)
        logger.debug(f"Readdir for path {path} (inode {inode}), off={off}")

        try:
            entries = []

            # '.' entry
            entries.append(('.', await self.getattr(inode)))
            # '..' entry
            parent_path = os.path.dirname(path.rstrip('/')) or '/'
            parent_inode = self.path_to_inode.get(parent_path, pyfuse3.ROOT_INODE)
            entries.append(('..', await self.getattr(parent_inode)))

            # Child entries
            child_entries = self.redis.lrange(f"fs:{path}:children", 0, -1)
            for child_name in child_entries:
                child_path = os.path.join(path, child_name)
                attrs = self.redis.hgetall(f'fs:{child_path}')
                if not attrs:
                    continue  # Entry might have been deleted
                child_inode = int(attrs.get('inode', 0))
                if child_inode <= 0:
                    child_inode = int(self.redis.incr('fs:inode_counter'))
                    self.redis.hset(f'fs:{child_path}', 'inode', str(child_inode))
                self.path_to_inode[child_path] = child_inode
                self.inode_to_path[child_inode] = child_path
                entry_attr = await self.getattr(child_inode)
                entries.append((child_name, entry_attr))

            logger.debug(f"Directory {path} contains: {[e[0] for e in entries]}")

            # Use the offset 'off' to continue listing
            for idx, (name, attr) in enumerate(entries):
                if idx < off:
                    continue
                if not pyfuse3.readdir_reply(
                    token,
                    name.encode('utf-8'),
                    attr,
                    idx + 1  # Next offset
                ):
                    break
        except Exception as e:
            logger.error(f"Error in readdir for {path}: {e}")
            raise

    async def releasedir(self, fh):
        """Release a directory handle"""
        logger.debug(f"releasedir: fh={fh}")
        self.fd_inode_map.pop(fh, None)
        return

    async def fsyncdir(self, fh, datasync):
        """Synchronize directory contents (no-op)"""
        logger.debug(f"fsyncdir: fh={fh}, datasync={datasync}")
        return

    async def rmdir(self, parent_inode: int, name: bytes, ctx) -> None:
        """Remove a directory with proper checks"""
        parent = self.inode_to_path.get(parent_inode)
        if parent is None:
            logger.warning(f"rmdir: parent inode {parent_inode} not found")
            raise pyfuse3.FUSEError(errno.ENOENT)
        name = name.decode('utf-8') if isinstance(name, bytes) else name
        path = os.path.join(parent, name).replace('//', '/')
        logger.debug(f"rmdir: {path}")

        try:
            # Check if directory exists
            if not self.redis.exists(f'fs:{path}'):
                raise pyfuse3.FUSEError(errno.ENOENT)

            # Check if directory is empty
            if self.redis.llen(f'fs:{path}:children') > 0:
                raise pyfuse3.FUSEError(errno.ENOTEMPTY)

            # Remove directory entry and its children list
            self.redis.delete(f'fs:{path}')
            self.redis.delete(f'fs:{path}:children')

            # Remove entry from parent's children list
            self.redis.lrem(f'fs:{parent}:children', 0, name)

            # Clean up inode mappings
            inode = self.path_to_inode.pop(path, None)
            if inode:
                self.inode_to_path.pop(inode, None)

        except Exception as e:
            logger.error(f"Error removing directory {path}: {e}")
            raise

    async def unlink(self, parent_inode: int, name: bytes, ctx) -> None:
        """Remove a file with efficient block cleanup"""
        parent = self.inode_to_path.get(parent_inode)
        if parent is None:
            logger.warning(f"unlink: parent inode {parent_inode} not found")
            raise pyfuse3.FUSEError(errno.ENOENT)
        name = name.decode('utf-8') if isinstance(name, bytes) else name
        path = os.path.join(parent, name).replace('//', '/')
        logger.debug(f"unlink: {path}")

        try:
            # Get file metadata
            attrs = self.redis.hgetall(f'fs:{path}')
            if not attrs:
                raise pyfuse3.FUSEError(errno.ENOENT)

            # Remove entry from parent's children list
            self.redis.lrem(f'fs:{parent}:children', 0, name)

            # Decrease link count
            nlink = int(attrs.get('nlink', '1')) - 1
            if nlink <= 0:
                # Delete the file
                pipe = self.redis.pipeline()
                pipe.delete(f'fs:{path}')
                num_blocks = int(attrs.get('blocks', 0))
                for block_num in range(num_blocks):
                    pipe.delete(f'fs:{path}:block:{block_num}')
                pipe.execute()
            else:
                # Update link count
                self.redis.hset(f'fs:{path}', 'nlink', str(nlink))

            # Clean up inode mappings
            inode = self.path_to_inode.pop(path, None)
            if inode:
                self.inode_to_path.pop(inode, None)

        except Exception as e:
            logger.error(f"Error unlinking {path}: {e}")
            raise

    async def mkdir(self, parent_inode: int, name: bytes, mode: int, ctx) -> pyfuse3.EntryAttributes:
        """Create a directory"""
        parent = self.inode_to_path.get(parent_inode)
        if parent is None:
            logger.warning(f"mkdir: parent inode {parent_inode} not found")
            raise pyfuse3.FUSEError(errno.ENOENT)
        name = name.decode('utf-8') if isinstance(name, bytes) else name
        path = os.path.join(parent, name).replace('//', '/')
        logger.debug(f"mkdir: {path} mode={mode:o}")

        try:
            if self.redis.exists(f'fs:{path}'):
                raise pyfuse3.FUSEError(errno.EEXIST)

            now = time.time()
            inode = int(self.redis.incr('fs:inode_counter'))
            self.redis.hset(f'fs:{path}', mapping={
                'type': 'dir',
                'mode': f'{mode & 0o777:o}',
                'uid': str(ctx.uid),
                'gid': str(ctx.gid),
                'size': '0',
                'atime': str(now),
                'mtime': str(now),
                'ctime': str(now),
                'blocks': '0',
                'inode': str(inode),
                'nlink': '2',
            })
            self.redis.set('fs:inode_counter', max(self.next_inode, inode + 1))

            # Update parent's children list
            self.redis.rpush(f'fs:{parent}:children', name)

            self.path_to_inode[path] = inode
            self.inode_to_path[inode] = path
            self.next_inode = max(self.next_inode, inode + 1)

            return await self.getattr(inode)

        except Exception as e:
            logger.error(f"Error creating directory {path}: {e}")
            raise

    async def setattr(self, inode: int, attr, fields, fh, ctx) -> pyfuse3.EntryAttributes:
        """Set file attributes with proper validation"""
        path = self.inode_to_path.get(inode)
        if path is None:
            logger.warning(f"setattr: inode {inode} not found")
            raise pyfuse3.FUSEError(errno.ENOENT)
        logger.debug(f"setattr: {path}")

        try:
            current_attrs = self.redis.hgetall(f'fs:{path}')
            if not current_attrs:
                raise pyfuse3.FUSEError(errno.ENOENT)

            updates = {}

            if fields.update_mode:
                updates['mode'] = f'{attr.st_mode & 0o777:o}'
            if fields.update_uid:
                updates['uid'] = str(attr.st_uid)
            if fields.update_gid:
                updates['gid'] = str(attr.st_gid)
            if fields.update_size:
                old_size = int(current_attrs['size'])
                new_size = attr.st_size
                updates['size'] = str(new_size)

                if new_size < old_size:
                    # Remove unnecessary blocks
                    old_blocks = (old_size + BLOCK_SIZE - 1) // BLOCK_SIZE
                    new_blocks = (new_size + BLOCK_SIZE - 1) // BLOCK_SIZE

                    pipe = self.redis.pipeline()
                    for block_num in range(new_blocks, old_blocks):
                        pipe.delete(f'fs:{path}:block:{block_num}')
                    pipe.execute()

                updates['blocks'] = str((new_size + BLOCK_SIZE - 1) // BLOCK_SIZE)

            if fields.update_atime:
                updates['atime'] = str(attr.st_atime_ns / 1e9)
            if fields.update_mtime:
                updates['mtime'] = str(attr.st_mtime_ns / 1e9)

            if updates:
                self.redis.hset(f'fs:{path}', mapping=updates)

            return await self.getattr(inode)

        except Exception as e:
            logger.error(f"Error setting attributes for {path}: {e}")
            raise

    async def create(self, parent_inode: int, name: bytes, mode: int, flags: int, ctx) -> Tuple[pyfuse3.FileInfo, pyfuse3.EntryAttributes]:
        """Create a new file"""
        parent = self.inode_to_path.get(parent_inode)
        if parent is None:
            logger.warning(f"create: parent inode {parent_inode} not found")
            raise pyfuse3.FUSEError(errno.ENOENT)
        name = name.decode('utf-8') if isinstance(name, bytes) else name
        path = os.path.join(parent, name).replace('//', '/')
        logger.debug(f"create: {path} mode={mode:o}")

        try:
            if self.redis.exists(f'fs:{path}'):
                raise pyfuse3.FUSEError(errno.EEXIST)

            now = time.time()
            inode = int(self.redis.incr('fs:inode_counter'))
            self.redis.hset(f'fs:{path}', mapping={
                'type': 'file',
                'mode': f'{mode & 0o777:o}',
                'uid': str(ctx.uid),
                'gid': str(ctx.gid),
                'size': '0',
                'atime': str(now),
                'mtime': str(now),
                'ctime': str(now),
                'blocks': '0',
                'inode': str(inode),
                'nlink': '1',
            })
            self.redis.set('fs:inode_counter', max(self.next_inode, inode + 1))

            # Update parent's children list
            self.redis.rpush(f'fs:{parent}:children', name)

            self.path_to_inode[path] = inode
            self.inode_to_path[inode] = path
            self.next_inode = max(self.next_inode, inode + 1)

            # Open the file and return the file handle
            self.fd_counter += 1
            fh = self.fd_counter
            self.fd_inode_map[fh] = inode
            self.fd_open_flags[fh] = flags

            return pyfuse3.FileInfo(fh=fh), await self.getattr(inode)

        except Exception as e:
            logger.error(f"Error creating file {path}: {e}")
            raise

    async def open(self, inode: int, flags: int, ctx) -> pyfuse3.FileInfo:
        """Open a file handle"""
        path = self.inode_to_path.get(inode)
        if path is None:
            logger.warning(f"open: inode {inode} not found")
            raise pyfuse3.FUSEError(errno.ENOENT)
        logger.debug(f"open: {path} flags={flags:o}")

        if not self.redis.exists(f'fs:{path}'):
            raise pyfuse3.FUSEError(errno.ENOENT)

        self.fd_counter += 1
        fh = self.fd_counter
        self.fd_inode_map[fh] = inode
        self.fd_open_flags[fh] = flags

        return pyfuse3.FileInfo(fh=fh)

    async def flush(self, fh: int) -> None:
        """Flush file contents (no-op as Redis handles this)"""
        logger.debug(f"flush: fh={fh}")
        return

    async def fsync(self, fh: int, datasync: bool) -> None:
        """Sync file contents (no-op as Redis handles this)"""
        logger.debug(f"fsync: fh={fh}, datasync={datasync}")
        return

    async def release(self, fh: int) -> None:
        """Release an open file"""
        logger.debug(f"release: fh={fh}")
        self.fd_inode_map.pop(fh, None)
        self.fd_open_flags.pop(fh, None)
        return

    # Implement other methods as needed...

    async def statfs(self, ctx):
        logger.debug("statfs called")
        stat_ = pyfuse3.StatvfsData()
        stat_.f_bsize = BLOCK_SIZE
        stat_.f_frsize = BLOCK_SIZE
        stat_.f_blocks = 1024 * 1024
        stat_.f_bfree = 1024 * 512
        stat_.f_bavail = 1024 * 512
        stat_.f_files = 1024 * 1024
        stat_.f_ffree = 1024 * 512
        stat_.f_favail = 1024 * 512
    # Assign f_flag only if it exists
        if hasattr(stat_, 'f_flag'):
            stat_.f_flag = 0
        stat_.f_namemax = 255
        return stat_

    async def access(self, inode, mode, ctx):
        """Check file access permissions"""
        path = self.inode_to_path.get(inode)
        if path is None:
            logger.warning(f"access: inode {inode} not found")
            raise pyfuse3.FUSEError(errno.ENOENT)
        logger.debug(f"access: {path} mode={mode}")

        try:
            attrs = self.redis.hgetall(f'fs:{path}')
            if not attrs:
                raise pyfuse3.FUSEError(errno.ENOENT)

            file_mode = int(attrs['mode'], 8)
            uid = int(attrs['uid'])
            gid = int(attrs['gid'])

            # Check permissions
            if ctx.uid == 0:
                # Root can access everything
                return
            if ctx.uid == uid:
                user_mode = (file_mode >> 6) & 0o7
            elif ctx.gid == gid:
                user_mode = (file_mode >> 3) & 0o7
            else:
                user_mode = file_mode & 0o7

            if (user_mode & mode) == mode:
                return
            else:
                raise pyfuse3.FUSEError(errno.EACCES)
        except Exception as e:
            logger.error(f"Error in access for {path}: {e}")
            raise

    # Implement getxattr, setxattr, listxattr, removexattr if needed, or return ENOTSUP as before.

def main():
    """Main entry point with proper argument handling"""
    import argparse

    parser = argparse.ArgumentParser(description='Redis-based FUSE filesystem')
    parser.add_argument('mountpoint', help='Where to mount the filesystem')
    parser.add_argument('--redis-host', default='localhost', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--redis-db', type=int, default=0, help='Redis database')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    # Configure logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Ensure mountpoint exists
    mountpoint = os.path.abspath(args.mountpoint)
    if not os.path.exists(mountpoint):
        os.makedirs(mountpoint)

    logger.info(f"Starting RedisFUSE, mounting at {mountpoint}")

    # Initialize filesystem
    operations = RedisFUSE(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db
    )

    # Set up FUSE options
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=redisfs')
    if args.debug:
        fuse_options.add('debug')

    try:
        logger.info("Initializing FUSE")
        pyfuse3.init(operations, mountpoint, list(fuse_options))

        logger.info("Starting FUSE main loop")
        trio.run(pyfuse3.main)

    except Exception as e:
        logger.error(f"Error during FUSE initialization: {e}")
        raise
    finally:
        try:
            logger.info("Attempting to unmount filesystem")
            pyfuse3.close(unmount=True)
        except Exception as e:
            logger.error(f"Error during unmount: {e}")

if __name__ == '__main__':
    main()

