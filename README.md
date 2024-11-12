# RedisFUSE

A distributed FUSE filesystem implementation using Redis as a persistent storage backend. The filesystem supports distributed operations across multiple nodes with Redis serving as the source of truth for both file data and metadata.

## Overview

RedisFUSE implements a fully functional filesystem that stores all data in Redis, enabling distributed access across multiple mount points. The implementation provides:

- Full POSIX filesystem semantics
- Distributed locking for concurrent access
- Real-time change notifications across nodes
- Efficient block-based storage
- Transparent compression for large blocks

## Architecture

### Distributed Design
- All file data and metadata stored in Redis
- Distributed locking using Redis locks
- Real-time change propagation using Redis Pub/Sub
- Local metadata cache with invalidation via Pub/Sub messages

### Data Organization
```
fs:{path}              -> Hash (file/directory metadata)
fs:{path}:block:{num}  -> Hash (file data blocks)
fs:{path}:children     -> List (directory entries)
fs:inode_counter       -> String (global inode counter)
fs:path_to_inode       -> Hash (path to inode mapping)
fs:inode_to_path       -> Hash (inode to path mapping)
```

## Features

- **Distributed Operations**
  - Multiple nodes can mount the same filesystem
  - Consistent view across all mount points
  - Real-time change propagation

- **Performance Optimizations**
  - Block-based storage (64KB blocks)
  - Transparent compression for blocks > 1KB
  - Pipelined Redis operations
  - Local metadata caching with invalidation

- **Full POSIX Support**
  - Standard file operations (read, write, create, delete)
  - Directory operations
  - Permission handling
  - File attributes
  - Hard link support

## Installation

### Prerequisites
```bash
# Ubuntu/Debian
sudo apt-get install python3-pip python3-dev fuse3 libfuse3-dev pkg-config

# CentOS/RHEL
sudo yum install python3-pip python3-devel fuse3 fuse3-devel pkg-config
```

### Python Dependencies
```bash
pip install pyfuse3 redis trio
```

## Usage

### Basic Mount
```bash
python redisfuse.py /mount/point
```

### Advanced Mount Options
```bash
python redisfuse.py /mount/point \
  --redis-host localhost \
  --redis-port 6379 \
  --redis-db 0 \
  --debug
```

## Configuration

### Command Line Options
- `mountpoint`: Filesystem mount point
- `--redis-host`: Redis server hostname (default: localhost)
- `--redis-port`: Redis server port (default: 6379)
- `--redis-db`: Redis database number (default: 0)
- `--debug`: Enable debug logging

### Performance Tuning
```python
BLOCK_SIZE = 64 * 1024  # 64KB blocks
COMPRESSION_THRESHOLD = 1024  # Compress blocks > 1KB
MAX_BLOCKS_PER_READ = 8  # Maximum blocks per read operation
REDIS_BATCH_SIZE = 100  # Redis pipeline batch size
```

## Implementation Details

### Distributed Locking
- Redis-based distributed locks for all write operations
- Automatic lock release with timeouts
- Retry mechanism for lock acquisition

### Change Notification
```python
# Subscribe to changes
await self.subscribe_to_changes()

# Publish changes
self.redis.publish('fs_changes', f'MODIFY_FILE {path}')
```

### Block Management
- File data split into fixed-size blocks
- Transparent compression using zlib
- Efficient block allocation and deallocation
- Pipelined block operations

## Monitoring and Debugging

### Logging
- Comprehensive logging with multiple handlers
- Debug mode for detailed operation tracing
- Performance metrics logging

### Common Operations
```bash
# Mount filesystem
python redisfuse.py /mnt/redisfs

# Monitor filesystem operations
tail -f redisfs.log

# Unmount filesystem
fusermount -u /mnt/redisfs
```

## Error Handling

- Comprehensive error handling for Redis operations
- Automatic retry for transient failures
- Proper cleanup on unmount
- Cache invalidation on errors

## Limitations

- No client-side data caching (all data reads/writes go to Redis)
- Limited to Redis cluster size for total storage
- Network latency affects performance
- File data compression is one-size-fits-all

### Development Setup
```bash
# Setup development environment
python -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt

# Run tests
pytest tests/
```
