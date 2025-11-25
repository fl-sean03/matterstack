from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Optional, Sequence
from pathlib import PurePosixPath

try:
    import paramiko  # type: ignore[import]
except ImportError:  # pragma: no cover - optional dependency for HPC backends
    # paramiko is optional at import time so that local-only workflows and tests
    # that do not touch HPC backends can run without it. An informative error is
    # raised if SSHClient.connect is actually used.
    paramiko = None  # type: ignore[assignment]

@dataclass
class CommandResult:
    """Result of a remote command run over SSH."""
    stdout: str
    stderr: str
    exit_status: int

@dataclass
class SSHConfig:
    """Configuration for SSH connection."""
    host: str
    user: str
    port: int = 22
    key_path: Optional[str] = None

class SSHClient:
    """
    Wrapper around paramiko.SSHClient with async interface.
    """

    def __init__(self, client: paramiko.SSHClient) -> None:
        self._client = client
        self._sftp: Optional[paramiko.SFTPClient] = None

    @classmethod
    async def connect(cls, config: SSHConfig) -> "SSHClient":
        """
        Open an SSH connection.
        
        Raises:
            ImportError: If paramiko is not installed.
            RuntimeError: If connection fails (authentication, host key, or network issues).
        """
        if paramiko is None:  # type: ignore[truthy-function]
            raise ImportError(
                "paramiko is required to use SSHClient and SlurmBackend. "
                "Install the 'paramiko' extra to enable HPC backends."
            )

        def _connect():
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            key_filename = None
            if config.key_path:
                expanded_path = os.path.expanduser(config.key_path)
                if os.path.exists(expanded_path):
                    key_filename = expanded_path

            try:
                client.connect(
                    hostname=config.host,
                    port=config.port,
                    username=config.user,
                    key_filename=key_filename,
                    timeout=30.0,  # Default timeout
                )
            except paramiko.AuthenticationException as e:
                raise RuntimeError(
                    f"SSH authentication failed for {config.user}@{config.host}. "
                    "Please check your credentials or SSH key."
                ) from e
            except paramiko.BadHostKeyException as e:
                raise RuntimeError(
                    f"SSH host key verification failed for {config.host}. "
                    "Host key may have changed."
                ) from e
            except (paramiko.SSHException, OSError) as e:
                raise RuntimeError(
                    f"Failed to connect to {config.host}: {str(e)}"
                ) from e
            
            return client

        client = await asyncio.to_thread(_connect)
        return cls(client)

    async def _ensure_sftp(self) -> paramiko.SFTPClient:
        if self._sftp is None:
            self._sftp = await asyncio.to_thread(self._client.open_sftp)
        return self._sftp

    async def close(self) -> None:
        """Close underlying SSH/SFTP connections."""
        def _close():
            if self._sftp is not None:
                self._sftp.close()
            self._client.close()
        
        await asyncio.to_thread(_close)

    async def run(self, command: str, *, cwd: Optional[str] = None) -> CommandResult:
        """
        Run a shell command on the remote host.
        
        Raises:
            RuntimeError: If the command execution fails due to SSH connection issues.
        """
        if cwd:
            import shlex
            full_cmd = f"cd {shlex.quote(cwd)} && {command}"
        else:
            full_cmd = command

        def _exec():
            try:
                # exec_command returns (stdin, stdout, stderr)
                # Note: exec_command does not block, but reading from the channels will.
                stdin, stdout, stderr = self._client.exec_command(full_cmd)
                
                # Wait for command to complete and get exit status
                exit_status = stdout.channel.recv_exit_status()
                
                # Read output
                out_str = stdout.read().decode('utf-8', errors='replace')
                err_str = stderr.read().decode('utf-8', errors='replace')
                
                return out_str, err_str, exit_status
            except paramiko.SSHException as e:
                raise RuntimeError(f"SSH connection lost during command execution: {command}") from e
            except Exception as e:
                raise RuntimeError(f"Unexpected error executing command '{command}': {str(e)}") from e

        stdout, stderr, exit_status = await asyncio.to_thread(_exec)
        return CommandResult(
            stdout=stdout,
            stderr=stderr,
            exit_status=exit_status,
        )

    async def mkdir_p(self, path: str) -> None:
        """
        Recursively create a directory path, like `mkdir -p`.
        
        Raises:
            IOError: If directory creation fails (e.g. permission denied) and it's not because it already exists.
        """
        sftp = await self._ensure_sftp()
        pure = PurePosixPath(path)
        parts: Sequence[str] = pure.parts

        cur = PurePosixPath("/")
        
        def _mkdir_recursive():
            nonlocal cur
            for part in parts:
                if part == "/":
                    continue
                cur = cur / part
                try:
                    sftp.stat(str(cur))
                except IOError:  # FileNotFoundError is a subclass of IOError in paramiko usually, or mapped
                    try:
                        sftp.mkdir(str(cur))
                    except IOError as e:
                        # Check if it was created concurrently
                        try:
                            sftp.stat(str(cur))
                        except IOError:
                            # It really failed
                            raise IOError(f"Failed to create directory {cur}: {e}") from e

        await asyncio.to_thread(_mkdir_recursive)

    async def write_text(self, path: str, content: str) -> None:
        """
        Write UTF-8 text to a remote file, creating parent directories as needed.
        """
        sftp = await self._ensure_sftp()
        pure = PurePosixPath(path)
        parent = pure.parent
        if parent and str(parent) not in ("", "."):
            await self.mkdir_p(str(parent))

        def _write():
            try:
                with sftp.open(str(pure), "w") as f:
                    f.write(content)
            except IOError as e:
                raise IOError(f"Failed to write text to {path}: {e}") from e
                
        await asyncio.to_thread(_write)

    async def read_bytes(
        self,
        path: str,
        *,
        offset: Optional[int] = None,
        max_bytes: Optional[int] = None,
    ) -> bytes:
        """
        Read bytes from a file.
        
        Raises:
            IOError: If file cannot be read.
        """
        sftp = await self._ensure_sftp()
        
        def _read():
            try:
                with sftp.open(path, "rb") as f:
                    if offset:
                        f.seek(offset)
                    if max_bytes is None:
                        return f.read()
                    return f.read(max_bytes)
            except IOError as e:
                raise IOError(f"Failed to read bytes from {path}: {e}") from e

        return await asyncio.to_thread(_read)

    async def get(self, remote_path: str, local_path: str, recursive: bool = False) -> None:
        """
        Download a file or directory from the remote host.
        If recursive is True, downloads a directory.
        
        Raises:
            IOError: If download fails.
        """
        sftp = await self._ensure_sftp()
        
        def _get():
            if recursive:
                # Check if remote is dir
                try:
                    r_stat = sftp.stat(remote_path)
                    import stat
                    if not stat.S_ISDIR(r_stat.st_mode):
                        # Not a dir, just get it
                        sftp.get(remote_path, local_path)
                        return
                except IOError as e:
                    # File not found or permission denied
                    raise IOError(f"Failed to stat remote path {remote_path}: {e}") from e

                # It is a dir. Ensure local dir exists
                os.makedirs(local_path, exist_ok=True)
                
                # Stack for iteration: (remote_dir, local_dir)
                stack = [(remote_path, local_path)]
                
                while stack:
                    curr_r, curr_l = stack.pop()
                    # Ensure local dir
                    if not os.path.exists(curr_l):
                        try:
                            os.makedirs(curr_l)
                        except OSError as e:
                            raise IOError(f"Failed to create local directory {curr_l}: {e}") from e
                        
                    try:
                        file_list = sftp.listdir_attr(curr_r)
                    except IOError as e:
                        raise IOError(f"Failed to list remote directory {curr_r}: {e}") from e

                    for attr in file_list:
                        r_item = str(PurePosixPath(curr_r) / attr.filename)
                        l_item = os.path.join(curr_l, attr.filename)
                        
                        if stat.S_ISDIR(attr.st_mode):
                            stack.append((r_item, l_item))
                        else:
                            try:
                                sftp.get(r_item, l_item)
                            except IOError as e:
                                raise IOError(f"Failed to download {r_item} to {l_item}: {e}") from e
            else:
                try:
                    sftp.get(remote_path, local_path)
                except IOError as e:
                    raise IOError(f"Failed to download {remote_path} to {local_path}: {e}") from e

        await asyncio.to_thread(_get)

    async def put(self, local_path: str, remote_path: str, recursive: bool = False) -> None:
        """
        Upload a file or directory to the remote host.
        If recursive is True, uploads a directory.
        
        Raises:
            IOError: If upload fails.
        """
        sftp = await self._ensure_sftp()
        
        # Ensure parent remote directory exists
        parent = str(PurePosixPath(remote_path).parent)
        if parent and parent not in (".", "/"):
             await self.mkdir_p(parent)

        def _put():
            if recursive:
                 if not os.path.isdir(local_path):
                     # Not a dir, just put it
                     try:
                         sftp.put(local_path, remote_path)
                     except IOError as e:
                         raise IOError(f"Failed to upload {local_path} to {remote_path}: {e}") from e
                     return

                 # It is a dir. Ensure remote dir exists
                 try:
                     sftp.mkdir(remote_path)
                 except IOError:
                     # Check if exists
                     try:
                         sftp.stat(remote_path)
                     except IOError as e:
                         raise IOError(f"Failed to create remote directory {remote_path}: {e}") from e
                 
                 # Walk local dir
                 for root, dirs, files in os.walk(local_path):
                     # Calculate relative path to mirror structure
                     rel_root = os.path.relpath(root, local_path)
                     remote_root = str(PurePosixPath(remote_path) / rel_root)
                     if rel_root == ".":
                         remote_root = remote_path
                    
                     # Create directories
                     for d in dirs:
                         r_dir = str(PurePosixPath(remote_root) / d)
                         try:
                             sftp.mkdir(r_dir)
                         except IOError:
                             try:
                                 sftp.stat(r_dir)
                             except IOError as e:
                                 raise IOError(f"Failed to create remote directory {r_dir}: {e}") from e
                     
                     # Upload files
                     for f in files:
                         l_file = os.path.join(root, f)
                         r_file = str(PurePosixPath(remote_root) / f)
                         try:
                            sftp.put(l_file, r_file)
                         except IOError as e:
                             raise IOError(f"Failed to upload {l_file} to {r_file}: {e}") from e

            else:
                try:
                    sftp.put(local_path, remote_path)
                except IOError as e:
                    raise IOError(f"Failed to upload {local_path} to {remote_path}: {e}") from e

        await asyncio.to_thread(_put)
