import pytest
import fcntl
import time
import multiprocessing
from pathlib import Path
from matterstack.storage.state_store import SQLiteStateStore

def hold_lock(lock_path: Path, event):
    """
    Helper function to hold a lock until event is set.
    """
    print(f"Child: locking {lock_path}")
    with open(lock_path, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        print("Child: locked")
        event.wait()
        print("Child: unlocking")
        fcntl.flock(f, fcntl.LOCK_UN)

def test_lock_basic(tmp_path):
    """
    Test that we can acquire and release the lock.
    """
    db_path = tmp_path / "state.sqlite"
    store = SQLiteStateStore(db_path)
    
    with store.lock():
        assert (tmp_path / "run.lock").exists()

def test_lock_contention(tmp_path):
    """
    Test that we cannot acquire lock if held by another process.
    """
    db_path = tmp_path / "state.sqlite"
    lock_path = tmp_path / "run.lock"
    
    # Initialize store (creates directory etc)
    store = SQLiteStateStore(db_path)
    
    # Start a child process that holds the lock
    event = multiprocessing.Event()
    p = multiprocessing.Process(target=hold_lock, args=(lock_path, event))
    p.start()
    
    # Wait for child to acquire lock (busy wait is ugly but simple for test)
    # We can't easily signal "I have the lock" without another queue/event
    # So we'll just wait a bit.
    time.sleep(0.5) 
    
    try:
        # Try to acquire lock in main process
        with pytest.raises(RuntimeError, match="Could not acquire lock"):
            with store.lock():
                pass
    finally:
        # Clean up child
        event.set()
        p.join()

def test_lock_release(tmp_path):
    """
    Test that lock is released after block.
    """
    db_path = tmp_path / "state.sqlite"
    store = SQLiteStateStore(db_path)
    
    with store.lock():
        pass
        
    # Should be able to acquire again
    with store.lock():
        pass