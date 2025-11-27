import pytest
from pathlib import Path
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from matterstack.storage.state_store import SQLiteStateStore, CURRENT_SCHEMA_VERSION
from matterstack.storage.schema import Base, SchemaInfo

def test_schema_initialization(tmp_path):
    """
    Test that a fresh database is initialized with the correct schema version.
    """
    db_path = tmp_path / "state.sqlite"
    store = SQLiteStateStore(db_path)
    
    # Check that file exists
    assert db_path.exists()
    
    # Check content manually
    engine = create_engine(f"sqlite:///{db_path}")
    with Session(engine) as session:
        stmt = select(SchemaInfo).where(SchemaInfo.key == "version")
        info = session.scalar(stmt)
        assert info is not None
        assert info.value == CURRENT_SCHEMA_VERSION

def test_schema_check_pass(tmp_path):
    """
    Test that opening an existing valid database passes.
    """
    db_path = tmp_path / "state.sqlite"
    
    # 1. Create and init
    store1 = SQLiteStateStore(db_path)
    
    # 2. Re-open
    store2 = SQLiteStateStore(db_path)
    # Should not raise

def test_schema_mismatch_error(tmp_path):
    """
    Test that a version mismatch raises a RuntimeError.
    """
    db_path = tmp_path / "state.sqlite"
    
    # 1. Create a dummy DB manually with wrong version
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    
    with Session(engine) as session:
        info = SchemaInfo(key="version", value="999")
        session.add(info)
        session.commit()
        
    # 2. Try to open with StateStore
    with pytest.raises(RuntimeError) as excinfo:
        SQLiteStateStore(db_path)
        
    assert "Schema version mismatch" in str(excinfo.value)
    assert "v999" in str(excinfo.value)