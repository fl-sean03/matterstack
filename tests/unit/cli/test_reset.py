"""Characterization tests for CLI reset cascade.

These tests capture existing behavior of BFS traversal for cascade reset
and get_dependents() to prevent regressions during refactoring.
"""

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Set
from unittest.mock import MagicMock, Mock, patch

import pytest

from matterstack.cli.reset import get_dependents
from matterstack.core.run import RunHandle
from matterstack.core.workflow import Task, Workflow
from matterstack.storage.state_store import SQLiteStateStore


@pytest.fixture
def temp_store():
    """Create a temporary SQLite store for testing."""
    with TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = SQLiteStateStore(db_path)
        yield store


@pytest.fixture
def store_with_run(temp_store):
    """Create a store with a run ready for task addition."""
    run_id = "test_run_001"
    root_path = Path("/tmp/test")

    handle = RunHandle(
        run_id=run_id,
        workspace_slug="test_workspace",
        root_path=root_path,
    )
    temp_store.create_run(handle)

    return temp_store, run_id


class TestGetDependents:
    """Tests for get_dependents() BFS traversal."""

    def test_returns_empty_for_no_dependents(self, store_with_run):
        """Should return empty set when task has no dependents."""
        store, run_id = store_with_run

        # Create standalone tasks (no dependencies between them)
        task1 = Task(task_id="task_001", image="test:latest", command="echo 1")
        task2 = Task(task_id="task_002", image="test:latest", command="echo 2")
        task3 = Task(task_id="task_003", image="test:latest", command="echo 3")
        
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        workflow.add_task(task3)
        store.add_workflow(workflow, run_id)

        dependents = get_dependents(store, run_id, "task_001")

        assert dependents == set()

    def test_finds_immediate_dependents(self, store_with_run):
        """Should find tasks that directly depend on target."""
        store, run_id = store_with_run

        # Create linear dependency: task_001 <- task_002
        task1 = Task(task_id="task_001", image="test:latest", command="echo 1")
        task2 = Task(task_id="task_002", image="test:latest", command="echo 2", dependencies={"task_001"})
        
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        store.add_workflow(workflow, run_id)

        dependents = get_dependents(store, run_id, "task_001")

        assert dependents == {"task_002"}

    def test_finds_transitive_dependents(self, store_with_run):
        """Should find transitive dependents via BFS."""
        store, run_id = store_with_run

        # Create chain: task_001 <- task_002 <- task_003
        task1 = Task(task_id="task_001", image="test:latest", command="echo 1")
        task2 = Task(task_id="task_002", image="test:latest", command="echo 2", dependencies={"task_001"})
        task3 = Task(task_id="task_003", image="test:latest", command="echo 3", dependencies={"task_002"})
        
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        workflow.add_task(task3)
        store.add_workflow(workflow, run_id)

        dependents = get_dependents(store, run_id, "task_001")

        assert dependents == {"task_002", "task_003"}

    def test_handles_diamond_dependencies(self, store_with_run):
        """Should handle diamond-shaped dependency graphs correctly."""
        store, run_id = store_with_run

        # Diamond: task_001 <- (task_002, task_003) <- task_004
        #          A
        #         / \
        #        B   C
        #         \ /
        #          D
        task1 = Task(task_id="task_001", image="test:latest", command="echo A")
        task2 = Task(task_id="task_002", image="test:latest", command="echo B", dependencies={"task_001"})
        task3 = Task(task_id="task_003", image="test:latest", command="echo C", dependencies={"task_001"})
        task4 = Task(task_id="task_004", image="test:latest", command="echo D", dependencies={"task_002", "task_003"})
        
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        workflow.add_task(task3)
        workflow.add_task(task4)
        store.add_workflow(workflow, run_id)

        dependents = get_dependents(store, run_id, "task_001")

        # All downstream tasks should be found
        assert dependents == {"task_002", "task_003", "task_004"}

    def test_handles_multiple_roots(self, store_with_run):
        """Should handle tasks with multiple root dependencies."""
        store, run_id = store_with_run

        # task_003 depends on both task_001 and task_002
        task1 = Task(task_id="task_001", image="test:latest", command="echo 1")
        task2 = Task(task_id="task_002", image="test:latest", command="echo 2")
        task3 = Task(task_id="task_003", image="test:latest", command="echo 3", dependencies={"task_001", "task_002"})
        
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        workflow.add_task(task3)
        store.add_workflow(workflow, run_id)

        # Getting dependents of task_001
        dependents = get_dependents(store, run_id, "task_001")
        assert dependents == {"task_003"}

        # Getting dependents of task_002
        dependents = get_dependents(store, run_id, "task_002")
        assert dependents == {"task_003"}

    def test_handles_wide_fanout(self, store_with_run):
        """Should handle task with many immediate dependents."""
        store, run_id = store_with_run

        # task_001 has many dependents
        task1 = Task(task_id="task_001", image="test:latest", command="echo 1")
        
        workflow = Workflow()
        workflow.add_task(task1)
        
        expected_dependents = set()
        for i in range(2, 12):  # 10 dependents
            task = Task(task_id=f"task_{i:03d}", image="test:latest", command=f"echo {i}", dependencies={"task_001"})
            workflow.add_task(task)
            expected_dependents.add(f"task_{i:03d}")
        
        store.add_workflow(workflow, run_id)

        dependents = get_dependents(store, run_id, "task_001")

        assert dependents == expected_dependents

    def test_handles_deep_chain(self, store_with_run):
        """Should handle long dependency chains."""
        store, run_id = store_with_run

        # Create a chain of 10 tasks
        workflow = Workflow()
        prev_task_id = None
        expected_dependents = set()
        
        for i in range(1, 11):
            task_id = f"task_{i:03d}"
            if prev_task_id:
                task = Task(task_id=task_id, image="test:latest", command=f"echo {i}", dependencies={prev_task_id})
            else:
                task = Task(task_id=task_id, image="test:latest", command=f"echo {i}")
            workflow.add_task(task)
            
            if i > 1:  # All tasks except the first are dependents of task_001
                expected_dependents.add(task_id)
            
            prev_task_id = task_id
        
        store.add_workflow(workflow, run_id)

        dependents = get_dependents(store, run_id, "task_001")

        assert dependents == expected_dependents

    def test_does_not_include_self(self, store_with_run):
        """Should not include the target task itself in dependents."""
        store, run_id = store_with_run

        task1 = Task(task_id="task_001", image="test:latest", command="echo 1")
        task2 = Task(task_id="task_002", image="test:latest", command="echo 2", dependencies={"task_001"})
        
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        store.add_workflow(workflow, run_id)

        dependents = get_dependents(store, run_id, "task_001")

        assert "task_001" not in dependents

    def test_handles_nonexistent_task(self, store_with_run):
        """Should return empty set for non-existent task."""
        store, run_id = store_with_run

        task1 = Task(task_id="task_001", image="test:latest", command="echo 1")
        workflow = Workflow()
        workflow.add_task(task1)
        store.add_workflow(workflow, run_id)

        dependents = get_dependents(store, run_id, "nonexistent_task")

        assert dependents == set()

    def test_handles_empty_workflow(self, store_with_run):
        """Should return empty set for empty workflow."""
        store, run_id = store_with_run

        # No tasks added
        dependents = get_dependents(store, run_id, "task_001")

        assert dependents == set()


class TestGetDependentsEdgeCases:
    """Edge case tests for get_dependents()."""

    def test_handles_task_with_no_dependencies(self, store_with_run):
        """Leaf tasks (no dependencies) should still be traversed."""
        store, run_id = store_with_run

        # task_001 <- task_002
        # task_003 is independent but should not be returned for task_001
        task1 = Task(task_id="task_001", image="test:latest", command="echo 1")
        task2 = Task(task_id="task_002", image="test:latest", command="echo 2", dependencies={"task_001"})
        task3 = Task(task_id="task_003", image="test:latest", command="echo 3")
        
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        workflow.add_task(task3)
        store.add_workflow(workflow, run_id)

        dependents = get_dependents(store, run_id, "task_001")

        assert dependents == {"task_002"}
        assert "task_003" not in dependents

    def test_handles_partial_dependency_graph(self, store_with_run):
        """Should handle graphs where some tasks have dependencies and others don't."""
        store, run_id = store_with_run

        # Mixed graph:
        # task_001 <- task_002 <- task_004
        # task_003 (independent)
        task1 = Task(task_id="task_001", image="test:latest", command="echo 1")
        task2 = Task(task_id="task_002", image="test:latest", command="echo 2", dependencies={"task_001"})
        task3 = Task(task_id="task_003", image="test:latest", command="echo 3")
        task4 = Task(task_id="task_004", image="test:latest", command="echo 4", dependencies={"task_002"})
        
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        workflow.add_task(task3)
        workflow.add_task(task4)
        store.add_workflow(workflow, run_id)

        dependents = get_dependents(store, run_id, "task_001")

        assert dependents == {"task_002", "task_004"}

    def test_middle_of_chain_dependents(self, store_with_run):
        """Getting dependents of a middle task should return only downstream tasks."""
        store, run_id = store_with_run

        # Chain: task_001 <- task_002 <- task_003 <- task_004
        task1 = Task(task_id="task_001", image="test:latest", command="echo 1")
        task2 = Task(task_id="task_002", image="test:latest", command="echo 2", dependencies={"task_001"})
        task3 = Task(task_id="task_003", image="test:latest", command="echo 3", dependencies={"task_002"})
        task4 = Task(task_id="task_004", image="test:latest", command="echo 4", dependencies={"task_003"})
        
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        workflow.add_task(task3)
        workflow.add_task(task4)
        store.add_workflow(workflow, run_id)

        # Dependents of task_002 should only be downstream (task_003, task_004)
        dependents = get_dependents(store, run_id, "task_002")

        assert dependents == {"task_003", "task_004"}
        assert "task_001" not in dependents

    def test_leaf_task_has_no_dependents(self, store_with_run):
        """Leaf tasks (end of chain) should have no dependents."""
        store, run_id = store_with_run

        # Chain: task_001 <- task_002 <- task_003
        task1 = Task(task_id="task_001", image="test:latest", command="echo 1")
        task2 = Task(task_id="task_002", image="test:latest", command="echo 2", dependencies={"task_001"})
        task3 = Task(task_id="task_003", image="test:latest", command="echo 3", dependencies={"task_002"})
        
        workflow = Workflow()
        workflow.add_task(task1)
        workflow.add_task(task2)
        workflow.add_task(task3)
        store.add_workflow(workflow, run_id)

        # task_003 is a leaf task, no dependents
        dependents = get_dependents(store, run_id, "task_003")

        assert dependents == set()


class TestBFSTraversalBehavior:
    """Tests specifically for BFS traversal behavior."""

    def test_visits_each_node_once(self, store_with_run):
        """BFS should visit each dependent only once even in complex graphs."""
        store, run_id = store_with_run

        # Complex graph with multiple paths to the same node:
        #       A
        #      / \
        #     B   C
        #    / \ / \
        #   D   E   F
        #    \ | /
        #      G
        task_a = Task(task_id="task_A", image="test:latest", command="echo A")
        task_b = Task(task_id="task_B", image="test:latest", command="echo B", dependencies={"task_A"})
        task_c = Task(task_id="task_C", image="test:latest", command="echo C", dependencies={"task_A"})
        task_d = Task(task_id="task_D", image="test:latest", command="echo D", dependencies={"task_B"})
        task_e = Task(task_id="task_E", image="test:latest", command="echo E", dependencies={"task_B", "task_C"})
        task_f = Task(task_id="task_F", image="test:latest", command="echo F", dependencies={"task_C"})
        task_g = Task(task_id="task_G", image="test:latest", command="echo G", dependencies={"task_D", "task_E", "task_F"})
        
        workflow = Workflow()
        workflow.add_task(task_a)
        workflow.add_task(task_b)
        workflow.add_task(task_c)
        workflow.add_task(task_d)
        workflow.add_task(task_e)
        workflow.add_task(task_f)
        workflow.add_task(task_g)
        store.add_workflow(workflow, run_id)

        dependents = get_dependents(store, run_id, "task_A")

        # All downstream tasks should be found exactly once
        assert dependents == {"task_B", "task_C", "task_D", "task_E", "task_F", "task_G"}

    def test_handles_parallel_branches(self, store_with_run):
        """Should handle parallel independent branches correctly."""
        store, run_id = store_with_run

        # Two parallel chains from the same root:
        # A <- B <- C
        # A <- D <- E
        task_a = Task(task_id="task_A", image="test:latest", command="echo A")
        task_b = Task(task_id="task_B", image="test:latest", command="echo B", dependencies={"task_A"})
        task_c = Task(task_id="task_C", image="test:latest", command="echo C", dependencies={"task_B"})
        task_d = Task(task_id="task_D", image="test:latest", command="echo D", dependencies={"task_A"})
        task_e = Task(task_id="task_E", image="test:latest", command="echo E", dependencies={"task_D"})
        
        workflow = Workflow()
        workflow.add_task(task_a)
        workflow.add_task(task_b)
        workflow.add_task(task_c)
        workflow.add_task(task_d)
        workflow.add_task(task_e)
        store.add_workflow(workflow, run_id)

        dependents = get_dependents(store, run_id, "task_A")

        assert dependents == {"task_B", "task_C", "task_D", "task_E"}

        # Getting dependents of B should only return C (not D or E)
        dependents_b = get_dependents(store, run_id, "task_B")
        assert dependents_b == {"task_C"}

        # Getting dependents of D should only return E (not B or C)
        dependents_d = get_dependents(store, run_id, "task_D")
        assert dependents_d == {"task_E"}
