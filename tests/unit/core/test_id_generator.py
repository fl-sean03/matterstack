"""
Unit tests for the id_generator module.
"""

import re
import time

from matterstack.core.id_generator import (
    generate_attempt_id,
    generate_chronological_id,
    generate_run_id,
    generate_task_id,
    with_collision_retry,
)

# Regex pattern for chronological IDs: YYYYMMDD_HHMMSS_<8 hex chars>
CHRONOLOGICAL_ID_PATTERN = re.compile(r'^\d{8}_\d{6}_[a-f0-9]{8}$')

# Regex pattern for chronological IDs with prefix
PREFIXED_ID_PATTERN = re.compile(r'^[a-z0-9_]+_\d{8}_\d{6}_[a-f0-9]{8}$')


class TestGenerateChronologicalId:
    """Tests for generate_chronological_id function."""

    def test_format_without_prefix(self):
        """ID matches YYYYMMDD_HHMMSS_uuid8 format."""
        id_ = generate_chronological_id()
        assert CHRONOLOGICAL_ID_PATTERN.match(id_), f"ID {id_} doesn't match expected format"

    def test_format_with_prefix(self):
        """ID with prefix matches prefix_YYYYMMDD_HHMMSS_uuid8 format."""
        id_ = generate_chronological_id(prefix="test")
        assert id_.startswith("test_")
        assert PREFIXED_ID_PATTERN.match(id_), f"ID {id_} doesn't match expected format"

    def test_uniqueness(self):
        """Multiple rapid calls generate unique IDs."""
        ids = [generate_chronological_id() for _ in range(100)]
        assert len(set(ids)) == 100, "Generated duplicate IDs"

    def test_chronological_sorting(self):
        """IDs generated in sequence are different."""
        id1 = generate_chronological_id()
        time.sleep(0.001)  # Ensure different UUID suffix
        id2 = generate_chronological_id()
        # Within same second, sorting is by UUID suffix (random)
        # Across seconds, sorting is by timestamp
        assert id1 != id2

    def test_timestamp_component_is_valid(self):
        """Timestamp component represents a valid date/time."""
        id_ = generate_chronological_id()
        date_part = id_[:8]  # YYYYMMDD
        time_part = id_[9:15]  # HHMMSS

        # Validate date format
        year = int(date_part[:4])
        month = int(date_part[4:6])
        day = int(date_part[6:8])

        assert 2020 <= year <= 2100, f"Year {year} out of reasonable range"
        assert 1 <= month <= 12, f"Month {month} out of range"
        assert 1 <= day <= 31, f"Day {day} out of range"

        # Validate time format
        hour = int(time_part[:2])
        minute = int(time_part[2:4])
        second = int(time_part[4:6])

        assert 0 <= hour <= 23, f"Hour {hour} out of range"
        assert 0 <= minute <= 59, f"Minute {minute} out of range"
        assert 0 <= second <= 59, f"Second {second} out of range"


class TestGenerateRunId:
    """Tests for generate_run_id function."""

    def test_format(self):
        """Run ID matches chronological format."""
        run_id = generate_run_id()
        assert CHRONOLOGICAL_ID_PATTERN.match(run_id), f"Run ID {run_id} doesn't match expected format"

    def test_uniqueness(self):
        """Multiple run IDs are unique."""
        ids = [generate_run_id() for _ in range(50)]
        assert len(set(ids)) == 50, "Generated duplicate run IDs"

    def test_length(self):
        """Run ID has expected length: 8 (date) + 1 (_) + 6 (time) + 1 (_) + 8 (uuid) = 24."""
        run_id = generate_run_id()
        assert len(run_id) == 24, f"Run ID length {len(run_id)} != 24"


class TestGenerateAttemptId:
    """Tests for generate_attempt_id function."""

    def test_format(self):
        """Attempt ID matches chronological format."""
        attempt_id = generate_attempt_id()
        assert CHRONOLOGICAL_ID_PATTERN.match(attempt_id), f"Attempt ID {attempt_id} doesn't match expected format"

    def test_uniqueness(self):
        """Multiple attempt IDs are unique."""
        ids = [generate_attempt_id() for _ in range(50)]
        assert len(set(ids)) == 50, "Generated duplicate attempt IDs"

    def test_length(self):
        """Attempt ID has expected length: 24 characters."""
        attempt_id = generate_attempt_id()
        assert len(attempt_id) == 24, f"Attempt ID length {len(attempt_id)} != 24"


class TestGenerateTaskId:
    """Tests for generate_task_id function."""

    def test_format_without_hint(self):
        """Task ID without hint matches chronological format."""
        task_id = generate_task_id()
        assert CHRONOLOGICAL_ID_PATTERN.match(task_id), f"Task ID {task_id} doesn't match expected format"

    def test_format_with_hint(self):
        """Task ID with hint is properly prefixed."""
        task_id = generate_task_id(hint="equilibrate")
        assert task_id.startswith("equilibrate_")
        assert PREFIXED_ID_PATTERN.match(task_id), f"Task ID {task_id} doesn't match expected format"

    def test_hint_sanitization_lowercase(self):
        """Hint is converted to lowercase."""
        task_id = generate_task_id(hint="UPPERCASE")
        assert task_id.startswith("uppercase_")

    def test_hint_sanitization_spaces(self):
        """Spaces in hint are converted to underscores."""
        task_id = generate_task_id(hint="phase one")
        assert task_id.startswith("phase_one_")

    def test_hint_sanitization_hyphens(self):
        """Hyphens in hint are converted to underscores."""
        task_id = generate_task_id(hint="phase-one")
        assert task_id.startswith("phase_one_")

    def test_hint_sanitization_special_chars(self):
        """Special characters are replaced with underscores and collapsed."""
        task_id = generate_task_id(hint="phase-1!")
        # Should become "phase_1" (! becomes _, collapsed, trailing stripped)
        parts = task_id.split('_')
        # First part should be the sanitized hint
        assert parts[0] == "phase" or parts[0] + "_" + parts[1] == "phase_1"

    def test_hint_truncation(self):
        """Long hints are truncated to 20 characters."""
        long_hint = "a" * 50
        task_id = generate_task_id(hint=long_hint)
        # The prefix before the timestamp should be at most 20 chars
        # Find where the timestamp starts (8 digits after underscore)
        prefix_match = re.match(r'^([a-z0-9_]+)_\d{8}_', task_id)
        assert prefix_match is not None
        prefix = prefix_match.group(1)
        assert len(prefix) <= 20, f"Prefix {prefix} exceeds 20 chars"

    def test_empty_hint_after_sanitization(self):
        """Empty hint after sanitization produces plain chronological ID."""
        task_id = generate_task_id(hint="!!!")
        assert CHRONOLOGICAL_ID_PATTERN.match(task_id), f"Task ID {task_id} should be plain chronological"

    def test_empty_string_hint(self):
        """Empty string hint produces plain chronological ID."""
        task_id = generate_task_id(hint="")
        assert CHRONOLOGICAL_ID_PATTERN.match(task_id)

    def test_uniqueness(self):
        """Multiple task IDs are unique."""
        ids = [generate_task_id() for _ in range(50)]
        assert len(set(ids)) == 50, "Generated duplicate task IDs"

    def test_uniqueness_with_same_hint(self):
        """Multiple task IDs with same hint are unique."""
        ids = [generate_task_id(hint="same_hint") for _ in range(50)]
        assert len(set(ids)) == 50, "Generated duplicate task IDs with same hint"


class TestWithCollisionRetry:
    """Tests for with_collision_retry wrapper."""

    def test_wrapper_returns_callable(self):
        """Wrapper returns a callable function."""
        wrapped = with_collision_retry(generate_run_id)
        assert callable(wrapped)

    def test_wrapper_generates_valid_ids(self):
        """Wrapped function generates valid IDs."""
        wrapped = with_collision_retry(generate_run_id)
        id_ = wrapped()
        assert CHRONOLOGICAL_ID_PATTERN.match(id_), f"Wrapped ID {id_} doesn't match format"

    def test_wrapper_handles_rapid_calls(self):
        """Wrapped function handles rapid sequential calls."""
        wrapped = with_collision_retry(generate_run_id, max_retries=5)
        ids = [wrapped() for _ in range(20)]
        # All should be unique due to UUID suffix
        assert len(set(ids)) == 20, "Wrapped generator produced duplicates"

    def test_wrapper_with_custom_delay(self):
        """Wrapper respects custom delay parameter when collision occurs."""
        # Create a generator that always returns the same value
        def mock_generator():
            return "always_same"

        wrapped = with_collision_retry(mock_generator, max_retries=3, delay_ms=50)

        # First call - should return immediately (no prior ID to compare)
        result1 = wrapped()
        assert result1 == "always_same"

        # Second call - last_id is now "always_same", new_id will also be "always_same"
        # This will trigger retries with delay
        start = time.time()
        result2 = wrapped()
        elapsed = time.time() - start

        # Should have waited at least 150ms (3 retries * 50ms)
        # Using 140ms to account for timing imprecision
        assert elapsed >= 0.14, f"Expected at least 140ms delay, got {elapsed*1000:.1f}ms"
        assert result2 == "always_same"  # Still returns same value after retries

    def test_wrapper_preserves_generator_args(self):
        """Wrapper passes arguments to underlying generator."""
        wrapped = with_collision_retry(generate_task_id)
        task_id = wrapped(hint="test_arg")
        assert task_id.startswith("test_arg_")

    def test_wrapper_max_retries_exhausted(self):
        """Wrapper makes final attempt after retries exhausted."""
        call_count = 0
        def always_same():
            nonlocal call_count
            call_count += 1
            return "always_same"

        wrapped = with_collision_retry(always_same, max_retries=2, delay_ms=1)

        # First call - no collision check, returns immediately
        result1 = wrapped()
        assert result1 == "always_same"
        assert call_count == 1

        # Second call - will retry since last_id == new_id
        # max_retries=2 means: 2 attempts in loop + 1 final = 3 total calls for this invocation
        result2 = wrapped()

        # Total calls should be: 1 (first wrapped call) + 3 (second wrapped call) = 4
        assert call_count == 4, f"Expected 4 calls, got {call_count}"
        assert result2 == "always_same"


class TestIdFormatConsistency:
    """Tests to verify format consistency across all ID types."""

    def test_all_ids_use_same_base_format(self):
        """All ID types use the same YYYYMMDD_HHMMSS_uuid8 base format."""
        run_id = generate_run_id()
        attempt_id = generate_attempt_id()
        task_id = generate_task_id()

        for id_, name in [(run_id, "run"), (attempt_id, "attempt"), (task_id, "task")]:
            assert CHRONOLOGICAL_ID_PATTERN.match(id_), f"{name} ID {id_} doesn't match format"

    def test_all_ids_have_same_length(self):
        """All IDs without prefix have the same length."""
        run_id = generate_run_id()
        attempt_id = generate_attempt_id()
        task_id = generate_task_id()

        assert len(run_id) == len(attempt_id) == len(task_id) == 24

    def test_ids_are_filesystem_safe(self):
        """All generated IDs are safe for use in filesystem paths."""
        for _ in range(20):
            for id_ in [generate_run_id(), generate_attempt_id(), generate_task_id()]:
                # Should only contain alphanumeric and underscore
                assert re.match(r'^[a-z0-9_]+$', id_), f"ID {id_} contains unsafe chars"

    def test_ids_sort_chronologically(self):
        """IDs generated at different times sort in chronological order."""
        id1 = generate_run_id()
        time.sleep(1.1)  # Wait over 1 second to ensure different timestamp
        id2 = generate_run_id()

        # Lexicographic sort should match chronological order
        sorted_ids = sorted([id2, id1])
        assert sorted_ids == [id1, id2], "IDs don't sort chronologically"
