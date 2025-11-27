from __future__ import annotations

from typing import Optional
from ....core.backend import JobState, JobStatus
from .ssh import SSHClient, CommandResult

def _parse_sacct_line(line: str) -> Optional[JobStatus]:
    """
    Parse a single sacct --parsable2 line into a JobStatus.
    Expected format (no header):
        JobID|State|ExitCode|Start|End|Elapsed
    """
    parts = line.strip().split("|")
    if len(parts) < 6:
        return None

    job_id, state_raw, exit_raw, start_raw, end_raw, elapsed_raw = parts[:6]

    state = _normalize_state_from_sacct(state_raw)

    exit_code = None
    if exit_raw:
        # ExitCode looks like "0:0"
        try:
            exit_code = int(exit_raw.split(":", 1)[0])
        except (ValueError, IndexError):
            exit_code = None

    return JobStatus(
        job_id=job_id,
        state=state,
        exit_code=exit_code,
        reason=None
    )


def _map_slurm_state(raw_state: str) -> JobState:
    """
    Map raw Slurm state strings to canonical JobState.
    Handles both long forms (sacct) and short codes (squeue) where unambiguous,
    though squeue typically returns short codes.
    """
    s = raw_state.upper()

    # Map PENDING, REQUEUED -> QUEUED
    if s.startswith("PENDING") or s.startswith("REQUEUED") or s == "PD":
        return JobState.QUEUED
    
    # Map RUNNING, COMPLETING -> RUNNING
    # CG = Completing, R = Running
    if s.startswith("RUNNING") or s.startswith("COMPLETING") or s in {"R", "CG"}:
        return JobState.RUNNING

    # Map COMPLETED -> COMPLETED_OK
    # CD = Completed (squeue)
    if s.startswith("COMPLETED") or s == "CD":
        return JobState.COMPLETED_OK

    # Map FAILED, TIMEOUT, NODE_FAIL, etc. -> COMPLETED_ERROR
    # F = Failed, TO = Timeout, NF = Node Fail, BF = Boot Fail
    error_states = {"FAILED", "TIMEOUT", "NODE_FAIL", "BOOT_FAIL", "OUT_OF_MEMORY", "DEADLINE"}
    error_codes = {"F", "TO", "NF", "BF", "OOM", "DL"}
    
    if any(s.startswith(st) for st in error_states) or s in error_codes:
        return JobState.COMPLETED_ERROR

    # Map CANCELLED* -> CANCELLED
    # CA = Cancelled
    if s.startswith("CANCELLED") or s == "CA":
        return JobState.CANCELLED

    return JobState.UNKNOWN


def _normalize_state_from_sacct(state_raw: str) -> JobState:
    """Map sacct state strings to JobState."""
    return _map_slurm_state(state_raw)


def _normalize_state_from_squeue(code: str) -> JobState:
    """
    Map squeue two-letter state codes to JobState.
    """
    return _map_slurm_state(code)


async def submit_job(ssh: SSHClient, workspace: str, batch_script_rel_path: str) -> str:
    """
    Submit a Slurm job using sbatch and return the job ID.
    Runs: cd <workspace> && sbatch <batch_script_rel_path>
    """
    cmd = f"sbatch {batch_script_rel_path}"
    result: CommandResult = await ssh.run(cmd, cwd=workspace)

    if result.exit_status != 0:
        raise RuntimeError(
            f"sbatch failed with status {result.exit_status}: {result.stderr}"
        )

    # Typical sbatch output: "Submitted batch job 123456"
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        if "Submitted batch job" in line:
            parts = line.split()
            job_id = parts[-1]
            return job_id

    raise RuntimeError(f"Could not parse job ID from sbatch output: {result.stdout!r}")


async def get_job_status(ssh: SSHClient, job_id: str) -> JobStatus:
    """
    Query Slurm for job status using sacct, with squeue as a fallback.
    """
    # First try sacct
    sacct_cmd = (
        f"sacct -j {job_id} --format=JobID,State,ExitCode,Start,End,Elapsed "
        "--parsable2 --noheader"
    )
    sacct_res: CommandResult = await ssh.run(sacct_cmd)

    if sacct_res.exit_status == 0:
        for line in sacct_res.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            status = _parse_sacct_line(line)
            if status:
                return status

    # Fallback to squeue
    sq_cmd = f'squeue -j {job_id} -o "%i|%T|%M|%R" --noheader'
    squeue_res: CommandResult = await ssh.run(sq_cmd)

    if squeue_res.exit_status == 0:
        for line in squeue_res.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 4:
                continue
            jid, state_raw, elapsed_raw, reason = parts[:4]
            state = _normalize_state_from_squeue(state_raw)
            
            return JobStatus(
                job_id=jid,
                state=state,
                reason=reason or None,
                exit_code=None
            )

    # If both sacct and squeue fail, return LOST state
    return JobStatus(
        job_id=job_id,
        state=JobState.LOST,
        reason="Job not found in sacct or squeue",
        exit_code=None
    )


async def get_job_io_paths(ssh: SSHClient, job_id: str) -> dict[str, str]:
    """
    Retrieve StdOut and StdErr paths for a job.
    Tries scontrol first (active/recent), then sacct (history).
    """
    paths = {}

    # 1. Try scontrol (active jobs)
    # Output format is typically `Key=Value` pairs, separated by spaces or newlines.
    cmd = f"scontrol show job {job_id} -o" # -o for one line per job
    res = await ssh.run(cmd)
    
    if res.exit_status == 0:
        # Parse key=value pairs
        # We need to handle quoted values if any, but simplistic split might work for paths
        # scontrol -o usually gives "JobId=... Name=... ... StdOut=/path/to/file ..."
        tokens = res.stdout.strip().split()
        for token in tokens:
            if "=" in token:
                k, v = token.split("=", 1)
                if k == "StdOut":
                    paths["stdout"] = v
                elif k == "StdErr":
                    paths["stderr"] = v
                elif k == "WorkDir":
                    paths["workdir"] = v

    if "stdout" in paths:
        return paths

    # 2. Try sacct (completed jobs)
    # We request wide columns to avoid truncation
    cmd = f"sacct -j {job_id} -o WorkDir%256,StdOut%256,StdErr%256 --parsable2 --noheader"
    res = await ssh.run(cmd)
    
    if res.exit_status == 0:
        lines = res.stdout.strip().splitlines()
        if lines:
            # Use the first line (main job step)
            parts = lines[0].strip().split("|")
            if len(parts) >= 3:
                paths["workdir"] = parts[0]
                paths["stdout"] = parts[1]
                paths["stderr"] = parts[2]
                return paths

    return paths
