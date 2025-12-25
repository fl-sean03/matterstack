import time

from rich import box
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

from matterstack.core.run import RunHandle
from matterstack.storage.state_store import SQLiteStateStore


class CampaignMonitor:
    def __init__(self, handle: RunHandle, poll_interval: float = 1.0):
        self.handle = handle
        self.poll_interval = poll_interval
        self.store = SQLiteStateStore(handle.db_path)
        self.console = Console()

    def get_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(Layout(name="header", size=3), Layout(name="body"), Layout(name="footer", size=3))
        return layout

    def generate_header(self) -> Panel:
        try:
            status = self.store.get_run_status(self.handle.run_id) or "UNKNOWN"
        except Exception:
            status = "DB_ERROR"

        # Color code status
        style = (
            "green"
            if status == "RUNNING"
            else "yellow"
            if status == "PENDING"
            else "red"
            if status == "FAILED"
            else "blue"
        )

        grid = Table.grid(expand=True)
        grid.add_column(justify="left", ratio=1)
        grid.add_column(justify="right", ratio=1)

        grid.add_row(
            f"Workspace: [bold]{self.handle.workspace_slug}[/bold]", f"Run ID: [bold]{self.handle.run_id}[/bold]"
        )

        return Panel(grid, title=f"MatterStack Mission Control - [{style}]{status}[/{style}]", border_style=style)

    def generate_task_table(self) -> Table:
        try:
            tasks = self.store.get_tasks(self.handle.run_id)
        except Exception as e:
            err_table = Table(title="Error fetching tasks")
            err_table.add_row(str(e))
            return err_table

        table = Table(expand=True, border_style="dim", box=box.SIMPLE_HEAD)
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Type")
        table.add_column("Status")
        table.add_column("Info")

        # Sort tasks by ID
        tasks.sort(key=lambda t: t.task_id)

        for task in tasks:
            status = self.store.get_task_status(task.task_id) or "PENDING"

            # Status styling
            status_style = "white"
            if status == "COMPLETED":
                status_style = "green"
            elif status == "FAILED":
                status_style = "red"
            elif status == "RUNNING":
                status_style = "yellow"
            elif status == "WAITING_EXTERNAL":
                status_style = "magenta"

            # Info column
            info = ""
            if status == "WAITING_EXTERNAL":
                ext_run = self.store.get_external_run(task.task_id)
                if ext_run:
                    info = f"{ext_run.operator_type} ({ext_run.status.value})"

            table.add_row(task.task_id, task.__class__.__name__, Text(status, style=status_style), info)

        return table

    def generate_footer(self) -> Panel:
        try:
            status = self.store.get_run_status(self.handle.run_id)
            reason = self.store.get_run_status_reason(self.handle.run_id)
        except Exception:
            status = "UNKNOWN"
            reason = "Database connection error"

        if status == "RUNNING":
            content = Spinner("dots", text=f" Monitor active... {reason if reason else ''}")
        else:
            content = Text(f"Run is {status}. {reason if reason else ''}")

        return Panel(content, title="Status Log", border_style="dim")

    def run(self):
        layout = self.get_layout()

        with Live(layout, refresh_per_second=4, screen=True) as _live:
            while True:
                layout["header"].update(self.generate_header())
                layout["body"].update(self.generate_task_table())
                layout["footer"].update(self.generate_footer())

                time.sleep(self.poll_interval)
