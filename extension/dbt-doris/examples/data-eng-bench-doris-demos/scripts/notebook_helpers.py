import csv
import html
import os
import re
import shutil
import subprocess
import time
from io import StringIO
from pathlib import Path

from IPython.display import HTML, display


ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
DBT_SUMMARY = re.compile(
    r"Done\. PASS=(\d+) WARN=(\d+) ERROR=(\d+) SKIP=(\d+) .*? TOTAL=(\d+)"
)

STYLES = """
<style>
.doris-status {
  border: 1px solid #d9dee5;
  border-left: 4px solid #6b7280;
  border-radius: 4px;
  background: #f8fafc;
  padding: 14px 16px;
  margin: 8px 0 12px;
  color: #18212f;
}
.doris-status.running { border-left-color: #d97706; background: #fffbeb; }
.doris-status.success { border-left-color: #16803c; background: #f0fdf4; }
.doris-status.failure { border-left-color: #c62828; background: #fff5f5; }
.doris-status-title { font-size: 16px; font-weight: 650; margin-bottom: 7px; }
.doris-meta { display: flex; flex-wrap: wrap; gap: 7px; }
.doris-chip {
  display: inline-block;
  border: 1px solid #cbd5e1;
  border-radius: 4px;
  background: #ffffff;
  padding: 3px 8px;
  font-size: 12px;
  color: #334155;
}
.doris-result { margin: 10px 0 18px; }
.doris-result-title { font-size: 14px; font-weight: 650; margin: 0 0 6px; color: #18212f; }
.doris-result-count { color: #64748b; font-size: 12px; font-weight: 400; margin-left: 6px; }
table.doris-table { border-collapse: collapse; width: auto; min-width: 420px; font-size: 13px; }
table.doris-table th {
  background: #eef2f6;
  border: 1px solid #cbd5e1;
  color: #1f2937;
  padding: 7px 10px;
  text-align: left;
}
table.doris-table td { border: 1px solid #d9dee5; padding: 7px 10px; }
table.doris-table tbody tr:nth-child(even) { background: #f8fafc; }
details.doris-log { margin: 4px 0 16px; color: #475569; }
details.doris-log summary { cursor: pointer; font-size: 12px; user-select: none; }
details.doris-log pre {
  max-height: 360px;
  overflow: auto;
  white-space: pre-wrap;
  border: 1px solid #d9dee5;
  border-radius: 4px;
  background: #111827;
  color: #e5e7eb;
  padding: 12px;
  font-size: 11px;
  line-height: 1.45;
}
</style>
"""


def find_repo_root(start):
    for candidate in (start, *start.parents):
        if (candidate / "extension/dbt-doris/examples").is_dir():
            return candidate
    raise FileNotFoundError("请从 Apache Doris 仓库目录或其子目录启动 Jupyter。")


def clean_log(output):
    return ANSI_ESCAPE.sub("", output).strip()


class DemoRunner:
    def __init__(self, start=None):
        self.repo_root = find_repo_root((start or Path.cwd()).resolve())
        self.examples_root = self.repo_root / "extension/dbt-doris/examples"
        self.dbt_bin = os.environ.get("DBT_BIN") or shutil.which("dbt")
        self.mysql_bin = os.environ.get("MYSQL_BIN") or shutil.which("mysql")
        if not self.dbt_bin:
            raise RuntimeError("找不到 dbt，请在启动 Jupyter 前设置 DBT_BIN。")
        if not self.mysql_bin:
            raise RuntimeError("找不到 mysql，请在服务器安装 MySQL Client 或设置 MYSQL_BIN。")

        self.env = os.environ.copy()
        self.env.update(
            {
                "DBT_BIN": self.dbt_bin,
                "MYSQL_BIN": self.mysql_bin,
                "DORIS_HOST": os.environ.get("DORIS_HOST", "127.0.0.1"),
                "DORIS_PORT": os.environ.get("DORIS_PORT", "9030"),
                "DORIS_USER": os.environ.get("DORIS_USER", "root"),
                "DORIS_PASSWORD": os.environ.get("DORIS_PASSWORD", ""),
            }
        )
        if self.env["DORIS_PASSWORD"]:
            self.env["MYSQL_PWD"] = self.env["DORIS_PASSWORD"]
        display(HTML(STYLES))

    def _run(self, command, cwd=None):
        return subprocess.run(
            command,
            cwd=cwd,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )

    def _mysql_command(self, sql):
        return [
            self.mysql_bin,
            "--batch",
            "--raw",
            "-h",
            self.env["DORIS_HOST"],
            "-P",
            self.env["DORIS_PORT"],
            "-u",
            self.env["DORIS_USER"],
            "-e",
            sql,
        ]

    @staticmethod
    def _status(kind, title, chips):
        chip_html = "".join(
            f'<span class="doris-chip">{html.escape(str(chip))}</span>' for chip in chips
        )
        return HTML(
            f'<div class="doris-status {kind}">'
            f'<div class="doris-status-title">{html.escape(title)}</div>'
            f'<div class="doris-meta">{chip_html}</div></div>'
        )

    @staticmethod
    def _log_details(output, opened=False):
        open_attribute = " open" if opened else ""
        return HTML(
            f'<details class="doris-log"{open_attribute}>'
            '<summary>查看完整运行日志</summary>'
            f'<pre>{html.escape(clean_log(output))}</pre></details>'
        )

    def show_environment(self):
        result = self._run([self.dbt_bin, "--version"])
        if result.returncode != 0:
            display(self._status("failure", "环境检查失败", ["dbt --version", f"exit {result.returncode}"]))
            display(self._log_details(result.stdout, opened=True))
            raise RuntimeError("dbt 环境检查失败。")

        installed = re.search(r"installed:\s*([^\s]+)", clean_log(result.stdout))
        dbt_version = installed.group(1) if installed else "可执行"
        display(
            self._status(
                "success",
                "执行环境已就绪",
                [
                    f"dbt Core {dbt_version}",
                    f"Doris {self.env['DORIS_HOST']}:{self.env['DORIS_PORT']}",
                    f"Repository {self.repo_root.name}",
                ],
            )
        )
        self.query(
            "Doris Backend",
            "show backends",
            columns=["Host", "Alive", "Version", "TabletNum"],
        )

    def run_demo(self, title, relative_path):
        project_dir = self.examples_root / relative_path
        run_script = project_dir / "scripts/run.sh"
        if not run_script.is_file():
            raise FileNotFoundError(run_script)

        handle = display(
            self._status("running", f"正在运行：{title}", ["准备 fixture", "执行 dbt", "运行 verifier"]),
            display_id=True,
        )
        started = time.perf_counter()
        result = self._run([str(run_script)], cwd=project_dir)
        elapsed = time.perf_counter() - started
        summaries = DBT_SUMMARY.findall(clean_log(result.stdout))
        passed = sum(int(summary[0]) for summary in summaries)
        errors = sum(int(summary[2]) for summary in summaries)

        if result.returncode != 0:
            handle.update(
                self._status(
                    "failure",
                    f"运行失败：{title}",
                    [f"{elapsed:.1f} 秒", f"exit {result.returncode}", f"dbt errors {errors}"],
                )
            )
            display(self._log_details(result.stdout, opened=True))
            raise RuntimeError(f"{title} 执行失败。")

        handle.update(
            self._status(
                "success",
                f"运行通过：{title}",
                [f"{elapsed:.1f} 秒", f"{len(summaries)} 个 dbt 阶段", f"{passed} 个成功节点", "verifier 通过"],
            )
        )
        display(self._log_details(result.stdout))

    def query(self, title, sql, columns=None):
        result = self._run(self._mysql_command(sql))
        if result.returncode != 0:
            display(self._status("failure", f"查询失败：{title}", [f"exit {result.returncode}"]))
            display(self._log_details(result.stdout, opened=True))
            raise RuntimeError(f"Doris 查询失败：{title}")

        rows = list(csv.reader(StringIO(result.stdout), delimiter="\t"))
        if not rows:
            display(self._status("success", title, ["0 行"]))
            return

        headers = rows[0]
        data = rows[1:]
        if columns:
            missing = [column for column in columns if column not in headers]
            if missing:
                raise ValueError(f"查询结果缺少列：{', '.join(missing)}")
            indexes = [headers.index(column) for column in columns]
            headers = columns
            data = [[row[index] for index in indexes] for row in data]

        header_html = "".join(f"<th>{html.escape(value)}</th>" for value in headers)
        body_html = "".join(
            "<tr>" + "".join(f"<td>{html.escape(value)}</td>" for value in row) + "</tr>"
            for row in data
        )
        display(
            HTML(
                '<div class="doris-result">'
                f'<div class="doris-result-title">{html.escape(title)}'
                f'<span class="doris-result-count">{len(data)} 行</span></div>'
                f'<table class="doris-table"><thead><tr>{header_html}</tr></thead>'
                f'<tbody>{body_html}</tbody></table></div>'
            )
        )
