import nox
from nox import session, Session


package = "meta_memcache"
nox.options.sessions = "lint", "format", "types", "tests"
locations = "src", "tests", "noxfile.py", "benchmark.py"
DEFAULT_VERSION = "3.11"
DEFAULT_BENCHMARK_VERSIONS = ["3.12"]
VERSIONS = ["3.12", "3.11", "3.10"]

# Default to uv backend:
nox.options.default_venv_backend = "uv|virtualenv"


@session(python=DEFAULT_VERSION)
def lint(session: Session) -> None:
    """Lint using ruff."""
    args = session.posargs or locations
    session.install("ruff", ".")
    session.run("ruff", "check", *args)


@session(python=DEFAULT_VERSION)
def format(session: Session) -> None:
    """Format check using ruff."""
    args = session.posargs or locations
    session.install("ruff", ".")
    session.run("ruff", "format", "--diff", *args)


@session(python=DEFAULT_VERSION)
def fix_format(session: Session) -> None:
    """Fix format using ruff."""
    args = session.posargs or locations
    session.install("ruff", ".")
    session.run("ruff", "format", *args)


@session(python=DEFAULT_VERSION)
def types(session: Session) -> None:
    """Type-check using mypy."""
    # session.run("poetry", "install", "--with", "extras", external=True)
    # session.install(".[cicd]")  # Install the project and optional dependencies
    session.install("mypy", ".[metrics]")
    session.run("mypy", "src/")


@session(python=VERSIONS)
def tests(session: Session) -> None:
    """Run the test suite."""
    args = session.posargs or ["--cov"]
    session.install(
        "pytest",
        "pytest-cov",
        "pytest-mock",
        ".[metrics]",
    )
    session.run("pytest", *args, env={"PYTHONHASHSEED": "0"})


@session(python=DEFAULT_BENCHMARK_VERSIONS)
def benchmark(session: Session) -> None:
    """Run the benchmark suite."""
    args = session.posargs
    session.install("click", ".")
    session.run("python", "--version")
    session.run("python", "benchmark.py", *args)
