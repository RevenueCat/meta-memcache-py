import nox
from nox import session, Session


package = "meta_memcache"
nox.options.sessions = "lint", "types", "tests"
locations = "src", "tests", "noxfile.py"
DEFAULT_VERSION = "3.8"
DEFAULT_BENCHMARK_VERSION = "3.11"
VERSIONS = ["3.8", "3.11"]


@session(python=DEFAULT_VERSION)
def black(session: Session) -> None:
    """Run black code formatter."""

    args = session.posargs or locations
    session.install("black", ".")
    session.run("black", *args)


@session(python=VERSIONS)
def lint(session: Session) -> None:
    """Lint using flake8."""
    args = session.posargs or locations
    session.install(
        "flake8",
        "flake8-annotations",
        "flake8-bandit",
        "flake8-black",
        "flake8-bugbear",
        "flake8-docstrings",
        "bandit",
        ".",
    )
    session.run("flake8", *args)


@session(python=DEFAULT_VERSION)
def types(session: Session) -> None:
    """Type-check using mypy."""
    session.run("poetry", "install", "--with", "extras", external=True)
    session.install("mypy", ".")
    session.run("mypy", "src/")


@session(python=VERSIONS)
def tests(session: Session) -> None:
    """Run the test suite."""
    args = session.posargs or ["--cov"]
    session.run("poetry", "install", "--with", "extras", external=True)
    session.install(
        "pytest",
        "pytest-cov",
        "pytest-mock",
    )
    session.run("pytest", *args, env={"PYTHONHASHSEED": "0"})


@session(python=DEFAULT_BENCHMARK_VERSION)
def benchmark(session: Session) -> None:
    """Run the benchmark suite."""
    args = session.posargs
    session.run("poetry", "install", "--with", "extras", external=True)
    session.install(
        "click",
    )
    session.run("python", "--version")
    session.run("python", "benchmark.py", *args)
