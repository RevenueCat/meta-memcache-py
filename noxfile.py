import nox
from nox import session, Session


package = "meta_memcache"
nox.options.sessions = "lint", "pyre", "tests"
locations = "src", "tests", "noxfile.py"
DEFAULT_VERSION = "3.8"
VERSIONS = ["3.8", "3.10"]


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
        # "darglint",
        ".",
    )
    session.run("flake8", *args)


@session(python=DEFAULT_VERSION)
def pyre(session: Session) -> None:
    """Type-check using mypy."""
    session.install("pyre-check", ".")
    session.run("pyre", "--strict", "check")


@session(python=VERSIONS)
def tests(session: Session) -> None:
    """Run the test suite."""
    args = session.posargs or ["--cov"]
    session.run("poetry", "install", "--no-dev", external=True)
    session.install(
        # "coverage[toml]",
        "pytest",
        "pytest-cov",
        "pytest-mock",
    )
    session.run("pytest", *args)
