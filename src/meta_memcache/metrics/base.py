from abc import ABC, abstractmethod
from typing import (
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Union,
)


class MetricDefinition(NamedTuple):
    name: str
    documentation: str
    labelnames: Iterable[str] = ()


class BaseMetricsCollector(ABC):
    """
    Base class for metrics collectors.

    To be used as:
    * Initializing metrics:
    metrics_collector.init_metrics(
        metrics=[MetricDefinition(name="foo", documentation="bar")],
        gauges=[MetricDefinition(name="baz", documentation="qux")],
    )
    * For incrementing metrics:
    metrics_collector.metric_inc(key="foo")
    metrics_collector.metric_inc(key="foo", value=2)
    * For setting gauges:
    metrics_collector.gauge_set(key="baz", value=1.5)
    """

    def __init__(self, namespace: str = "") -> None:
        self._namespace = namespace

    @abstractmethod
    def init_metrics(
        self,
        metrics: List[MetricDefinition],
        gauges: List[MetricDefinition],
        namespace: str = "",
    ) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def metric_inc(
        self,
        key: str,
        value: Union[float, int] = 1,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def gauge_set(
        self,
        key: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def get_counters(self) -> Dict[str, float]:
        ...  # pragma: no cover
