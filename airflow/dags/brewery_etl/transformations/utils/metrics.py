"""Helper functions for ETL process metrics."""

import time
from prometheus_client import Counter, Gauge, Histogram, Summary, CollectorRegistry, push_to_gateway

PUSHGATEWAY_HOST = 'pushgateway:9091'

class PrometheusMetrics:
    """
    Prometheus metrics implementation following best practices:
    1. Consistent naming conventions with app prefix
    2. Avoiding high cardinality labels
    3. Tracking totals and failures instead of successes
    4. Properly scoped metrics
    5. Initializing metrics to avoid missing data
    """

    def __init__(self, service_name):
        """Initialize metrics with service name as prefix"""
        self.service_name = service_name
        self.registry = CollectorRegistry()

        self.operations_total = Counter(
            f'{service_name}_operations_total',
            'Total number of operations performed',
            ['operation', 'status'],
            registry=self.registry
        )

        self.processing_duration_seconds = Histogram(
            f'{service_name}_processing_duration_seconds',
            'Time spent processing operations',
            ['operation'],
            registry=self.registry
        )

        self.data_processed_bytes = Gauge(
            f'{service_name}_data_processed_bytes',
            'Amount of data processed in bytes',
            ['operation'],
            registry=self.registry
        )

        self.records_processed_total = Counter(
            f'{service_name}_records_processed_total',
            'Total number of records processed',
            ['operation'],
            registry=self.registry
        )

        self._initialize_metrics()

    def _initialize_metrics(self):
        """Initialize metrics with zero values to avoid missing metrics"""
        operations = ['extract', 'transform', 'load']
        statuses = ['success', 'failure']

        for operation in operations:
            for status in statuses:
                self.operations_total.labels(operation=operation, status=status)

            self.records_processed_total.labels(operation=operation)
            self.data_processed_bytes.labels(operation=operation)


    def push_metrics(self, job_name):
        """Push metrics to Prometheus Pushgateway"""
        try:
            push_to_gateway(PUSHGATEWAY_HOST, job=job_name, registry=self.registry)
            return True
        except Exception as e:
            print(f"Failed to push metrics to Prometheus: {e}")
            return False


    def register_metric(self, metric_type, name, description, labels=None):
        """Register a new metric with the registry"""
        labels = labels or []

        for metric in self.registry._names_to_collectors.values():
            if metric._name == name:
                return metric

        if metric_type == 'counter':
            return Counter(name, description, labels, registry=self.registry)
        elif metric_type == 'gauge':
            return Gauge(name, description, labels, registry=self.registry)
        elif metric_type == 'histogram':
            return Histogram(name, description, labels, registry=self.registry)
        elif metric_type == 'summary':
            return Summary(name, description, labels, registry=self.registry)
        raise ValueError(f"Unknown metric type: {metric_type}")


class ETLMetricsContext:
    """Context manager for ETL metrics with proper error handling"""

    def __init__(self, metrics, operation, **kwargs):
        self.metrics = metrics
        self.operation = operation
        self.kwargs = kwargs
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self.metrics

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time

        if exc_type is not None:
            self.metrics.operations_total.labels(operation=self.operation, status='failure').inc()
        else:
            self.metrics.operations_total.labels(operation=self.operation, status='success').inc()

        self.metrics.processing_duration_seconds.labels(operation=self.operation).observe(duration)

        dag_id = self.kwargs.get(
            'dag', {}).dag_id if hasattr(self.kwargs.get('dag', {}), 'dag_id') else 'unknown_dag'
        task_id = self.kwargs.get(
            'task', {}).task_id if hasattr(self.kwargs.get('task', {}), 'task_id') else 'unknown_task'

        job_name = f'{dag_id}_{task_id}'
        self.metrics.push_metrics(job_name)

        return False

brewery_metrics = PrometheusMetrics('brewery_etl')
