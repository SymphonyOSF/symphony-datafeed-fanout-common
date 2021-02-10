export default class Telemetry {

    constructor(environment, hostname) {
        this.telemetry = {
            hostname: hostname,
            service: 'df2-fanout',
            podId: 'unknown',
            metrics: []
        };
        this.telemetryCreatedDate = Date.now();
    }

    getTelemetry() {
        return this.telemetry;
    }

    getTelemetryCreatedDate() {
        return this.telemetryCreatedDate;
    }

    setBatchSize(batchSize) {
        this.telemetry.batchSize = batchSize;
    }

    setPodId(podId) {
        this.telemetry.podId = podId;
    }

    computeTelemetryTotalElapsedTime() {
        this.telemetry.totalElapsedTime = Date.now() - this.telemetryCreatedDate;
    }

    pushMetricsToTelemetry(metrics) {
        this.telemetry.metrics.push(metrics);
    }

}
