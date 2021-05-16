[![Go](https://github.com/its-luca/ttestSuite/actions/workflows/go.yml/badge.svg)](https://github.com/its-luca/ttestSuite/actions/workflows/go.yml)

# ttestSuite
ttestSuite is a multithreaded implementation of Welch's T-test in go.
It reads the data from wfm files and saves the result as a csv files. 
**The WFM file parser is very basic and was only tested with an Tektronix MSO64**.
For plotting you can use external tools
like Matlab.


ttestSuite supports two usage modes: normal and streaming. 
In "normal mode", it is assumed that all the trace files are already stored to disk.
The "streaming mode" is intended to process files as they are stored by the oscilloscope.
For this, ttestSuite provides two http endpoints. See `examples/measure-dummy.py` to get
and idea of how to implement this in your measurement script.

# Build and Use
Run `go build ./cmd/ttestSuite` to build. This will create the main binary `ttestSuite`.

For a basic usage is suffices to pass the  following arguments:
- `-traceFolder` point to a folder with WFM files named `trace (x).wfm` where x in the range `[1,traceFileCount]`.
- `-caseFile` name of a text file in  `traceFolder`, containing as many lines as traces (not trace/wfm files). Each line may either
  contain `0` to indicate that this trace belongs to the fixed set, or `1` to indicate that it belongs to the random set.
- `-caseFileCount` number of case files that should be processed.

To activate the streaming mode use the  `-streamFromAddr <ip:port>` argument .
Now ttestSuite is listening `<ip:port>` waiting to be controlled via two http endpoints (see `examples/measure-dummy.py`).
In the streaming mode `-traceFileCount` is not needed, as it is epxected to be sent by the measurement script.

## Monitor results
Beside printing some basic statistics on the CLI, ttestSuite comes with [Promethreus](https://prometheus.io/) prometheus
based metrics to monitor the throughput of the tool itself as well as the maximal T-test value, and the normalized cross-correlation.
For the latter, we first wait for the  first 5% of the expected traces to complete. Then we use them as a reference
to compare against periodically selected trace files. The correlation measure is 
`dot(a,b)/sqrt(dot(a,a)*dot(b,b))`.


To use, first setup [Promethreus](https://prometheus.io/) and [Prometheus Pushgateway](https://github.com/prometheus/pushgateway).
To visualize the metrics I used [Grafana](https://grafana.com/). See `examples/monitoringSetup` for a docker-compose file
,and an example Grafana dashboard (use `ttestSuite` for the `job` variable in the Grafana dashboard).

## Performance Tweaking
ttestSuite uses a pipeline for processing. A feeder reads the input files into an internal
buffer of size `-fileBufferInGB` which gets drained by `-numWorkers`. A periodic "Buffer Usage" message
will inform you about the fill level of the buffer (also visible in the Prometheus metrics). If the buffers runs full you can either increase the
worker count (if you have reamining cpu cores) or increase the buffer size. Note that depending on the computed
payload function increasing the workers can also cost significant amount of RAM.


# Contribute
Contributions are appreciated! Feel free to submit issues and pull requests.

If you require a different parser for the files take a look at the interface defined `wfm/wfmParser.go`.
If you would like to compute a different function than Welch's T-test, take a look at the interface defined in 
`payloadComputation/registry.go`

