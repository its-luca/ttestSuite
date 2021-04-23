[![Go](https://github.com/its-luca/ttestSuite/actions/workflows/go.yml/badge.svg)](https://github.com/its-luca/ttestSuite/actions/workflows/go.yml)

# ttestSuite
ttestSuite is a multithreaded implementation of Welch's T-test in go.
It reads the data from wfm files and saves the result as a csv file and also creates a plot.

ttestSuite supports two usage modes: normal and streaming. 
In "normal mode", it is assumed that all the trace files are already stored to disk.
The "streaming mode" is intended to process files as they are stored by the oscilloscope.
For this, ttestSuite provides two http endpoints. See `examples/measure-dummy.py` to get
and idea of how to implement this in your measurement script

# Build and Use
Run `go build ./cmd/ttestSuite` to build. This will create the main binary `ttestSuite`.
For a basic usage is suffices to pass the `-caseFile`, `-traceFolder` and `-traceFileCount` flags.
You can also build a standalone plotting binary with `go build ./cmd/plot`.



## Performance Tweaking
ttestSuite uses a pipeline for processing. A feeder reads the input files into an internal
buffer of size `-fileBufferInGB` which gets drained by `-numWorkers`. A periodic "Buffer Usage" message
will inform you about the fill level of the buffer. If the buffers runs full you can either increase the
worker count (if you have reamining cpu cores) or increase the buffer size. Note that depending on the computed
payload function increasing the workers can also cost significant amount of RAM.



## Limitations
The wfm file parser is *very* basic at this point. It was only tested with one specific
setup on a Tektronix MSO64.

