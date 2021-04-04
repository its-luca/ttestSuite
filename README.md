#ttestSuite
ttestSuite is a multithreaded implementation of Welch's T-test in go.
It reads the data from wfm files and saves the result as a csv file and also creates a plot.

ttestSuite supports two usage modes: normal and streaming. 
In "normal mode", it is assumed that all the trace files are already stored to disk.
The "streaming mode" is intended to process files as they are stored by the oscilloscope.
For this, ttestSuite provides two http endpoints. See `examples/measure-dummy.py` to get
and idea of how to implement this in your measurement script

#Build and Use
Run `go build ./...` to build. This will create the main binary `ttestSuite`.
For a basic usage is suffices to pass the `-caseFile`, `-traceFolder` and `-traceFileCount` flags.
You can also build a standalone plotting binary with `go build ./tPlot/plotCLI/`.



##Performance Tweaking
The default worker and buffer settings assume fast read speeds (SSD). While running, ttestSuite
periodically reports "Buffer Usage" in "buffered wfm files". As long as this does not fill up, it is safe to 
decrease the number of workers and/or the buffer size without sacrificing performance.



##Limitations
The wfm file parser is *very* basic at this point. It was only tested with one specific
setup on a Tektronix MSO64.

