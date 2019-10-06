# mit6.824
The labs of [mit 6.824](https://pdos.csail.mit.edu/6.824/schedule.html)


## Prerequisites

[Install golang](https://golang.org/doc/install)

## Lab1

### Run tests of lab1

```bash
cd src/mapreduce
go test -run Sequential
go test -run TestParallel
go test -run Failure
```