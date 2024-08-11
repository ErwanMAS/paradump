ALL: bin/parasync bin/paradump

bin/parasync: src/parasync/parasync.go
	go build -C src/parasync -o ../../bin/parasync  -ldflags "-s -w" -v parasync.go

bin/paradump: src/paradump/paradump.go
	go build -C src/paradump -o ../../bin/paradump  -ldflags "-s -w" -v paradump.go
