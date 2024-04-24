ALL: bin/parasync bin/paradump

bin/parasync:
	go build -C src/parasync -o ../../bin/parasync  -ldflags "-s -w" -v parasync.go

bin/paradump:
	go build -C src/paradump -o ../../bin/paradump  -ldflags "-s -w" -v paradump.go
