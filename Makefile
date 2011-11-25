include $(GOROOT)/src/Make.inc

TARG=cass
GOFMT=gofmt -s -spaces=true -tabindent=false -tabwidth=2


GOFILES=\
	cass.go\

include $(GOROOT)/src/Make.pkg

format:
	${GOFMT} -w ${GOFILES}
	${GOFMT} -w cass_test.go
