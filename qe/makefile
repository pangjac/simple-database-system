
include ../makefile.inc

all: libqe.a qetest

# lib file dependencies
libqe.a: libqe.a(qe.o)  # and possibly other .o files

# c file dependencies
qe.o: qe.h

qetest.o: qe.h

# binary dependencies
qetest: qetest.o libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/pf/libpf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/pf/libpf.a
$(CODEROOT)/pf/libpf.a:
	$(MAKE) -C $(CODEROOT)/pf libpf.a

.PHONY: $(CODEROOT)/rm/librm.a
$(CODEROOT)/rm/librm.a:
	$(MAKE) -C $(CODEROOT)/rm librm.a

.PHONY: $(CODEROOT)/ix/libix.a
$(CODEROOT)/ix/libix.a:
	$(MAKE) -C $(CODEROOT)/ix libix.a

.PHONY: clean
clean:
	-rm qetest *.a *.o *~
	$(MAKE) -C $(CODEROOT)/ix clean
