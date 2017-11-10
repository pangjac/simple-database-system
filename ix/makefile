
include ../makefile.inc

all: libix.a ixtest

# lib file dependencies
libix.a: libix.a(ix.o)  # and possibly other .o files

# c file dependencies
ix.o: ix.h

ixtest.o: ix.h

# binary dependencies
ixtest: ixtest.o libix.a $(CODEROOT)/pf/libpf.a $(CODEROOT)/rm/librm.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/pf/libpf.a
$(CODEROOT)/pf/libpf.a:
	$(MAKE) -C $(CODEROOT)/pf libpf.a


.PHONY: $(CODEROOT)/pf/librm.a
$(CODEROOT)/rm/librm.a:
	$(MAKE) -C $(CODEROOT)/rm librm.a

.PHONY: clean
clean:
	-rm ixtest *.a *.o *~
	$(MAKE) -C $(CODEROOT)/pf clean
	$(MAKE) -C $(CODEROOT)/rm clean
