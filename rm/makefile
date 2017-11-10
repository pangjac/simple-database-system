
include ../makefile.inc

all: librm.a rmtest

# lib file dependencies
librm.a: librm.a(rm.o)  # and possibly other .o files

# c file dependencies
rm.o: rm.h

rmtest.o: rm.h

# binary dependencies
rmtest: rmtest.o librm.a $(CODEROOT)/pf/libpf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/pf/libpf.a
$(CODEROOT)/pf/libpf.a:
	$(MAKE) -C $(CODEROOT)/pf libpf.a

.PHONY: clean
clean:
	-rm rmtest *.a *.o *~
	$(MAKE) -C $(CODEROOT)/pf clean
