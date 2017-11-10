Simple Relational Database Management System (DBMS)
====
-------------------------------------------------------------------------------
ABOUT
-------------------------------------------------------------------------------

- This Project is to built a simple relational database system by C/C++ to store and manage data. It is based on Stanford University CS346 Project, which is about practicing principles involved in DBMS implementation.

- The implementation provided some basic database functionalities such as paged file management(./pf ), record management (./rm), index structure(./ix) and SQL relational operators (./qe )

- Implemented index structure by B+ tree to speed up searching operation

              
-------------------------------------------------------------------------------
FILE STRUCTURE
-------------------------------------------------------------------------------

 /ix
	 ix.cc
	 ix.h
	 ixtest.cc
	 makefile
 /pf
	 makefile
	 pf.cc
	 pf.h
 /qe
	 makefile
	 qe.cc
	 qe.h
	 qetest.cc
 /rm
	 makefile
	 rm.cc
	 rm.h
 README.md
 makefile.inc
   
-------------------------------------------------------------------------------
HOW TO BUILD 
-------------------------------------------------------------------------------

```
$ make clean
$ make
```
      
-------------------------------------------------------------------------------
Tech Stack
-------------------------------------------------------------------------------
``` C/C++  Database management  SQL```