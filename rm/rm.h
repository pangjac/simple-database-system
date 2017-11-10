
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <ostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>

#include "../pf/pf.h"

using namespace std;


// Return code
typedef int RC;


// Record ID
typedef struct
{
  unsigned pageNum;
  unsigned slotNum;
} RID;

typedef struct
{
  unsigned pageNum;
  unsigned slotNum;
  char data[1000];
} scanObj;

// Attribute
typedef enum { TypeInt=0 , TypeReal=1, TypeVarChar=2 } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

struct CatalogEntry
{
	    char tableName[50];  //tableName
	    char   name[50];     // attribute name
	    AttrType type;     // attribute type
	    AttrLength length; // attribute length
	    int position;
	    //int version;//ExtraPart we can get the latest version of the entry
	    //int offset_catalog;
};

// Comparison Operator
typedef enum { EQ_OP = 0,  // =
           LT_OP =1,      // <
           GT_OP =2,      // >
           LE_OP =3,      // <=
           GE_OP =4,      // >=
           NE_OP =5,      // !=
           NO_OP=6       // no condition
} CompOp;


# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through records
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator {
public:
  RM_ScanIterator();
  ~RM_ScanIterator(){};
  vector <char *> sObj;
  int i;
  // "data" follows the same format as RM::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close() { return -1; };
};


// Record Manager
class RM
{
public:
  static RM* Instance();
  //FILE * catalog;
  //char * catalogname;
 // int tableID;
 // PF_Manager *pf;
 // PF_FileHandle catalogHandle;

  RC createTable(const string tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string tableName);

  RC getAttributes(const string tableName, vector<Attribute> &attrs);

  //  Format of the data passed into the function is the following:
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!!The same format is used for updateTuple(), the returned data of readTuple(), and readAttribute()
  RC insertTuple(const string tableName, const void *data, RID &rid);

  RC deleteTuples(const string tableName);

  RC deleteTuple(const string tableName, const RID &rid);

  // Assume the rid does not change after update
  RC updateTuple(const string tableName, const void *data, const RID &rid);

  RC readTuple(const string tableName, const RID &rid, void *data);

  RC readAttribute(const string tableName, const RID &rid, const string attributeName, void *data);

  RC readAttributeLength(const string tableName, const RID &rid, const string attributeName, int *length);

  AttrType readAttributeType(const string tableName,const RID &rid, const string attributeName);

  RC reorganizePage(const string tableName, const unsigned pageNumber);



  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(const string tableName,
      const string conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);




  /*my own functions*/

  int GetAttributeLength(const string tableName,const string attributeName,void *data);

  AttrType GetAttributeType(const string tableName,const string attributeName);

  RC GetAttributeData(const string tableName,const string attributeName,void *data,void *data_read);
  vector<Attribute> NewLength(vector<CatalogEntry> ca,const void *data);

  RC InsertCatalog(const string tableName,vector<Attribute> systemAttribute);
  //Build the catalog

  RC InsertAsPageFile(PF_FileHandle& pf, void* data,int data_length);
  //Insert data into the pf pointed file Sealed in For the data in the same

  //Get the length of record
 // RC GetRecordLength(const string tableName, RID &rid);
  int RecordLength(const string tableName, const void *data);

  //find all the pointer to the attribute
  vector<CatalogEntry> FindCataLog(const string tableName);

  //Check whether the table exists, the table should be in the catalog and catalog file should exist
  RC isTableExist(const string tableName);

  char* Change2String(char name[],int length);

  RC createVector(int number,string names[],int lengths[],AttrType types[],vector<Attribute> &catalog);




// Extra credit
public:
  RC dropAttribute(const string tableName, const string attributeName);

  RC addAttribute(const string tableName, const Attribute attr);

  RC reorganizeTable(const string tableName);



protected:
  RM();
  ~RM();

private:
  static RM *_rm;
  PF_Manager *pf;//the PF_Manager which can control the propcessing over the page
  PF_FileHandle ph;// Control the page
};

#endif
