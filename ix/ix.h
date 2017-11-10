
#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <math.h>

#include "../pf/pf.h"
#include "../rm/rm.h"

using namespace std;

#define notfound 999999

#define FANOUT 4


typedef struct
{
    vector<RID> rids;
}Bucket;


typedef struct {
	//nodeID
     int nodeID;

     //store the key of the node
     vector<void *> keys;

     //in interior node store the nodeID, in leaf node store the bucketID, all pageID
     vector<int> childrenNums;

     // each bucket is stored in the page, and hold many rids
     vector<Bucket> buckets;

     //judge if it is the leaf node
     bool leaf;

     //next leaf node id, only leaf node can do this
     int nextLeafNum;

      //symbol that the node has been deleted
      int deletedNode;

} BtreeNode;


typedef struct
{
     //the float int varchar in the Btree index
     AttrType attributeType;

    //root id of the interior node
    int rootPage;

    //first leaf node
    int firstLeaf;

    // lastLeaf node
    int lastNodeID;
} Btree;


class IX_IndexHandle;

class IX_Manager {
 public:
  static IX_Manager* Instance();

  RC CreateIndex(const string tableName,       // create new index
		 const string attributeName);
  RC DestroyIndex(const string tableName,      // destroy an index
		  const string attributeName);
  RC OpenIndex(const string tableName,         // open an index
	       const string attributeName,
	       IX_IndexHandle &indexHandle);
  RC CloseIndex(IX_IndexHandle &indexHandle);  // close index

 protected:
  IX_Manager   ();                             // Constructor
  ~IX_Manager  ();                             // Destructor
 
 private:
  static IX_Manager *_ix_manager;
  RM *rm;
  PF_Manager *pf;
};


class IX_IndexHandle {
 public:
  IX_IndexHandle  ();                           // Constructor
  ~IX_IndexHandle ();                           // Destructor

  RC InsertEntry(void *key, const RID &rid);  // Insert new index entry
  RC DeleteEntry(void *key, const RID &rid);  // Delete index entry
  string indexFilename;
  AttrType indexAttrType;
  PF_FileHandle fileHandle;
};

class BtreeOperation{
public:
	BtreeOperation();
	~BtreeOperation(){};


	RC GetBtree( Btree *tree, string indexFilename);
	RC PutBtree(Btree *tree, string indexFilename);
	RC InsertBegin( Btree *tree, void *key, RID rid, string indexFilename );
	RC PutLeafNodeValue( BtreeNode *node, void *key, RID rid, AttrType attributeType );
	RC PutInteriorNodeValue( BtreeNode *node, void *key, int* new_child_id, AttrType attributeType );
	RC PutNode( BtreeNode *node, Btree *tree, string indexFilename  );
	RC WriteNodeToMem(BtreeNode *node,  Btree *tree, string indexFilename );
	RC Insert( Btree *tree, void *key, RID rid, string indexFilename,
			int nodePage, int* newChildPage,
			void* new_key);
	RC GetNode( BtreeNode *node, int nodeID, Btree *tree, string indexFilename);
	RC GetBuckets(  BtreeNode *node, Btree *tree, int bucketsSize, string indexFilename );
	RC PutBucket( Bucket *bucket, Btree *tree, int *bucketPage, string indexFilename );
	RC FindPosition( BtreeNode *node, void* key,AttrType attributeType );
	RC SplitInteriorNode(Btree *tree, BtreeNode *old,
			BtreeNode *news, int* newChildPage, void* key);
	RC CopyKey(void* from, void *to, AttrType attributeType);
	bool isDuplicateKey( BtreeNode *node, void *key);
	RC SplitLeafNode( Btree *tree, BtreeNode *old, BtreeNode *news,
			string indexFilename );
	int Compare(void *key, void *nodeValue,AttrType attributeType);
	RC DeleteBegin(Btree *tree,void *key, RID rid, string indexFilename);
	RC Remove_key(Btree *node, int nodeID, void *key,RID rid, string indexFilename);


	PF_FileHandle fileHandle;
private:
	PF_Manager *pf;
};

class IX_IndexScan {
 public:
  IX_IndexScan();  								// Constructor
  ~IX_IndexScan(); 								// Destructor

  // for the format of "value", please see IX_IndexHandle::InsertEntry()
  RC OpenScan(const IX_IndexHandle &indexHandle, // Initialize index scan
	      CompOp      compOp,
	      void        *value);           

  RC GetNextEntry(RID &rid);  // Get next matching entry
  RC CloseScan();             // Terminate index scan
 private:
	vector<RID> srids;
	unsigned int curIndex;
	int closed;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
