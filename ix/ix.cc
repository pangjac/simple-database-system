#include "ix.h"

IX_Manager* IX_Manager::_ix_manager = 0;
IX_Manager* IX_Manager::Instance()
{
    if(!_ix_manager)
    	_ix_manager = new IX_Manager();

    return _ix_manager;
}

IX_Manager::IX_Manager() {
	rm = RM::Instance();
	pf = PF_Manager::Instance();
}
IX_Manager::~IX_Manager() {

}

BtreeOperation::BtreeOperation() {
	pf = PF_Manager::Instance();
}

//Put the Btree into the file system
RC BtreeOperation::PutBtree(Btree *tree, string indexFilename)
{
	RC rc;
	void* treeData = (char*) malloc(4096);
	//from the beiginning
	int cursor = 0;

	//attributeType
	memcpy((char*) treeData + cursor, &tree->attributeType, sizeof(int));
	cursor += sizeof(int);

	//rootPage
	memcpy((char*) treeData + cursor, &tree->rootPage, sizeof(int));
	cursor += sizeof(int);

	//firstLeaf
	memcpy((char*) treeData + cursor, &tree->firstLeaf, sizeof(int));
	cursor += sizeof(int);

	//lastnodeID
	memcpy((char*) treeData + cursor, &tree->lastNodeID, sizeof(int));
	cursor += sizeof(int);

	PF_FileHandle fileHandle;
	rc = pf->OpenFile(indexFilename.c_str(), fileHandle);

	//the default location is the first page
	rc = (fileHandle).WritePage(0,treeData);
	rc = pf->CloseFile(fileHandle);
	free(treeData);
	return rc;
}

//get the tree information from the file
RC BtreeOperation::GetBtree(Btree *tree, string indexFilename) {
	RC rc;
	void* treeData = (char*) malloc(4096);
	int cursor = 0;
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(indexFilename.c_str(), fileHandle);
	rc = (fileHandle).ReadPage(0, treeData);

	if (rc == 0) {
		//attributeType
		memcpy(&tree->attributeType, (char*) treeData + cursor, sizeof(int));
		cursor += sizeof(int);

		//rootPage
		memcpy(&tree->rootPage, (char*) treeData + cursor, sizeof(int));
		cursor += sizeof(int);

		//firstLeaf
		memcpy(&tree->firstLeaf, (char*) treeData + cursor, sizeof(int));
		cursor += sizeof(int);

		//lastnodeID
		memcpy(&tree->lastNodeID, (char*) treeData + cursor, sizeof(int));
		cursor += sizeof(int);

		pf->CloseFile(fileHandle);
		free(treeData);
		return rc;
	} else {
		pf->CloseFile(fileHandle);
		free(treeData);
		return rc;
	}
}

//compare the key and node's value according to their attribute
RC BtreeOperation::Compare(void *key, void *nodeValue,AttrType attributeType){
	int eq;
	if (attributeType == TypeInt) {
		int *keya = (int*) malloc(sizeof(int));
		memcpy(keya, nodeValue, sizeof(int));

		int *keyb = (int*) malloc(sizeof(int));
		memcpy(keyb, key, sizeof(int));
		//int eq;
		if (*keyb > *keya) {
			eq = 1;
		} else if (*keyb == *keya) {
			eq = 0;
		} else {
			eq = -1;
		}
	}
	if (attributeType == TypeReal) {
		float *keya = (float*) malloc(sizeof(float));
		memcpy(keya, nodeValue, sizeof(float));

		float *keyb = (float*) malloc(sizeof(float));
		memcpy(keyb, key, sizeof(float));
		//int eq;
		if (*keyb > *keya) {
			eq = 1;
		} else if (*keyb == *keya) {
			eq = 0;
		} else {
			eq = -1;
		}
	}
	if (attributeType == TypeVarChar) {
		int point = 0;
		int charSize = 0;
		int keySize = 0;
		char *value1;
		char *value2;
		memcpy(&charSize, (char*) key + point, sizeof(int));
		point += sizeof(int);
		value1 = (char*) malloc(charSize);
		memcpy(value1, key + point, charSize);
		keySize = charSize;
		point = 0;
		charSize = 0;
		memcpy(&charSize, (char*) nodeValue + point, sizeof(int));
		point += sizeof(int);
		value2 = (char*) malloc(charSize);
		memcpy(value2, ((char*) nodeValue) + point, charSize);
		eq = memcmp(value1, value2, keySize);
	}
	return eq;
}

//decide where to put the key and rid into the leaf node when inserting
RC BtreeOperation::PutLeafNodeValue(BtreeNode *node, void *key, RID rid,
		AttrType attributeType) {
	RC rc = 0;
	int cursor = 0;
	int* intData;
	float* floatData;
	char* charData;
	//convert the key to their type
	if (attributeType == TypeInt) {
		intData = (int*) malloc(sizeof(int));
		memcpy(intData, (char*) key + cursor, sizeof(int));
	}
	if (attributeType == TypeReal) {
		floatData = (float*) malloc(sizeof(float));
		memcpy(floatData, (char*) key + cursor, sizeof(float));
	}
	if (attributeType == TypeVarChar) {
		int charSize = 0;
		memcpy(&charSize, (char*) key + cursor, sizeof(int));
		cursor += sizeof(int);
		charData = (char*) malloc(charSize + sizeof(int));
		memcpy(charData, &charSize, sizeof(int));
		memcpy(charData + sizeof(int), ((char*) key) + sizeof(int), charSize);
	}
	int position;
	bool duplicate = false;
	//find the place to insert the key
	for (position = 0; position < node->keys.size(); position++) {

		int eq = Compare(key,node->keys[position],attributeType);
		//int eq = memcmp(key, node->keys[position], sizeof(key));
		if (eq > 0) {
			continue;
		} else if (eq == 0) {
			duplicate = true;
			break;
		} else {
			break;
		}
	}
	if (!duplicate && (node->keys.size() >= FANOUT))
	{
		return 1;
	}
	//already the key inside
	if (duplicate)
	{
		node->buckets[position].rids.push_back(rid);
	}
	else
	{
		if (attributeType == TypeInt) {
			node->keys.insert(node->keys.begin() + position, intData);
		}
		if (attributeType == TypeReal) {
			node->keys.insert(node->keys.begin() + position, floatData);
		}
		if (attributeType == TypeVarChar) {
			node->keys.insert(node->keys.begin() + position, charData);
		}
        //ensure the bucketID is null
		if (node->childrenNums[position] != notfound)
		{
		    int nextNum = node->childrenNums[FANOUT];
			node->childrenNums.insert(node->childrenNums.begin() + position,
					notfound);
			node->childrenNums[FANOUT] = nextNum;
		}

		Bucket bucket;
		bucket.rids.push_back(rid);
		node->buckets.insert(node->buckets.begin() + position, bucket);
	}
	return rc;

}

RC BtreeOperation::WriteNodeToMem(BtreeNode *node, Btree *tree,
		string indexFilename) {
	char* nodeData = (char*) malloc(4096);
	RC rc = 0;
	int cursor = 0;
	//has not assigned it with nodeID we can find it for it
	if (node->nodeID == notfound)
	{
		memcpy((char*) nodeData, &cursor, sizeof(int));
		node->nodeID = tree->lastNodeID;

		PF_FileHandle fileHandle;
		rc = pf->OpenFile(indexFilename.c_str(), fileHandle);
		if (rc != 0)
			return 1;
		rc = (fileHandle).AppendPage(nodeData);
		if (rc != 0)
			return 1;

		tree->lastNodeID = (fileHandle).pNum;

		rc = pf->CloseFile(fileHandle);
		if (rc != 0)
			return 1;
	}
	//node ID
	memcpy((char*) nodeData + cursor, &node->nodeID, sizeof(int));
	cursor += sizeof(int);

	//is deleted or not
	memcpy((char*) nodeData + cursor, &node->deletedNode, sizeof(int));
	cursor += sizeof(int);

	//length of keys
	int key_size = node->keys.size();
	memcpy((char*) nodeData + cursor, &key_size, sizeof(int));
	cursor += sizeof(int);

	int intData = 0;
	float floatData = 0;

	char* charData;
	int charSize = 0;

	for (int i = 0; i < node->keys.size(); ++i) {
		if (tree->attributeType == TypeInt) {
			intData = *((int*) node->keys[i]);
			memcpy((char*) nodeData + cursor, &intData, sizeof(int));
			cursor += sizeof(int);
		}
		if (tree->attributeType == TypeReal) {
			floatData = *((float*) node->keys[i]);
			memcpy((char*) nodeData + cursor, &floatData, sizeof(float));
			cursor += sizeof(float);
		}
		if (tree->attributeType == TypeVarChar) {
			memcpy(&charSize, ((char*) node->keys[i]), sizeof(int));
			memcpy((char*) nodeData + cursor, node->keys[i],
					charSize + sizeof(int));
			cursor += sizeof(int);
			cursor += charSize;
		}
	}

	//whether or not this is a leaf
	int leaf = 0;
	if (node->leaf)
		leaf = 1;
	memcpy((char*) nodeData + cursor, &leaf, sizeof(int));
	cursor += sizeof(int);

	if (node->leaf) {
		// if leaf, write bucket size
		int bucketsSize = node->buckets.size();
		memcpy((char*) nodeData + cursor, &bucketsSize, sizeof(int));
		cursor += sizeof(int);
		for (unsigned i = 0; i < node->buckets.size(); i++) {
			if (node->childrenNums[i] == notfound) {
				int bucketNum = notfound;
				PutBucket(&node->buckets[i], tree, &bucketNum, indexFilename);
				node->childrenNums[i] = bucketNum;

			} else {
				PutBucket(&node->buckets[i], tree, &node->childrenNums[i],
						indexFilename);
			}

		}
		node->childrenNums[FANOUT] = node->nextLeafNum;

	}


	for (unsigned i = 0; i < FANOUT + 1; ++i) {
		memcpy((char*) nodeData + cursor, &node->childrenNums[i], sizeof(int));
		cursor += sizeof(int);
	}
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(indexFilename.c_str(), fileHandle);
	if (rc != 0)
		return 1;
	(fileHandle).WritePage(node->nodeID, nodeData);
	if (rc != 0)
		return 1;
	rc = pf->CloseFile(fileHandle);
	if (rc != 0)
		return 1;

	return rc;
}

//get the bucket though the node and bucketID
RC BtreeOperation::GetBuckets(BtreeNode *node, Btree *tree, int bucketsSize,
		string indexFilename) {
	RC rc = 0;
	vector<Bucket> buckets;
	vector<int> ptrs;

	ptrs = node->childrenNums;

	int bucketPage;
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(indexFilename.c_str(), fileHandle);
	if (rc != 0)
		return 1;

	for (int j = 0; j < bucketsSize; j++) {
		bucketPage = ptrs[j];
		Bucket bucket;

		if (bucketPage != notfound) {

			char* bucketData = (char*) malloc(4096);
			rc = (fileHandle).ReadPage(bucketPage, bucketData);
			if (rc != 0)
				return 1;
			int cursor = 0;
			// avoid reading the first value
			cursor += sizeof(int);

			int ridsSize;
			memcpy(&ridsSize, (char*) bucketData + cursor, sizeof(int));
			cursor += sizeof(int);

			for (int i = 0; i < ridsSize; i++) {
				RID rid;
				memcpy(&rid.pageNum, (char*) bucketData + cursor, sizeof(unsigned));
				cursor += sizeof(unsigned);
				memcpy(&rid.slotNum, (char*) bucketData + cursor, sizeof(unsigned));
				cursor += sizeof(unsigned);
				bucket.rids.push_back(rid);
			}
		}
		buckets.push_back(bucket);
	}
	node->buckets = buckets;
	rc = pf->CloseFile(fileHandle);
	if (rc != 0)
		return 1;

	return 0;
}

//but bucket into the file get the ID though bucketPage
RC BtreeOperation::PutBucket(Bucket *bucket, Btree *tree, int *bucketPage,
		string indexFilename) {
	RC rc = 0;

	char* bucketData = (char*) malloc(4096);
	int cursor = 0;

	memcpy((char*) bucketData + cursor, &cursor, sizeof(int));
	cursor += sizeof(int);

	int ridsSize = bucket->rids.size();
	memcpy((char*) bucketData + cursor, &ridsSize, sizeof(int));
	cursor += sizeof(int);


	for (int i = 0; i < bucket->rids.size(); i++) {
		memcpy((char*) bucketData + cursor, &bucket->rids[i].pageNum, sizeof(int));
		cursor += sizeof(int);
		memcpy((char*) bucketData + cursor, &bucket->rids[i].slotNum, sizeof(int));
		cursor += sizeof(int);
	}

	if (*bucketPage == notfound)
	{
		*bucketPage = tree->lastNodeID;

		PF_FileHandle fileHandle;
		rc = pf->OpenFile(indexFilename.c_str(), fileHandle);
		if (rc != 0)
			return 1;
		rc = (fileHandle).AppendPage(bucketData);
		if (rc != 0)
			return 1;
		tree->lastNodeID = (fileHandle).pNum;

		rc = pf->CloseFile(fileHandle);
		if (rc != 0)
			return 1;

	} else {
		PF_FileHandle fileHandle;
		rc = pf->OpenFile(indexFilename.c_str(), fileHandle);
		if (rc != 0)
			return 1;
		rc = (fileHandle).WritePage(*bucketPage, bucketData);
		if (rc != 0)
			return 1;

		rc = pf->CloseFile(fileHandle);
		if (rc != 0)
			return 1;
	}
	free(bucketData);
	return rc;

}

//get node from the ID
RC BtreeOperation::GetNode(BtreeNode *node, int nodeID, Btree *tree,
		string indexFilename) {
	RC rc;
	char* nodeData = (char*) malloc(4096);
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(indexFilename.c_str(), fileHandle);
	if (rc != 0)
		return 1;
	rc = (fileHandle).ReadPage(nodeID, nodeData);
	if (rc != 0)
		return 1;

	rc = pf->CloseFile(fileHandle);
	if (rc != 0)
		return 1;
	int cursor = 0;

	//myblock
	memcpy(&node->nodeID, (char*) nodeData + cursor, sizeof(int));
	cursor += sizeof(int);

	memcpy(&node->deletedNode, (char*) nodeData + cursor, sizeof(int));
	cursor += sizeof(int);

	//length of keys
	int key_size;
	memcpy(&key_size, (char*) nodeData + cursor, sizeof(int));
	cursor += sizeof(int);

	for (int i = 0; i < key_size; i++) {
		if (tree->attributeType == TypeInt) {
			int *intData = (int*) malloc(sizeof(int));
			memcpy(intData, (char*) nodeData + cursor, sizeof(int));
			node->keys.push_back(intData);
			cursor += sizeof(int);
		}
		if (tree->attributeType == TypeReal) {
			float *floatData = (float*) malloc(sizeof(float));
			memcpy(floatData, (char*) nodeData + cursor, sizeof(float));
			node->keys.push_back(floatData);
			cursor += sizeof(float);
		}
		if (tree->attributeType == TypeVarChar) {
			int charSize = 0;
			memcpy(&charSize, (char*) nodeData + cursor, sizeof(int));
			cursor += sizeof(int);
			char *charData = (char*) malloc(charSize + sizeof(int));
			memcpy(charData, &charSize, sizeof(int));
			memcpy(charData + sizeof(int), ((char*) nodeData) + cursor,
					charSize);
			node->keys.push_back(charData);
			cursor += charSize;
		}
	}

	int leaf = 0;
	memcpy(&leaf, (char*) nodeData + cursor, sizeof(int));
	cursor += sizeof(int);

	int bucketsSize;
	if (leaf) {
		node->leaf = true;
		memcpy(&bucketsSize, (char*) nodeData + cursor, sizeof(int));
		cursor += sizeof(int);

	} else {
		node->leaf = false;
	}

	for (int i = 0; i < FANOUT + 1; i++) {
		int nTmp;
		memcpy(&nTmp, (char*) nodeData + cursor, sizeof(int));

		node->childrenNums.push_back(nTmp);
		cursor += sizeof(int);
	}

	if (node->leaf)
	{
		node->nextLeafNum = node->childrenNums[FANOUT];
		GetBuckets(node, tree, bucketsSize, indexFilename);
	}

	return 0;
}


//write the node to the tree
RC BtreeOperation::PutNode(BtreeNode *node, Btree *tree,
		string indexFilename) {

	RC rc;
	rc = WriteNodeToMem(node, tree, indexFilename);

	return rc;
}

RC BtreeOperation::FindPosition(BtreeNode *node, void* key,AttrType attributeType) {
	int position;
	for (position = 0; position < node->keys.size(); position++)
	{

		int eq = Compare(key,node->keys[position],attributeType);
		if (node->leaf == true) {
			if (eq > 0) {
				continue;
			} else {
				break;
			}
		} else {
			if (eq >= 0) {
				continue;
			} else {
				break;
			}
		}
	}

	return position;
}

//put the value into the interiornode
RC BtreeOperation::PutInteriorNodeValue(BtreeNode *node, void *key,
		int* newChildPage, AttrType attributeType) {
	int cursor = 0;
	int* intData;
	float* floatData;
	char* charData;

	if (node->keys.size() >= FANOUT)
		return 1;

	if (attributeType == TypeInt) {
		intData = (int*) malloc(sizeof(int));
		memcpy(intData, (char*) key + cursor, sizeof(int));
	}
	if (attributeType == TypeReal) {
		floatData = (float*) malloc(sizeof(float));
		memcpy(floatData, (char*) key + cursor, sizeof(float));
	}
	if (attributeType == TypeVarChar) {
		int charSize = 0;
		memcpy(&charSize, (char*) key + cursor, sizeof(int));
		cursor += sizeof(int);
		charData = (char*) malloc(charSize + sizeof(int));
		memcpy(charData, &charSize, sizeof(int));
		memcpy(charData + sizeof(int), ((char*) key) + sizeof(int), charSize);


	}

	int pos = FindPosition(node, key,attributeType);

	for (int i = FANOUT ; i > pos + 1; i--) {
		node->childrenNums[i] = node->childrenNums[i - 1];
	}
	node->childrenNums[pos + 1] = *newChildPage;

	if (attributeType == TypeInt) {
		node->keys.insert(node->keys.begin() + pos, intData);
	}
	if (attributeType == TypeReal) {
		node->keys.insert(node->keys.begin() + pos, floatData);
	}
	if (attributeType == TypeVarChar) {
		node->keys.insert(node->keys.begin() + pos, charData);
	}

	return 0;

}

RC BtreeOperation::CopyKey(void* from, void *to, AttrType attributeType)
{
	if (attributeType == TypeInt) {

		memcpy((char*) to, (char*) from, sizeof(int));
	}
	if (attributeType == TypeReal) {
		memcpy((char*) to, (char*) from, sizeof(int));
	}
	if (attributeType == TypeVarChar) {
		int chSize = 0;
		memcpy(&chSize, (char*) from + 0, sizeof(int));
		memcpy((char*) to, (char*) from , chSize+sizeof(int));
	}

	return 0;
}

//split the interior node one the original one for the childpage node
RC BtreeOperation::SplitInteriorNode(Btree *tree, BtreeNode *old,
		BtreeNode *news, int* newChildPage, void* key) {
	int toOldPointer = (int) ceil((FANOUT + 2) / 2);
	int toOldKeys = (int) ceil(FANOUT / 2);

	int cursor = 0;
	int* intData;
	float* floatData;
	char* charData;

	//if (node->keys.size() >= FANOUT)
	//return 1;

	if (tree->attributeType == TypeInt) {
		intData = (int*) malloc(sizeof(int));
		memcpy(intData, (char*) key + cursor, sizeof(int));
	}
	if (tree->attributeType == TypeReal) {
		floatData = (float*) malloc(sizeof(float));
		memcpy(floatData, (char*) key + cursor, sizeof(float));
	}
	if (tree->attributeType == TypeVarChar) {
		int charSize = 0;
		memcpy(&charSize, (char*) key + cursor, sizeof(int));
		cursor += sizeof(int);
		charData = (char*) malloc(charSize + sizeof(int));
		memcpy(charData, &charSize, sizeof(int));
		memcpy(charData + sizeof(int), ((char*) key) + sizeof(int), charSize);
	}

	old->childrenNums.push_back(notfound);

	int pos = FindPosition(old, key,tree->attributeType);
	for (int i = FANOUT+1 ; i > pos + 1; i--){
	//for (int i = FANOUT + 1; i > pos + 1; i--) {
		old->childrenNums[i] = old->childrenNums[i - 1];
	}
	old->childrenNums[pos + 1] = *newChildPage;

	if (tree->attributeType == TypeInt) {
		old->keys.insert(old->keys.begin() + pos, intData);
	}
	if (tree->attributeType == TypeReal) {
		old->keys.insert(old->keys.begin() + pos, floatData);
	}
	if (tree->attributeType == TypeVarChar) {
		old->keys.insert(old->keys.begin() + pos, charData);
	}

	CopyKey(old->keys[toOldKeys], key, tree->attributeType);

	for (int i = toOldKeys + 1; i < old->keys.size(); i++) {
		news->keys.push_back(old->keys[i]);
	}
	old->keys.erase(old->keys.begin() + toOldKeys, old->keys.end());

	for (int i = toOldPointer; i < old->childrenNums.size(); i++) {
		news->childrenNums[i - toOldPointer] = old->childrenNums[i];
		old->childrenNums[i] = notfound;

	}

	return 0;

}

//split leaf node one the old one for the new
RC BtreeOperation::SplitLeafNode(Btree *tree, BtreeNode *old, BtreeNode *news,
		string indexFilename) {
	int toOldKeys = (int) ceil((FANOUT) / 2);

	for (int i = 0; i < FANOUT - toOldKeys; i++) {
		news->buckets.push_back(old->buckets[i + toOldKeys]);
		news->childrenNums[i] = old->childrenNums[i + toOldKeys];
		news->keys.push_back(old->keys[i + toOldKeys]);

		old->childrenNums[i + toOldKeys] = notfound;

	}
	old->buckets.erase(old->buckets.begin() + toOldKeys, old->buckets.end());
	old->keys.erase(old->keys.begin() + toOldKeys, old->keys.end());

	news->leaf = true;

	news->nextLeafNum = old->childrenNums[FANOUT];
	PutNode(news, tree, indexFilename);

	old->nextLeafNum = news->nodeID;
	PutNode(old, tree, indexFilename);

	return 0;
}

bool BtreeOperation::isDuplicateKey(BtreeNode *node, void *key) {
	int pos;
	bool duplicate = false;
	// search all the key until find the equal one
	for (pos = 0; pos < node->keys.size(); pos++) {
		int eq = memcmp(key, node->keys[pos], sizeof(key));
		if (eq > 0) {
			continue;
		} else if (eq == 0) {
			duplicate = true;
			break;
		}
	}
	return duplicate;
}

//insert the node into the tree
RC BtreeOperation::Insert(Btree *tree, void *key, RID rid, string indexFilename,
		int nodePage, int* newChildPage,
		void* new_key ) {
	BtreeNode curNode;
	GetNode(&curNode, nodePage, tree, indexFilename);

	if (!curNode.leaf) {

		int pos = FindPosition(&curNode, key,tree->attributeType);

		int nextNodeId = curNode.childrenNums[pos];
		Insert(tree, key, rid, indexFilename, nextNodeId, newChildPage,
				new_key);
		if (*newChildPage == -1)
			return 0;

		if (curNode.keys.size() < FANOUT)
		{
			PutInteriorNodeValue(&curNode, new_key, newChildPage,
					tree->attributeType);
			PutNode(&curNode, tree, indexFilename);

			*newChildPage = -1;
			return 0;
		} else {
			BtreeNode newNode;
			newNode.deletedNode = 0;
			newNode.nodeID = notfound;
			newNode.nextLeafNum = notfound;
			newNode.childrenNums.clear();
			for (int i = 0; i < FANOUT + 1; i++) {
				newNode.childrenNums.push_back(notfound);
			}

			newNode.leaf = false;

			SplitInteriorNode(tree, &curNode, &newNode, newChildPage, new_key);

			PutNode(&curNode, tree, indexFilename);
			PutNode(&newNode, tree, indexFilename);
			*newChildPage = newNode.nodeID;
			if (tree->rootPage == curNode.nodeID) {
				// create a new node with a ptr to curNode and newNode
				BtreeNode newRoot;
				newRoot.deletedNode = 0;
				newRoot.nodeID = notfound;
				newRoot.nextLeafNum = notfound;
				newRoot.childrenNums.clear();
				for (int i = 0; i < FANOUT + 1; i++) {
					newRoot.childrenNums.push_back(notfound);
				}
				newRoot.leaf = false;

				newRoot.keys.push_back(new_key);
				newRoot.childrenNums[0] = curNode.nodeID;
				newRoot.childrenNums[1] = newNode.nodeID;
				PutNode(&curNode, tree, indexFilename);
				PutNode(&newRoot, tree, indexFilename);
				tree->rootPage = newRoot.nodeID;
				*newChildPage = -1;

				return 0;

			}

		}
	} else {
		if (curNode.deletedNode == 1) {

			curNode.deletedNode = 0;
		}

		if ((curNode.keys.size() < FANOUT)) {
			PutLeafNodeValue(&curNode, key, rid, tree->attributeType);
			PutNode(&curNode, tree, indexFilename);
			*newChildPage = -1;

		} else {

			BtreeNode newNode;
			newNode.deletedNode = 0;
			newNode.nodeID = notfound;
			newNode.nextLeafNum = notfound;
			newNode.childrenNums.clear();
			for (int i = 0; i < FANOUT + 1; i++) {
				newNode.childrenNums.push_back(notfound);
			}
			newNode.leaf = true;
			int pos = FindPosition(&curNode, key,tree->attributeType);
			SplitLeafNode(tree, &curNode, &newNode, indexFilename);

			if (pos < (int) ceil((FANOUT ) / 2)) {
			//if (pos < (int) ceil((FANOUT + 1) / 2)) {
				PutLeafNodeValue(&curNode, key, rid, tree->attributeType);
				PutNode(&curNode, tree, indexFilename);
				PutNode(&newNode, tree, indexFilename);

			} else {
				PutLeafNodeValue(&newNode, key, rid, tree->attributeType);
				PutNode(&curNode, tree, indexFilename);
				PutNode(&newNode, tree, indexFilename);
			}

			*newChildPage = newNode.nodeID;

			//int charsize = 0;
			//char *data;
			//memcpy(&charsize, newNode.keys[0],4);
			//cout<<"newNode.0  keysize:  "<<charsize<<endl;

			CopyKey(newNode.keys[0], new_key, /*new_key_size,*/
			tree->attributeType);

			//the first node is the root
			if (curNode.nodeID == tree->rootPage) {
				BtreeNode newRoot;
				newRoot.deletedNode = 0;
				newRoot.nodeID = notfound;
				newRoot.nextLeafNum = notfound;
				newRoot.childrenNums.clear();
				for (int i = 0; i < FANOUT + 1; i++) {
					newRoot.childrenNums.push_back(notfound);
				}
				newRoot.leaf = false;
				curNode.leaf = true;

				newRoot.keys.push_back(newNode.keys[0]);
				newRoot.childrenNums[0] = curNode.nodeID;
				newRoot.childrenNums[1] = newNode.nodeID;

				PutNode(&curNode, tree, indexFilename);
				PutNode(&newNode, tree, indexFilename);

				tree->firstLeaf = curNode.nodeID;
				PutNode(&newRoot, tree, indexFilename);

				tree->rootPage = newRoot.nodeID;
				*newChildPage = -1;
			}
		}

	}
	return 0;

}

//start the insert
RC BtreeOperation::InsertBegin(Btree *tree, void *key, RID rid,
		string indexFilename) {
	RC rc;
	BtreeNode root;
	if (tree->rootPage == notfound) {
		root.deletedNode = 0;
		root.nodeID = notfound;
		root.nextLeafNum = notfound;
		root.childrenNums.clear();
		for (int i = 0; i < FANOUT + 1; i++) {
			root.childrenNums.push_back(notfound);
		}
		root.leaf = true;
		PutLeafNodeValue(&root, key, rid, tree->attributeType);
		PutNode(&root, tree, indexFilename);
		tree->firstLeaf = root.nodeID;
		tree->rootPage = root.nodeID;
		return 0;
	}

	int *newChildPage = (int*)malloc(sizeof(int));
	*newChildPage = notfound;

	 void *newKey = malloc(200);

	Insert(tree, key, rid, indexFilename, tree->rootPage, newChildPage,newKey);
	return 0;

}


RC BtreeOperation::Remove_key(Btree *tree, int nodeID, void *key,RID rid, string indexFilename)
{

   BtreeNode node;
   //get the node in the file
   GetNode(&node,nodeID,tree,indexFilename);
   if(node.leaf!=true)
	   //not the leaf node
   {
	   int num=node.childrenNums.size();
	   int pos=FindPosition(&node, key,tree->attributeType);
	   return Remove_key(tree,node.childrenNums[pos], key,rid,indexFilename);
   }
   else
	   //in the leaf node
   {
	   if(node.deletedNode==1)
		   return -1;
	   else
	   {
		   /*cout<<"bucketSize is"<<node.buckets.size()<<endl;
		   for (int i=0;i<node.buckets.size();i++)
		     {
			   cout<<"rids size is"<<node.buckets[i].rids.size()<<endl;
			   //cout<<"key is "<<*((int *)node.keys[i])<<endl;
		  	   for (int j=0;j<node.buckets[i].rids.size();j++)
		  	   {
		  		   cout<<node.buckets[i].rids[j].pageNum<<node.buckets[i].rids[j].slotNum;
		  	   }

		     }
		    cout<<endl;*/
            int position=FindPosition(&node,key,tree->attributeType);

            if(position==node.keys.size())
            {
            	return -1;
            }
            else
            {
            	int eq=Compare(key,node.keys[position],tree->attributeType);
            	if(eq!=0)
            		return -1;
            }
            //cout<<"keysize is"<<node.keys.size()<<endl;
            //cout<<position<<"position"<<position<<endl;
            bool has_ridinside=false;
            //the default is false that no rid inside
            //search to the end of the node's keys
            //cout<<"rid size are"<<node.buckets[position].rids.size()<<endl;
            for (unsigned i=0;i<node.buckets[position].rids.size();i++)
            {
            	//cout<<"size is"<<node.buckets[position].rids.size()<<endl;
            	//cout<<"nodeID and slotNum"<<node.buckets[position].rids[i].pageNum<<node.buckets[position].rids[i].slotNum<<endl;
            	//cout<<"key is"<<*(int*)node.keys[position]<<endl;
            	if(node.buckets[position].rids[i].pageNum==rid.pageNum&&node.buckets[position].rids[i].slotNum==rid.slotNum)
            	{

            		node.buckets[position].rids.erase(node.buckets[position].rids.begin()+i);
            		has_ridinside=true;
            	}

            }
            //no the rid we want to delete
            if(has_ridinside==false)
            {
            	return -1;
            }
            if(node.buckets[position].rids.size()==0)
            {
            node.keys.erase(node.keys.begin()+position);
            node.buckets.erase(node.buckets.begin()+position);
            }
	   }

	   if(node.keys.size() == 0)
	   node.deletedNode = 1;
   }

   PutNode( &node, tree, indexFilename);
   return 0;

}

//give the rid and key pair, remove the key
RC BtreeOperation::DeleteBegin(Btree *tree,void *key, RID rid, string indexFilename)
{
	if (tree->rootPage == notfound) {
		return -1;
		
	}
	return Remove_key(tree, tree->rootPage, key, rid, indexFilename);
}


//Create the index in the file
RC IX_Manager::CreateIndex(const string tableName,       // create new index
		const string attributeName) {
	RC rc;
	string indexFilename;
	//string tableFilename;

	indexFilename = tableName + "_" + attributeName;
	rc = pf->CreateFile(indexFilename.c_str());
	if(rc!=0) return 1;
	Btree tree;
	//know what type is from the catalog
	AttrType indexAttrType = rm->GetAttributeType(tableName, attributeName);
	tree.rootPage = notfound;
	tree.firstLeaf = notfound;
	tree.attributeType = indexAttrType;
	tree.lastNodeID = 1;
	PF_FileHandle fileHandle;
	pf->OpenFile(indexFilename.c_str(),fileHandle);
	BtreeOperation bo;
	rc = bo.PutBtree(&tree, indexFilename);
	pf->CloseFile(fileHandle);
	return rc;
}

//Directly destroy the index file in the system
RC IX_Manager::DestroyIndex(const string tableName,
		const string attributeName) {
	RC rc;
	string indexFilename;

	indexFilename = tableName + "_" + attributeName;
	rc = pf->DestroyFile(indexFilename.c_str());
	return rc;
}

//pass the value of the paged file handler to the indexhandler
RC IX_Manager::OpenIndex(const string tableName,const string attributeName, IX_IndexHandle &indexHandle)
{
	RC rc;
	string indexFilename;
	indexFilename = tableName + "_" + attributeName;
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(indexFilename.c_str(), fileHandle);
	if (rc == 0)
	{
		AttrType indexAttrType = rm->GetAttributeType(tableName, attributeName);
		indexHandle.indexAttrType = indexAttrType;
		indexHandle.indexFilename = indexFilename;
		indexHandle.fileHandle.file = fileHandle.file;
		indexHandle.fileHandle.pNum = fileHandle.pNum;
		indexHandle.fileHandle.pfile = fileHandle.pfile;
		pf->CloseFile(fileHandle);
		return rc;
	}
	else
	{

		return rc;
	}
	return rc;
}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle) {
	RC rc;
	//remove the filename
	indexHandle.indexFilename = "";
	return 0;
}

IX_IndexHandle::IX_IndexHandle() {

	indexFilename = "";
}
IX_IndexHandle::~IX_IndexHandle() // Destructor
{

}

//insert the entry into the bucker
RC IX_IndexHandle::InsertEntry(void *key, const RID &rid) {
	RC rc;
	Btree tree;
	BtreeOperation bo;
	bo.fileHandle = this->fileHandle;
	rc = bo.GetBtree( &tree, indexFilename);
	rc = bo.InsertBegin( &tree, key, rid, indexFilename);
	bo.PutBtree( &tree, indexFilename);
	return rc;
}

//delete the tree from the bucket
RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid)
{
	RC rc;
	Btree tree;
	BtreeOperation bo;
	bo.fileHandle = this->fileHandle;
	//know the tree
	rc = bo.GetBtree(&tree, indexFilename);
	//delete the entry
	rc = bo.DeleteBegin(&tree, key, rid, indexFilename);
	//put tree back
	bo.PutBtree(&tree, indexFilename);
	return rc;

}

IX_IndexScan::IX_IndexScan() // Constructor
{
        closed = 0;
        curIndex = 0;
}
IX_IndexScan::~IX_IndexScan()// Destructor
{

}

//Open the scan
RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle,
                CompOp compOp, void *value){

	if(indexHandle.indexFilename == "")
		return 1;

	closed = 0;
	Btree tree;
	string indexFilename = indexHandle.indexFilename;

	BtreeOperation bo;
	bo.GetBtree(&tree,indexFilename);

	int leafPage = tree.firstLeaf;
	int nodePage = tree.rootPage;

	//int *v = (int*) malloc(sizeof(int));
	//memcpy(v, value, sizeof(int));
	//cout << "value: " << *v << endl;

	//if no_op, return everything
	if (compOp == NO_OP){
		while (leafPage != notfound){
			BtreeNode curNode;
			bo.GetNode(&curNode, leafPage, &tree,indexFilename);
			if (curNode.deletedNode == 1) {
				leafPage = curNode.nextLeafNum;
				continue;
			}
			for (unsigned int pos = 0; pos < curNode.buckets.size(); ++pos) {
				for (unsigned int i = 0; i < curNode.buckets[pos].rids.size();
						++i) {
					srids.push_back(curNode.buckets[pos].rids[i]);
				}
			}
			leafPage = curNode.nextLeafNum;
		}
		return 0;
	}

	
	if (compOp == EQ_OP){
		while (nodePage != notfound) {
			BtreeNode node;
			bo.GetNode(&node, nodePage, &tree, indexFilename);
			if (node.leaf == true) {
				leafPage = node.nodeID;
				break;
			}
			int pos = bo.FindPosition(&node, value, tree.attributeType);
			nodePage = node.childrenNums[pos];
			//bo.ReadNode(&node, nextNode, &tree, indexFilename);

		}

		BtreeNode curNode;
		bo.GetNode(&curNode, leafPage, &tree,indexFilename);
		//while (leafPage != notfound) {
			//BtreeNode curNode;
			//bo.ReadNode(&curNode, leafPage, &tree,indexFilename);

			//if tombstoned
			if (curNode.deletedNode == 1) {
				//leafPage = curNode.nextLeafNum;
				//continue;
				return -1;
			}

			for (unsigned int pos = 0; pos < curNode.keys.size(); pos++) {
				//??????
				int *key = (int*) malloc(sizeof(int));
				memcpy(key, curNode.keys[pos], sizeof(int));

				//cout<<"pos key: "<<*key<<endl;

				int *keya = (int*) malloc(sizeof(int));
				memcpy(keya, value, sizeof(int));
				int eq;
				//if (*key < *keya) {
				//	eq = -1;
				//} else if (*key == *keya) {
				//	eq = 0;
				//} else {
				//	eq = 1;
			//	}
				eq = bo.Compare(value,curNode.keys[pos],tree.attributeType);
				//int eq = memcmp(curNode.keys[pos], value, sizeof(value));
				if (eq == 0) {
					for (unsigned int i = 0;
							i < curNode.buckets[pos].rids.size(); ++i) {
						srids.push_back(curNode.buckets[pos].rids[i]);
					}
				}
			}
			//leafPage = curNode.nextLeafNum;
		//}
		return 0;
	}

	if (compOp == NE_OP) {

		while (leafPage != notfound) {
			BtreeNode curNode;
			bo.GetNode(&curNode, leafPage, &tree, indexFilename);

			//if tombstoned
			if (curNode.deletedNode == 1) {
				leafPage = curNode.nextLeafNum;
				continue;
			}

			for (unsigned int pos = 0; pos < curNode.keys.size(); pos++) {
				//?????sizeof(value)???

				int *key = (int*) malloc(sizeof(int));
				memcpy(key, curNode.keys[pos], sizeof(int));

				int *keya = (int*) malloc(sizeof(int));
				memcpy(keya, value, sizeof(int));
				int eq;
				//if (*key < *keya) {
				//	eq = -1;
				//} else if (*key == *keya) {
				//	eq = 0;
				//} else {
				//	eq = 1;
				//}
				eq = bo.Compare(value,curNode.keys[pos],tree.attributeType);

				//int eq = memcmp(curNode.keys[pos], value, sizeof(value));

				if (eq != 0) {
					for (unsigned int i = 0;
							i < curNode.buckets[pos].rids.size(); ++i) {
						srids.push_back(curNode.buckets[pos].rids[i]);
					}
				}
			}
			leafPage = curNode.nextLeafNum;
		}

		return 0;
	}

	 //current_key OP value
	if (compOp == LT_OP || compOp == LE_OP) {
		while (nodePage != notfound) {
			BtreeNode node;
			bo.GetNode(&node, nodePage, &tree, indexFilename);
			if (node.leaf == true) {
				leafPage = node.nodeID;
				break;
			}
			int pos = bo.FindPosition(&node, value, tree.attributeType);
			nodePage = node.childrenNums[pos];
			//bo.ReadNode(&node, nextNode, &tree, indexFilename);

		}

		BtreeNode curNode;
		bo.GetNode(&curNode, leafPage, &tree, indexFilename);
		leafPage = tree.firstLeaf;

		while (leafPage != notfound) {
			BtreeNode leafNode;
			bo.GetNode(&leafNode, leafPage, &tree, indexFilename);

			//bo.printNode(&curNode,"printNode",tree.attributeType);
			//if tombstoned
			if (leafNode.deletedNode == 1) {
				leafPage = leafNode.nextLeafNum;
				continue;
			}

			for (unsigned int pos = 0; pos < leafNode.keys.size(); pos++) {
				int *key = (int*) malloc(sizeof(int));
				memcpy(key, leafNode.keys[pos], sizeof(int));

				int *keya = (int*) malloc(sizeof(int));
				memcpy(keya, value, sizeof(int));
				int eq;
				//if(*key < *keya){
				//	eq =-1;
				//}else if(*key == *keya){
				//	eq = 0;
				//}else{
				//	eq = 1;
				//}
				eq = bo.Compare(leafNode.keys[pos],value,tree.attributeType);

				//memcpy(key, curNode.keys[pos], sizeof(int));
				//cout << "key value: " << *key << endl;
				if (eq < 0) {
					int *key = (int*)malloc(sizeof(int));
					memcpy(key,leafNode.keys[pos],sizeof(int));
					//cout<<"key: "<<*key<<endl;
					for (unsigned int i = 0;
							i < leafNode.buckets[pos].rids.size(); ++i) {
						srids.push_back(leafNode.buckets[pos].rids[i]);
					}
				}
				if (compOp == LE_OP && eq == 0) {
					int *key = (int*)malloc(sizeof(int));
					memcpy(key, leafNode.keys[pos], sizeof(int));
					cout << "key: " << *key << endl;
					for (unsigned int i = 0;
							i < leafNode.buckets[pos].rids.size(); ++i) {
						srids.push_back(leafNode.buckets[pos].rids[i]);
					}
				}
			}
			if(leafNode.nodeID == curNode.nodeID){
				break;
			}
			leafPage = leafNode.nextLeafNum;
		}

		return 0;
	}

	//                     GT_OP,      // >
	//                     GE_OP,      // >=
	//current_key OP value
	if (compOp == GT_OP || compOp == GE_OP) {
		/*
		if (nodePage != notfound) {
			BtreeNode node;
			bo.ReadNode(&node, nodePage, &tree, indexFilename);

			while (node.leaf == false) {
				int pos = bo.FindPosition(&node, value, tree.attributeType);
				int nextNode = node.childrenNums[pos];
				BtreeNode node;
				bo.ReadNode(&node, nextNode, &tree, indexFilename);
				if (node.leaf == true) {
					leafPage = node.nodeID;
					break;
				}
			}
		}
		*/
		while(nodePage != notfound){
			BtreeNode node;
			bo.GetNode(&node, nodePage, &tree, indexFilename);
			if (node.leaf == true) {
				leafPage = node.nodeID;
				break;
			}
			int pos = bo.FindPosition(&node, value, tree.attributeType);
			nodePage = node.childrenNums[pos];
			//bo.ReadNode(&node, nextNode, &tree, indexFilename);

		}
		//BtreeNode curNode;
		//bo.ReadNode(&curNode, leafPage, &tree, indexFilename);

		//leafPage = curNode.nodeID;
		while (leafPage != notfound) {
			BtreeNode curNode;
			bo.GetNode(&curNode, leafPage, &tree, indexFilename);

			//if tombstoned
			if (curNode.deletedNode == 1) {
				leafPage = curNode.nextLeafNum;
				continue;
			}

			//cout<<"key size: "<<curNode.keys.size()<<endl;
			for (unsigned int pos = 0; pos < curNode.keys.size(); pos++) {

				int *key = (int*) malloc(sizeof(int));
				memcpy(key, curNode.keys[pos], sizeof(int));
				//cout<<"poskey:   "<<*key<<endl;
				int *keya = (int*) malloc(sizeof(int));
				memcpy(keya, value, sizeof(int));
				int eq;
				//if (*key < *keya) {
				//	eq = -1;
				//} else if (*key == *keya) {
				//	eq = 0;
				//} else {
				//	eq = 1;
				//}
				eq = bo.Compare(curNode.keys[pos],value,tree.attributeType);

				//int eq = memcmp(curNode.keys[pos], value, sizeof(value));
				if (eq > 0) {
					for (unsigned int i = 0;
							i < curNode.buckets[pos].rids.size(); ++i) {
						srids.push_back(curNode.buckets[pos].rids[i]);
					}
				}
				if (compOp == GE_OP && eq == 0) {
					for (unsigned int i = 0;
							i < curNode.buckets[pos].rids.size(); ++i) {
						srids.push_back(curNode.buckets[pos].rids[i]);
					}
				}
			}
			leafPage = curNode.nextLeafNum;
		}

		return 0;
	}

	return 0;

}

//get next entry ID in the bucket
RC IX_IndexScan::GetNextEntry(RID &rid) {
        if(closed)
                return -1;
        if (curIndex >= srids.size())
                return -1;

        rid = srids[curIndex];
        curIndex++;
        return 0;
}

//close the index scan
RC IX_IndexScan::CloseScan() {
	curIndex = 0;
	closed = 1;
	srids.clear();
	return 0;
}
