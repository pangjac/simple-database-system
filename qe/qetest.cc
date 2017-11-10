#include <fstream>
#include <iostream>
#include <cassert>

#include <vector>

#include "qe.h"

using namespace std;

// Global Initialization
RM *rm = RM::Instance();
IX_Manager *ixManager = IX_Manager::Instance();

const int success = 0;

// Number of tuples in each relation
const int tuplecount = 100;

// Buffer size and character buffer size
const unsigned bufsize = 200;


void createLeftTable()
{
    // Functions Tested;
    // 1. Create Table
    cout << "****Create Left Table****" << endl;

    vector<Attribute> attrs;

    Attribute attr;
    attr.name = "A";
    attr.type = TypeInt;
    attr.length = 4;
    attrs.push_back(attr);

    attr.name = "B";
    attr.type = TypeInt;
    attr.length = 4;
    attrs.push_back(attr);

    attr.name = "C";
    attr.type = TypeReal;
    attr.length = 4;
    attrs.push_back(attr);

    RC rc = rm->createTable("left", attrs);
    assert(rc == success);
    cout << "****Left Table Created!****" << endl;
}

   
void createRightTable()
{
    // Functions Tested;
    // 1. Create Table
    cout << "****Create Right Table****" << endl;

    vector<Attribute> attrs;

    Attribute attr;
    attr.name = "B";
    attr.type = TypeInt;
    attr.length = 4;
    attrs.push_back(attr);

    attr.name = "C";
    attr.type = TypeReal;
    attr.length = 4;
    attrs.push_back(attr);

    attr.name = "D";
    attr.type = TypeInt;
    attr.length = 4;
    attrs.push_back(attr);

    RC rc = rm->createTable("right", attrs);
    assert(rc == success);
    cout << "****Right Table Created!****" << endl;
}


// Prepare the tuple to left table in the format conforming to Insert/Update/ReadTuple and readAttribute
void prepareLeftTuple(const int a, const int b, const float c, void *buf)
{    
    int offset = 0;
    
    memcpy((char *)buf + offset, &a, sizeof(int));
    offset += sizeof(int);
    
    memcpy((char *)buf + offset, &b, sizeof(int));
    offset += sizeof(int);
    
    memcpy((char *)buf + offset, &c, sizeof(float));
    offset += sizeof(float);
}


// Prepare the tuple to right table in the format conforming to Insert/Update/ReadTuple, readAttribute
void prepareRightTuple(const int b, const float c, const int d, void *buf)
{
    int offset = 0;
    
    memcpy((char *)buf + offset, &b, sizeof(int));
    offset += sizeof(int);
    
    memcpy((char *)buf + offset, &c, sizeof(float));
    offset += sizeof(float);
    
    memcpy((char *)buf + offset, &d, sizeof(int));
    offset += sizeof(int);
}


void populateLeftTable(vector<RID> &rids)
{
    // Functions Tested
    // 1. InsertTuple
    RID rid;
    void *buf = malloc(bufsize);
    for(int i = 0; i < tuplecount; ++i)

    {
        memset(buf, 0, bufsize);
        
        // Prepare the tuple data for insertion
        // a in [0,99], b in [10, 109], c in [50, 149.0]
        int a = i;
        int b = i + 10;
        float c = (float)(i + 50);
        prepareLeftTuple(a, b, c, buf);
        
        RC rc = rm->insertTuple("left", buf, rid);
        assert(rc == success);
        rids.push_back(rid);
    }
    
    free(buf);
}


void populateRightTable(vector<RID> &rids)
{
    // Functions Tested
    // 1. InsertTuple
    RID rid;
    void *buf = malloc(bufsize);

    for(int i = 0; i < tuplecount; ++i)
    {
        memset(buf, 0, bufsize);
        
        // Prepare the tuple data for insertion
        // b in [20, 120], c in [25, 124.0], d in [0, 99]
        int b = i + 20;
        float c = (float)(i + 25);
        int d = i;
        prepareRightTuple(b, c, d, buf);
        
        RC rc = rm->insertTuple("right", buf, rid);
        assert(rc == success);
        rids.push_back(rid);
    }

    free(buf);
}


void createIndexforLeftB(vector<RID> &rids)
{
    RC rc;
    // Create Index
    rc = ixManager->CreateIndex("left", "B");
    assert(rc == success);
    
    // Open Index
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex("left", "B", ixHandle);
    assert(rc == success);
    
    // Insert Entry
    for(int i = 0; i < tuplecount; ++i)
    {
        // key in [10, 109]
        int key = i + 10;
              
        rc = ixHandle.InsertEntry(&key, rids[i]);
        assert(rc == success);
    }
    
    // Close Index
    rc = ixManager->CloseIndex(ixHandle);
    assert(rc == success);    
}


void createIndexforLeftC(vector<RID> &rids)
{
    RC rc;
    // Create Index
    rc = ixManager->CreateIndex("left", "C");
    assert(rc == success);
    
    // Open Index
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex("left", "C", ixHandle);
    assert(rc == success);
    
    // Insert Entry
    for(int i = 0; i < tuplecount; ++i)
    {
        // key in [50, 149.0]
        float key = (float)(i + 50);
        
        rc = ixHandle.InsertEntry(&key, rids[i]);
        assert(rc == success);
    }
    
    // Close Index
    rc = ixManager->CloseIndex(ixHandle);
    assert(rc == success);
}


void createIndexforRightB(vector<RID> &rids)
{
    RC rc;
    // Create Index
    rc = ixManager->CreateIndex("right", "B");
    assert(rc == success);
    
    // Open Index
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex("right", "B", ixHandle);
    assert(rc == success);
    
    // Insert Entry
    for(int i = 0; i < tuplecount; ++i)
    {
        // key in [20, 120]
        int key = i + 20;
              
        rc = ixHandle.InsertEntry(&key, rids[i]);
        assert(rc == success);
    }
    
    // Close Index
    rc = ixManager->CloseIndex(ixHandle);
    assert(rc == success);    
}


void createIndexforRightC(vector<RID> &rids)
{
    RC rc;
    // Create Index
    rc = ixManager->CreateIndex("right", "C");
    assert(rc == success);
    
    // Open Index
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex("right", "C", ixHandle);
    assert(rc == success);
    
    // Insert Entry
    for(int i = 0; i < tuplecount; ++i)
    {
        // key in [25, 124]
        float key = (float)(i + 25);
        
        // Insert the key into index
        rc = ixHandle.InsertEntry(&key, rids[i]);
        assert(rc == success);
    }
    
    // Close Index
    rc = ixManager->CloseIndex(ixHandle);
    assert(rc == success);
}


void testCase_1()
{
    // Functions Tested;
    // 1. Filter -- TableScan as input, on Integer Attribute
    cout << "****In Test Case 1****" << endl;
    
    TableScan *ts = new TableScan(*rm, "left");
    
    // Set up condition
    Condition cond;
    cond.lhsAttr = "left.B";
    cond.op = LE_OP;
    cond.bRhsIsAttr = false;
    Value value;
    value.type = TypeInt;
    value.data = malloc(bufsize);
    *(int *)value.data = 25;
    cond.rhsValue = value;
    
    // Create Filter 
    Filter filter(ts, cond);
    
    // Go over the data through iterator
    void *data = malloc(bufsize);
    while(filter.getNextTuple(data) != QE_EOF)
    {
        int offset = 0;
        // Print left.A
        cout << "left.A " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        // Print left.B
        cout << "left.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        // Print left.C
        cout << "left.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        memset(data, 0, bufsize);
    }
   
    free(value.data);
    free(data);
    return;
}


void testCase_2()
{
    // Functions Tested
    // 1. Filter -- IndexScan as input, on TypeReal attribute
    cout << "****In Test Case 2****" << endl;
    
    IX_IndexHandle ixHandle;
    ixManager->OpenIndex("right", "C", ixHandle);
    IndexScan *is = new IndexScan(*rm, ixHandle, "right");
    
    // Set up condition
    Condition cond;
    cond.lhsAttr = "right.C";
    cond.op = GE_OP;
    cond.bRhsIsAttr = false;
    Value value;
    value.type = TypeReal;
    value.data = malloc(bufsize);
    *(float *)value.data = 100.0;
    cond.rhsValue = value;
    
    // Create Filter
    is->setIterator(GE_OP, value.data);
    Filter filter(is, cond);
    
    // Go over the data through iterator
    void *data = malloc(bufsize);
    while(filter.getNextTuple(data) != QE_EOF)
    {
        int offset = 0;
        // Print right.B
        cout << "right.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        // Print right.C
        cout << "right.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        // Print right.D
        cout << "right.D " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        memset(data, 0, bufsize);
    }

    ixManager->CloseIndex(ixHandle);
    free(value.data);
    free(data);
    return;
}


void testCase_3()
{
    // Functions Tested
    // 1. Project -- TableScan as input  
    cout << "****In Test Case 3****" << endl;
    
    TableScan *ts = new TableScan(*rm, "left");
    
    vector<string> attrNames;
    attrNames.push_back("left.A");
    attrNames.push_back("left.B");
    
    // Create Projector 
    Project project(ts, attrNames);
    
    // Go over the data through iterator
    void *data = malloc(bufsize);
    while(project.getNextTuple(data) != QE_EOF)
    {
        int offset = 0;
 
        // Print right.C
        cout << "left.A " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        // Print right.D
        cout << "left.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        memset(data, 0, bufsize);
    }
    
    free(data);
    return;
}


void testCase_4()
{
    // Functions Tested
    // 1. NLJoin -- on TypeInt Attribute
    cout << "****In Test Case 4****" << endl;
    
    // Prepare the iterator and condition
    TableScan *leftIn = new TableScan(*rm, "left");
    TableScan *rightIn = new TableScan(*rm, "right");
    
    Condition cond;
    cond.lhsAttr = "left.B";
    cond.op= EQ_OP;
    cond.bRhsIsAttr = true;
    cond.rhsAttr = "right.B";
    
    // Create NLJoin
    NLJoin nljoin(leftIn, rightIn, cond, 10);
        
    // Go over the data through iterator
    void *data = malloc(bufsize);
    while(nljoin.getNextTuple(data) != QE_EOF)
    {
        int offset = 0;
 
        // Print left.A
        cout << "left.A " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        // Print left.B
        cout << "left.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
 
        // Print left.C
        cout << "left.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        // Print right.B
        cout << "right.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
 
        // Print right.C
        cout << "right.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        // Print right.D
        cout << "right.D " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        memset(data, 0, bufsize);
    }
    
    free(data);
    return;
}


void testCase_5()
{
    // Functions Tested
    // 1. INLJoin -- on TypeReal Attribute
    cout << "****In Test Case 5****" << endl;
    
    // Prepare the iterator and condition
    TableScan *leftIn = new TableScan(*rm, "left");
    
    IX_IndexHandle ixRightHandle;
    ixManager->OpenIndex("right", "C", ixRightHandle);
    IndexScan *rightIn = new IndexScan(*rm, ixRightHandle, "right");
    
    Condition cond;
    cond.lhsAttr = "left.C";
    cond.op = EQ_OP;
    cond.bRhsIsAttr = true;
    cond.rhsAttr = "right.C";
    
    // Create INLJoin
    INLJoin inljoin(leftIn, rightIn, cond, 10);
        
    // Go over the data through iterator
    void *data = malloc(bufsize);
    while(inljoin.getNextTuple(data) != QE_EOF)
    {
        int offset = 0;
 
        // Print left.A
        cout << "left.A " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        // Print left.B
        cout << "left.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
 
        // Print left.C
        cout << "left.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);

        // Print right.B
        cout << "right.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
 
        // Print right.C
        cout << "right.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        // Print right.D
        cout << "right.D " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        memset(data, 0, bufsize);
    }
   
    ixManager->CloseIndex(ixRightHandle); 
    free(data);
    return;
}


void testCase_6()
{
    // Functions Tested
    // 1. HashJoin -- on TypeInt Attribute
    cout << "****In Test Case 6****" << endl;
    
    // Prepare the iterator and condition
    TableScan *leftIn = new TableScan(*rm, "left");
    TableScan *rightIn = new TableScan(*rm, "right");
    
    Condition cond;
    cond.lhsAttr = "left.B";
    cond.op = EQ_OP;
    cond.bRhsIsAttr = true;
    cond.rhsAttr = "right.B";
    
    // Create HashJoin
    HashJoin hashjoin(leftIn, rightIn, cond, 5);
        
    // Go over the data through iterator
    void *data = malloc(bufsize);
    while(hashjoin.getNextTuple(data) != QE_EOF)
    {
        int offset = 0;
 
        // Print left.A
        cout << "left.A " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        // Print left.B
        cout << "left.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
 
        // Print left.C
        cout << "left.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        // Print right.B
        cout << "right.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
 
        // Print right.C
        cout << "right.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        // Print right.D
        cout << "right.D " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        memset(data, 0, bufsize);
    }
   
    free(data);
    return;
}


void testCase_7()
{
    // Functions Tested
    // 1. INLJoin -- on TypeInt Attribute
    // 2. Filter -- on TypeInt Attribute
    cout << "****In Test Case 7****" << endl;
    
    // Prepare the iterator and condition
    TableScan *leftIn = new TableScan(*rm, "left");
    
    IX_IndexHandle ixHandle;
    ixManager->OpenIndex("right", "B", ixHandle);
    IndexScan *rightIn = new IndexScan(*rm, ixHandle, "right");
    
    Condition cond_j;
    cond_j.lhsAttr = "left.B";
    cond_j.op = EQ_OP;
    cond_j.bRhsIsAttr = true;
    cond_j.rhsAttr = "right.B";
    
    // Create INLJoin
    INLJoin *inljoin = new INLJoin(leftIn, rightIn, cond_j, 5);
    
    // Create Filter
    Condition cond_f;
    cond_f.lhsAttr = "right.B";
    cond_f.op = EQ_OP;
    cond_f.bRhsIsAttr = false;
    Value value;
    value.type = TypeInt;
    value.data = malloc(bufsize);
    *(int *)value.data = 50;
    cond_f.rhsValue = value;
    
    Filter filter(inljoin, cond_f);
            
    // Go over the data through iterator
    void *data = malloc(bufsize);
    while(filter.getNextTuple(data) != QE_EOF)
    {
        int offset = 0;
 
        // Print left.A
        cout << "left.A " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        // Print left.B
        cout << "left.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
 
        // Print left.C
        cout << "left.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
    
        // Print right.B
        cout << "right.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
 
        // Print right.C
        cout << "right.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
         
        // Print right.D
        cout << "right.D " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        memset(data, 0, bufsize);
    }
   
    ixManager->CloseIndex(ixHandle); 
    free(value.data); 
    free(data);
    return;
}


void testCase_8()
{
    // Functions Tested
    // 1. HashJoin -- on TypeReal Attribute
    // 2. Project
    cout << "****In Test Case 8****" << endl;
    
    // Prepare the iterator and condition
    TableScan *leftIn = new TableScan(*rm, "left");
    TableScan *rightIn = new TableScan(*rm, "right");
    
    Condition cond_j;
    cond_j.lhsAttr = "left.C";
    cond_j.op = EQ_OP;
    cond_j.bRhsIsAttr = true;
    cond_j.rhsAttr = "right.C";
    
    // Create HashJoin
    HashJoin *hashjoin = new HashJoin(leftIn, rightIn, cond_j, 10);
    
    // Create Projector
    vector<string> attrNames;
    attrNames.push_back("left.A");
    attrNames.push_back("right.D");
    
    Project project(hashjoin, attrNames);
        
    // Go over the data through iterator
    void *data = malloc(bufsize);
    while(project.getNextTuple(data) != QE_EOF)
    {
        int offset = 0;
 
        // Print left.A
        cout << "left.A " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
                
        // Print right.D
        cout << "right.D " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        memset(data, 0, bufsize);
    }
   
    free(data);
    return;
}


void testCase_9()
{
    // Functions Tested
    // 1. NLJoin -- on TypeFloat Attribute
    // 2. HashJoin -- on TypeInt Attribute
    
    cout << "****In Test Case 9****" << endl;
    
    // Prepare the iterator and condition
    TableScan *leftIn = new TableScan(*rm, "left");
    TableScan *rightIn = new TableScan(*rm, "right");
    
    Condition cond;
    cond.lhsAttr = "left.C";
    cond.op = EQ_OP;
    cond.bRhsIsAttr = true;
    cond.rhsAttr = "right.C";
    
    // Create NLJoin
    NLJoin *nljoin = new NLJoin(leftIn, rightIn, cond, 10);
    
    // Create HashJoin
    TableScan *thirdIn = new TableScan(*rm, "left", "leftSecond");
    Condition cond_h;
    cond_h.lhsAttr = "left.B";
    cond_h.op = EQ_OP;
    cond_h.bRhsIsAttr = true;
    cond_h.rhsAttr = "right.B";
    HashJoin hashjoin(leftIn, rightIn, cond_h, 8);
        
    // Go over the data through iterator
    void *data = malloc(bufsize);
    while(hashjoin.getNextTuple(data) != QE_EOF)
    {
        int offset = 0;
 
        // Print left.A
        cout << "left.A " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        // Print left.B
        cout << "left.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
 
        // Print left.C
        cout << "left.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        // Print right.B
        cout << "right.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
 
        // Print right.C
        cout << "right.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        // Print right.D
        cout << "right.D " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        // Print leftSecond.A
        cout << "leftSecond.A " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        // Print left.B
        cout << "leftSecond.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
 
        // Print leftSecond.C
        cout << "leftSecond.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        memset(data, 0, bufsize);
    }
   
    free(data);
    return;
}


void testCase_10()
{
    // Functions Tested
    // 1. Filter  
    // 2. Project
    // 3. INLJoin
    
    cout << "****In Test Case 10****" << endl;

    // Create Filter
    IX_IndexHandle ixLeftHandle;
    ixManager->OpenIndex("left", "B", ixLeftHandle);
    IndexScan *leftIn = new IndexScan(*rm, ixLeftHandle, "left");

    Condition cond_f;
    cond_f.lhsAttr = "left.B";
    cond_f.op = LT_OP;
    cond_f.bRhsIsAttr = false;
    Value value;
    value.type = TypeInt;
    value.data = malloc(bufsize);
    *(int *)value.data = 75;
    cond_f.rhsValue = value;
   
    leftIn->setIterator(LT_OP, value.data); 
    Filter *filter = new Filter(leftIn, cond_f);

    // Create Projector
    vector<string> attrNames;
    attrNames.push_back("left.A");
    attrNames.push_back("left.C");
    Project *project = new Project(filter, attrNames);
    
    // Create INLJoin
    IX_IndexHandle ixRightHandle;
    ixManager->OpenIndex("right", "C", ixRightHandle);
    IndexScan *rightIn = new IndexScan(*rm, ixRightHandle, "right");

    Condition cond_j;
    cond_j.lhsAttr = "left.C";
    cond_j.op = EQ_OP;
    cond_j.bRhsIsAttr = true;
    cond_j.rhsAttr = "right.C";
    
    INLJoin inljoin(project, rightIn, cond_j, 8); 
    
    // Go over the data through iterator
    void *data = malloc(bufsize);
    while(inljoin.getNextTuple(data) != QE_EOF)
    {
        int offset = 0;
 
        // Print left.A
        cout << "left.A " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        // Print left.C
        cout << "left.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        // Print right.B
        cout << "right.B " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
 
        // Print right.C
        cout << "right.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        // Print right.D
        cout << "right.D " << *(int *)((char *)data + offset) << endl;
        offset += sizeof(int);
        
        memset(data, 0, bufsize);
    }

    ixManager->CloseIndex(ixLeftHandle);
    ixManager->CloseIndex(ixRightHandle);
    free(value.data);
    free(data);
    return;
}


/*void extraTestCase_1()
{
    // Functions Tested
    // 1. TableScan
    // 2. Aggregate -- MAX
    cout << "****In Extra Test Case 1****" << endl;
    
    // Create TableScan
    TableScan *input = new TableScan(*rm, "left");
    
    // Create Aggregate
    Attribute aggAttr;
    aggAttr.name = "left.B";
    aggAttr.type = TypeInt;
    aggAttr.length = 4;   
    Aggregate agg(input, aggAttr, MAX);
    
    void *data = malloc(bufsize);
    while(agg.getNextTuple(data) != QE_EOF)
    {
        cout << "MAX(left.B) " << *(float *)data << endl;
        memset(data, 0, sizeof(float));
    }
    
    free(data);
    return;
}


void extraTestCase_2()
{
    // Functions Tested
    // 1. TableScan
    // 2. Aggregate -- AVG
    cout << "****In Extra Test Case 2****" << endl;
    
    // Create TableScan
    TableScan *input = new TableScan(*rm, "right");
    
    // Create Aggregate
    Attribute aggAttr;
    aggAttr.name = "right.B";
    aggAttr.type = TypeInt;
    aggAttr.length = 4;   
    Aggregate agg(input, aggAttr, AVG);
    
    void *data = malloc(bufsize);
    while(agg.getNextTuple(data) != QE_EOF)
    {
        cout << "AVG(right.B) " << *(float *)data << endl;
        memset(data, 0, sizeof(float));
    }
    
    free(data);
    return;
}


void extraTestCase_3()
{
    // Functions Tested
    // 1. TableScan
    // 2. Aggregate -- MIN
    cout << "****In Extra Test Case 3****" << endl;
    
    // Create TableScan
    TableScan *input = new TableScan(*rm, "left");
    
    // Create Aggregate
    Attribute aggAttr;
    aggAttr.name = "left.B";
    aggAttr.type = TypeInt;
    aggAttr.length = 4;

    Attribute gAttr;
    gAttr.name = "left.C";
    gAttr.type = TypeReal;
    gAttr.length = 4;
    Aggregate agg(input, aggAttr, gAttr, MIN);
    
    void *data = malloc(bufsize);
    while(agg.getNextTuple(data) != QE_EOF)
    {
        int offset = 0;
        
        // Print left.C
        cout << "left.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);

        // Print left.B
        cout << "MIN(left.B) " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(int);

        memset(data, 0, bufsize);
    }
    
    free(data);
    return;
}


void extraTestCase_4()
{
    // Functions Tested
    // 1. TableScan
    // 2. Aggregate -- SUM
    cout << "****In Extra Test Case 4****" << endl;
    
    // Create TableScan
    TableScan *input = new TableScan(*rm, "right");
    
    // Create Aggregate
    Attribute aggAttr;
    aggAttr.name = "right.B";
    aggAttr.type = TypeInt;
    aggAttr.length = 4;

    Attribute gAttr;
    gAttr.name = "right.C";
    gAttr.type = TypeReal;
    gAttr.length = 4;
    Aggregate agg(input, aggAttr, gAttr, SUM);
    
    void *data = malloc(bufsize);
    while(agg.getNextTuple(data) != QE_EOF)
    {
        int offset = 0;
        
        // Print right.C
        cout << "right.C " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(float);
        
        // Print right.B
        cout << "SUM(right.B) " << *(float *)((char *)data + offset) << endl;
        offset += sizeof(int);

        memset(data, 0, bufsize);
    }
    
    free(data);
    return;
}
*/
void My_ownTest()
{

	  TableScan *input = new TableScan(*rm, "right");
	   Condition cond;
	    cond.lhsAttr = "left.B";
	    cond.op = LE_OP;
	    cond.bRhsIsAttr = false;
	    Value value;
	    value.type = TypeInt;
	    value.data = malloc(bufsize);
	    *(int *)value.data = 25;
	    cond.rhsValue = value;
	    Filter *aa=new Filter(input,cond);
	    vector <Attribute> attr;
	    input->getAttributes(attr);
	    string a=aa->GetAttributeName(attr[0].name);
	    cout<<a;

}
int main() 
{
    // Create the left table, and populate the table
    vector<RID> leftRIDs;
    createLeftTable();
    populateLeftTable(leftRIDs);
    
    // Create the right table, and populate the table
    vector<RID> rightRIDs;
    createRightTable();
    populateRightTable(rightRIDs);
    
    // Create index for attribute B and C of the left table
    createIndexforLeftB(leftRIDs);
     createIndexforLeftC(leftRIDs);
    
    // Create index for attribute B and C of the right table
   createIndexforRightB(rightRIDs);
    createIndexforRightC(rightRIDs);
   
    //My_ownTest();
	//cout<<"wewewew"<<endl;
    // Test Cases
  // testCase_1();
    //testCase_2();
    testCase_3();
    //testCase_4();
    //testCase_5();
    //testCase_6();
    //testCase_7();
   // testCase_8();
   // testCase_9();
    //testCase_10();

    // Extra Credit
    //extraTestCase_1();
    //extraTestCase_2();
    //extraTestCase_3();
    //extraTestCase_4();

    return 0;
}

