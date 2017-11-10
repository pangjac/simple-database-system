#include <fstream>
#include <iostream>
#include <cassert>
#include <vector>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"

using namespace std;

//Global Initializations
IX_Manager *ixManager = IX_Manager::Instance();
const int success = 0;


void createTable(RM *rm, const string tablename)
{
    // Functions tested
    // 1. Create Table
    vector<Attribute> attrs;

    Attribute attr;
    attr.name = "EmpName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)100;
    attrs.push_back(attr);

    attr.name = "Age";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "Height";
    attr.type = TypeReal;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "Salary";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    RC rc = rm->createTable(tablename, attrs);
    assert(rc == success);
    cout << "****Table Created: " << tablename << " ****" << endl << endl;
}


void testCase_1(const string tablename, const string attrname)
{    
    // Functions tested
    // 1. Create Index **
    // 2. OpenIndex **
    // 3. CreateIndex -- when index is already created on that key **
    // 4. CloseIndex **
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 1****" << endl;

    RC rc;
    // Test CreateIndex
    rc = ixManager->CreateIndex(tablename, attrname);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }
    
    // Test OpenIndex
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex(tablename, attrname, ixHandle);
    if(rc == success) 
    {
        cout << "Index on " << attrname << " Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test create duplicate index
    rc = ixManager->CreateIndex(tablename, attrname);
    if(rc != success)
    {
        cout << "Duplicate Index not Created -- correct!" << endl;
    }
    else
    {
        cout << "Duplicate Index Created -- failure..." << endl;
    }
    
    // Test CloseIndex
    rc = ixManager->CloseIndex(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  
    return;
}


void testCase_2(const string tablename, const string attrname)
{    
    // Functions tested
    // 1. OpenIndex 
    // 2. Insert entry **
    // 3. Delete entry **
    // 4. Delete entry -- when the value is not there **
    // 5. Close Index
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 2****" << endl;
    
    RID rid;
    RC rc;

    // Test Open Index
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex(tablename, attrname, ixHandle);
    if(rc == success) 
    {
        cout << "Index on " << attrname << " Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }
    
    unsigned numOfTuples = 100;
    unsigned key = 100;
    rid.pageNum = key;
    rid.slotNum = key+1;

    void *payload = malloc(sizeof(int));
    int age = 18;
	memcpy(payload, &age, sizeof(int));
    
    // Test Insert Entry
    for(unsigned i = 0; i < numOfTuples; i++) 
    {
        rc = ixHandle.InsertEntry(&age+i, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Entry..." << endl;
        }     
    }
    rid.slotNum=key+2;
    rc = ixHandle.InsertEntry(&age, rid);
    

    // Test Delete Entry
    rc = ixHandle.DeleteEntry(payload, rid);
    if(rc != success)
    {
        cout << "Failed Deleting Entry..." << endl;
    }      
    
    // Test Delete Entry again
    rc = ixHandle.DeleteEntry(payload, rid);
    if(rc == success) //This time it should NOT give success because entry is not there.
    {
        cout << "Entry deleted again...failure" << endl;
    }
    
    // Test Close Index
    rc = ixManager->CloseIndex(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }
    

    return;
}


void testCase_3(const string tablename, const string attrname)
{    
    // Functions tested
    // 1. Destroy Index **
    // 2. OpenIndex -- should fail
    // 3. Scan  -- should fail
    cout << endl << "****In Test Case 3****" << endl;
    
    RC rc;
    // Test Destroy Index
    rc = ixManager->DestroyIndex(tablename, attrname);
    if(rc != success)
    {
        cout << "Failed Destroying Index..." << endl;
    }     
    
    // Test Open the destroyed index
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex(tablename, attrname, ixHandle);
    if(rc == success) //should not work now
    {
        cout << "Index opened again...failure" << endl;
    }  
    
    IX_IndexScan *ixScan = new IX_IndexScan();
    // Test Open Scan
    rc = ixScan->OpenScan(ixHandle, NO_OP, NULL);
    if(rc == success) 
    {
        cout << "Scan opened again...failure" << endl;
    }  

    return;
}


void testCase_4(const string tablename, const string attrname)
{    
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry
    // 4. Scan entries NO_OP -- open**
    // 5. Scan close **
    // 6. CloseIndex
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 4****" << endl;
 
    RID rid;
    RC rc;
    // Test CreateIndex
    rc = ixManager->CreateIndex(tablename, attrname);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }
    
    // Test OpenIndex
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex(tablename, attrname, ixHandle);
    if(rc == success) 
    {
        cout << "Index on " << attrname << " Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }
    
    // Test InsertEntry
    unsigned numOfTuples = 100;
    unsigned maxValue = 500;
    for(unsigned i = 0; i <= numOfTuples; i++) 
    {
        unsigned key = i+1;//just in case somebody starts pageNum and recordId from 1
        RID rid;
        rid.pageNum = key;
        rid.slotNum = key+1;
        
        rc = ixHandle.InsertEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Entry..." << endl;
        }     
    }
    
    // Scan
    IX_IndexScan *ixScan = new IX_IndexScan();
    int a = 50;
    rc = ixScan->OpenScan(ixHandle, NE_OP, &a);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan!" << endl;
    }  
    
    while(ixScan->GetNextEntry(rid) == success) 
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;
    }
    
    // Close Scan
    rc = ixScan->CloseScan();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  
    
    // Close Index
    rc = ixManager->CloseIndex(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }

    // Destroy Index
    rc = ixManager->DestroyIndex(tablename, attrname);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
    
    return;  
}



void testCase_5(const string tablename, const string attrname)
{    
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry 
    // 4. Scan entries using GE_OP operator and checking if the values returned are correct. **
    // 5. Scan close 
    // 6. CloseIndex 
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.   
    cout << endl << "****In Test Case 5****" << endl;
        
    RID rid;
    RC rc;
    // Test Create Index
    rc = ixManager->CreateIndex(tablename, attrname);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }
    
    // Test Open Index
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex(tablename, attrname, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test Insert Entry
    unsigned numOfTuples = 100;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        unsigned key = i;
        RID rid;
        rid.pageNum = key;
        rid.slotNum = key+1;
        
        rc = ixHandle.InsertEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }
    
    numOfTuples = 100;
    for(unsigned i = 501; i < numOfTuples+500; i++) 
    {
        unsigned key = i;
        RID rid;
        rid.pageNum = key;
        rid.slotNum = key+1;
        
        rc = ixHandle.InsertEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }
    
    // Test Open Scan
    IX_IndexScan *ixScan = new IX_IndexScan();
    int value = 501;

    rc= ixScan->OpenScan(ixHandle, GE_OP, &value);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }  
    
    // Test IndexScan iterator
    while(ixScan->GetNextEntry(rid) == success) 
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;
        if (rid.pageNum < 501 || rid.slotNum < 501)
        {
            cout << "Wrong entries output...failure" << endl;
        }
    }
    
    // Test Closing Scan
    rc= ixScan->CloseScan();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  
    
    // Test Closing Index
    rc = ixManager->CloseIndex(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  
    
    // Test Destroying Index
    rc = ixManager->DestroyIndex(tablename, attrname);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
    return;
}


void testCase_6(const string tablename, const string attrname)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry 
    // 4. Scan entries using LT_OP operator and checking if the values returned are correct. Returned values are part of two separate insertions **
    // 5. Scan close 
    // 6. CloseIndex 
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 6****" << endl;

    // Test CreateIndex
    RC rc;
    RID rid;
    rc = ixManager->CreateIndex(tablename, attrname);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex    
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex(tablename, attrname, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  

    // Test InsertEntry    
    unsigned numOfTuples = 2000;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        float key = (float)i + 76.5;
        rid.pageNum = i;
        rid.slotNum = i;
        
        rc = ixHandle.InsertEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    numOfTuples = 2000;
    for(unsigned i = 6000; i <= numOfTuples+6000; i++) 
    {
        float key = (float)i + 76.5;
        rid.pageNum = i;
        rid.slotNum = i-(unsigned)500;
        
        rc = ixHandle.InsertEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    // Test Scan    
    IX_IndexScan *ixScan = new IX_IndexScan();
    float compVal = 6500;

    rc = ixScan->OpenScan(ixHandle, LT_OP, &compVal);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }  

    // Test IndexScan Iterator    
    while(ixScan->GetNextEntry(rid) == success) 
    {
        if(rid.pageNum % 500 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;
        if ((rid.pageNum > 2000 && rid.pageNum < 6000) || rid.pageNum >= 6500)
        {
            cout << "Wrong entries output...failure" << endl;
        }
    }

    // Test CloseScan
    rc = ixScan->CloseScan();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  

    // Test CloseIndex    
    rc = ixManager->CloseIndex(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  

    // Test DestroyIndex    
    rc = ixManager->DestroyIndex(tablename, attrname);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
    return;
}


void testCase_7(const string tablename, const string attrname)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry 
    // 4. Scan entries, and delete entries
    // 5. Scan close 
    // 6. CloseIndex 
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 7****" << endl;
    
    RC rc;
    RID rid;
    // Test CreateIndex
    rc = ixManager->CreateIndex(tablename, attrname);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex(tablename, attrname, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test InsertEntry
    unsigned numOfTuples = 200;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        float key = (float)i;
        rid.pageNum = i;
        rid.slotNum = i;
        
        rc = ixHandle.InsertEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    // Test Scan
    IX_IndexScan *ixScan = new IX_IndexScan();
    float compVal = 200.0;

    rc = ixScan->OpenScan(ixHandle, LE_OP, &compVal);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    // Test DeleteEntry in IndexScan Iterator
    while(ixScan->GetNextEntry(rid) == success) 
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;

        float key = (float)rid.pageNum;
        rc = ixHandle.DeleteEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed deleting entry in Scan..." << endl;
        }
    }
    cout << endl;

    // Test CloseScan
    rc = ixScan->CloseScan();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }

    compVal = 200.0;

    // Open Scan again
    rc = ixScan->OpenScan(ixHandle, LE_OP, &compVal);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    // Test IndexScan Iterator
    while(ixScan->GetNextEntry(rid) == success) 
    {
        cout << "Entry returned: " << rid.pageNum << " " << rid.slotNum << "--- failure" << endl;

        if(rid.pageNum > 200)
        {
            cout << "Wrong entries output...failure" << endl;
        }
    }

    // Test CloseScan
    rc = ixScan->CloseScan();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  

    // Test CloseIndex    
    rc = ixManager->CloseIndex(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  

    // Test DestroyIndex    
    rc = ixManager->DestroyIndex(tablename, attrname);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
        
    return;
}


void testCase_8(const string tablename, const string attrname)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry 
    // 4. Scan entries, and delete entries
    // 5. Scan close
    // 6. Insert entries again
    // 7. Scan entries
    // 8. CloseIndex 
    // 9. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 8****" << endl;
    
    RC rc;
    RID rid;
    // Test CreateIndex
    rc = ixManager->CreateIndex(tablename, attrname);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex(tablename, attrname, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test InsertEntry
    unsigned numOfTuples = 200;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        float key = (float)i;
        rid.pageNum = i;
        rid.slotNum = i;
        
        rc = ixHandle.InsertEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    // Test Scan
    IX_IndexScan *ixScan = new IX_IndexScan();
    float compVal = 200.0;

    rc = ixScan->OpenScan(ixHandle, LE_OP, &compVal);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    // Test DeleteEntry in IndexScan Iterator
    while(ixScan->GetNextEntry(rid) == success) 
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;
        
        float key = (float)rid.pageNum;
        rc = ixHandle.DeleteEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed deleting entry in Scan..." << endl;
        }
    }
    cout << endl;

    // Test CloseScan
    rc = ixScan->CloseScan();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }

    // Test InsertEntry Again
    numOfTuples = 500;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        float key = (float)i;
        rid.pageNum = i;
        rid.slotNum = i;
        
        rc = ixHandle.InsertEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    // Test Scan
    compVal = 450.0;
    rc = ixScan->OpenScan(ixHandle, GT_OP, &compVal);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    while(ixScan->GetNextEntry(rid) == success)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;

        if(rid.pageNum <= 450 || rid.slotNum <= 450)
        {
            cout << "Wrong entries output...failure" << endl;
        }
    }
    cout << endl;

    // Test CloseScan
    rc = ixScan->CloseScan();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  

    // Test CloseIndex    
    rc = ixManager->CloseIndex(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  

    // Test DestroyIndex    
    rc = ixManager->DestroyIndex(tablename, attrname);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
        
    return;
}



void testCase_extra_1(const string tablename, const string attrname)
{    
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex 
    // 3. Insert entry 
    // 4. Scan entries.
    // 5. Scan close 
    // 6. CloseIndex 
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Extra Test Case 1****" << endl;
    
    RC rc;
    RID rid;
    // Test CreateIndex
    rc = ixManager->CreateIndex(tablename, attrname);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex(tablename, attrname, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test InsertEntry
    unsigned numOfTuples = 200;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        unsigned key = 1234;
        rid.pageNum = i;
        rid.slotNum = i;
        
        rc = ixHandle.InsertEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    numOfTuples = 200;
    for(unsigned i = 500; i < numOfTuples+500; i++) 
    {
        unsigned key = 4321;
        rid.pageNum = i;
        rid.slotNum = i-5;
        
        rc = ixHandle.InsertEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        } 
    }
    
    // Test Scan
    IX_IndexScan *ixScan = new IX_IndexScan();
    int compVal = 4321;

    rc = ixScan->OpenScan(ixHandle, EQ_OP, &compVal);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    // Test IndexScan Iterator
    int count = 0;
    while(ixScan->GetNextEntry(rid) == success) 
    {
       // if(count % 1000 == 0)
    	 //rc = ixHandle.DeleteEntry(&compVal, rid);
            cout << rid.pageNum << " " << rid.slotNum << endl;
            rc = ixHandle.DeleteEntry(&compVal, rid);
        count++;
    }
    cout << count << endl;


    // Test CloseScan
    rc = ixScan->CloseScan();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  

    cout<<"after delete......................"<<endl;
    compVal = 4321;
    rc = ixScan->OpenScan(ixHandle, EQ_OP, &compVal);
	if (rc == success) {
		cout << "Scan Opened Successfully!" << endl;
	} else {
		cout << "Failed Opening Scan..." << endl;
	}

	 while (ixScan->GetNextEntry(rid) == success) {
		// if(count % 1000 == 0)
		//rc = ixHandle.DeleteEntry(&compVal, rid);
		cout << rid.pageNum << " " << rid.slotNum << endl;
		//rc = ixHandle.DeleteEntry(&compVal, rid);
		//count++;
	}

	rc = ixScan->CloseScan();
	if (rc == success) {
		cout << "Scan Closed Successfully!" << endl;
	} else {
		cout << "Failed Closing Scan..." << endl;
	}

    // Test CloseIndex    
    rc = ixManager->CloseIndex(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  

    // Test DestroyIndex    
    rc = ixManager->DestroyIndex(tablename, attrname);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }
        
    return;
}


void testCase_extra_2(const string tablename, const string attrname)
{
    // Functions Tested:
    // 1. Create Index
    // 2. Open Index
    // 3. Insert Entry
    // 4. Scan
    // 5. Close Scan
    // 6. Close Index
    // 7. Destroy Index
    cout << endl << "****In Extra Test Case 2****" << endl;
    
    RC rc;
    RID rid;
    // Test CreateIndex
    rc = ixManager->CreateIndex(tablename, attrname);
    if(rc == success) 
    {
        cout << "Index Created!" << endl;
    }
    else 
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex(tablename, attrname, ixHandle);
    if(rc == success) 
    {
        cout << "Index Opened!" << endl;
    }
    else 
    {
        cout << "Failed Opening Index..." << endl;
    }  
    
    // Test InsertEntry
    unsigned numOfTuples = 500;
    for(unsigned i = 1; i <= numOfTuples; i++) 
    {
        void *key = malloc(100);
        unsigned count = ((i-1) % 26) + 1;
        *(int *)key = count;
   
        for(unsigned j = 0; j < count; j++)
        {
            *((char *)key+4+j) = 96+count;
        }

        rid.pageNum = i;
        rid.slotNum = i;
        
        rc = ixHandle.InsertEntry(key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }     
    }

    cout<<"test scan..............."<<endl;

    // Test Scan
    IX_IndexScan *ixScan = new IX_IndexScan();
    unsigned offset = 20;
    void *key = malloc(100);
    *(int *)key = offset;
    for(unsigned j = 0; j < offset; j++)
    {
        *((char *)key+4+j) = 96+offset;
    }



    rc = ixScan->OpenScan(ixHandle, EQ_OP, key);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Opening Scan..." << endl;
    }

    int count=0;

    // Test IndexScan Iterator
    while(ixScan->GetNextEntry(rid) == success)
    {
        cout << rid.pageNum << " " << rid.slotNum << endl;
        rc = ixHandle.DeleteEntry(key, rid);
        count++;
    }
    cout << endl;
    cout<<"count: .........."<<count<<endl;

    // Test CloseScan
    rc = ixScan->CloseScan();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Scan..." << endl;
    }  


    cout<<"after delete......................"<<endl;
      // compVal = 1234;
    count = 0;
       rc = ixScan->OpenScan(ixHandle, NE_OP, key);
   	if (rc == success) {
   		cout << "Scan Opened Successfully!" << endl;
   	} else {
   		cout << "Failed Opening Scan..." << endl;
   	}

   	 while (ixScan->GetNextEntry(rid) == success) {
   		// if(count % 1000 == 0)
   		//rc = ixHandle.DeleteEntry(&compVal, rid);
   		cout << rid.pageNum << " " << rid.slotNum << endl;
   		//rc = ixHandle.DeleteEntry(&compVal, rid);
   		count++;
   	}
   	cout<<"after delete count:...."<<count<<endl;
   	rc = ixScan->CloseScan();
   	if (rc == success) {
   		cout << "Scan Closed Successfully!" << endl;
   	} else {
   		cout << "Failed Closing Scan..." << endl;
   	}


    // Test CloseIndex    
    rc = ixManager->CloseIndex(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Closing Index..." << endl;
    }  

    // Test DestroyIndex
    rc = ixManager->DestroyIndex(tablename, attrname);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else 
    {
        cout << "Failed Destroying Index..." << endl;
    }

    return;
}

void testCase_9(const string tablename, const string attrname)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex
    // 3. Insert entry
    // 4. Scan entries using LT_OP operator and checking if the values returned are correct. Returned values are part of two separate insertions **
    // 5. Scan close
    // 6. CloseIndex
    // 7. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 6****" << endl;

    // Test CreateIndex
    RC rc;
    RID rid;
    rc = ixManager->CreateIndex(tablename, attrname);
    if(rc == success)
    {
        cout << "Index Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index..." << endl;
    }

    // Test OpenIndex
    IX_IndexHandle ixHandle;
    rc = ixManager->OpenIndex(tablename, attrname, ixHandle);
    if(rc == success)
    {
        cout << "Index Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index..." << endl;
    }

    // Test InsertEntry
    unsigned numOfTuples = 100;
    for(unsigned i = 1; i <= numOfTuples; i++)
    {
        float key = (float)i + 76.5;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = ixHandle.InsertEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }
    }

    numOfTuples = 100;
    for(unsigned i = 6000; i <= numOfTuples+6000; i++)
    {
        float key = (float)i + 76.5;
        rid.pageNum = i;
        rid.slotNum = i;

        rc = ixHandle.InsertEntry(&key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        }
    }

    // Test Scan
    IX_IndexScan *ixScan = new IX_IndexScan();
    float compVal = 6000.2;

    rc = ixScan->OpenScan(ixHandle, NE_OP, &compVal);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
    }

    // Test IndexScan Iterator
    while(ixScan->GetNextEntry(rid) == success)
    {

            cout << rid.pageNum << " " << rid.slotNum << endl;

    }

    // Test CloseScan
    rc = ixScan->CloseScan();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
    }

    // Test CloseIndex
    rc = ixManager->CloseIndex(ixHandle);
    if(rc == success)
    {
        cout << "Index Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index..." << endl;
    }

    // Test DestroyIndex
    rc = ixManager->DestroyIndex(tablename, attrname);
    if(rc == success)
    {
        cout << "Index Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index..." << endl;
    }
    return;
}

int main()
{
    cout << "****Starting Test Cases****" << endl;
    
    RM *rm = RM::Instance();
    createTable(rm, "tbl_employee");
    
   testCase_1("tbl_employee", "Age");
   testCase_2("tbl_employee", "Age");
    testCase_3("tbl_employee", "Age");
    testCase_4("tbl_employee", "Age");
    testCase_5("tbl_employee", "Age");
    testCase_6("tbl_employee", "Height");
   //
    testCase_7("tbl_employee", "Height");
    testCase_8("tbl_employee", "Height");
    testCase_9("tbl_employee", "Height");
    // Extra Credit Work
    // Duplicat Entries
    testCase_extra_1("tbl_employee", "Age");
    // TypeVarChar
    testCase_extra_2("tbl_employee", "EmpName");
    
    return 0;
}
