#ifndef _qe_h_
#define _qe_h_

#include <vector>

#include "../pf/pf.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

# define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN = 0, MAX, SUM, AVG, COUNT } AggregateOp;


// The following functions use  the following 
// format for the passed data.
//    For int and real: use 4 bytes
//    For varchar: use 4 bytes for the length followed by
//                          the characters

struct Value {
    AttrType type;          // type of value               
    void     *data;         // value                       
};


struct Condition {
    string lhsAttr;         // left-hand side attribute                     
    CompOp  op;             // comparison operator                          
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string rhsAttr;         // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
	    string GetAttributeName(string attribute);
	    string GetTableName(string attribute);
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
        int TupleLength(void *data,vector<Attribute> &attrs);
        AttrType GetAttrType(vector<Attribute> &attrs,string att);
        void GetDataFromTurple(void *data,vector<Attribute> &attrs,string attr,void *dataout);
        int GetAttrLength(void *data,vector<Attribute> &attrs,string attr);
        void CombineAttr(vector<Attribute> r1,vector<Attribute> r2,vector<Attribute> &r);
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RM &rm;
        RM_ScanIterator *iter;
        string tablename;
        vector<Attribute> attrs;
        vector<string> attrNames;
        
        TableScan(RM &rm, const string tablename, const char *alias = NULL):rm(rm)
        {
            // Get Attributes from RM
            rm.getAttributes(tablename, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs[i].name);
            }
            // Call rm scan to get iterator
            iter = new RM_ScanIterator();
            rm.scan(tablename, "", NO_OP, NULL, attrNames, *iter);

            // Store tablename
            this->tablename = tablename;
            if(alias) this->tablename = alias;
        };
       
        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tablename, "", NO_OP, NULL, attrNames, *iter);
        };
       
        RC getNextTuple(void *data)
        {
            RID rid;
            return iter->getNextTuple(rid, data);
        };
        
        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;
            
            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tablename;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };
        
        ~TableScan() 
        {
            iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RM &rm;
        IX_IndexScan *iter;
        IX_IndexHandle handle;
        string tablename;
        vector<Attribute> attrs;
        
        IndexScan(RM &rm, const IX_IndexHandle &indexHandle, const string tablename, const char *alias = NULL):rm(rm)
        {
            // Get Attributes from RM
            rm.getAttributes(tablename, attrs);
                     
            // Store tablename
            this->tablename = tablename;
            if(alias) this->tablename = string(alias);
            
            // Store Index Handle
            iter = NULL;
            this->handle = indexHandle;
        };
       
        // Start a new iterator given the new compOp and value
        void setIterator(CompOp compOp, void *value)
        {
            if(iter != NULL)
            {
                iter->CloseScan();
                delete iter;
            }
            iter = new IX_IndexScan();
            iter->OpenScan(handle, compOp, value);
        };
       
        RC getNextTuple(void *data)
        {
            RID rid;
            int rc = iter->GetNextEntry(rid);
            if(rc == 0)
            {
                rc = rm.readTuple(tablename.c_str(), rid, data);
            }
            return rc;
        };
        
        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tablename;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };
        
        ~IndexScan() 
        {
            iter->CloseScan();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:

        Filter(Iterator *input,                         // Iterator of input R
               const Condition &condition               // Selection condition 
        );
        ~Filter();

        RC Compare(void *data,Value value,CompOp op,AttrType attr);
        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
    private:

        int num=0;
        string tableName;
        vector<char*> filter_result;
        vector <Attribute> attr;

};


class Project : public Iterator {
    // Projection operator
    public:
	    vector <Attribute> attribute;
        Iterator *instance;
    	vector<Attribute> attrs;
        Project(Iterator *input,                            // Iterator of input R
                const vector<string> &attrNames);           // vector containing attribute names
        ~Project();
        
        RC getNextTuple(void *data) ;
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};

class NLJoin : public Iterator {
    // Nested-Loop join operator
    public:
        NLJoin(Iterator *leftIn,                             // Iterator of input R
               TableScan *rightIn,                           // TableScan Iterator of input S
               const Condition &condition,                   // Join condition
               const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
        );
        ~NLJoin();
        
        RC getNextTuple(void *data) ;
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
    private:
           vector<void*> joinResults;
           int a=0;
           vector<Attribute> attribute;
};


class INLJoin : public Iterator {
    // Index Nested-Loop join operator
    public:
        INLJoin(Iterator *leftIn,                               // Iterator of input R
                IndexScan *rightIn,                             // IndexScan Iterator of input S
                const Condition &condition,                     // Join condition
                const unsigned numPages                         // Number of pages can be used to do join (decided by the optimizer)
        );
        
        ~INLJoin();

        RC getNextTuple(void *data) ;
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
    private:
        vector<void*> joinResults;
        int a=0;
        vector<Attribute> attribute;
};


class HashJoin : public Iterator {
    // Hash join operator
    public:
        HashJoin(Iterator *leftIn,                                // Iterator of input R
                 Iterator *rightIn,                               // Iterator of input S
                 const Condition &condition,                      // Join condition
                 const unsigned numPages                          // Number of pages can be used to do join (decided by the optimizer)
        );
        
        ~HashJoin();

        RC getNextTuple(void *data) ;
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
    private:
        int a=0;
        PF_Manager *pf;
        vector<void*> joinResults;
        vector<Attribute> attribute;
        void Hash_Int(Iterator *leftIn,                                // Iterator of input R
                Iterator *rightIn,                               // Iterator of input S
                const Condition &condition,                      // Join condition
                const unsigned numPages, PF_FileHandle &lefthandle,
        PF_FileHandle &righthandle,string rightAttriname,string leftAttriname);
        void Hash_Float(Iterator *leftIn,                                // Iterator of input R
                       Iterator *rightIn,                               // Iterator of input S
                       const Condition &condition,                      // Join condition
                       const unsigned numPages, PF_FileHandle &lefthandle,
               PF_FileHandle &righthandle,string rightAttriname,string leftAttriname);
};

/*class Aggregate : public Iterator {
    // Aggregation operator
    public:
        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  AggregateOp op                                // Aggregate operation
        );

        // Extra Credit
        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  Attribute gAttr,                              // The attribute over which we are grouping the tuples
                  AggregateOp op                                // Aggregate operation
        );     

        ~Aggregate();
        
        RC getNextTuple(void *data) {return QE_EOF;};
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;
};*/

#endif
