# include "qe.h"
#include <string>
#include <vector>

AttrType Iterator::GetAttrType(vector<Attribute> &attrs,string att)
{
     for (int i=0;i<attrs.size();i++)
     {
    	 if(strcmp(attrs[i].name.c_str(),att.c_str()))
    			 return attrs[i].type;
     }

    return TypeInt;
}

void Iterator::CombineAttr(vector<Attribute> r1,vector<Attribute> r2,vector<Attribute> &r)
{
	r.clear();
    for (int i=0;i<r1.size();i++)
    {
    	r.push_back(r1[i]);
    }
    for (int j=0;j<r2.size();j++)
    {
    	r.push_back(r2[j]);

    }
}






int Iterator::TupleLength(void *data,vector<Attribute> &attrs)
{
	int length=0;
	for (int i=0;i<attrs.size();i++)
	{
	    if(attrs[i].type==TypeReal)
	    {
	    	length=length+sizeof(float);
	    	continue;
	    }
	    if(attrs[i].type==TypeInt)
	    	    {
	    	    	length=length+sizeof(int);
	    	    	continue;

	    	    }
         if(attrs[i].type==TypeVarChar)
         {
        	 int length2=0;
        	 memcpy(&length2,(char*)data+length,sizeof(int));
        	 int string_length=length2+sizeof(int);
        	 length+=string_length;
         }

	}
	return length;
}



void Iterator::GetDataFromTurple(void *data,vector<Attribute> &attrs,string attr,void *dataout)
{

	int length = 0;
	bool got = false;
	for (int i = 0; i < attrs.size(); i++) {
		if (attr == attrs[i].name ) {
			got = true;
		}
		if (attrs[i].type == TypeReal) {
			if (got == true) {
				memcpy(dataout, data + length, sizeof(float));
				break;
			}
			length = length + sizeof(float);
			continue;
		}
		if (attrs[i].type == TypeInt) {
			if (got == true) {
				memcpy(dataout, data + length, sizeof(int));
				break;
			}
			length = length + sizeof(int);
			continue;

		}
		if (attrs[i].type == TypeVarChar) {
			int length2 = 0;
			memcpy(&length2, (char*) data + length, sizeof(int));
			if (got == true) {
				memcpy(dataout, data + length, sizeof(int) + length2);
				break;
			}
			int string_length = length2 + sizeof(int);
			length += string_length;
		}

	}

}

int Iterator::GetAttrLength(void *data,vector<Attribute> &attrs,string attr)
{

	int length = 0;
	int Attr_length=0;
	bool got = false;
	for (int i = 0; i < attrs.size(); i++) {
		if (attr == attrs[i].name ) {
			got = true;
		}
		if (attrs[i].type == TypeReal) {
			if (got == true) {
				return sizeof(float);
				break;
			}
			length = length + sizeof(float);
			continue;
		}
		if (attrs[i].type == TypeInt) {
			if (got == true) {
				return sizeof(int);
				break;
			}
			length = length + sizeof(int);
			continue;

		}
		if (attrs[i].type == TypeVarChar) {
			int length2 = 0;
			memcpy(&length2, (char*) data + length, sizeof(int));
			if (got == true) {
				return length2+sizeof(int);
				break;
			}
			int string_length = length2 + sizeof(int);
			length += string_length;
		}

	}

}




string Iterator::GetAttributeName(string attribute)
{
	string attrname="";
	int next=0;
	int current=0;
	string delimiters=".";
	next = attribute.find_first_of(delimiters, current );
	attrname=attribute.substr( next+1, attribute.size() );
	return attrname;
}
string Iterator::GetTableName(string attribute)
{
	string tablename="";
	int next=0;
	int current=0;
	string delimiters=".";
	next = attribute.find_first_of(delimiters, current );
	tablename=attribute.substr( current, next-current );
	//string left="left";
	return tablename;
}

Filter::Filter(Iterator *input,const Condition &condition)
{
	vector<Attribute> attr;
	input->getAttributes(attr);
	tableName=this->GetTableName(attr[0].name);
	cout<<"table name is"<<tableName<<endl;
	void *datain=malloc(1000);
	void *dataout=malloc(200);
	AttrType type =GetAttrType(attr, condition.lhsAttr);
	while(input->getNextTuple(datain)!=RM_EOF)
	{
        void *datatrans=malloc(1000);
        GetDataFromTurple(datain, attr,condition.lhsAttr, dataout);
        cout<<*(int*)dataout<<endl;
		int a = Compare(dataout, condition.rhsValue, condition.op,type
						);
				if (a == 0)
				{
					memcpy(datatrans,datain,1000);
					filter_result.push_back((char*)datatrans);
				}
	   memset(datain, 0, 1000);
	   memset(dataout, 0, 200);
	}
}
Filter::~Filter()
{

}
void Filter::getAttributes(vector<Attribute> &attrs) const
{
	 attrs.clear();
	 for (int i=0;i<attr.size();i++)
	 {
		attrs.push_back(attr[i]);
	 }

}
RC Filter::getNextTuple(void *data)
{
   if(num<filter_result.size())
   {
	   memcpy(data,filter_result.at(num),200);
	   num++;
	   return 0;
   }
   else
   {
	num=0;
	return QE_EOF;
   }

}
RC Filter::Compare(void *data,Value value,CompOp op,AttrType attr)
{
	//different type
      if(value.type!=attr)
    	  return -1;
      else
      {
		int eq = 0;
		if (attr == TypeVarChar) {
			int charSize1 = 0;
			int charSize2 = 0;
			char *value1;
			char *value2;
			memcpy(&charSize1, (char*) data, sizeof(int));
			value1 = (char*) malloc(charSize1);
			memcpy(value1, data + sizeof(int), charSize1);
			memcpy(&charSize2, (char*) value.data, sizeof(int));
			value2 = (char*) malloc(charSize2);
			memcpy(value2, ((char*) value.data) + sizeof(int), charSize2);
			if (charSize1 != charSize2)
				return -1;
			else {
				eq = memcmp(value1, value2, charSize2);
				if (op == EQ_OP && eq == 0)
					return 0;
				if (op == NE_OP && eq != 0)
					return 0;
				if(op==NO_OP)
					return 0;
			}
			return -1;

		}
		else {
			if (attr == TypeInt) {
				int *keya = (int*) malloc(sizeof(int));
				memcpy(keya, data, sizeof(int));
				int *keyb = (int*) malloc(sizeof(int));
				memcpy(keyb, value.data, sizeof(int));
				//int eq;
				switch(op)
				{
				case EQ_OP:
					if (*keya == *keyb)
						return 0;
					break;
				case LT_OP:
					if (*keya < *keyb)
						return 0;
					break;
				case GT_OP:
					if (*keya > *keyb)
						return 0;
					break;
				case LE_OP:
					if (*keya <= *keyb)
						return 0;
					break;
				case GE_OP:
					if (*keya >= *keyb)
						return 0;
					break;
				case NE_OP:
					if (*keya != *keyb)
						return 0;
					break;
				case NO_OP:
					return 0;
					break;
				default:
					break;
				}
			}
			if (attr == TypeReal) {
				float *keya = (float*) malloc(sizeof(float));
				memcpy(keya, data, sizeof(float));

				float *keyb = (float*) malloc(sizeof(float));
				memcpy(keyb, value.data, sizeof(float));
				switch (op) {
				case EQ_OP:
					if (*keya == *keyb)
						return 0;
					break;
				case LT_OP:
					if (*keya < *keyb)
						return 0;
					break;
				case GT_OP:
					if (*keya > *keyb)
						return 0;
					break;
				case LE_OP:
					if (*keya <= *keyb)
						return 0;
					break;
				case GE_OP:
					if (*keya >= *keyb)
						return 0;
					break;
				case NE_OP:
					if (*keya != *keyb)
						return 0;
					break;
				case NO_OP:
					return 0;
					break;
				default:
					break;
				}

		}
		}
		return -1;
      }
	}



Project::Project(Iterator *input,const vector<string> &attr)
{
	instance=input;
	input->getAttributes(attrs);
	for (int i=0;i<attr.size();i++)
	{

        for (int j=0;j<attrs.size();j++)
        {
        	int a=attr[i].compare(attrs[j].name);
        	if(a==0)
            {
        		attribute.push_back(attrs[j]);
        		//cout<<attrs[j].name<<endl;
        	}
        }

	}
	 //cout<<"attribute size is"<<attribute.size()<<endl;
}
Project::~Project()
{

}
RC Project::getNextTuple(void *data)
{
	void *datain=malloc(1000);
    int offset=0;
	if(instance->getNextTuple(datain)!=0)
		return QE_EOF;
    for(int i=0;i<attribute.size();i++)
    {
    	void *data_attr=malloc(1000);
    	//cout<<attribute[i].name<<attributeName<<tableName<<endl;
       int length=GetAttrLength(datain,attrs,attribute[i].name);
       //cout<<attribute[i].name<<attributeName<<tableName<<length<<endl;
       GetDataFromTurple(datain,attrs,attribute[i].name,data_attr);
       //cout<<"data out"<<*(char*)data_attr<<endl;
       memcpy(data+offset,data_attr,length);
       offset=offset+length;
    }
    return 0;

}
void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
    for (int i=0;i<attribute.size();i++)
    {
    	attrs.push_back(attribute[i]);
    }
}


void HashJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	 for (int i=0;i<attribute.size();i++)
	    {
	    	attrs.push_back(attribute[i]);
	    }
}



HashJoin::~HashJoin(){}
HashJoin::HashJoin(Iterator *leftIn,                                // Iterator of input R
                 Iterator *rightIn,                               // Iterator of input S
                 const Condition &condition,                      // Join condition
                 const unsigned numPages                          // Number of pages can be used to do join (decided by the optimizer)
        )
{
	pf = PF_Manager::Instance();
	vector<Attribute> attr1;
	vector<Attribute> attr2;
	leftIn->getAttributes(attr1);
	rightIn->getAttributes(attr2);
    CombineAttr(attr1,attr2,attribute);
	a = 0;
	//void *leftData = malloc(1000);

	string rightAttriname = condition.rhsAttr;
	string leftAttriname = condition.lhsAttr;

	pf->CreateFile((leftAttriname+"_partitions").c_str());
	pf->CreateFile((rightAttriname+"_partitions").c_str());

	PF_FileHandle lefthandle;
	PF_FileHandle righthandle;

	pf->OpenFile((leftAttriname+"_partitions").c_str(),lefthandle);
	pf->OpenFile((rightAttriname+"_partitions").c_str(),righthandle);
	//vector<char *> rightDatas;


	AttrType attrType = GetAttrType(attribute, leftAttriname);
	AttrType attrType2 = GetAttrType(attribute, rightAttriname);
	if(attrType!=attrType2)
	{}
//区分了什么类型
	else
	{
		switch(attrType)
		{
		case TypeInt:
			Hash_Int(leftIn,                                // Iterator of input R
			        rightIn,                               // Iterator of input S
			        condition,                      // Join condition
			        numPages, lefthandle,
			 righthandle,rightAttriname,leftAttriname);
		break;
		case TypeReal:
			Hash_Float(leftIn,                                // Iterator of input R
						        rightIn,                               // Iterator of input S
						        condition,                      // Join condition
						        numPages, lefthandle,
						 righthandle,rightAttriname,leftAttriname);

			break;
		case TypeVarChar:

			break;


		}
	}
	pf->CloseFile(lefthandle);
	pf->CloseFile(righthandle);

}

void HashJoin::Hash_Float(
		Iterator *leftIn,                                // Iterator of input R
		Iterator *rightIn,                               // Iterator of input S
		const Condition &condition,                      // Join condition
		const unsigned numPages,  PF_FileHandle &lefthandle,
		PF_FileHandle &righthandle,string rightAttriname,string leftAttriname)
{

    void * leftData=malloc(1000);
    void * rightData=malloc(1000);
    int lSize=0;//the length of the left turple
    int RSize=0;// the length of the right turple
	vector<int> lTsizes;//record the records that has been sended into
	vector<int> rTsizes;
	vector<int> lnums;
	vector<int> rnums;
	vector<Attribute> attr1;
		vector<Attribute> attr2;
		leftIn->getAttributes(attr1);
		rightIn->getAttributes(attr2);
	for (int s = 0; s < 16; s++) {
		lTsizes.push_back(0);
		rTsizes.push_back(0);
		lnums.push_back(0);
		rnums.push_back(0);
	}
   while(leftIn->getNextTuple(leftData)!=QE_EOF)
   {
	   lSize = TupleLength(leftData,attr1);
	   void *leftAttribute=malloc(1000);
	   GetDataFromTurple(leftData, attr1,leftAttriname, leftAttribute);
       float a=*(float*)leftAttribute;
       int remainder=0;//which partition
       memcpy(&remainder,&a,sizeof(float));
       remainder=(remainder & 0x000000ff)%numPages;
		void *data = malloc(1000);
		memcpy(data, &lSize, sizeof(int));
		memcpy((char *) data + sizeof(int), leftData, lSize);
		FILE *fileStream = lefthandle.pfile;
		fseek(fileStream, remainder * 4096 * numPages + lTsizes.at(remainder),
				SEEK_SET);
		fwrite(data, sizeof(int) + lSize, 1, fileStream);
		lTsizes.at(remainder) = lTsizes.at(remainder) + lSize + sizeof(int);
		lnums.at(remainder)++;
		memset(leftData, 0, 1000);
   }
	while(rightIn->getNextTuple(rightData)!=QE_EOF)
	{
		RSize =TupleLength(rightData,attr2);
		void *rightAttribute = malloc(1000);
		GetDataFromTurple(rightData, attr2,rightAttriname, rightAttribute);
		float b = *(float*) rightAttribute;
		int remainder = 0;                      //which partition
		memcpy(&remainder, &a, sizeof(float));
		remainder = remainder & 0x0000000f;
		void *data = malloc(1000);
		memcpy(data, &lSize, sizeof(int));
		memcpy((char *) data + sizeof(int), rightData, RSize);
		FILE *fileStream = righthandle.pfile;
		fseek(fileStream, remainder * 4096 * numPages + rTsizes.at(remainder),
				SEEK_SET);
		fwrite(data, sizeof(int) + RSize, 1, fileStream);
		rTsizes.at(remainder) = rTsizes.at(remainder) + RSize + sizeof(int); //where the next tuple should write
		rnums.at(remainder)++;                      //for the phobing part
		memset
		(rightData, 0, 1000);
	}
	FILE *rfileStream = righthandle.pfile;
		    FILE *lfileStream = lefthandle.pfile;
		    vector<void*> partition;
		    vector<int> ldatasize;
    //left table
	for (int i=0;i<numPages;i++)
	{
		for (int j=0;j<lnums[i];j++)
		{
			void *datasize2 = malloc(4);
			int totalsize = 0;
			fseek(lfileStream, i * 4096 * numPages + totalsize, SEEK_SET);
			fread(datasize2, sizeof(int), 1, lfileStream);
			void *data = malloc(1000);
			int datasize=*(int*)datasize2;
			fseek(rfileStream, i * 4096 * numPages + totalsize + sizeof(int),
					SEEK_SET);
			fread(data, datasize, 1, lfileStream);
			totalsize = sizeof(int) + datasize;
			partition.push_back(data);
			ldatasize.push_back(datasize);
		}
		for (int j=0;j<rnums[i];j++)
		{
					void *datasize2 = malloc(sizeof(int));
					int totalsize = 0;
					fseek(rfileStream, i * 4096 * numPages + totalsize, SEEK_SET);
					fread(datasize2, sizeof(int), 1, lfileStream);
					void *data = malloc(1000);
					int datasize=*(int*)datasize2;
					fseek(rfileStream, i * 4096 * numPages + totalsize + sizeof(int),
							SEEK_SET);
					fread(data, datasize, 1, lfileStream);
					totalsize = sizeof(int) + datasize;
                    void *rAttribute=malloc(sizeof(float));
                    GetDataFromTurple(rightData, attr2,rightAttriname, rAttribute);
                    void *lAttribute=malloc(sizeof(float));
                    for (int ii=0;ii<partition.size();ii++)
                    {
                    	  GetDataFromTurple(leftData, attr1,leftAttriname, lAttribute);

                    	 if (condition.op == EQ_OP)
                    	 {
                    	 					if (*(float*)rAttribute == *(float*)lAttribute)
                    	 					{
                    	 						void *result = malloc(ldatasize[ii] + datasize);
                    	 						memcpy(result, (char*)partition[ii] ,ldatasize[ii] );
                    	 						memcpy(result + ldatasize[ii], (char*) rightData, datasize);
                    	 						this->joinResults.push_back((char*) result);
                    	 					}
                    	 				}
                    	 				if (condition.op == NO_OP)
                    	 				{
                    	 					void *result = malloc(ldatasize[ii] + datasize);
                    	 					memcpy(result, (char*)partition[ii] ,ldatasize[ii] );
                    	 					memcpy(result + ldatasize[ii], (char*) rightData, datasize);
                    	 					this->joinResults.push_back((char*) result);
                    	 				}

                          memset(lAttribute,0,sizeof(float));
                    }
				}

	}

}

void HashJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	for (int i = 0; i < attribute.size(); i++) {
		attrs.push_back(attribute[i]);
	}

}


void HashJoin::Hash_Int(
		Iterator *leftIn,                                // Iterator of input R
		Iterator *rightIn,                               // Iterator of input S
		const Condition &condition,                      // Join condition
		const unsigned numPages, PF_FileHandle &lefthandle,
		PF_FileHandle &righthandle,string rightAttriname,string leftAttriname)
{
	void *leftData = malloc(1000);
	int lSize = 0;
	int rSize = 0;
	int rPnum;
	vector<int> lTsizes;
	vector<int> rTsizes;
	vector<int> lnums;
	vector<int> rnums;
	for (int s = 0; s < numPages; s++) {
		lTsizes.push_back(0);
		rTsizes.push_back(0);
		lnums.push_back(0);
		rnums.push_back(0);
	}

	while (leftIn->getNextTuple(leftData) == 0) {

		lSize = rm->RecordLength(leftTablename, leftData);
		//void *rightData = malloc(1000);
		void *lAdata = malloc(1000);
		rm->GetAttributeData(leftTablename, leftAttriname, leftData, lAdata);
		cout<<leftAttriname;
		//void *rightData = malloc(1000);
		int mod = numPages;
		int *ld = (int*) malloc(sizeof(int));
		memcpy(ld, (char*) lAdata, sizeof(int));
		cout<<*ld<<endl;
		int remainder = (*ld) % mod;
		void *data = malloc(1000);
		memcpy(data, &lSize, sizeof(int));
		memcpy((char *) data + sizeof(int), leftData, lSize);
		FILE *fileStream = lefthandle.pfile;
		fseek(fileStream, remainder * 4096 * numPages + lTsizes.at(remainder),
				SEEK_SET);
		fwrite(data, lSize + sizeof(int), 1, fileStream);
		lTsizes.at(remainder) = lTsizes.at(remainder) + lSize + sizeof(int);
		lnums.at(remainder)++;memset
		(leftData, 0, 1000);
	}

	void *rightData = malloc(1000);
	while (rightIn->getNextTuple(rightData) == 0) {
		rSize = rm->RecordLength(rightTablename, rightData);
		void *rAdata = malloc(1000);
		rm->GetAttributeData(rightTablename, rightAttriname, rightData, rAdata);
		//AttrType attrType = rm->GetAttributeType(rightTablename, rightAttriname);
		int mod = numPages;
		rPnum = mod;
		int *rd = (int*) malloc(sizeof(int));
		memcpy(rd, (char*) rAdata, sizeof(int));
		int remainder = (*rd) % mod;
		void *data = malloc(1000);
		memcpy(data, &rSize, sizeof(int));
		memcpy((char *) data + sizeof(int), rightData, rSize);
		FILE *fileStream = righthandle.pfile;
		fseek(fileStream, remainder * 4096 * numPages + rTsizes.at(remainder),
				SEEK_SET);
		fwrite(data, rSize + sizeof(int), 1, fileStream);
		rTsizes.at(remainder) = rTsizes.at(remainder) + rSize + sizeof(int);
		rnums.at(remainder)++;
		//rTSize = rTSize + rSize + sizeof(int);
		memset(rightData, 0, 1000);
	}

	for (int a = 0; a < rPnum; a++) {
		FILE *rfileStream = righthandle.pfile;
		FILE *lfileStream = lefthandle.pfile;
		int rtotalS = 0;
		for (int b = 0; b < rnums.at(a); b++) {
			int *size = (int*) malloc(sizeof(int));
			fseek(rfileStream, a * 4096 * numPages + rtotalS, SEEK_SET);
			fread(size, sizeof(int), 1, rfileStream);
			void *rdata = malloc(1000);
			fseek(rfileStream, a * 4096 * numPages + rtotalS + sizeof(int),
					SEEK_SET);
			fread(rdata, *size, 1, rfileStream);
			rtotalS = rtotalS + *size + sizeof(int);
			void *rad = malloc(1000);
			rm->GetAttributeData(rightTablename, rightAttriname, rdata, rad);
			int ltotalS = 0;
			for (int c = 0; c < lnums.at(a); c++) {
				int *size2 = (int*) malloc(sizeof(int));
				fseek(lfileStream, a * 4096 * numPages + ltotalS, SEEK_SET);
				fread(size2, sizeof(int), 1, lfileStream);
				void *ldata = malloc(1000);
				fseek(lfileStream, a * 4096 * numPages + ltotalS + sizeof(int),
						SEEK_SET);
				fread(ldata, *size2, 1, lfileStream);
				ltotalS = ltotalS + *size2 + sizeof(int);
				void *lad = malloc(1000);
				rm->GetAttributeData(leftTablename, leftAttriname, ldata, lad);
				int* ld = (int*) malloc(sizeof(int));
				memcpy(ld, (char*) lad, sizeof(int));
				int* rd = (int*) malloc(sizeof(int));
				memcpy(rd, (char*) rad, sizeof(int));
				cout << "left: " << *ld << " right: " << *rd << endl;
				if (condition.op == EQ_OP) {
					if (*ld == *rd) {
						void *result = malloc(*size + *size2);
						memcpy(result, (char*) ldata, *size2);
						memcpy(result + *size2, (char*) rdata, *size);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == NO_OP) {
					void *result = malloc(*size + *size2);
					memcpy(result, (char*) ldata, *size2);
					memcpy(result + *size2, (char*) rdata, *size);
					this->joinResults.push_back((char*) result);
				}

			}
		}
		//fseek(fileStream, a * 4096 + rTSize, SEEK_SET);
	}

}


RC HashJoin::getNextTuple(void *data){
	if (a < this->joinResults.size()) {
			//scanObj sj = sObj.at(i);
			memcpy(data, joinResults.at(a), 1000);
			//free(joinResults.at(a));
			a++;
			return 0;
		} else {
			a = 0;
			return -1;
		}
}



void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	 for (int i=0;i<attribute.size();i++)
	    {
	    	attrs.push_back(attribute[i]);
	    }
}




NLJoin::NLJoin(Iterator *leftIn,                             // Iterator of input R
               TableScan *rightIn,                           // TableScan Iterator of input S
               const Condition &condition,                   // Join condition
               const unsigned numPages)
{
	//vector <char *> joinResults;
	a=0;
	void *leftData = malloc(1000);
	int i =0;

	vector<Attribute> attr1;
		vector<Attribute> attr2;
		leftIn->getAttributes(attr1);
		rightIn->getAttributes(attr2);
	    CombineAttr(attr1,attr2,attribute);

	string rightAttriname = condition.rhsAttr;
	string leftAttriname = condition.lhsAttr;
	vector <char *> rightDatas;
	void *data = malloc(1000);
	while(rightIn->getNextTuple(data)==0){
		rightDatas.push_back((char*)data);
		memset(data, '\0', 1000);
	}
	while(leftIn->getNextTuple(leftData)==0){
		i++;
		int j=0;
		int lSize = rm->RecordLength(leftTablename,leftData);
		//void *rightData = malloc(1000);
		void *lAdata = malloc(1000);
		rm->GetAttributeData(leftTablename,leftAttriname,leftData,lAdata);
		void *rightData = malloc(1000);
		while(rightIn->getNextTuple(rightData)==0){
			//void *rightData = malloc(1000);
			//memcpy((char *)rightData,rightDatas.at(j),1000);
			j++;

			//void *lAdata = malloc(1000);
			void *rAdata = malloc(1000);
			//rm->GetAttributeData(leftTablename,leftAttriname,leftData,lAdata);
			rm->GetAttributeData(rightTablename,rightAttriname,rightData,rAdata);
			int rSize = rm->RecordLength(rightTablename,rightData);
			AttrType attrType = rm->GetAttributeType(leftTablename, leftAttriname);
			if(attrType == TypeInt){
				int* ld = (int*)malloc(sizeof(int));
				memcpy(ld, (char*) lAdata, sizeof(int));
				int* rd = (int*)malloc(sizeof(int));
				memcpy(rd, (char*) rAdata, sizeof(int));
				cout<<"left: "<<*ld<<" right: "<<*rd<<endl;
				if(condition.op ==  EQ_OP){
					if(*ld == *rd){
						void *result = malloc(rSize+lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result+lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*)result);
					}
				}
				if (condition.op == LT_OP) {
					if (*ld < *rd) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == GT_OP) {
					if (*ld > *rd) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == LE_OP) {
					if (*ld <= *rd) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == GE_OP) {
					if (*ld >= *rd) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == NE_OP) {
					if (*ld != *rd) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == NO_OP) {
					void *result = malloc(rSize + lSize);
					memcpy(result, (char*) leftData, lSize);
					memcpy(result + lSize, (char*) rightData, rSize);
					this->joinResults.push_back((char*) result);
				}
			}
			if (attrType == TypeReal) {
				float* ld = (float*)malloc(sizeof(float));
				memcpy(ld, (char*) lAdata, sizeof(float));
				float* rd = (float*)malloc(sizeof(float));
				memcpy(rd, (char*) rAdata, sizeof(float));
				if (condition.op == EQ_OP) {
					if (*ld == *rd) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == LT_OP) {
					if (*ld < *rd) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == GT_OP) {
					if (*ld > *rd) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == LE_OP) {
					if (*ld <= *rd) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == GE_OP) {
					if (*ld >= *rd) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == NE_OP) {
					if (*ld != *rd) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == NO_OP) {
					void *result = malloc(rSize + lSize);
					memcpy(result, (char*) leftData, lSize);
					memcpy(result + lSize, (char*) rightData, rSize);
					this->joinResults.push_back((char*) result);
				}
			}
			if(attrType == TypeVarChar){
				int lCharSize;
				int rCharSize;
				memcpy(&lCharSize,lAdata+0,sizeof(int));
				int compare = memcmp((char*)lAdata+sizeof(int),(char*)rAdata+sizeof(int),lCharSize);
				if(condition.op == EQ_OP){
					if(compare == 0){
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == LT_OP) {
					if (compare<0) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == GT_OP) {
					if (compare>0) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == LE_OP) {
					if (compare<=0) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == GE_OP) {
					if (compare>=0) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == NE_OP) {
					if (compare!=0) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (condition.op == NO_OP) {
					void *result = malloc(1000);
					memcpy(result, (char*) leftData, lSize);
					memcpy(result + lSize, (char*) rightData, rSize);
					this->joinResults.push_back((char*) result);
				}
			}
			memset(rightData, 0, 1000);
		}
		memset(leftData, 0, 1000);
	}
}

RC NLJoin::getNextTuple(void *data){
	if (a < this->joinResults.size()) {
		//scanObj sj = sObj.at(i);
		memcpy(data, joinResults.at(a), 1000);
		//free(joinResults.at(a));
		a++;
		return 0;
	} else {
		a = 0;
		return -1;
	}
}
INLJoin::~INLJoin(){}
INLJoin::INLJoin(Iterator *leftIn,                        // Iterator of input R
		IndexScan *rightIn,                     // IndexScan Iterator of input S
		const Condition &condition,                   // Join condition
		const unsigned numPages)
{
	a = 0;

	void *leftData = malloc(1000);
	int i = 0;

	vector<Attribute> attr1;
			vector<Attribute> attr2;
			leftIn->getAttributes(attr1);
			rightIn->getAttributes(attr2);
		    CombineAttr(attr1,attr2,attribute);
		string rightAttriname = condition.rhsAttr;
		string leftAttriname = condition.lhsAttr;
	vector<char *> rightDatas;
	void *data = malloc(1000);
//	while (rightIn->getNextTuple(data) == 0) {
//		rightDatas.push_back((char*) data);
//		memset(data, 0, 1000);
//	}

	while(leftIn->getNextTuple(leftData)==0){
			i++;
			int j=0;
			int lSize = rm->RecordLength(leftTablename,leftData);
			//void *rightData = malloc(1000);
			void *lAdata = malloc(1000);
			rm->GetAttributeData(leftTablename,leftAttriname,leftData,lAdata);
			void *rightData = malloc(1000);
			rightIn->setIterator(condition.op, lAdata);
			while(rightIn->getNextTuple(rightData)==0){
				//void *rightData = malloc(1000);
				//memcpy((char *)rightData,rightDatas.at(j),1000);
				j++;

				//void *lAdata = malloc(1000);
				void *rAdata = malloc(1000);
				//rm->GetAttributeData(leftTablename,leftAttriname,leftData,lAdata);
				rm->GetAttributeData(rightTablename,rightAttriname,rightData,rAdata);
				int rSize = rm->RecordLength(rightTablename,rightData);
				AttrType attrType = rm->GetAttributeType(leftTablename, leftAttriname);
				if(attrType == TypeInt){
					int* ld = (int*)malloc(sizeof(int));
					memcpy(ld, (char*) lAdata, sizeof(int));
					int* rd = (int*)malloc(sizeof(int));
					memcpy(rd, (char*) rAdata, sizeof(int));
					cout<<"left: "<<*ld<<" right: "<<*rd<<endl;
					if(condition.op ==  EQ_OP){
						if(*ld == *rd){
							void *result = malloc(rSize+lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result+lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*)result);
						}
					}
					if (condition.op == LT_OP) {
						if (*ld < *rd) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == GT_OP) {
						if (*ld > *rd) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == LE_OP) {
						if (*ld <= *rd) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == GE_OP) {
						if (*ld >= *rd) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == NE_OP) {
						if (*ld != *rd) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == NO_OP) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if (attrType == TypeReal) {
					float* ld = (float*)malloc(sizeof(float));
					memcpy(ld, (char*) lAdata, sizeof(float));
					float* rd = (float*)malloc(sizeof(float));
					memcpy(rd, (char*) rAdata, sizeof(float));
					if (condition.op == EQ_OP) {
						if (*ld == *rd) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == LT_OP) {
						if (*ld < *rd) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == GT_OP) {
						if (*ld > *rd) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == LE_OP) {
						if (*ld <= *rd) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == GE_OP) {
						if (*ld >= *rd) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == NE_OP) {
						if (*ld != *rd) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == NO_OP) {
						void *result = malloc(rSize + lSize);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				if(attrType == TypeVarChar){
					int lCharSize;
					int rCharSize;
					memcpy(&lCharSize,lAdata+0,sizeof(int));
					int compare = memcmp((char*)lAdata+sizeof(int),(char*)rAdata+sizeof(int),lCharSize);
					if(condition.op == EQ_OP){
						if(compare == 0){
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == LT_OP) {
						if (compare<0) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == GT_OP) {
						if (compare>0) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == LE_OP) {
						if (compare<=0) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == GE_OP) {
						if (compare>=0) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == NE_OP) {
						if (compare!=0) {
							void *result = malloc(rSize + lSize);
							memcpy(result, (char*) leftData, lSize);
							memcpy(result + lSize, (char*) rightData, rSize);
							this->joinResults.push_back((char*) result);
						}
					}
					if (condition.op == NO_OP) {
						void *result = malloc(1000);
						memcpy(result, (char*) leftData, lSize);
						memcpy(result + lSize, (char*) rightData, rSize);
						this->joinResults.push_back((char*) result);
					}
				}
				memset(rightData,0,1000);
			}
			memset(leftData, 0, 1000);
		}
}
NLJoin::~NLJoin(){}
RC INLJoin::getNextTuple(void *data){
	if (a < this->joinResults.size()) {
		//scanObj sj = sObj.at(i);
		memcpy(data, joinResults.at(a), 1000);
		//free(joinResults.at(a));
		a++;
		return 0;
	} else {
		a = 0;
		return -1;
	}
}


void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	 for (int i=0;i<attribute.size();i++)
	    {
	    	attrs.push_back(attribute[i]);
	    }
}

