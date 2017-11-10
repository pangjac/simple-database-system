
#include "rm.h"

RM* RM::_rm = 0;

RM* RM::Instance()
{
    if(!_rm)
        _rm = new RM();
    
    return _rm;
}

RM::RM()
{
	pf = PF_Manager::Instance();
	pf->CreateFile("SystemCatalog.dat");
	//ph=NULL;
	string names[] =
			{ "TableName", "AttrName", "AttrType", "AttrLen", "AttrPos" };
	int lengths[] = { 50, 50, 4, 4, 4 };
	AttrType types[] = { TypeVarChar, TypeVarChar, TypeInt, TypeInt, TypeInt };
	vector<Attribute> catalog;
	createVector(5, names, lengths, types, catalog);
	InsertCatalog("SystemCatalog", catalog);
}

RM::~RM()
{
	//pf->CloseFile(catalogHandle);
}

RC RM::createVector(int number,string names[],int lengths[],AttrType types[],vector<Attribute> &catalog)
{
    Attribute attr;
    for(int i=0;i<number;i++)
    {
    	attr.length=lengths[i];
    	attr.name=names[i];
    	attr.type=types[i];
    	catalog.push_back(attr);
    }
    return 0;
}

vector<CatalogEntry> RM::FindCataLog(const string tableName)
//all the catalog in tableName
{
	vector<CatalogEntry> ca;
	if(isTableExist(tableName)!=0)
		//Such table is already in the catalog
	{

	}
	else
	{
		 pf->OpenFile("SystemCatalog.dat",ph);
		 void *data=malloc(112);
		// FILE *fileStream=ph.GetHandleFile();
		 FILE *fileStream=ph.pfile;
		 fseek (fileStream,0,SEEK_END);
		 int lSize = ftell (fileStream);
		 int num=(sizeof(char)*lSize)/112;
		 for (int i=0;i<num;i++)
		 {
			 fseek(fileStream,112*(i),SEEK_SET);//set the pointer to the specific position of file stream
			 fread((char*)data,1,112,fileStream);
			 CatalogEntry *en=new CatalogEntry;
			 memcpy(en,data,112);//every entry is 112 bytes long
			 const char *cc=en->tableName;
			  if(strcmp(cc,tableName.c_str())==0)
			  {
				 //en->offset_catalog=i+1;
			     ca.push_back(*en);
			  }
			  delete en;
	     }
		 pf->CloseFile(ph);
	}

	 return ca;
}

vector<Attribute> RM::NewLength(vector<CatalogEntry> ca,const void *data)
//store the length in the vector
{
	int num=ca.size();//how many attributes in the catalog
	vector<Attribute> attr;
	Attribute att;
	int offset=0;
	const void* iterator=data;//record how many bytes we have scan the record
	for (int i=0;i<num;i++)
	{
		if(ca[i].type==TypeInt||ca[i].type==TypeReal)
		{
			att.length=4;
			att.name=ca[i].name;
			att.type=ca[i].type;
			attr.push_back(att);
			offset+=4;
			//move forward 4 bytes
		}
		else
			//meet the string attribute
		{
			int length;
			memcpy(&length,(char*)iterator+offset,sizeof(int));
			int string_length=length+sizeof(int);
			offset+=string_length;
			att.length=string_length;
			att.name=ca[i].name;
		    att.type=ca[i].type;
		    attr.push_back(att);
		}
	}
	return attr;
}

int RM::RecordLength(const string tableName, const void *data)
{
	vector<CatalogEntry> a=FindCataLog(tableName);
	vector<Attribute> ar=NewLength(a,data);
	int length=0;
	int aa=a.size();
	for (int i=0;i<a.size();i++)
	{
	    int len=ar.at(i).length;
	    length+=len;
	}
	    return length;
}

int RM::GetAttributeLength(const string tableName,const string attributeName,void *data)
{
	    vector<CatalogEntry> a=FindCataLog(tableName);
		vector<Attribute> ar=NewLength(a,data);
		int offset=0;
	    for (int i=0;i<a.size();i++)
	    {
	    	if(strcmp(Change2String(a.at(i).name,50),attributeName.c_str())==0)
	    	{
	    		int len=ar.at(i).length;
                if(a.at(i).type==TypeVarChar)
                {
                	len-=sizeof(int);
                }
	            return len;
	    	}
	    	else
	    	{
	    		offset+=ar.at(i).length;
	    	}
	    }
	    return 0;

}

AttrType RM::GetAttributeType(const string tableName,const string attributeName)
{
	vector<CatalogEntry> a=FindCataLog(tableName);
	int offset=0;
    for (int i=0;i<a.size();i++)
    {
    	if(strcmp(Change2String(a.at(i).name,50),attributeName.c_str())==0)
    	{

            return a.at(i).type;
    	}
    }
    return AttrType(0);
}

RC RM::GetAttributeData(const string tableName,const string attributeName,void *data,void *data_read)
{
	vector<CatalogEntry> a=FindCataLog(tableName);
	vector<Attribute> ar=NewLength(a,data);
	int offset=0;
    for (int i=0;i<a.size();i++)
    {
    	if(strcmp(Change2String(a.at(i).name,50),attributeName.c_str())==0)
    	{
    		int len=ar.at(i).length;
            memcpy((char*)data_read,(char*)data+offset,len);
            return 0;
    	}
    	else
    	{
    		offset+=ar.at(i).length;
    	}
    }
    return 0;
}

RC RM::InsertCatalog(const string tableName,vector<Attribute> catalogInfo)
{
    if(isTableExist(tableName)==0)
	//Such table is already in the catalog
    return -1;
    else
   {
    	pf->OpenFile("SystemCatalog.dat",ph);
    	for (int i=0;i<catalogInfo.size();i++)
    	{
    		CatalogEntry *ca=new CatalogEntry;
    		ca->type=catalogInfo[i].type;
    		strcpy(ca->name,catalogInfo[i].name.c_str());
    		strcpy(ca->tableName,tableName.c_str());
    		ca->position=i;
    		ca->length=catalogInfo[i].length;
    		//ca->version=0;
            int ii=InsertAsPageFile(ph,(void*)ca,112);
            //cout<<ii<<endl;
    	}
    	pf->CloseFile(ph);
    	return 0;
   }
}

//insert the data into the end of the catalog
RC RM::InsertAsPageFile(PF_FileHandle &pf,void *data, int length)
{
	  // FILE *file=pf.GetHandleFile();
	FILE *file=pf.pfile;
	fseek(file,0,SEEK_END);//Find the end of file
	return fwrite((char *)data,1,length,file);
}

//split the char array
char* RM::Change2String(char name[],int length)
{
char * point=name;
int length2=0;
for (int i=0;i<length;i++)
{
if(point[i]=='\0')
{
	length2=i+1;
	break;
}
}
char *cc=new char[length2+1];
memcpy(cc,name,length2);
cc[length2]='\0';
return cc;
}

//Judge whether the tableName exist in the catalog
RC RM::isTableExist(const string tableName)
{

if(pf->OpenFile("SystemCatalog.dat",ph)==0)
{
	//the catalog file exists
	//the number of the pages in catalog file
	void *data=malloc(112);
	//FILE *fileStream=ph.GetHandleFile();
	FILE *fileStream=ph.pfile;
	fseek (fileStream,0,SEEK_END);
	int lSize = ftell (fileStream);
	int num=(sizeof(char)*lSize)/112;

	for (int i=0;i<num;i++)
	{
	   fseek(fileStream,112*(i),SEEK_SET);//set the pointer to the specific position of file stream
       fread((char*)data,1,112,fileStream);
       CatalogEntry *en=new CatalogEntry;
       memcpy(en,data,112);//every entry is 112 bytes long
       const char *cc=en->tableName;
       if(strcmp(cc,tableName.c_str())==0)
       {
    	 pf->CloseFile(ph);
         return 0;
       }
        delete en;

	 }
	}
return -1;
}


RC RM::createTable(const string tableName, const vector<Attribute> &attrs){
	RC rc = pf->CreateFile(tableName.c_str());
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(tableName.c_str(),fileHandle);
	string tablePage = tableName+"page";
	FILE * pFile;
	pFile = fopen(tablePage.c_str(), "wb+");
	void *data = malloc(4);
	//int j = 0;
	int i =0;
	int offset = 0;
	int totalSlot = 10;
	int pagesize = PF_PAGE_SIZE - (totalSlot*2+2)*sizeof(int);
	//memcpy((char *)data + offset, &pagesize, sizeof(int));
	fseek(pFile,0,SEEK_SET);
	fwrite(&pagesize, sizeof(int), 1, pFile);
	if (pFile != NULL) {
		fclose(pFile);
	}
	free(data);
	void *pInfo = malloc(PF_PAGE_SIZE);
	i = 0;
	int slotOffset = -1;
	int recordLengtn = 0;
	int freePlace = 0;
	offset = PF_PAGE_SIZE-sizeof(int)*2;
	for (i = 1; i <= totalSlot*2; i=i+2) {
		memcpy((char *)pInfo + offset-i*sizeof(int), &slotOffset, sizeof(int));
		memcpy((char *)pInfo + offset-(i+1)*sizeof(int), &recordLengtn, sizeof(int));
	}
	memcpy((char *)pInfo + offset, &totalSlot, sizeof(int));
	offset = PF_PAGE_SIZE-sizeof(int);
	memcpy((char *)pInfo + offset, &freePlace, sizeof(int));
	rc = fileHandle.AppendPage(pInfo);
	rc = pf->CloseFile(fileHandle);
	free(pInfo);
	rc = InsertCatalog(tableName,attrs);
	cout<<"creat table:"<<tableName<<endl;
	//do something in catalog file...
	return rc;
}

RC RM::deleteTable(const string tableName){
	RC rc;
	rc = pf->DestroyFile(tableName.c_str());
	//remove(tableName.c_str());
	rc = pf->DestroyFile((tableName+"page").c_str());
	if(pf->OpenFile("SystemCatalog.dat",ph)==0)
			//the catalog file exists and then delete all the entries related to the tableName
		{
				void *data=malloc(112);
				//FILE *fileStream=ph.GetHandleFile();
				FILE *fileStream=ph.pfile;
				fseek (fileStream,0,SEEK_END);
				int lSize = ftell (fileStream);
				int num=(sizeof(char)*lSize)/112;
				void *remove=malloc(112);
				for (int i=0;i<112;i++)
				{
					*((char *) remove + i)='\0';
				}
				for (int i=0;i<num;i++)
				{
				   fseek(fileStream,112*(i),SEEK_SET);//set the pointer to the specific position of file stream
			       fread((char*)data,1,112,fileStream);
			       CatalogEntry *en=new CatalogEntry;
			       memcpy(en,data,112);//every entry is 112 bytes long
			       const char *cc=en->tableName;
			       if(strcmp(cc,tableName.c_str())==0)
			       {
			    	 fseek(fileStream,112*(i),SEEK_SET);
			    	 fwrite((char *)remove,1,112,fileStream);

			       }
			        delete en;

				 }

				pf->CloseFile(ph);
				//return 0;
		}
	//do something in catalog file...
	return rc;
}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs){
	if (isTableExist(tableName) != 0)
		return -1;
	else {
		vector<CatalogEntry> ca = FindCataLog(tableName);
		Attribute att;
		for (int i = 0; i < ca.size(); i++) {
			att.length = ca[i].length;
			att.name = ca[i].name;
			att.type = ca[i].type;
			attrs.push_back(att);
		}
		return 0;
	}
}

RC RM::insertTuple(const string tableName, const void *data, RID &rid){
	//void *pageNum = malloc(sizeof(int));
	//void *pageInfo = malloc(sizeof(int));
	RC rc;
	int pageNum = -1;
	int slotNum = -1;
	int pageSpace = 0;
	int i = 0;
	int totalSlot = 10;
	int offset = PF_PAGE_SIZE-sizeof(int)*2;
	//vector<CatalogEntry> a=FindCataLog(tableName);
	//vector<Attribute> ar=NewLength(a,data);

	//int turpleSize = 100;//need to change later!!!!!!!!!
	int turpleSize = this->RecordLength(tableName,data);
	//cout<<"recordLength:"<<turpleSize<<endl;

	PF_FileHandle fileHandle;
	rc = pf->OpenFile(tableName.c_str(),fileHandle);
	FILE * pFile;
	pFile = fopen((tableName+"page").c_str(), "rb+");
	fseek(pFile, 0, SEEK_END);
	int size = ftell(pFile);
	//cout<<"size: "<<size<<endl;
	void *pageInfo = malloc(size);
	fseek(pFile,0,SEEK_SET);
	fread(pageInfo, sizeof(char), size, pFile);
	for (i = 0; i < size/4; i++){
		//fseek(pFile,i*sizeof(int),SEEK_SET);
		//fread(&pageSpace, sizeof(int), 1, pFile);
		memcpy(&pageSpace, (char *) pageInfo + i*sizeof(int), sizeof(int));
		//cout<<"pageSpace:"<<pageSpace<<endl;
		if(pageSpace >= turpleSize){
			pageNum = i;
			fseek(pFile,i*sizeof(int),SEEK_SET);
			pageSpace= pageSpace - turpleSize;
			fwrite(&pageSpace, sizeof(int), 1, pFile);
			//cout<<"pageNum:"<<pageNum<<" "<<i<<endl;
			break;
		}
	}
	if(pageNum == -1){
		//cout<<"pageNumsfdf:"<<pageNum<<endl;
		void *newInfo = malloc(4);
		int pagesize = 4096 - (totalSlot*2+2)*sizeof(int)-turpleSize;
		//i=size/sizeof(int);
		memcpy((char *) newInfo+0, &pagesize, sizeof(int)); // free space of the page j
		fseek(pFile,size,SEEK_SET);
		fwrite(newInfo, sizeof(int), 1, pFile);
		pageNum = size/sizeof(int);
		void *newData = malloc(PF_PAGE_SIZE);
		memcpy((char *)newData + offset, &totalSlot, sizeof(int));
		int freePlace = 0;
		int slotOffset = -1;
		int recordLength = 0;
		memcpy((char *)newData + offset+sizeof(int), &freePlace, sizeof(int));
		for (i = 1; i <= totalSlot*2; i=i+2) {
			memcpy((char *) newData + offset - i * sizeof(int), &slotOffset,
					sizeof(int));
			memcpy((char *)newData + offset-(i+1)*sizeof(int), &recordLength, sizeof(int));
		}
		fileHandle.AppendPage(newData);
		free(newInfo);
		free(newData);
	}
	rid.pageNum = (unsigned) pageNum;
	free(pageInfo);
	void *pData = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(pageNum,pData);
	//int totalSlot=0;
	int freePlace=0;
	int slotOffset=-2;
	int outofslot = 1;
	int recordLength = 0;
	offset = PF_PAGE_SIZE-sizeof(int)*2;
	memcpy(&totalSlot,(char *)pData + offset, sizeof(int));
	memcpy(&freePlace,(char *)pData + offset+sizeof(int), sizeof(int));
	//cout<<"totalSlot:"<<totalSlot<<"freePlace:"<<freePlace<<endl;
	for(i=1;i<=totalSlot*2;i=i+2){
		memcpy(&slotOffset,(char *)pData + offset-i*sizeof(int), sizeof(int));
		memcpy(&recordLength,(char *)pData + offset-(i+1)*sizeof(int), sizeof(int));
		//cout<<"slotOffset:"<<slotOffset<<"i:"<<i<<endl;
		if(slotOffset == -1&&recordLength==0){
			memcpy((char *)pData + freePlace, data,turpleSize);
			slotOffset = freePlace;
			freePlace = freePlace + turpleSize;
			recordLength = turpleSize;
			memcpy((char *)pData + offset-i*sizeof(int), &slotOffset,sizeof(int));
			memcpy((char *)pData + offset-(i+1)*sizeof(int), &recordLength,sizeof(int));
			memcpy((char *)pData + offset+sizeof(int),&freePlace, sizeof(int));
			outofslot = 0;
			slotNum = i/2;
			break;
		}
	}
	if(outofslot == 1){
		memcpy((char *)pData + freePlace, data,turpleSize);
		slotOffset = freePlace;
		//cout<<"freePlace: "<<freePlace<<" slotoffset:"<<slotOffset<<endl;
		recordLength = turpleSize;
		freePlace = freePlace + turpleSize;
		memcpy((char *) pData + offset - (totalSlot*2+1) * sizeof(int), &slotOffset,
				sizeof(int));
		memcpy((char *) pData + offset - (totalSlot*2+2) * sizeof(int), &recordLength,
						sizeof(int));
		memcpy((char *) pData + offset + sizeof(int),&freePlace,sizeof(int));
		slotNum = totalSlot;
		totalSlot++;
		memcpy((char *)pData + offset, &totalSlot,sizeof(int));
		fseek(pFile, pageNum * sizeof(int), SEEK_SET);
		fread(&pageSpace,sizeof(int),1,pFile);
		pageSpace = pageSpace - sizeof(int)*2;
		fseek(pFile, pageNum * sizeof(int), SEEK_SET);
		fwrite(&pageSpace, sizeof(int), 1, pFile);
	}
	rid.slotNum = (unsigned) slotNum;
	rc = fileHandle.WritePage(pageNum,pData);
	rc = pf->CloseFile(fileHandle);
	free(pData);
	fclose(pFile);
	return rc;
}

RC RM::deleteTuples(const string tableName){
	RC rc;
	vector<Attribute> attrs;
	rc = getAttributes(tableName,attrs);
	rc = deleteTable(tableName);
	createTable(tableName,attrs);
    return rc;
}

RC RM::deleteTuple(const string tableName, const RID &rid){
	RC rc;

	PF_FileHandle fileHandle;
	rc = pf->OpenFile(tableName.c_str(),fileHandle);
	int pageNum = rid.pageNum;
	int slotNum = rid.slotNum;
	int freeSpace;
	int turpleSize = 100;
	int offset = PF_PAGE_SIZE-sizeof(int)*2;
	void *data = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(pageNum,data);
	int slotOffset;
	int recordLength = 0;
	int nullNum = -1;
	memcpy(&slotOffset,(char *)data + offset-(slotNum*2+1)*sizeof(int), sizeof(int));
	memcpy(&recordLength,(char *)data + offset-(slotNum*2+1+1)*sizeof(int), sizeof(int));
	if(slotOffset<0&&recordLength<0){
		int newPageNum = -recordLength-1;
		int newSlotNum = -slotOffset-1;
		RID newRid;
		newRid.pageNum=newPageNum;
		newRid.slotNum = newSlotNum;
		this->deleteTuple(tableName,newRid);
	}else{
		turpleSize = recordLength;
		recordLength = 0;
		memcpy((char *)data + offset-(slotNum*2+1)*sizeof(int), &nullNum,sizeof(int));
		memcpy((char *)data + offset-(slotNum*2+1+1)*sizeof(int), &recordLength,sizeof(int));
		fileHandle.WritePage(pageNum,data);
		this->reorganizePage(tableName,pageNum);
		FILE * pFile;
		pFile = fopen((tableName + "page").c_str(), "rb+");
		int location = pageNum*sizeof(int);
		fseek(pFile, location, SEEK_SET);
		fread(&freeSpace, sizeof(int), 1, pFile);
		freeSpace = freeSpace + turpleSize;
		fseek(pFile, location, SEEK_SET);
		fwrite(&freeSpace, sizeof(int), 1, pFile);
		fclose(pFile);
	}
	free(data);
	pf->CloseFile(fileHandle);
	return rc;
}

RC RM::readTuple(const string tableName, const RID &rid, void *data){
	RC rc;
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(tableName.c_str(),fileHandle);
	int pageNum = rid.pageNum;
	int slotNum = rid.slotNum;
	//cout<<"pageNum:"<<pageNum<<" slotNum:"<<slotNum<<endl;
	int turpleSize;
	int slotOffset;
	int totalSlot;
	void *pData = malloc(PF_PAGE_SIZE);
	rc = fileHandle.ReadPage(pageNum,pData);
	//cout<<rc<<endl;
	if(rc<0){
		cout<<"Do not exist this turple!"<<endl;
		pf->CloseFile(fileHandle);
		free(pData);
		return -1;
	}
	int offset = PF_PAGE_SIZE-sizeof(int)*2;
	memcpy(&totalSlot,(char *)pData + offset, sizeof(int));
	//cout<<"totalSlot: "<<totalSlot<<endl;
	memcpy(&slotOffset,(char *)pData + offset-(slotNum*2+1)*sizeof(int), sizeof(int));
	memcpy(&turpleSize,(char *)pData + offset-(slotNum*2+1+1)*sizeof(int), sizeof(int));
	//cout<<slotOffset<<turpleSize<<endl;
	//cout<<"read turple  slotOffset:"<<slotOffset<<" turpleSize:"<<turpleSize<<endl;
	if(slotOffset==-1&&turpleSize ==0){
		cout<<"Do not exist this turple!"<<endl;
		pf->CloseFile(fileHandle);
		free(pData);
		return -1;
	}
	if(slotNum>=totalSlot){
		cout<<"Do not exist this turple!"<<endl;
		pf->CloseFile(fileHandle);
		free(pData);
		return -1;
	}
	if(slotOffset<0&&turpleSize<0){
		int newpageNum = -turpleSize-1;
		int newslotNum = -slotOffset-1;
		RID newRid;
		newRid.pageNum= newpageNum;
		newRid.slotNum = newslotNum;
		this->readTuple(tableName,newRid,data);
	}else{
		memcpy(data,(char *)pData+slotOffset,turpleSize);
	}
	//return rc;
	free(pData);
	pf->CloseFile(fileHandle);
	return rc;
}

RC RM::updateTuple(const string tableName, const void *data, const RID &rid){
	RC rc = 0;
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(tableName.c_str(),fileHandle);
	void *pData = malloc(PF_PAGE_SIZE);
	int pageNum = rid.pageNum;
	int slotNum = rid.slotNum;
	//cout<<"pageNum"<<pageNum<<endl;
	int freeSpace;
	int freePlace;
	int recordLength;
	int slotOffset;
	int newRLength = this->RecordLength(tableName,data);    //need to change later!!!
	//cout<<"newRecordLength:"<<newRLength<<endl;
	fileHandle.ReadPage(pageNum,pData);
	int offset = PF_PAGE_SIZE-sizeof(int)*2;
	memcpy(&freePlace,(char *)pData + offset+sizeof(int), sizeof(int));
	memcpy(&slotOffset,(char *)pData + offset-(slotNum*2+1)*sizeof(int), sizeof(int));
	memcpy(&recordLength,(char *)pData + offset-(slotNum*2+1+1)*sizeof(int), sizeof(int));
	//cout<<"slotOffset:"<<slotOffset<<endl;
	FILE * pFile;
	pFile = fopen((tableName + "page").c_str(), "rb+");
	fseek(pFile, pageNum*sizeof(int), SEEK_SET);
	fread(&freeSpace, sizeof(int), 1, pFile);
	//cout<<"freeSpace:"<<freeSpace<<endl;
	if(newRLength<=recordLength){
		memcpy((char *)pData + slotOffset,data, newRLength);
		memcpy((char *)pData + offset-(slotNum*2+1+1)*sizeof(int),&newRLength, sizeof(int));
		fileHandle.WritePage(pageNum,pData);
	}
	if(newRLength>recordLength&&newRLength<=freeSpace){
		memcpy((char *)pData + freePlace,data, newRLength);
		memcpy((char *)pData + offset-(slotNum*2+1)*sizeof(int),&freePlace, sizeof(int));
		memcpy((char *)pData + offset-(slotNum*2+1+1)*sizeof(int),&newRLength, sizeof(int));
		freePlace = freePlace + newRLength;
		memcpy((char *)pData + offset+sizeof(int),&freePlace, sizeof(int));
		fileHandle.WritePage(pageNum,pData);
		this->reorganizePage(tableName,pageNum);
		freeSpace = freeSpace -(newRLength-recordLength);
		fseek(pFile, pageNum*sizeof(int), SEEK_SET);
		fwrite(&freeSpace, sizeof(int), 1, pFile);
	}
	if(newRLength>freeSpace){
		RID newRid;
		this->insertTuple(tableName,data,newRid);
		int newSlotNum = -(newRid.slotNum)-1;
		int newPageNum = -(newRid.pageNum)-1;
		//cout<<newSlotNum<<newPageNum<<endl;
		memcpy((char *)pData + offset-(slotNum*2+1)*sizeof(int),&newSlotNum, sizeof(int));
		memcpy((char *)pData + offset-(slotNum*2+1+1)*sizeof(int),&newPageNum, sizeof(int));
		fileHandle.WritePage(pageNum,pData);
		this->reorganizePage(tableName,pageNum);
		freeSpace = freeSpace +recordLength;
		fseek(pFile, pageNum*sizeof(int), SEEK_SET);
		fwrite(&freeSpace, sizeof(int), 1, pFile);
		
	}
	free(pData);
	pf->CloseFile(fileHandle);
	fclose(pFile);
	return rc;
}

RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data){
	//vector<Attribute> attrs;
	RC rc = 0;
	//void *turple = malloc(100);
	/*void *iterator;
	int pageNum = rid.pageNum;
	int slotNum = rid.slotNum;
	int recordLength;
	int slotOffset;
	int offset = PF_PAGE_SIZE - sizeof(int) * 2;
	if (getAttributes(tableName, attrs) == -1) {
		delete[] turple;
		return -1;
	}
	int attrPos;
	for (int i = 0; i < attrs.size(); i++) {
		if (attrs.at(i).name == attributeName) {
			attrPos = i;
			break;
		}
	}
	this->readTuple(tableName, rid, turple);*/
	void *turple = malloc(1000);
	rc = this->readTuple(tableName,rid,turple);
	rc = GetAttributeData(tableName,attributeName,turple,data);
	return rc;
}

RC RM::readAttributeLength(const string tableName, const RID &rid, const string attributeName, int *size){
	RC rc =0;
	return rc;
}

AttrType RM::readAttributeType(const string tableName,const RID &rid, const string attributeName){
	AttrType at= TypeInt;
	return at;
}

RC RM::reorganizePage(const string tableName, const unsigned pageNumber){
	RC rc;
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(tableName.c_str(), fileHandle);
	void *data = malloc(PF_PAGE_SIZE);
	void *newData = malloc(PF_PAGE_SIZE);
	rc = fileHandle.ReadPage(pageNumber,data);
	int offset = PF_PAGE_SIZE-sizeof(int)*2;
	int totalSlot = 0;
	//int freeSpace = 0;
	int newFreeSpace =0;
	int slotOffset;
	int newSlotOffset;
	int recordLength;
	int i;
	memcpy(&totalSlot,(char *)data + offset, sizeof(int));
	//memcpy(&freeSpace,(char *)data + offset+sizeof(int), sizeof(int));
	for(i=1;i<=totalSlot*2;i=i+2){
		memcpy(&slotOffset,(char *)data + offset -i*sizeof(int), sizeof(int));
		memcpy(&recordLength,(char *)data + offset -(i+1)*sizeof(int), sizeof(int));
		//cout<<"slotoffset:"<<slotOffset<<" rLength:"<<recordLength<<" i:"<<i<<endl;
		if(slotOffset > -1&&recordLength>0){
			void *record = malloc(recordLength);
			memcpy(record,(char *)data + slotOffset, recordLength);
			memcpy((char *)newData + newFreeSpace,record, recordLength);
			newSlotOffset = newFreeSpace;
			newFreeSpace = newFreeSpace + recordLength;
			memcpy((char *)data + offset -i*sizeof(int),&newSlotOffset, sizeof(int));
			free(record);
		}
	}
	memcpy((char *)data + offset+sizeof(int),&newFreeSpace, sizeof(int));
	void *newSlotInfo = malloc((totalSlot*2+2)*sizeof(int));
	memcpy(newSlotInfo,(char *)data + offset-totalSlot*2*sizeof(int), (totalSlot*2+2)*sizeof(int));
	memcpy((char *)newData + offset-totalSlot*2*sizeof(int),newSlotInfo, (totalSlot*2+2)*sizeof(int));
	fileHandle.WritePage(pageNumber,newData);
	pf->CloseFile(fileHandle);
	free(newSlotInfo);
	free(data);
	free(newData);
	return rc;
}

RM_ScanIterator::RM_ScanIterator(){
	vector <char*> sObj;
	i=0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
	if(i<sObj.size()){
		//scanObj sj = sObj.at(i);
		memcpy(data,sObj.at(i),1000);
		free(sObj.at(i));
		i++;
		return 0;
	}else{
		i=0;
		return RM_EOF;
	}
}
RC RM::scan(const string tableName,
     const string conditionAttribute,
     const CompOp compOp,                  // comparision type such as "<" and "="
     const void *value,                    // used in the comparison
     const vector<string> &attributeNames, // a list of projected attributes
     RM_ScanIterator &rm_ScanIterator){
	vector<Attribute> attrs;
	this->getAttributes(tableName,attrs);
	PF_FileHandle fileHandle;
	pf->OpenFile(tableName.c_str(), fileHandle);
	int numPages = fileHandle.GetNumberOfPages();
	void* pData = malloc(PF_PAGE_SIZE);
	int offset = PF_PAGE_SIZE-sizeof(int)*2;
	int i;
	for(i=0;i<numPages;i++){
		fileHandle.ReadPage(i,pData);
		int totalSlot;
		memcpy(&totalSlot,(char *)pData + offset, sizeof(int));
		for(int j=0;j<totalSlot;j++){
			int slotOffset;
			int recordLength;
			RID rid;
			bool get=false;
			memcpy(&slotOffset,(char *)pData + offset-(j*2+1)*sizeof(int), sizeof(int));
			memcpy(&recordLength,(char *)pData + offset-(j*2+1+1)*sizeof(int), sizeof(int));
			if(slotOffset>-1&&recordLength>0){
				rid.pageNum=i;
				rid.slotNum=j;
				get = true;
			}
			if(slotOffset<0&&recordLength<0){
				rid.pageNum=-recordLength-1;
				rid.slotNum=-slotOffset-1;
				get = true;
			}
			if(get == true){
				//rid.pageNum=i;
				//rid.slotNum=j;
				//void *total = malloc(100*attributeNames.size());
				bool match= true;
				for(int k =0;k<attributeNames.size();k++){
					void *turpleAttri = malloc(1000);
					void *turple = malloc(1000);
					int attriLength;
					//int turpleSize;
					bool isVar=false;
					this->readTuple(tableName,rid,turple);
					this->readAttribute(tableName,rid,attributeNames.at(k),turpleAttri);
					attriLength = this->GetAttributeLength(tableName,attributeNames.at(k),turple);
					//cout<<"attriLength:"<<attriLength<<endl;
					//cout<<"value:"<<(char *)value<<endl;
					//this->readAttributeLength(tableName,rid,attributeNames.at(k),&turpleSize);
					if(this->GetAttributeType(tableName,attributeNames.at(k))==TypeVarChar){
						isVar=true;
					}
					//memcpy(total,turple,100);
					if(attributeNames.at(k)==conditionAttribute){
						int compare ;
						if(isVar==true){
							compare= memcmp((char*)turpleAttri+sizeof(int),value,attriLength);
						}else{
							compare= memcmp(turpleAttri,value,attriLength);
						}
						//cout<<"compare: "<<compare<<endl;
						if(compOp == 0){
							if(compare == 0){
								match = true;
							}else{
								match = false;
							}
						}
						if (compOp == 1) {
							if (compare<0) {
								match = true;
							}else{
								match = false;
							}
						}
						if (compOp == 2) {
							if (compare>0) {
								match = true;
							}else{
								match = false;
							}
						}
						if (compOp == 3) {
							if (compare<=0) {
								match = true;
							}else{
								match = false;
							}
						}
						if (compOp == 4) {
							if (compare>=0) {
								match = true;
							}else{
								match = false;
							}
						}
						if (compOp == 5) {
							if (compare!=0) {
								match = true;
							}else{
								match = false;
							}
						}
						if (compOp == 6) {
							match = true;
						}
					}
					free(turple);
					free(turpleAttri);
					//this->readTuple(tableName,rid,turple);
				}
				if (match == true) {
					void *require = malloc(1000);
					memset(require,'\0',1000);
					int offset = 0;
					for (int k = 0; k < attributeNames.size(); k++) {
						void *turple = malloc(1000);
						void *attri = malloc(1000);
						int attriLength;
						//int turpleSize;
						this->readAttribute(tableName, rid,
								attributeNames.at(k), attri);
						this->readTuple(tableName,rid,turple);
						attriLength = this->GetAttributeLength(tableName,attributeNames.at(k),turple);
						//this->readAttributeLength(tableName, rid,
						//		attributeNames.at(k), &turpleSize);
						if(this->GetAttributeType(tableName,attributeNames.at(k))==TypeVarChar){
							//memcpy((char *)require+offset,&attriLength,4);
							//offset=offset+4;
							memcpy((char *)require+offset,attri,attriLength+sizeof(int));
							offset=offset+attriLength+sizeof(int);
							//cout<<"offset: "<<offset<<endl;

						}else{
							memcpy((char *)require+offset,attri,4);
							offset=offset+4;
						}
						//memcpy((char *)require+k*100,turple,100);
						free(turple);
						free(attri);
					}
					scanObj sj;
					sj.pageNum = i;
					sj.slotNum = j;
					memcpy(sj.data, require,offset);
					rm_ScanIterator.sObj.push_back((char*)require);
					//free(require);
				}
			}
		}
	}
	pf->CloseFile(fileHandle);
	RC rc =0;
	return rc;
}

RC RM::reorganizeTable(const string tableName){
	RC rc;
	PF_FileHandle fileHandle;
	FILE *pFile;
	pFile = fopen((tableName + "page").c_str(), "rb+");
	rc = pf->OpenFile(tableName.c_str(), fileHandle);
	int numPages = fileHandle.GetNumberOfPages();
	int offset = PF_PAGE_SIZE - sizeof(int) * 2;
	int i;
	for (i = 0; i < numPages; i++) {
		void* pData = malloc(PF_PAGE_SIZE);
		fileHandle.ReadPage(i, pData);
		int totalSlot;
		memcpy(&totalSlot, (char *) pData + offset, sizeof(int));
		for (int j = 0; j < totalSlot; j++) {
			int slotOffset;
			int recordLength;
			RID rid;
			//bool get=false;
			memcpy(&slotOffset,
					(char *) pData + offset - (j * 2 + 1) * sizeof(int),
					sizeof(int));
			memcpy(&recordLength,
					(char *) pData + offset - (j * 2 + 1 + 1) * sizeof(int),
					sizeof(int));
			if (slotOffset > -1 && recordLength >= 0) {
				//rid.pageNum=i;
				//rid.slotNum=j;
				void *turple = malloc(recordLength);
				memcpy(turple, (char *) pData + slotOffset, recordLength);
				slotOffset = -1;
				recordLength = 0;
				memcpy((char *) pData + offset - (j * 2 + 1) * sizeof(int),
						&slotOffset, sizeof(int));
				memcpy((char *) pData + offset - (j * 2 + 1 + 1) * sizeof(int),
						&recordLength, sizeof(int));
				this->insertTuple(tableName, turple, rid);
				int freeSpace;
				fseek(pFile, i * sizeof(int), SEEK_SET);
				fread(&freeSpace, sizeof(int), 1, pFile);
				freeSpace = freeSpace + recordLength;
				fseek(pFile, i * sizeof(int), SEEK_SET);
				fwrite(&freeSpace, sizeof(int), 1, pFile);
				free(turple);
				//get = true;
			}
			if (slotOffset < 0 && recordLength < 0) {
				rid.pageNum = -recordLength-1;
				rid.slotNum = -slotOffset-1;
				void *turple = malloc(1000);
				this->readTuple(tableName,rid,turple);
				this->deleteTuple(tableName,rid);
				slotOffset = -1;
				recordLength = 0;
				memcpy((char *) pData + offset - (j * 2 + 1) * sizeof(int),
						&slotOffset, sizeof(int));
				memcpy((char *) pData + offset - (j * 2 + 1 + 1) * sizeof(int),
						&recordLength, sizeof(int));
				this->insertTuple(tableName, turple, rid);
				free(turple);
				//get = true;
			}

		}
		for(int k =0;k<=i;k++){
			this->reorganizePage(tableName,k);
		}
		free(pData);
	}
	fclose(pFile);
	pf->CloseFile(fileHandle);
	return rc;
}
