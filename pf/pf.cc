#include "pf.h"
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <iostream>

PF_Manager* PF_Manager::_pf_manager = 0;


PF_Manager* PF_Manager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PF_Manager();
    
    return _pf_manager;    
}


PF_Manager::PF_Manager()
{
}


PF_Manager::~PF_Manager()
{
}

    
RC PF_Manager::CreateFile(const char *fileName)
{
	struct stat stFileInfo;
	//string file = *fileName;

	if (stat(fileName, &stFileInfo) == 0) {
		cout << "Do not create a exist file!"<<endl;
		return -1;
	} else {

		FILE * pFile;
		pFile = fopen(fileName, "wb+");
		if (pFile != NULL) {
			fclose(pFile);
			return 0;
		}
	}
	return -1;
}


RC PF_Manager::DestroyFile(const char *fileName)
{
	struct stat stFileInfo;
	//string file = *fileName;
	if (stat(fileName, &stFileInfo) != 0) {
		cout << "Do not delete a file that do not exist!"<<endl;
		return -1;
	}
	if (remove(fileName) == 0) {
		return 0;
	}

	return 0;
}


RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
	//RC rc=0;
	struct stat stFileInfo;
	//string file = *fileName;
	if (stat(fileName, &stFileInfo) != 0) {
		cout << "Do not open a file that do not exist!"<<endl;
		return -1;
	}

	if (fileHandle.file != NULL) {
			cout<<"The file Handler has already been allocate to a file!"<<endl;
			return -1;
		}
	FILE * pFile;
	pFile = fopen(fileName, "rb+");
	if (pFile != NULL) {
		fileHandle.file = fileName;
		fileHandle.pfile = pFile;
		fseek(pFile, 0, SEEK_END);
		int size = ftell(pFile);
		//cout<<"page size is "<<size<<endl;
		fileHandle.pNum = size / PF_PAGE_SIZE;
		//cout<<"page number is "<<fileHandle.pNum<<endl;
		return 0;
	}

	return 0;
}


RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
	// FILE * pFile;
	// pFile = fopen ("myfile.txt","wt");
	 //fprintf (pFile, "fclose example");

	if(fclose (fileHandle.pfile)==0){;
	 fileHandle.pfile=NULL;
	 fileHandle.file=NULL;
	 fileHandle.pNum=0;
	// delete &fileHandle;
	 return 0;
	}
	 return -1;
    //return -1;
}


PF_FileHandle::PF_FileHandle()
{
	pfile=NULL;
	file=NULL;
	pNum=0;
}
 

PF_FileHandle::~PF_FileHandle()
{
}


RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
	if(pageNum>pNum-1){
		cout<<"Can not read a page that is not exist!"<<endl;
		return -1;
	}
	fseek(pfile,pageNum*PF_PAGE_SIZE,SEEK_SET);
	fread(data, sizeof(char), PF_PAGE_SIZE, pfile);
	return 0;
}


RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
	if(pageNum>pNum-1){
		cout<<"Can not write a page that is not exist!"<<endl;
		return -1;
	}
	fseek(pfile,pageNum*PF_PAGE_SIZE,SEEK_SET);
	fwrite(data, sizeof(char), PF_PAGE_SIZE, pfile);
	//pNum = pageNum+1;
    return 0;
}


RC PF_FileHandle::AppendPage(const void *data)
{
	//cout<<"page number is "<<pNum<<endl;
	fseek(pfile,pNum*PF_PAGE_SIZE,SEEK_SET);
	//cout<<sizeof(data);
	fwrite(data, sizeof(char), PF_PAGE_SIZE, pfile);
	pNum = pNum+1;
    return 0;
}


unsigned PF_FileHandle::GetNumberOfPages()
{
	return pNum;
}


