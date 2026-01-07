/*
	MS - Artificial Intelligence and Machine Learning
	Course: CSC507 Foundations of Operating Systems
	Module 8: Portfolio Project
	Professor: Dr. Joseph Issa
	Created by Mukul Mondal
	December 31, 2025
	
	Problem statement: 
	Option #1: Working with Big Data using Multithreading
	The goal of this project is to use the concepts taught in this course to develop an efficient way of working with Big Data.

	You should have 2 files in your Linux system: hugefile1.txt and hugefile2.txt, with one billion lines in each one. 
	If you do not, please go back to the Module 7 Portfolio Reminder and complete the steps there.

	Create a program, using a programming language of your choice, to produce a new file: totalfile.txt, 
	by taking the numbers from each line of the two files and adding them. So, each line in 
	file #3 is the sum of the corresponding line in hugefile1.txt and hugefile2.txt.
*/

// Handling large(>10 GB) data files.
//
// cpp used instead of c for some feature level support of 
// modern cpp and it's std lib usage.

#include <iostream>
#include <string>
#include <cstring>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>
#include <pthread.h>

#include <fcntl.h>      // open
#include <unistd.h>     // close
#include <sys/mman.h>   // mmap64, munmap, msync 
#include <sys/stat.h>   // ftruncate

#include <random>

using namespace std;


std::mutex filePart_mutex;  		 // global mutex
int g_largeFileCreationCount = 0;    // global count of large file created.


struct ThreadArgs2
{
	int threadIndex;	
	vector<long> processedDataItems;
	char* pchFile1PartDataStart;
	char* pchFile2PartDataStart;
	int64_t tobeProcessedLineCount;   // how many lines to process by this thread
};

// This is thread function that does processing for a part of the input data files.
// Stores the processed data for this part/thread into the thread's data structure.
void* ProcessFile(void* arg)
{
	if(!arg) return NULL;
	ThreadArgs2* threadArg = (ThreadArgs2*)arg;
	if(!threadArg || !threadArg->pchFile1PartDataStart || !threadArg->pchFile2PartDataStart 
		|| threadArg->tobeProcessedLineCount < 0) return NULL;
	
	char* file1Data = threadArg->pchFile1PartDataStart;
	if(!file1Data || !*file1Data) return NULL;
	char* file2Data = threadArg->pchFile2PartDataStart;
	if(!file2Data || !*file2Data) return NULL;
	
	// if not the first thread then adjustment for file1Data and file2Data may be needed to point to the begining
	// (just after '\n') of this line. Also adjust for any empty spaces or blank lines.
	if(threadArg->threadIndex > 0)
	{
		// move to the begining of the line
		while(*file1Data != '\n') file1Data--;
		while(*file2Data != '\n') file2Data--;
		
		if(*file1Data == '\n') file1Data++;		
		if(*file2Data == '\n') file2Data++;
	}

	char strCurrentLongNumber[32]; 
	int64_t readDoneLineCount = 0;

	int64_t kLine1 = 0, kLine2 = 0;
	char file1LongNumber[32]; char file2LongNumber[32];
	bool bDataOk = true;
	
	while (bDataOk && readDoneLineCount < threadArg->tobeProcessedLineCount && file1Data && *file1Data && file2Data && *file2Data)
	{	
		bDataOk = false;	
		try  
		{
			// process the line (convert to long number)
			while(file1Data[kLine1] != '\n')
			{
				file1LongNumber[kLine1] = file1Data[kLine1];
				kLine1++;
			}
			file1LongNumber[kLine1] = '\0';
			while(file2Data[kLine2] != '\n')
			{
				file2LongNumber[kLine2] = file2Data[kLine2];
				kLine2++;
			}
			file2LongNumber[kLine2] = '\0';
			long Line1PlusLine2 = atol(file1LongNumber) + atol(file2LongNumber);
			threadArg->processedDataItems.push_back(Line1PlusLine2);
			
			readDoneLineCount++;
			if(file1Data[kLine1] == '\n') file1Data += 1;			
			if(file2Data[kLine2] == '\n') file2Data += 1;
			
			if( file1Data && *file1Data && file2Data && *file2Data)
			{
				bDataOk = true; // continue processing
				file1Data += kLine1;
				file2Data += kLine2;
				kLine1 = 0; kLine2 = 0;
			}
		}
		catch (const std::exception& e) { } // if exception then proceed to next line				
	}
	
	return NULL;
}


// Thread argument structure
struct ThreadArgs1
{
	char newFile[256];
	char pchReadFileName1[256];
	char pchReadFileName2[256];
	int64_t LineCount;
	int ThreadCount;
};

// input files: hugefile1.txt and hugefile2.txt -- already exist
// output file: totalfile.txt -- to be created
void* create_large_text_data_file_from_large_text_input_data_files(void* threadParams)
{
	if(!threadParams) return nullptr;
	ThreadArgs1* thArgs = (ThreadArgs1*)threadParams;
	if(!thArgs || !thArgs->newFile || !*thArgs->newFile) return nullptr;
	if(thArgs->LineCount < 1 || thArgs->ThreadCount < 1) return nullptr;
	if(!thArgs->pchReadFileName1 || !*thArgs->pchReadFileName1) return nullptr;
	if(!thArgs->pchReadFileName2 || !*thArgs->pchReadFileName2) return nullptr;
		
	// 1. Open file read-only 
	int fdRead1 = open(thArgs->pchReadFileName1, O_RDONLY); 
	if (fdRead1 == -1) 
	{ 
		std::cerr << "open(fdRead1) failed: " << strerror(errno) << "\n"; 
		return nullptr; 
	}
	int fdRead2 = open(thArgs->pchReadFileName2, O_RDONLY); 
	if (fdRead2 == -1) 
	{ 
		std::cerr << "open(fdRead2) failed: " << strerror(errno) << "\n"; 
		close(fdRead1);
		return nullptr; 
	}

	struct stat statRead1; if (fstat(fdRead1, &statRead1) == -1) { std::cerr << "fstat(fdRead1) failed: " << strerror(errno) << "\n"; close(fdRead1); return nullptr; }
	struct stat statRead2; if (fstat(fdRead2, &statRead2) == -1) { std::cerr << "fstat(fdRead2) failed: " << strerror(errno) << "\n"; close(fdRead1); close(fdRead2); return nullptr; }
	if (statRead1.st_size < 1 || statRead2.st_size < 1) 
	{ 
		std::cerr << "Input files sizes do not match.\n"; 
		close(fdRead1); 
		close(fdRead2); 
		return nullptr; 
	}

	// 2. Memory-map the input files
	// void* map1 = mmap64(nullptr, stRead1.st_size, PROT_READ, MAP_SHARED, fdRead1, 0); 
	void* mapFile1 = mmap64(nullptr, statRead1.st_size, PROT_READ, MAP_PRIVATE, fdRead1, 0); 
	if (mapFile1 == MAP_FAILED) 
	{ 
		std::cerr << "mmap64(fdRead1) failed: " << strerror(errno) << "\n"; 
		close(fdRead1); 
		close(fdRead2); 
		return nullptr; 
	}

	void* mapFile2 = mmap64(nullptr, statRead2.st_size, PROT_READ, MAP_PRIVATE, fdRead2, 0); 
	if (mapFile2 == MAP_FAILED) 
	{ 
		std::cerr << "mmap64(fdRead2) failed: " << strerror(errno) << "\n"; 
		munmap(mapFile1, statRead1.st_size);
		close(fdRead1); 
		close(fdRead2); 
		return nullptr; 
	}
	const char* pchMappedFile1Data = static_cast<const char*>(mapFile1);
	const char* pchMappedFile2Data = static_cast<const char*>(mapFile2);

	int threadCount = thArgs->ThreadCount; // number of threads to use for processing
	std::vector<pthread_t> threads(threadCount);	
	std::vector<ThreadArgs2*> threadDataList(threadCount, nullptr);
	int64_t chunkSizeFile1 = statRead1.st_size / threadCount;
	int64_t chunkSizeFile2 = statRead2.st_size / threadCount;
	for(int i=0; i<threadCount; i++)
	{
		// prepare thread data for each thread
		threadDataList[i] = new ThreadArgs2();
		threadDataList[i]->threadIndex = i;		
		threadDataList[i]->pchFile1PartDataStart = (char*)pchMappedFile1Data + (i * chunkSizeFile1);
		threadDataList[i]->pchFile2PartDataStart = (char*)pchMappedFile2Data + (i * chunkSizeFile2);
		threadDataList[i]->tobeProcessedLineCount = thArgs->LineCount / threadCount; // total lines to process per thread		

		pthread_create(&threads[i], NULL, ProcessFile, (void*)threadDataList[i]);
	}
	
	// wait for threads to finish	
	for (int i = 0; i < threads.size(); i++)
		pthread_join(threads[i], NULL);

	// close input files
	// ---- 7. Unmap and close input files ---- 	
	if (munmap(mapFile1, statRead1.st_size) == -1) 
	{ 
		std::cerr << "munmap(mapFile1) failed: " << strerror(errno) << "\n"; 
	}
	if (munmap(mapFile2, statRead2.st_size) == -1) 
	{ 
		std::cerr << "munmap(mapFile2) failed: " << strerror(errno) << "\n"; 
	}
	close(fdRead1);
	close(fdRead2);

	// ---- 1. Create the outputfile ---- 
	int fdWrite = open(thArgs->newFile, O_RDWR | O_CREAT | O_TRUNC, 0644); 
	if (fdWrite == -1) 
	{ 
		std::cerr << "open(WriteFile) failed: " << strerror(errno) << "\n"; 
		return nullptr; 
	}

	// ---- 2. Decide a max bytes per number (i.e. per line of text) ---- 
	// ?? up to 20 bytes per number (including newline)
	const int MAX_BYTES_PER_NUMBER = 20;
	const int64_t MAX_FILE_SIZE = thArgs->LineCount * MAX_BYTES_PER_NUMBER;

	// ---- 3. Set the file's initial size ---- 
	if (ftruncate(fdWrite, MAX_FILE_SIZE) == -1) 
	{ 
		std::cerr << "ftruncate(fdWrite) failed: " << strerror(errno) << "\n"; 
		close(fdWrite); 
		return nullptr; 
	}
	
	// ---- 4. Memory-map the file ---- 
	void* mapOutputFile = mmap64(nullptr, MAX_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fdWrite, 0); 
	if (mapOutputFile == MAP_FAILED) 
	{ 
		std::cerr << "mmap64(fdWrite) failed: " << strerror(errno) << "\n"; 
		close(fdWrite); 
		return nullptr; 
	}
	
	char* base = static_cast<char*>(mapOutputFile); 
	int64_t offset = 0;
		
	// Now sequentially write the processed data from each thread into the output file area.
	// I should not run this in multiple threads. Because, in advance, I don't know the total size of 'threadDataList[i]->processedDataItems'.
	// So writing in multiple threads may cause 'empty' data/line in the output file.
	for(int i=0; i<threadDataList.size(); i++)
	{
		for(int j=0; j<threadDataList[i]->processedDataItems.size(); j++)
		{
			char strCurrentLongNumber[32];
			sprintf(strCurrentLongNumber, "%ld\n", threadDataList[i]->processedDataItems[j]);
			int k = strlen(strCurrentLongNumber);
			memcpy(base + offset, strCurrentLongNumber, k);
			offset += k;
		}		
	}	
		
	// ---- 6. Flush changes to disk ----
	if (msync(base, MAX_FILE_SIZE, MS_SYNC) == -1) 
	{ 
		std::cerr << "msync(base = (char*)map) failed: " << strerror(errno) << "\n"; 
	}
	
	if (munmap(mapOutputFile, MAX_FILE_SIZE) == -1) 
	{ 
		std::cerr << "munmap(map / base) failed: " << strerror(errno) << "\n"; 
	}
	
	// ---- 8. Truncate the output file to actual used size ----
	if (ftruncate(fdWrite, offset) == -1) 
	{ 
		std::cerr << "ftruncate(fdWrite) to actual size failed: " << strerror(errno) << "\n"; 
	}

	std::cout << "Initial output file(totalfile.txt) size : " << MAX_FILE_SIZE << " bytes\n";
	std::cout << "Wrote by all threads: " << offset << " bytes\n";
	std::cout << "Blank space in the output file: " << (MAX_FILE_SIZE - offset) << " bytes\n";
	std::cout << "Now file size set to: " << offset << " bytes\n";

	close(fdWrite);

	return nullptr;		
}

// long executing function, so we need to run it in a seperate thread.
// thread to create large text data file with billions line random long numbers
void* create_large_text_data_file_with_billions_line_random_long_numbers(void* threadParams)
{
	if(!threadParams) return nullptr;
	ThreadArgs1* thArgs = (ThreadArgs1*)threadParams;
	if(!thArgs || !thArgs->newFile || !*thArgs->newFile || thArgs->LineCount < 1) return nullptr;
	
	// const int64_t N = 1000000000LL; // 1 billion	
	// thArgs->LineCount: how many lines of data to write into each file.

	// ---- 1. Open or create the file ---- 
	int fd = open(thArgs->newFile, O_RDWR | O_CREAT | O_TRUNC, 0644); 
	if (fd == -1) 
	{ 
		std::cerr << "File Create failed: " << strerror(errno) << "\n"; 
		return nullptr; 
	}

	// ---- 2. Decide a max bytes per number (i.e. per line of text) ---- 
	// ?? up to 20 bytes per number (including newline)
	const int MAX_BYTES_PER_NUMBER = 20;
	const int64_t MAX_FILE_SIZE = thArgs->LineCount * MAX_BYTES_PER_NUMBER;

	// ---- 3. Set the file size ---- 
	if (ftruncate(fd, MAX_FILE_SIZE) == -1) 
	{ 
		std::cerr << "ftruncate failed: " << strerror(errno) << "\n"; 
		close(fd); 
		return nullptr; 
	}
	
	// ---- 4. Memory-map the file ---- 
	void* map = mmap64(nullptr, MAX_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0); 
	if (map == MAP_FAILED) 
	{ 
		std::cerr << "mmap64 failed: " << strerror(errno) << "\n"; 
		close(fd); 
		return nullptr; 
	}
	
	char* base = static_cast<char*>(map); 
	int64_t offset = 0; // current write position

	// ---- 5. Write random numbers into the memory-mapped area ----
	for (int64_t i = 0; i < thArgs->LineCount; ++i) 
	{ 		
		char* ptr = base + offset; // Pointer to where we want to write
		int64_t remaining = MAX_FILE_SIZE - offset; // Remaining space check
		if (remaining <= 0) 
		{ 
			std::cerr << "Ran out of mapped space at i = " << i << "\n"; 
			break; 
		}
		int writtenBytesCount = std::snprintf(ptr, static_cast<size_t>(remaining), "%ld\n", random());
		if (writtenBytesCount < 0) 
		{ 
			std::cerr << "snprintf failed at i = " << i << "\n"; 
			break; 
		}
		if (writtenBytesCount >= remaining) 
		{ 
			std::cerr << "Not enough space for i = " << i << "\n"; 
			break; 
		}
		offset += writtenBytesCount;
	}
	
	// ---- 6. Flush changes to disk ----
	if (msync(base, MAX_FILE_SIZE, MS_SYNC) == -1) 
	{ 
		std::cerr << "msync failed: " << strerror(errno) << "\n"; 
	}

	// ---- 7. Unmap and close the file ---- 
	if (munmap(base, MAX_FILE_SIZE) == -1) 
	{ 
		std::cerr << "munmap failed: " << strerror(errno) << "\n"; 
	}

	// ---- 8. Truncate file to actual used size ----
	if (ftruncate(fd, offset) == -1) 
	{ 
		std::cerr << "ftruncate to actual size failed: " << strerror(errno) << "\n"; 
	}
	close(fd);
	std::cout << "Wrote " << offset << " bytes\n";
	
	filePart_mutex.lock();
	g_largeFileCreationCount++;	
	filePart_mutex.unlock();
	
	return nullptr;		
}


int main()
{
	cout << " ===  CSC507 - Module 8:  Portfolio Project  ===" << endl;	
	cout << " ===  Option #1: Working with Big Data using Multithreading  ===" << endl;	
	
	// Part 1:
	// ---- Create two large text data files with 1 billion random long numbers each. ----
	// test: void create_large_text_data_file_with_billions_line_random_long_numbers(void* threadParams)
	cout << " Part 1: Create two large text data files with 1 billion random long numbers each." << endl;
	cout << "       : hugefile1.txt  and  hugefile2.txt " << endl;
	g_largeFileCreationCount = 0;
	const int64_t N = 1000000000LL; // 1 billion
	auto start = std::chrono::steady_clock::now();
	auto end = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
	
	std::vector<pthread_t> threads(2); // we need 2 threads to create 2 large files(hugefile1.txt, hugefile2.txt).
	
	ThreadArgs1* thData1 = new ThreadArgs1();
	strcpy(thData1->newFile, "/mnt/c/Projs/WSL/Ubuntu/CCpp/csu/csc507/module8/datafiles/hugefile1.txt");
	thData1->LineCount = N; // 1 billion
	pthread_t thread1;
	pthread_create(&thread1, NULL, create_large_text_data_file_with_billions_line_random_long_numbers, (void*)thData1);
	pthread_detach(thread1); 	// detach thread to allow independent execution
	threads.push_back(thread1);

	ThreadArgs1* thData2 = new ThreadArgs1();
	strcpy(thData2->newFile, "/mnt/c/Projs/WSL/Ubuntu/CCpp/csu/csc507/module8/datafiles/hugefile2.txt");
	thData2->LineCount = N; // 1 billion
	pthread_t thread2;
	pthread_create(&thread2, NULL, create_large_text_data_file_with_billions_line_random_long_numbers, (void*)thData2);
	pthread_detach(thread2); 	// detach thread to allow independent execution
	threads.push_back(thread2);
	
	filePart_mutex.lock();
	while(g_largeFileCreationCount < 2)
	{
		// wait for both files to be created
		filePart_mutex.unlock();
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		filePart_mutex.lock();
	}
	filePart_mutex.unlock();

	// wait for both threads to finish	
	for (int i = 0; i < threads.size(); i++)		 
		pthread_join(threads[i], NULL);

	end = std::chrono::steady_clock::now();
	elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
	std::cout << "Created large text data files with 1 billion long random numbers.\n Files:\n" << thData1->newFile << "\n" << thData2->newFile << endl;
	std::cout << " Part1: Total execution time: " << elapsed/1000 << " seconds\n";
	
	threads.clear();
	
	std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	
	cout << " -------------------------------------------------" << endl;

	// Part 2
	// ---- Now create totalfile.txt by adding corresponding lines from hugefile1.txt and hugefile2.txt. ----
	// test: void create_large_text_data_file_from_large_text_input_data_files(void* threadParams)
	cout << " Part 2: Create totalfile.txt by adding corresponding lines from hugefile1.txt and hugefile2.txt." << endl;
	start = std::chrono::steady_clock::now();
	ThreadArgs1* thData12 = new ThreadArgs1();
	strcpy(thData12->newFile, "/mnt/c/Projs/WSL/Ubuntu/CCpp/csu/csc507/module8/datafiles/totalfile.txt");
	thData12->LineCount = N; // 1 billion
	strcpy(thData12->pchReadFileName1, "/mnt/c/Projs/WSL/Ubuntu/CCpp/csu/csc507/module8/datafiles/hugefile1.txt");
	strcpy(thData12->pchReadFileName2, "/mnt/c/Projs/WSL/Ubuntu/CCpp/csu/csc507/module8/datafiles/hugefile2.txt");	
	thData12->ThreadCount = 20; // number of threads to use for processing
	cout << " Thread Count = " << thData12->ThreadCount << endl;
	create_large_text_data_file_from_large_text_input_data_files((void*)thData12);
	// we can also call this in a thread, as shown below: 
	//pthread_t thread12;
	//pthread_create(&thread12, NULL, create_large_text_data_file_from_large_text_input_data_files, (void*)thData12);
	//pthread_detach(thread12); 	// detach thread to allow independent execution	
	//pthread_join(thread12, NULL); // wait for thread to finish
	end = std::chrono::steady_clock::now();
	elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
	std::cout << "Created totalfile.txt by adding corresponding lines from hugefile1.txt and hugefile2.txt.\n Result output File: " << thData12->newFile << endl;
	std::cout << " Part2: Total execution time: " << elapsed/1000 << " seconds\n";	

	return 0;
}
