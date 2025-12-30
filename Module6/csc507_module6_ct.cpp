/*
	MS - Artificial Intelligence and Machine Learning
	Course: CSC507 Foundations of Operating Systems
	Module 6: Critical Thinking Assignment
	Professor: Dr. Joseph Issa
	Created by Mukul Mondal
	December 21, 2025
	
	Problem statement: 
	Evaluate Real-Time Process Scheduling
	In an earlier module( Module 4: Portfolio Milestone), you created programs that 
	read the contents of a large file and process it, 
	writing the results into another large file. 
	What if the files were 10x bigger, i.e. instead of a million rows, they were 10 million rows? 
	Which of the following methods would have the fastest processing time:

	1. Run the process as it is, with the larger files.
	2. Break the input file up into 10 files and schedule the process on each one to run in real-time, then combine the resulting files into a single output file. 
	3. Break the input file up into 2 files and schedule the process on each one to run in real-time, then combine the resulting files into a single output file. 
	4. Break the input file up into 5 files and schedule the process on each one to run in real-time, then combine the resulting files into a single output file. 
	5. Break the input file up into 20 files and schedule the process on each one to run in real-time, then combine the resulting files into a single output file.
	Can you think of other ways to increase efficiency and reduce processing time? 
	Describe in detail or provide a script to do so, with expected results for each method. 
	Feel free to create such scripts and run them, to have actual results instead of theoretical results.
*/

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



using namespace std;

std::mutex outputFile_Access_mutex;  // global mutex
std::mutex filePart_mutex;  		 // global mutex
int g_lastFilePartWrittenIndex = 0;  // global file part index, indicates to be written part index. 
									 // To keep the same order data as in the input file.

struct ThreadData
{
	int filePartIndex;
	char* filePartData;
};

// 1. Read the entire contents of file1.txt into memory, then process each row.
void read_file_into_memory_process_each_row(char* fileName, char* outputFileName)
{
	if (!fileName) return;

	FILE* fp = fopen(fileName, "r");
	if (fp == NULL)
	{
		perror("Failed to open file");
		return;
	}

	FILE* fpw = fopen(outputFileName, "w+");
	if (fpw == NULL)
	{
		perror("Failed to open file");
		return;
	}

	// get file size
	fseek(fp, 0, SEEK_END);
	long fileSize = ftell(fp);
	rewind(fp);

	// read entire file into memory
	char* fileBuffer = (char*)malloc(sizeof(char) * (fileSize + 1));
	size_t bytesRead = fread(fileBuffer, sizeof(char), fileSize, fp);
	fileBuffer[bytesRead] = '\0';

	// Test: I'll write 10,000 numbers into the output file at a time(by one IO write).
	int batchSize = 10000;
	int bufferSize = 1 + sizeof(long) * batchSize + 3 * batchSize; // to account for '\n' etc.
	char* currentWriteBatch = (char*)malloc(sizeof(char) * bufferSize);
	char strCurrentLongRandom[32]; long k = 0;
	int lineIndex = 0;
	// process each row
	char* line = strtok(fileBuffer, "\n");
	while (line != NULL)
	{		
		try  
		{
			// process the line (convert to number and double it)				
			long doubled = atol(line) * 2;
			sprintf(strCurrentLongRandom, "%ld\n", doubled);
			sprintf(currentWriteBatch + k, "%s", strCurrentLongRandom);
			k += strlen(strCurrentLongRandom);
			lineIndex++;
			
			if(lineIndex == batchSize-1)
			{
				*(currentWriteBatch + k) = '\0';
				fprintf(fpw, "%s", currentWriteBatch);
				k = 0;
				lineIndex = 0;				
			}
		}
		catch (const std::exception& e) { } // if exception then proceed to next line
		
		line = strtok(NULL, "\n"); // get next line
	}

	if(k > 0 && lineIndex > 0) // write remaining lines if any
	{
		*(currentWriteBatch + k) = '\0';
		fprintf(fpw, "%s", currentWriteBatch);
	}
	
	if(currentWriteBatch != NULL)
	{
		free(currentWriteBatch);
		currentWriteBatch = NULL;
	}
	if(fileBuffer != NULL)
	{
		free(fileBuffer);
		fileBuffer = NULL;
	}
	
	fclose(fp);
	fclose(fpw);
}	

// 2. Read one row of file1.txt at a time and process it.
void read_one_row_at_a_time_process(char* fileName, char* outputFileName)
{
	if (!fileName) return;

	FILE* fp = fopen(fileName, "r");
	if (fp == NULL)
	{
		perror("Failed to open file");
		return;
	}

	FILE* fpw = fopen(outputFileName, "w+");
	if (fpw == NULL)
	{
		perror("Failed to open output file");
		fclose(fp);
		return;
	}

	char line[256];
	while (fgets(line, sizeof(line), fp))
	{
		// process the line (for example, convert to number and double it)		
		// and write it back to the output file
		try  
                                   {				
		          long doubled = atol(line) * 2;				
		          fprintf(fpw, "%ld\n", doubled);
		} catch (const std::exception& e) { } // if exception then proceed to next line
	}

	fclose(fp);
	fclose(fpw);
}

// This is thread function that does processing for a part of the input data file.
// Then writes the processed data into the output file synchronously.
void* ProcessFile(void* arg)
{
	if(!arg) return NULL;
	ThreadData* threadData = (ThreadData*)arg;
	if(!threadData || !threadData->filePartData || threadData->filePartIndex < 0) return NULL;
	
	char* fileData = threadData->filePartData;
	if(!fileData || !*fileData) return NULL;

	int bufferSize = strlen(fileData);
	bufferSize += bufferSize / sizeof(long);
	char* writeBuffer = (char*)malloc(1 + sizeof(char) * bufferSize);
	char strCurrentLongNumber[32]; long k = 0;

	char* line = strtok(fileData, "\n");
	while (line != NULL)
	{		
		try  
		{
			// process the line (convert to number and double it)				
			long doubled = atol(line) * 2;
			sprintf(strCurrentLongNumber, "%ld\n", doubled);
			sprintf(writeBuffer + k, "%s", strCurrentLongNumber);
			k += strlen(strCurrentLongNumber);			
		}
		catch (const std::exception& e) { } // if exception then proceed to next line		
		
		line = strtok(NULL, "\n"); // get next line
	}

	*(writeBuffer + k) = '\0';
	
	// write the processed data into the output file synchronously.
	// all writes go to the same output file in the same order.
	filePart_mutex.lock();
	while(g_lastFilePartWrittenIndex != threadData->filePartIndex)
	{
		// wait for turn
		filePart_mutex.unlock();
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		filePart_mutex.lock();
	}
	outputFile_Access_mutex.lock();	
	char outputFile[] = "/mnt/c/Projs/WSL/Ubuntu/CCpp/csu/csc507/module6/datafiles/outputfile.txt";
	FILE* fp = NULL;
	if(threadData->filePartIndex == 0)
	{
		// first part, create new file or overwrite existing file
		fp = fopen(outputFile, "w+");
	}
	else
	{
		// append to existing file
		fp = fopen(outputFile, "a+");
	}
	
	if (fp != NULL)
	{
		fprintf(fp, "%s", writeBuffer);
		fclose(fp);
	}
		
	if(writeBuffer != NULL)
	{
		free(writeBuffer);
		writeBuffer = NULL;
	}
	if(fileData != NULL)
	{
		free(fileData);
		fileData = NULL;
	}
	outputFile_Access_mutex.unlock();
	g_lastFilePartWrittenIndex++; // next part can be written now.
	filePart_mutex.unlock();
	return NULL;
}

// Split file1.txt into multiple parts and then process each part in different threads.
void split_file_into__parts_then_process2(char* fileName, int partsCount)
{
	if (!fileName || !*fileName || partsCount < 1) return;

	FILE* fp = fopen(fileName, "r");
	if (fp == NULL)
	{
		perror("Failed to open file");
		return;
	}

	// get file size	
	fseek(fp, 0, SEEK_END);
	long fileSize = ftell(fp);	
	rewind(fp);
	long partSize = fileSize / partsCount;
	
	unsigned n = std::thread::hardware_concurrency(); // often equals number of logical CPUs 
	std::vector<std::thread> threads; 
	threads.reserve(n);
    
	for (int partIndex = 0; partIndex < partsCount; partIndex++)
	{
		// read part
		long currentPartSize = (partIndex == partsCount - 1) ? (fileSize - partSize * partIndex) : partSize;
		char* partBuffer = (char*)malloc(sizeof(char) * (currentPartSize + 1));
		size_t bytesRead = fread(partBuffer, sizeof(char), currentPartSize, fp);
		partBuffer[bytesRead] = '\0';

		// create thread to process this part
		ThreadData* threadData = new ThreadData();
		threadData->filePartIndex = partIndex;
		threadData->filePartData = partBuffer;				

		threads.emplace_back(ProcessFile, (void*)threadData);
	}

	// wait for all threads to finish
	for (auto &t : threads) 
	{
		if (t.joinable()) t.join();
	}
	
	// other cleanup done in thread function.
	fclose(fp);
}

// Split file1.txt into multiple parts and then process each part in different threads.
void split_file_into__parts_then_process(char* fileName, int partsCount)
{
	if (!fileName || !*fileName || partsCount < 1) return;

	FILE* fp = fopen(fileName, "r");
	if (fp == NULL)
	{
		perror("Failed to open file");
		return;
	}

	// get file size	
	fseek(fp, 0, SEEK_END);
	long fileSize = ftell(fp);	
	rewind(fp);
	long partSize = fileSize / partsCount;
	std::vector<pthread_t> threads(partsCount);
    //std::vector<int> ids(partsCount);
	for (int partIndex = 0; partIndex < partsCount; partIndex++)
	{
		// read part
		long currentPartSize = (partIndex == partsCount - 1) ? (fileSize - partSize * partIndex) : partSize;
		char* partBuffer = (char*)malloc(sizeof(char) * (currentPartSize + 1));
		size_t bytesRead = fread(partBuffer, sizeof(char), currentPartSize, fp);
		partBuffer[bytesRead] = '\0';

		// create thread to process this part
		ThreadData* threadData = new ThreadData();
		threadData->filePartIndex = partIndex;
		threadData->filePartData = partBuffer;

		//ids.push_back(partIndex+1);
		pthread_t thread;
		pthread_create(&thread, NULL, ProcessFile, (void*)threadData);
		pthread_detach(thread); // detach thread to allow independent execution
		threads.push_back(thread);
	}
	
	// wait for all threads to finish	
	for (int i = 0; i < threads.size(); i++)         
        pthread_join(threads[i], NULL);

	// other cleanup done in thread function.
	fclose(fp);
}

char outputFile3[] = "/mnt/c/Projs/WSL/Ubuntu/CCpp/file2_output3.txt";

// Create large data file with random numbers, written in batches
void create_large_text_file_with_random_numbers_batch(char* fileName, long randomCount, int batchSize)
{
	if (randomCount < 1 || batchSize < 1) return;

	FILE* fp = fopen(fileName, "w+");
	if (fp == NULL)
	{
		perror("Failed to open file");
		return;
	}
	
	batchSize = batchSize < 100 ? 100 : batchSize;
	batchSize = batchSize > 50000 ? 50000 : batchSize;
	int batchCount = randomCount / batchSize + (randomCount % batchSize == 0 ? 0 : 1);
	int bufferSize = 1 + sizeof(long) * batchSize + 3 * batchSize; // to account for '\n' etc.
	char strCurrentLongRandom[32]; long k = 0;
	int batchIndex = 0;
	for (batchIndex = 0; batchIndex < batchCount; batchIndex++)
	{
		char* currentWriteBatch = (char*)malloc(sizeof(char) * bufferSize);
		k = 0;
		for (int i = 0; i < batchSize; i++)
		{
			sprintf(strCurrentLongRandom, "%ld\n", random());
			sprintf(currentWriteBatch + k, "%s", strCurrentLongRandom);
			k += strlen(strCurrentLongRandom);
		}		
		*(currentWriteBatch + k) = '\0';
		fprintf(fp, "%s", currentWriteBatch);
		free(currentWriteBatch);
	}
	fclose(fp);
	std::cout << "Total random number Count written in the file = " << randomCount << std::endl;
	std::cout << "random numbers grouped into batch of Size = " << batchSize << std::endl;
	std::cout << "Total written batch Count = " << batchCount << std::endl;
	std::cout << "Last written batch Index = " << (batchIndex-1) << std::endl;
}

int main()
{
	cout << " ===  CSC507 - Module 6: Critical Thinking Assignment  ===" << endl;	
	
	// create large text data file with 10 million random numbers
	// void create_large_text_file_with_random_numbers_batch(char* fileName, long randomCount, int batchSize)
	char largefileName[] = "/mnt/c/Projs/WSL/Ubuntu/CCpp/csu/csc507/module6/datafiles/largeTextDatafile.txt";
	auto start = std::chrono::steady_clock::now();
	//create_large_text_file_with_random_numbers_batch(largefileName, 10000000, 50000);
	auto end = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
	//std::cout << "Created large text data file with 10,000,000 random long data items.\nFile: " << largefileName << endl;
	//std::cout << " Execution time: " << elapsed << " milliseconds\n";
	// done
	
	// void split_file_into__parts_then_process(char* fileName, int partsCount)
	// void split_file_into__parts_then_process(char* fileName, int partsCount) // multiple threads from different CPU cores
	int partsCount = 10; // 2. Break the input file up into 10 files and schedule the process on each one to run in real-time, then combine the resulting files into a single output file. 		
	partsCount = 2;  // 3. Break the input file up into 2 files and schedule the process on each one to run in real-time, then combine the resulting files into a single output file. 
	partsCount = 5;  // 4. Break the input file up into 5 files and schedule the process on each one to run in real-time, then combine the resulting files into a single output file. 
	partsCount = 20; // 5. Break the input file up into 20 files and schedule the process on each one to run in real-time, then combine the resulting files into a single output file.
	//partsCount = 40; //
	start = std::chrono::steady_clock::now();
	split_file_into__parts_then_process(largefileName, partsCount);
	end = std::chrono::steady_clock::now();
	elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
	std::cout << " Split the input file into " << partsCount << " parts and processed each part in a separate thread.\n Execution time: " << elapsed << " milliseconds\n";
	

	return 0;
}
