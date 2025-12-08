/*
	MS - Artificial Intelligence and Machine Learning
	Course: CSC507 Foundations of Operating Systems
	Module 4: Portfolio Milestone
	Professor: Dr. Joseph Issa
	Created by Mukul Mondal
	December 7, 2025
	
	Problem statement: Option #1
	From the previous exercises, you have 2 files in your Linux installation: file1.txt 
    and file2.txt, both with one million rows of random numbers. 
	Using C, create a program to perform this task (as defined for the Bash script), 
	with 3 methods of doing this: 
	1. Read the entire contents of file1.txt into memory, then process each row.
	2. Read one row of file1.txt at a time and process it.
	3. Split file1.txt into 2 parts and read each part into memory separately.
	Be sure to capture execution times for each method. How do they compare to each other, 
	and to that of the double_numbers.sh script? Were there any surprises for you, or 
	did the results match your expectations? Describe your findings in detail.
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

std::mutex file_mutex;  // global mutex


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

// This is thread function that does processing for a part of the input data file (raw stock data file).
// Then writes the processed data into the output file synchronously.
void* ProcessFile(void* arg)
{
	char* fileData = (char*)arg;
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
	// all writes go to the same output file.
	file_mutex.lock();
	FILE* fp = fopen("/mnt/c/Projs/WSL/Ubuntu/CCpp/file2_output3.txt", "a+");	
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
	file_mutex.unlock();	
	
	return NULL;
}

// 3. Split file1.txt into 2 parts and read each part into memory separately.
void split_file_into_two_parts_process(char* fileName)
{
	if (!fileName) return;

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
	long halfSize = fileSize / 2;
	// read first half
	char* firstHalfBuffer = (char*)malloc(sizeof(char) * (halfSize + 1));
	size_t bytesRead = fread(firstHalfBuffer, sizeof(char), halfSize, fp);
	firstHalfBuffer[bytesRead] = '\0';

	// create threads to process each half
	// max 2 threads needed as per requirement
	int max_thread_count = 2;
	std::vector<pthread_t> threads(max_thread_count);
                  std::vector<int> ids(max_thread_count);
                  int threadId = 0;
	threadId++;
                 ids.push_back(threadId);                
                 pthread_create(&threads[threadId], NULL, ProcessFile, firstHalfBuffer);
	
	// read second half
	char* secondHalfBuffer = (char*)malloc(sizeof(char) * (fileSize - halfSize + 1));
	bytesRead = fread(secondHalfBuffer, sizeof(char), fileSize - halfSize, fp);
	secondHalfBuffer[bytesRead] = '\0';
	threadId++;
                 ids.push_back(threadId);                
                pthread_create(&threads[threadId], NULL, ProcessFile, secondHalfBuffer);
	
	for (int i = 0; i < threadId; i++)         
                       pthread_join(threads[i], NULL);

	// other cleanup done in thread function.
	fclose(fp);
}


char outputFile3[] = "/mnt/c/Projs/WSL/Ubuntu/CCpp/file2_output3.txt";


int main()
{
	cout << " ===  CSC507 - Module 4: Portfolio Milestone   ===" << endl;
	cout << " ==      Option #1      ==" << endl;

	// input file with 1 million random numbers
	char fileName[] = "/mnt/c/Projs/WSL/Ubuntu/CCpp/file2.txt"; 
	auto start = std::chrono::steady_clock::now();
	auto end = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
		
	//
	// 1. Read the entire contents of file1.txt into memory, then process each row. : tested
	// void read_file_into_memory_process_each_row(char* fileName, char* outputFileName)
	char outputFileName1[] = "/mnt/c/Projs/WSL/Ubuntu/CCpp/file2_output1.txt";	
	start = std::chrono::steady_clock::now();
	read_file_into_memory_process_each_row(fileName, outputFileName1);
	end = std::chrono::steady_clock::now();	
	elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
	std::cout << "1. Read the entire contents of file1.txt into memory, then process each row. Execution time: " << elapsed << " milliseconds\n";
	//
	
	// 2. Read one row of file1.txt at a time and process it.
	// void read_one_row_at_a_time_process(char* fileName, char* outputFileName)
	char outputFileName2[] = "/mnt/c/Projs/WSL/Ubuntu/CCpp/file2_output2.txt";
	start = std::chrono::steady_clock::now();	
	read_one_row_at_a_time_process(fileName, outputFileName2);
	end = std::chrono::steady_clock::now();
	elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
	std::cout << "2. Read one row of file1.txt at a time and process it. Execution time: " << elapsed << " milliseconds\n";
		
	//
	// 3. Split file1.txt into 2 parts and read each part into memory separately.
	// void split_file_into_two_parts_process(char* fileName)
	char outputFileName3[] = "/mnt/c/Projs/WSL/Ubuntu/CCpp/file2_output3.txt";
	start = std::chrono::steady_clock::now();	
	split_file_into_two_parts_process(fileName);
	end = std::chrono::steady_clock::now();
	elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
	std::cout << "3. Split file1.txt into 2 parts and read each part into memory separately. Execution time: " << elapsed << " milliseconds\n";
		
	return 0;
}
