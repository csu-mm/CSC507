/*
	MS - Artificial Intelligence and Machine Learning
	Course: CSC507 Foundations of Operating Systems
	Module 3: Portfolio Milestone
	Professor: Dr. Joseph Issa
	Created by Mukul Mondal
	November 29, 2025
	
	Problem statement: Option #1
	Create a C program to perform this task, to create file2.txt, 
	and compare execution times. Can you use what you have learned 
	in the last 2 modules, like multithreading or synchronization, 
	to make this process run faster? How would you do that? Use at 
	least 2 other methods to try to improve execution time.
*/

// cpp used instead of c for some feature level support of 
// modern cpp and it's std lib usage.

#include <iostream>
#include <string>
#include <cstring>
#include <chrono>

using namespace std;


struct  LinesWords
{
	int lines;
	int words;
};

struct  LinesWords lines_words_count(char* fileName)
{
	struct LinesWords lw = { -1, -1 };
	if (!fileName) return lw;

	FILE* fp = fopen(fileName, "r");
	if (fp == NULL)
	{
		perror("Failed to open file");
		return lw;
	}

	lw.lines = 0;
	lw.words = 0;
	char ch;
	while ((ch = fgetc(fp)) != EOF)
	{
		if (ch == '\n') lw.lines++;
		if (ch == ' ' || ch == '\n' || ch == '\t') lw.words++;
	}
	fclose(fp);
	return lw;
}

void create_file_write_random_numbers_batch(char* fileName, long randomCount, int batchSize)
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
	std::cout << "Total random number Count written in file2.txt = " << randomCount << std::endl;
	std::cout << "random numbers grouped into batch of Size = " << batchSize << std::endl;
	std::cout << "Total written batch Count = " << batchCount << std::endl;
	std::cout << "Last written batch Index = " << (batchIndex-1) << std::endl;
}

int main()
{
	cout << " ===  CSC507 - Module 3: Portfolio Milestone   ===" << endl;
	cout << " ==      Option #1      ==" << endl;

	char fileName[] = "/mnt/c/Projs/WSL/Ubuntu/CCpp/file2.txt";
	auto start = std::chrono::steady_clock::now();
	create_file_write_random_numbers_batch(fileName, 1000000, 50000);
	auto end = std::chrono::steady_clock::now();	
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
	std::cout << "file2.txt write execution time: " << elapsed << " milliseconds\n";
	
	start = std::chrono::steady_clock::now();
	struct LinesWords lw2 = lines_words_count(fileName);
	end = std::chrono::steady_clock::now();	
	elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
	std::cout << fileName <<" : lines and words count execution time: " << elapsed << " milliseconds\n";
	
	if (lw2.lines < 0 || lw2.words < 0)
		printf("Error in counting lines and words\n");
	else
		printf("%s: Lines: %d, Words: %d\n", fileName, lw2.lines, lw2.words);

	return 0;
}
