/*
	MS - Artificial Intelligence and Machine Learning
	Course: CSC507 Foundations of Operating Systems
	Module 4: Critical Thinking
	Professor: Dr. Joseph Issa
	Created by Mukul Mondal
	December 7, 2025
	
	Problem statement: Simulate the First-Fit Algorithm in Allocating Memory to Processes.
	In memory management, the First-Fit algorithm allocates the first available memory block 
    that can accommodate a process. Your task is to create a program that simulates the 
    First-Fit algorithm for memory allocation in an operating system using a programming 
    language of your choice. Test your algorithm with varying memory block sizes and process sizes.
    In your summary paper, explain how the algorithm works, and compare it with the Best-Fit algorithm. 
    Include screenshots of the source code and the paper for reference. Can you think of situations 
    where the best-fit algorithm is preferable to the first-fit algorithm? Explain your reasons 
    for that opinion.
*/

#include <iostream>
#include <vector>
#include <list>

using namespace std;

struct MemoryBlock 
{
    int id;
    int availableMemorySize;
    bool free;  // true if block has any un-allocated memory, false if fully allocated.
};

struct Process 
{
    int pid;
    int memorySize; // memory size required by the process.
};

// First-Fit strategy scans memory sequentially and allocates the first block large enough 
// to satisfy the request, making it simple and efficient.
void SimulateFirstFit(vector<MemoryBlock>& memory, const vector<Process>& processes) 
{
    for (const auto& p : processes) 
    {
        bool allocated = false;
        for (auto& block : memory) 
        {
            if (block.free && block.availableMemorySize >= p.memorySize) 
            {
                cout << "Allocating Process " << p.pid 
                     << " (" << p.memorySize << " KB) to Block id " << block.id << " of size " 
                     << block.availableMemorySize << " KB\n";

                block.availableMemorySize -= p.memorySize;   // reduce block size
                block.free = (block.availableMemorySize > 0); // still free if leftover
                allocated = true;
                break;
            }
        }
        if (!allocated) 
        {
            cout << "Process " << p.pid << " (" << p.memorySize 
                 << " KB) could not be allocated\n";
        }
    }
}

// Best-Fit strategy searches the entire list of memory blocks and allocates the smallest 
// block that is large enough to satisfy the request, minimizing wasted space.
void SimulateBestFit(vector<MemoryBlock>& memory, const vector<Process>& processes) 
{
    for (const auto& p : processes)
    {
        bool allocated = false;
        MemoryBlock* bestBlock = nullptr;
        for (auto& block : memory)
        {
            if (block.free && block.availableMemorySize >= p.memorySize) 
            {
                if(bestBlock == nullptr || block.availableMemorySize < bestBlock->availableMemorySize)
                {
                    bestBlock = &block;
                }                
            }
        }
        if (bestBlock != nullptr) 
        {
            cout << "Allocating Process " << p.pid 
                 << " (" << p.memorySize << " KB) to Block id " << bestBlock->id << " of size " 
                 << bestBlock->availableMemorySize << " KB\n";

            bestBlock->availableMemorySize -= p.memorySize;   // reduce block size
            bestBlock->free = (bestBlock->availableMemorySize > 0); // still free if leftover
            allocated = true;
        }
        if (!allocated) 
        {
            cout << "Process " << p.pid << " (" << p.memorySize 
                 << " KB) could not be allocated\n";
        }
    }
}

int main() 
{
    printf(" === CSC507 - Module 4: Critical Thinking === \n");
    
    vector<MemoryBlock> memory = { {1, 110, true}, {2, 200, true}, {3, 300, true}, {4, 500, true}, {5, 600, true} };
    vector<Process> processes = { {1, 212}, {2, 417}, {3, 112}, {4, 426}, {14, 46} };
    
    printf(" === SimulateFirstFit(..) === \n");
    SimulateFirstFit(memory, processes);

    printf("\n === SimulateBestFit(..) === \n");
    SimulateBestFit(memory, processes);

    return 0;
}
