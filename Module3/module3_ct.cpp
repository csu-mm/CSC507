    /*
	MS - Artificial Intelligence and Machine Learning
	Course: CSC507 Foundations of Operating Systems
	Module 3: Critical Thinking Assignment
	Professor: Dr. Joseph Issa
	Created by Mukul Mondal
	November 30, 2025
	
	Problem statement: 
	Describe, in detail, 3 real-world examples of possible data files of at least one million rows, 
    and why it would be important for a business to be able to process such files 
    in fractions of a second instead of several seconds. You can use arenas 
    such as financial systems, inventory tracking, state drivers license information, 
    or anything else that you can think of. How would you use the methods that 
    were covered in this and the previous module to optimize processing times for such files?

    It also covers part of: Module 3: Portfolio Milestone
*/

// cpp used instead of c for some feature level support of modern cpp and it's std lib usage.
    
    #include <iostream>
    #include <stdexcept>
    #include <string>
    #include <dirent.h>
    #include <cstring>
    #include <vector>
    #include <pthread.h>
    #include <thread>
    #include <unistd.h> 
    #include <queue>
    #include <mutex>
    #include <condition_variable>


    using namespace std;


    // Stock data type with properties
    struct SingleStock
    {
        char stockName[20];
        char stockDT[50];
        float Open;
        float High;
        float Low;
        float Close;
        long Volume;
    };

    // This is thread safe container of the above stock object.
    // Each stock csv file parsed in seperate thread and pushed the stock data into this queue.
    // There is only one thread that pops the stock data from this queue and does the required processing.
    // Exclusive access ensured by using mutex and lock on it.
    class ThreadSafeStockQueue 
    {
        public:
            void push(SingleStock value) 
            {
                std::lock_guard<std::mutex> lock(m_);
                q_.push(std::move(value));
                cv_.notify_one();
            }

            bool pop(SingleStock& value) 
            {
                std::unique_lock<std::mutex> lock(m_);
                cv_.wait(lock, [this]{ return !q_.empty() || done_; });
                if (q_.empty()) return false;
                value = std::move(q_.front());
                q_.pop();
                return true;
            }

            void set_done() 
            {
                std::lock_guard<std::mutex> lock(m_);
                done_ = true;
                cv_.notify_all();
            }

            bool isEmpty()
            {
                std::unique_lock<std::mutex> lock(m_);
                return q_.empty();
            }

            bool get_done()
            {
                std::unique_lock<std::mutex> lock(m_);
                return done_;
            }

        private:
            std::queue<SingleStock> q_;
            std::mutex m_;
            std::condition_variable cv_;
            bool done_ = false;
    };

    // This is the single instance of the ThreadSafe stock container class.
    ThreadSafeStockQueue queueStocks;
    
    // This thread consumes the Stock objects from the thread safe container Queue.
    void* ConsumerThread(void* arg)
    {                
        while (queueStocks.get_done() == false)
        {
            if (queueStocks.isEmpty())
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            
            if(queueStocks.isEmpty() == false)
            {
                SingleStock popedStock;
                if(queueStocks.pop(popedStock))
                {
                    // process the stock objects
                    cout << "Stock (Date, Name, Close...) : (" << popedStock.stockDT <<", " << popedStock.stockName <<", " << popedStock.Close <<" ...)" << endl;
                    // do other required processing
                }
            }
        }        
        return NULL;
    }

    // This is thread function that does the processing for one csv file (raw stock data file)
    // This thread creates the stock object and pushes the Stock objects into the thread safe container Queue.
    void* ProcessFile(void* arg)
    {
        char* fileName = (char*)arg;
        char buffer[256];
        FILE* fp = fopen(fileName, "r");
        if(fp)
        {
            while (fgets(buffer, sizeof(buffer), fp) != NULL)
            {
                if(buffer[0] == '2' && buffer[1] == '0' && buffer[2] == '2' && buffer[3] == '5') // skip header line
                {   
                    try 
                    {             
                        // fgets includes the newline character if present
                        // std::cout << buffer << "\n";
                        buffer[strcspn(buffer, "\n")] = '\0';
                        char *token = strtok(buffer, ",");
                        SingleStock ss;
                        int dataIndex = -1;
                        while (token != NULL) 
                        {
                            // 2025-09-01 09:15:00,   // -1
                            // GRASIM,  // 1
                            // NSE,     // 2
                            // 2785.6,  // 3 // Open
                            // 2786.0,  // 4 // High
                            // 2783.8,  // 5 // Low
                            // 2786.0,  // 6 // Close
                            // 9        // 7 // Volume                    
                            std::string s = token;
                            if ( dataIndex == -1 && s.substr(0, 5) == "2025-")
                            {
                                dataIndex = 0;
                                strcpy(ss.stockDT, token);                            
                            }
                            if(dataIndex == 1)                     
                                strcpy(ss.stockName, token);                    
                            else if(dataIndex == 3)
                                ss.Open = atof(token);
                            else if(dataIndex == 4)
                                ss.High = atof(token);
                            else if(dataIndex == 5)
                                ss.Low = atof(token);
                            else if(dataIndex == 6)
                                ss.Close = atof(token);
                            else if(dataIndex == 7)
                                ss.Volume = atol(token);
                            token = strtok(NULL, ",");
                            dataIndex++;
                        }
                        if(dataIndex == 7)
                        {
                            queueStocks.push(ss);
                        }
                    } catch (const std::exception& e) { }  // proceed to the next line, if current line has some data error.
                }                
            }
        }
        
        fclose(fp);
        return NULL;
    }


    // This gets the full file names for all raw stock data from a folder.
    //     Then creates seperate thread to parse each file.
    // This also creates the queue data processing thread.
    // Then it waits to finish all theads to finish execution.
    void GetStockFilesAndParse(const char* strPath)
    {
        if(!strPath || !*strPath) return;

        DIR* dir = opendir(strPath);
        
        if (dir == nullptr) 
        {
            std::cerr << "Error opening directory: " << strerror(errno) << "\n";
            return;
        }

        // create queue data consumer thread
        pthread_t stockObjThread; 
        pthread_create(&stockObjThread, NULL, ConsumerThread, NULL);

        char fullName[200];        
        struct dirent* entry;
        int max_thread_count = 256;
        std::vector<pthread_t> threads(max_thread_count);
        std::vector<int> ids(max_thread_count);
        int threadId = 0;

        while ((entry = readdir(dir)) != nullptr) 
        {
            if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) 
            {                
                strcpy(fullName, strPath);             
                strcat(fullName, "/");
                strcat(fullName, entry->d_name);
                std::cout << fullName << "\n";

                threadId++;
                ids.push_back(threadId);

                // parse each stock file in seperate new thread
                pthread_create(&threads[threadId], NULL, ProcessFile, fullName);
            }
        }

        closedir(dir);


        // wait for all stock file processing threads
        for (int i = 0; i < threadId; i++)         
            pthread_join(threads[i], NULL);
        queueStocks.set_done();

        // wait for queue stock data processing thread
        pthread_join(stockObjThread, NULL);
        
    }



    int main()
    {
        printf("\nApplication started...\n");
                
        const char* stockDataFilesRootPath = "/mnt/c/Projs/ASM/DataFiles/raw/202509";
        GetStockFilesAndParse(stockDataFilesRootPath);

        printf("\nApplication closing. Press any key...\n"); getchar();

        return 0;
    }
