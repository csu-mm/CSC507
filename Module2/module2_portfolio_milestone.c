#include <stdio.h>
#include <stdlib.h>


struct  LinesWords
{
    int lines;
    int words;
};

void create_file_write_random_numbers(char* fileName) 
{
    FILE *fp = fopen(fileName, "w+");
    if (fp == NULL) 
    {
        perror("Failed to open file");
        return;
    }
    for(int i=0; i<1000; i++)          
        fprintf(fp, "%ld\n", random());
    
    fclose(fp);    
}

struct  LinesWords lines_words_count(char* fileName) 
{
    struct LinesWords lw = {-1, -1};
    if(!fileName) return lw;
    
    FILE *fp = fopen(fileName, "r");
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

int main() 
{
    printf(" === CSC507 - Module 2: Portfolio Milestone === \n");
    create_file_write_random_numbers("./file2.txt");    
    struct LinesWords lw2 = lines_words_count("./file2.txt");
    if(lw2.lines < 0 || lw2.words < 0)
        printf("Error in counting lines and words in file2.txt\n");
    else
        printf("file2.txt: Lines: %d, Words: %d\n", lw2.lines, lw2.words);

    struct LinesWords lw1 = lines_words_count("./file1.txt");
    if(lw1.lines < 0 || lw1.words < 0)
        printf("Error in counting lines and words in file1.txt\n");
    else
        printf("file1.txt: Lines: %d, Words: %d\n", lw1.lines, lw1.words);

    return 0;
}
